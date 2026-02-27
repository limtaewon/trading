#!/usr/bin/env python3
"""
시장 데이터 수집기: 지수, 환율, 금리, 원자재 → ClickHouse

사용법:
  python3 collect_market_data.py              # 전체 수집
  python3 collect_market_data.py --only index  # 지수만
  python3 collect_market_data.py --only fx     # 환율만
  python3 collect_market_data.py --only rate   # 금리만
  python3 collect_market_data.py --only flow   # 투자자동향만
  python3 collect_market_data.py --only commodity  # 원자재만
  python3 collect_market_data.py --days 30     # 최근 30일

의존성:
  pip install yfinance requests

데이터 소스:
  - yfinance: 글로벌 지수, 환율, 원자재 (무료, 제한 없음)
  - ECOS API: 한국은행 금리 (무료, 인증키 필요)
  - KIS API: 투자자별 매매동향 (인증 필요)
"""

import os
import sys
import json
import subprocess
import shutil

# ensure local imports work regardless of CWD (cron, manual run, etc.)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import requests
except ImportError:
    from _requests_compat import requests

import time
import logging
from datetime import datetime, timedelta
from collections import OrderedDict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("market-data")

# ─── 설정 ───────────────────────────────────────────────────
CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
MCPORTER_PATH = os.getenv("MCPORTER")
MCPORTER = None
if MCPORTER_PATH and os.path.isfile(MCPORTER_PATH) and os.access(MCPORTER_PATH, os.X_OK):
    MCPORTER = MCPORTER_PATH
else:
    MCPORTER = shutil.which("mcporter")
FLOW_SYMBOL_LIMIT = int(os.getenv("INVESTOR_FLOW_SYMBOL_LIMIT", "30"))

# ECOS (한국은행) - https://ecos.bok.or.kr/api/ 에서 인증키 발급
ECOS_API_KEY = os.environ.get("ECOS_API_KEY", "")

# ─── yfinance 지수 매핑 ─────────────────────────────────────
INDEX_SYMBOLS = {
    "KOSPI":  {"symbol": "^KS11",  "name": "코스피"},
    "KOSDAQ": {"symbol": "^KQ11",  "name": "코스닥"},
    "SPX":    {"symbol": "^GSPC",  "name": "S&P 500"},
    "NDX":    {"symbol": "^IXIC",  "name": "나스닥"},
    "N225":   {"symbol": "^N225",  "name": "닛케이 225"},
    "HSI":    {"symbol": "^HSI",   "name": "항셍"},
    "VIX":    {"symbol": "^VIX",   "name": "VIX 공포지수"},
    "DXY":    {"symbol": "DX-Y.NYB", "name": "달러 인덱스"},
}

FX_SYMBOLS = {
    "USDKRW": {"symbol": "KRW=X",   "name": "원/달러"},
    "JPYKRW": {"symbol": "KRWJPY=X", "name": "원/엔", "invert": True},
    "EURKRW": {"symbol": "EURKRW=X", "name": "원/유로"},
    "CNYKRW": {"symbol": "CNYKRW=X", "name": "원/위안"},
}

COMMODITY_SYMBOLS = {
    "WTI":    {"symbol": "CL=F",  "name": "WTI 원유"},
    "BRENT":  {"symbol": "BZ=F",  "name": "브렌트 원유"},
    "GOLD":   {"symbol": "GC=F",  "name": "금"},
    "COPPER": {"symbol": "HG=F",  "name": "구리"},
}

# ECOS 금리 코드
ECOS_RATES = {
    "BOK_BASE":  {"stat": "722Y001", "item": "0101000", "name": "한국은행 기준금리", "cycle": "D"},
    "KR_CD91":   {"stat": "817Y002", "item": "010502000", "name": "CD 91일물", "cycle": "D"},
    "KR_TB3Y":   {"stat": "817Y002", "item": "010200000", "name": "국고채 3년", "cycle": "D"},
    "KR_TB10Y":  {"stat": "817Y002", "item": "010210000", "name": "국고채 10년", "cycle": "D"},
}


# ─── ClickHouse 헬퍼 ─────────────────────────────────────────
def ch_query(query, data=None):
    """ClickHouse 쿼리 실행"""
    try:
        if data:
            resp = requests.post(CLICKHOUSE_URL, data=data.encode("utf-8"), timeout=10)
        else:
            resp = requests.get(CLICKHOUSE_URL, params={"query": query}, timeout=10)
        resp.raise_for_status()
        return resp.text.strip()
    except Exception as e:
        log.error(f"ClickHouse 오류: {e}")
        return None


def ch_insert(table, columns, rows, dedup_col=None, dedup_cols=None):
    """ClickHouse INSERT (중복 방지 옵션 포함)

    - dedup_col: 단일 컬럼 기준 중복 제거 (레거시)
    - dedup_cols: 복수 컬럼 기준 중복 제거 (권장)
      예) dedup_cols=["date", "index_code"]
    """
    if not rows:
        return 0

    # 레거시 호환
    if dedup_cols is None and dedup_col:
        dedup_cols = [dedup_col]

    # 중복 제거: dedup_cols 조합 키가 이미 있으면 스킵
    if dedup_cols and all(c in columns for c in dedup_cols):
        key_indices = [columns.index(c) for c in dedup_cols]
        existing_keys = set()

        try:
            where_parts = []
            if "date" in dedup_cols:
                where_parts.append("date >= today() - 14")
            where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

            key_sql = ", ".join(dedup_cols)
            q = f"SELECT DISTINCT {key_sql} FROM {table} {where_sql} LIMIT 50000"
            r = requests.get(CLICKHOUSE_URL, params={"query": q}, timeout=15)
            r.raise_for_status()

            for line in r.text.strip().splitlines():
                if not line.strip():
                    continue
                parts = line.split("\t")
                existing_keys.add(tuple(parts))
        except Exception:
            pass  # DB 조회 실패 시 그냥 전부 삽입 시도

        if existing_keys:
            before = len(rows)
            filtered = []
            for row in rows:
                key = tuple(str(row[i]) for i in key_indices)
                if key in existing_keys:
                    continue
                filtered.append(row)
            rows = filtered
            skipped = before - len(rows)
            if skipped > 0:
                log.info(f"  중복 스킵: {skipped}건 (이미 DB에 존재)")
            if not rows:
                return 0

    col_str = ", ".join(columns)
    val_strs = []
    for row in rows:
        vals = []
        for v in row:
            if isinstance(v, str):
                vals.append(f"'{v.replace(chr(39), chr(92)+chr(39))}'")
            elif isinstance(v, (int, float)):
                vals.append(str(v))
            else:
                vals.append(f"'{v}'")
        val_strs.append(f"({', '.join(vals)})")

    query = f"INSERT INTO {table} ({col_str}) VALUES {','.join(val_strs)}"
    try:
        resp = requests.post(CLICKHOUSE_URL, data=query.encode("utf-8"), timeout=10)
        resp.raise_for_status()
        return len(rows)
    except Exception as e:
        log.error(f"INSERT 실패 [{table}]: {e}")
        return 0


def _safe_float(val, default=0.0):
    """숫자 문자열/None을 안전하게 float로 변환."""
    if val is None:
        return default
    if isinstance(val, (int, float)):
        try:
            return float(val)
        except (TypeError, ValueError):
            return default
    if isinstance(val, str):
        try:
            return float(val.strip().replace(",", "").replace("%", ""))
        except (TypeError, ValueError):
            return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _safe_int(val, default=0):
    """숫자 문자열/None을 안전하게 int로 변환."""
    if val is None:
        return default
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    if isinstance(val, str):
        try:
            return int(float(val.strip().replace(",", "")))
        except (TypeError, ValueError):
            return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _run_kis_stock_quote(symbol: str):
    """inquery-stock-price 호출 결과를 dict로 반환."""
    if not MCPORTER:
        return None
    cmd = [
        MCPORTER,
        "call",
        f'kis-trading.inquery-stock-price(symbol: "{symbol}")',
        "--output",
        "json",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if result.returncode != 0:
            log.warning(f"  inquery-stock-price 실패 [{symbol}]: {result.stderr[:200]}")
            return None
        parsed = json.loads(result.stdout)
        if isinstance(parsed, dict):
            return parsed
    except subprocess.TimeoutExpired:
        log.warning(f"  inquery-stock-price 타임아웃 [{symbol}]")
    except json.JSONDecodeError:
        log.warning(f"  inquery-stock-price JSON 파싱 실패 [{symbol}]")
    except Exception as e:
        log.warning(f"  inquery-stock-price 호출 실패 [{symbol}]: {e}")
    return None


def _extract_stock_quote(payload):
    """KIS 응답에서 종목 속성 dict를 추출."""
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("output"), dict):
        return payload.get("output")
    if isinstance(payload.get("output"), list) and payload["output"]:
        item = payload["output"][0]
        if isinstance(item, dict):
            return item
    if isinstance(payload.get("output1"), dict):
        return payload.get("output1")
    if isinstance(payload.get("output1"), list) and payload["output1"]:
        item = payload["output1"][0]
        if isinstance(item, dict):
            return item
    return payload


def _query_top_tickers_for_flow(limit: int = 30) -> list[str]:
    """v_trading_dashboard 기준 상위 종목만 수집해 과도한 KIS 호출 방지."""
    if limit <= 0:
        return []
    q = (
        "SELECT ticker FROM trading.v_trading_dashboard "
        f"ORDER BY score DESC LIMIT {int(limit)} FORMAT JSONEachRow"
    )
    try:
        resp = requests.post(CLICKHOUSE_URL, data=q.encode("utf-8"), timeout=10)
        resp.raise_for_status()
        rows = []
        for line in resp.text.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            ticker = str(rec.get("ticker", "")).strip()
            if len(ticker) == 6 and ticker.isdigit():
                rows.append(ticker)
        # 중복 제거 (주문/표시 순서 보존)
        uniq = OrderedDict((t, True) for t in rows if t)
        return list(uniq.keys())
    except Exception as e:
        log.warning(f"  투자자 동향 ticker 조회 실패: {e}")
        return []


def _market_session_label(now_ts=None):
    now_ts = now_ts or datetime.now()
    hhmm = int(now_ts.strftime("%H%M"))
    if 900 <= hhmm < 1520:
        return "REGULAR"
    if 1530 <= hhmm < 2000:
        return "AFTER"
    if 800 <= hhmm < 900:
        return "PRE"
    return "OFF"


# ─── 1. 지수 수집 (yfinance) ────────────────────────────────
def collect_indices(days=7):
    """주요 지수 수집"""
    try:
        import yfinance as yf
    except ImportError:
        log.error("yfinance 미설치: pip install yfinance --break-system-packages")
        return 0

    rows = []
    for code, info in INDEX_SYMBOLS.items():
        try:
            ticker = yf.Ticker(info["symbol"])
            hist = ticker.history(period=f"{days}d")
            if hist.empty:
                log.warning(f"  {code}: 데이터 없음")
                continue

            for date_idx, row in hist.iterrows():
                date_str = date_idx.strftime("%Y-%m-%d")
                close = round(float(row["Close"]), 2)
                open_p = round(float(row["Open"]), 2)
                high = round(float(row["High"]), 2)
                low = round(float(row["Low"]), 2)
                volume = int(row.get("Volume", 0))
                # 등락률 계산
                change_pct = round((close - open_p) / open_p * 100, 2) if open_p > 0 else 0

                rows.append((
                    date_str, code, info["name"],
                    close, change_pct, volume, high, low, open_p
                ))

            log.info(f"  {code:8s} ({info['name']:12s}): {len(hist)}일")
            time.sleep(0.3)

        except Exception as e:
            log.warning(f"  {code} 수집 실패: {e}")

    if rows:
        columns = ["date", "index_code", "index_name",
                    "close_price", "change_pct", "volume", "high", "low", "open_price"]
        inserted = ch_insert("trading.market_index", columns, rows, dedup_cols=["date", "index_code"])
        log.info(f"  → 지수 {inserted}건 저장")
        return inserted
    return 0


# ─── 2. 환율 수집 (yfinance) ────────────────────────────────
def collect_fx(days=7):
    """환율 수집"""
    try:
        import yfinance as yf
    except ImportError:
        log.error("yfinance 미설치")
        return 0

    rows = []
    for code, info in FX_SYMBOLS.items():
        try:
            ticker = yf.Ticker(info["symbol"])
            hist = ticker.history(period=f"{days}d")
            if hist.empty:
                log.warning(f"  {code}: 데이터 없음")
                continue

            for date_idx, row in hist.iterrows():
                date_str = date_idx.strftime("%Y-%m-%d")
                close = float(row["Close"])
                high = float(row["High"])
                low = float(row["Low"])
                open_p = float(row["Open"])

                # 일부 환율은 역수 필요
                if info.get("invert"):
                    close = 1 / close if close > 0 else 0
                    high = 1 / high if high > 0 else 0
                    low = 1 / low if low > 0 else 0
                    open_p = 1 / open_p if open_p > 0 else 0

                change_pct = round((close - open_p) / open_p * 100, 3) if open_p > 0 else 0

                rows.append((
                    date_str, code,
                    round(close, 2), round(change_pct, 3),
                    round(high, 2), round(low, 2)
                ))

            log.info(f"  {code:8s} ({info['name']:8s}): {len(hist)}일")
            time.sleep(0.3)

        except Exception as e:
            log.warning(f"  {code} 수집 실패: {e}")

    if rows:
        columns = ["date", "currency_pair", "close_rate", "change_pct", "high", "low"]
        inserted = ch_insert("trading.exchange_rate", columns, rows, dedup_cols=["date", "currency_pair"])
        log.info(f"  → 환율 {inserted}건 저장")
        return inserted
    return 0


# ─── 3. 금리 수집 (ECOS API) ────────────────────────────────
def collect_rates(days=30):
    """한국은행 ECOS API로 금리 수집"""
    if not ECOS_API_KEY:
        log.warning("  ECOS_API_KEY 미설정 → 금리 수집 스킵")
        log.info("  발급: https://ecos.bok.or.kr/api/ → 인증키 신청")
        return 0

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

    rows = []
    for code, info in ECOS_RATES.items():
        try:
            url = (
                f"https://ecos.bok.or.kr/api/StatisticSearch/"
                f"{ECOS_API_KEY}/json/kr/1/100/"
                f"{info['stat']}/{info['cycle']}/{start_date}/{end_date}/"
                f"{info['item']}"
            )
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            stat_list = data.get("StatisticSearch", {}).get("row", [])
            if not stat_list:
                log.warning(f"  {code}: 데이터 없음")
                continue

            for item in stat_list:
                time_str = item.get("TIME", "")
                value = item.get("DATA_VALUE", "")
                if not time_str or not value:
                    continue

                # 날짜 포맷: YYYYMMDD → YYYY-MM-DD
                if len(time_str) == 8:
                    date_str = f"{time_str[:4]}-{time_str[4:6]}-{time_str[6:8]}"
                elif len(time_str) == 6:
                    date_str = f"{time_str[:4]}-{time_str[4:6]}-01"
                else:
                    continue

                rows.append((date_str, code, info["name"], float(value)))

            log.info(f"  {code:12s} ({info['name']:16s}): {len(stat_list)}건")
            time.sleep(0.3)

        except Exception as e:
            log.warning(f"  {code} 수집 실패: {e}")

    if rows:
        columns = ["date", "rate_code", "rate_name", "rate_value"]
        inserted = ch_insert("trading.interest_rate", columns, rows)
        log.info(f"  → 금리 {inserted}건 저장")
        return inserted
    return 0


# ─── 4. 원자재 수집 (yfinance) ──────────────────────────────
def collect_commodities(days=7):
    """원자재 가격 수집"""
    try:
        import yfinance as yf
    except ImportError:
        log.error("yfinance 미설치")
        return 0

    rows = []
    for code, info in COMMODITY_SYMBOLS.items():
        try:
            ticker = yf.Ticker(info["symbol"])
            hist = ticker.history(period=f"{days}d")
            if hist.empty:
                log.warning(f"  {code}: 데이터 없음")
                continue

            for date_idx, row in hist.iterrows():
                date_str = date_idx.strftime("%Y-%m-%d")
                close = round(float(row["Close"]), 2)
                open_p = round(float(row["Open"]), 2)
                change_pct = round((close - open_p) / open_p * 100, 2) if open_p > 0 else 0

                rows.append((date_str, code, info["name"], close, change_pct))

            log.info(f"  {code:8s} ({info['name']:10s}): {len(hist)}일")
            time.sleep(0.3)

        except Exception as e:
            log.warning(f"  {code} 수집 실패: {e}")

    if rows:
        columns = ["date", "commodity_code", "commodity_name", "close_price", "change_pct"]
        inserted = ch_insert("trading.commodity", columns, rows, dedup_cols=["date", "commodity_code"])
        log.info(f"  → 원자재 {inserted}건 저장")
        return inserted
    return 0


# ─── 5. 투자자 매매동향 (KIS API) ───────────────────────────
def collect_investor_flow(days=7):
    """
    KIS API 투자자별 매매동향
    """
    if not MCPORTER:
        log.info("  투자자 동향: mcporter 미설치 → 수집 스킵")
        return 0

    symbols = _query_top_tickers_for_flow(FLOW_SYMBOL_LIMIT)
    if not symbols:
        log.info("  투자자 동향: 상위 종목 후보 없음 → 수집 스킵")
        return 0

    rows = []
    session = _market_session_label()
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for symbol in symbols:
        payload = _run_kis_stock_quote(symbol)
        quote = _extract_stock_quote(payload)
        if not isinstance(quote, dict):
            continue

        foreign_ownership = _safe_float(quote.get("hts_frgn_ehrt"), 0.0)
        inst_flow = _safe_int(quote.get("pgtr_ntby_qty"), 0)
        price = _safe_float(quote.get("stck_prpr"), 0.0)
        foreign_net_flow = _safe_int(quote.get("frgn_ntby_qty"), 0)

        rows.append((
            now_ts,
            symbol,
            session,
            price,
            0.0,      # vwap
            0.0,      # atr14
            0.0,      # rsi14
            0.0,      # spread_bp
            0.0,      # liquidity_krw
            foreign_ownership,   # feature_snapshot.foreign_flow 사용
            inst_flow,           # feature_snapshot.inst_flow 사용
            foreign_net_flow,    # feature_snapshot.news_event_score 사용(외국인 순매수)
            0.0,                # dart_event_score
            ""                  # regime_label
        ))
        time.sleep(0.15)

    if rows:
        columns = [
            "ts", "symbol", "session", "price", "vwap", "atr14", "rsi14",
            "spread_bp", "liquidity_krw", "foreign_flow", "inst_flow",
            "news_event_score", "dart_event_score", "regime_label",
        ]
        inserted = ch_insert(
            "trading.feature_snapshot",
            columns,
            rows,
            dedup_cols=["ts", "symbol"],
        )
        log.info(f"  → 투자자 동향 {inserted}건 저장")
        return inserted
    return 0


# ─── 메인 ───────────────────────────────────────────────────
def main():
    start = time.time()
    days = 7  # 기본 7일

    # 인자 파싱
    only = None
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--only" and i < len(sys.argv) - 1:
            only = sys.argv[i + 1]
        elif arg == "--days" and i < len(sys.argv) - 1:
            days = int(sys.argv[i + 1])

    log.info("=" * 60)
    log.info(f"시장 데이터 수집 (최근 {days}일)")
    log.info("=" * 60)

    total = 0

    if only is None or only == "index":
        log.info("📊 지수 수집...")
        total += collect_indices(days)

    if only is None or only == "fx":
        log.info("💱 환율 수집...")
        total += collect_fx(days)

    if only is None or only == "rate":
        log.info("📈 금리 수집...")
        total += collect_rates(days)

    if only is None or only == "commodity":
        log.info("🛢️ 원자재 수집...")
        total += collect_commodities(days)

    if only is None or only == "flow":
        log.info("👥 투자자 동향...")
        total += collect_investor_flow(days)

    elapsed = time.time() - start
    log.info("=" * 60)
    log.info(f"완료: {total}건 저장 ({elapsed:.1f}초)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
