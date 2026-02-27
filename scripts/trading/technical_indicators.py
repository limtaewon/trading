#!/usr/bin/env python3
"""technical_indicators.py

관심 종목의 기술적 지표를 사전 계산하여 ClickHouse에 저장.
OpenClaw gpt-5.2가 HEARTBEAT 실행 시 한 번의 쿼리로 기술 분석 결과를 받을 수 있게 한다.

사용법:
  python3 technical_indicators.py              # 기본 워치리스트만
  python3 technical_indicators.py --dynamic    # 기본 + 뉴스 기반 전종목 (★권장)
  python3 technical_indicators.py --days 90    # 90일 데이터 기반
  python3 technical_indicators.py --top N      # STOCKS.csv 상위 N종목도 추가

계산 지표:
  - RSI (14일)
  - MACD (12, 26, 9)
  - Bollinger Bands (20, 2σ)
  - 이동평균 (5일, 20일, 60일)
  - 거래량 비율 (당일/20일 평균)
  - 종합 시그널 (-5 ~ +5)

의존성:
  pip install yfinance --break-system-packages
"""
from __future__ import annotations

import csv
import os
import sys
import json
import time
import math
import logging
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import requests
except ImportError:
    from _requests_compat import requests

try:
    from telegram_notify import notify as tg_notify
except ImportError:
    def tg_notify(text): pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tech-indicator")

CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
STOCKS_CSV = Path.home() / ".openclaw" / "workspace" / "STOCKS.csv"


# ─── STOCKS.csv 로더 ──────────────────────────────────────────

def load_stocks_csv() -> dict[str, tuple[str, str]]:
    """STOCKS.csv → {code: (name, market_suffix)}"""
    result = {}
    if not STOCKS_CSV.exists():
        log.warning(f"STOCKS.csv 없음: {STOCKS_CSV}")
        return result
    try:
        with open(STOCKS_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get("Code", "").strip()
                name = row.get("Name", "").strip()
                market = row.get("Market", "KOSPI").strip()
                if code and name:
                    suffix = "KQ" if "KOSDAQ" in market.upper() else "KS"
                    result[code] = (name, suffix)
        log.info(f"  STOCKS.csv 로드: {len(result)}종목")
    except Exception as e:
        log.warning(f"STOCKS.csv 로드 실패: {e}")
    return result

# ─── 핵심 워치리스트 (시총 상위 + 주요 테마) ─────────────────
# ticker: (name, market_suffix)
CORE_WATCHLIST = {
    # KOSPI 대형주
    "005930": ("삼성전자", "KS"),
    "000660": ("SK하이닉스", "KS"),
    "373220": ("LG에너지솔루션", "KS"),
    "207940": ("삼성바이오로직스", "KS"),
    "005380": ("현대차", "KS"),
    "068270": ("셀트리온", "KS"),
    "051910": ("LG화학", "KS"),
    "006400": ("삼성SDI", "KS"),
    "105560": ("KB금융", "KS"),
    "055550": ("신한지주", "KS"),
    "000270": ("기아", "KS"),
    "028260": ("삼성물산", "KS"),
    "012330": ("현대모비스", "KS"),
    "066570": ("LG전자", "KS"),
    "003550": ("LG", "KS"),
    "086790": ("하나금융지주", "KS"),
    "034730": ("SK", "KS"),
    "003670": ("포스코퓨처엠", "KS"),
    "042700": ("한미반도체", "KS"),
    "009150": ("삼성전기", "KS"),
    "034020": ("두산에너빌리티", "KS"),
    "402340": ("SK스퀘어", "KS"),
    "010130": ("고려아연", "KS"),
    "035420": ("NAVER", "KS"),
    "035720": ("카카오", "KS"),
    "329180": ("HD현대중공업", "KS"),
    "009540": ("HD한국조선해양", "KS"),
    "012450": ("한화에어로스페이스", "KS"),
    "042660": ("한화오션", "KS"),
    "003490": ("대한항공", "KS"),
    # KOSDAQ 주요
    "247540": ("에코프로비엠", "KQ"),
    "086520": ("에코프로", "KQ"),
    "196170": ("알테오젠", "KQ"),
    "328130": ("루닛", "KQ"),
    "403870": ("HPSP", "KQ"),
}


# ─── 기술 지표 계산 함수 ─────────────────────────────────────

def calc_sma(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def calc_rsi(closes: list[float], period: int = 14) -> float | None:
    """Wilder's RSI"""
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0) for d in deltas]
    losses = [abs(min(d, 0)) for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def calc_macd(closes: list[float], fast: int = 12, slow: int = 26, sig: int = 9):
    """MACD (line, signal, histogram)"""
    if len(closes) < slow + sig:
        return None, None, None

    def ema_series(data, period):
        mult = 2 / (period + 1)
        ema = sum(data[:period]) / period
        series = [ema]
        for v in data[period:]:
            ema = (v - ema) * mult + ema
            series.append(ema)
        return series

    fast_emas = ema_series(closes, fast)
    slow_emas = ema_series(closes, slow)

    # align: slow starts at index 0, fast starts at (slow - fast)
    offset = slow - fast
    macd_line = []
    for i in range(len(slow_emas)):
        fi = i + offset
        if 0 <= fi < len(fast_emas):
            macd_line.append(fast_emas[fi] - slow_emas[i])

    if len(macd_line) < sig:
        return None, None, None

    signal_emas = ema_series(macd_line, sig)
    macd_val = macd_line[-1]
    signal_val = signal_emas[-1]
    hist_val = macd_val - signal_val

    return round(macd_val, 4), round(signal_val, 4), round(hist_val, 4)


def calc_bollinger(closes: list[float], period: int = 20, num_std: float = 2.0):
    """Bollinger Bands (upper, middle, lower, %b)"""
    if len(closes) < period:
        return None, None, None, None

    recent = closes[-period:]
    middle = sum(recent) / period
    variance = sum((x - middle) ** 2 for x in recent) / period
    std = math.sqrt(variance)

    upper = middle + num_std * std
    lower = middle - num_std * std

    if upper == lower:
        pct_b = 0.5
    else:
        pct_b = (closes[-1] - lower) / (upper - lower)

    return round(upper, 2), round(middle, 2), round(lower, 2), round(pct_b, 4)


def calc_vol_ratio(volumes: list[int], period: int = 20) -> float:
    """거래량 비율: 최근 1일 / 20일 평균"""
    if len(volumes) < period or not volumes[-1]:
        return 1.0
    avg = sum(volumes[-period:]) / period
    if avg == 0:
        return 1.0
    return round(volumes[-1] / avg, 2)


def calc_signal_score(rsi, macd_hist, bb_pct, vol_ratio, ma5, ma20, close) -> tuple[int, str]:
    """종합 시그널 점수 (-5 ~ +5)"""
    score = 0

    # RSI (과매도=매수기회, 과매수=매도기회)
    if rsi is not None:
        if rsi < 25:
            score += 2
        elif rsi < 35:
            score += 1
        elif rsi > 75:
            score -= 2
        elif rsi > 65:
            score -= 1

    # MACD 히스토그램
    if macd_hist is not None:
        if macd_hist > 0:
            score += 1
        elif macd_hist < 0:
            score -= 1

    # Bollinger %B
    if bb_pct is not None:
        if bb_pct < 0.1:
            score += 2  # 밴드 하단 이탈 → 매수
        elif bb_pct < 0.25:
            score += 1
        elif bb_pct > 0.9:
            score -= 2  # 밴드 상단 이탈 → 매도
        elif bb_pct > 0.75:
            score -= 1

    # 이동평균 추세
    if ma5 and ma20 and close:
        if close > ma20 and ma5 > ma20:
            score += 1  # 상승 추세
        elif close < ma20 and ma5 < ma20:
            score -= 1  # 하락 추세

    # 거래량 (극단적일 때만)
    if vol_ratio is not None:
        if vol_ratio > 3.0:
            # 거래량 폭증: 방향에 따라 판단
            if score > 0:
                score += 1
            elif score < 0:
                score -= 1

    score = max(-5, min(5, score))

    if score >= 3:
        signal = "strong_buy"
    elif score >= 1:
        signal = "buy"
    elif score >= -1:
        signal = "neutral"
    elif score >= -3:
        signal = "sell"
    else:
        signal = "strong_sell"

    return score, signal


# ─── 데이터 수집 (yfinance) ───────────────────────────────────

def fetch_stock_data(ticker_code: str, suffix: str, days: int = 90):
    """yfinance로 개별 종목 OHLCV 조회"""
    try:
        import yfinance as yf
    except ImportError:
        log.error("yfinance 미설치: pip install yfinance --break-system-packages")
        return None

    symbol = f"{ticker_code}.{suffix}"
    try:
        tk = yf.Ticker(symbol)
        hist = tk.history(period=f"{days}d")
        if hist.empty:
            # KOSPI/KOSDAQ 반대로 시도
            alt = "KQ" if suffix == "KS" else "KS"
            tk = yf.Ticker(f"{ticker_code}.{alt}")
            hist = tk.history(period=f"{days}d")
        if hist.empty:
            return None

        closes = [float(r["Close"]) for _, r in hist.iterrows()]
        opens = [float(r["Open"]) for _, r in hist.iterrows()]
        highs = [float(r["High"]) for _, r in hist.iterrows()]
        lows = [float(r["Low"]) for _, r in hist.iterrows()]
        volumes = [int(r.get("Volume", 0)) for _, r in hist.iterrows()]
        dates = [idx.strftime("%Y-%m-%d") for idx in hist.index]

        return {
            "dates": dates,
            "closes": closes,
            "opens": opens,
            "highs": highs,
            "lows": lows,
            "volumes": volumes,
        }
    except Exception as e:
        log.warning(f"  {ticker_code}.{suffix} 데이터 실패: {e}")
        return None


# ─── 동적 워치리스트 (뉴스 기반 확장) ─────────────────────────

def get_dynamic_tickers(stocks_db: dict[str, tuple[str, str]] | None = None) -> dict:
    """최근 뉴스에서 언급된 전체 종목 추출 (테마주 포함)

    - 7일 이내 중요도 2+ 뉴스에 등장한 모든 종목
    - STOCKS.csv에서 종목명/마켓 확인
    - CORE_WATCHLIST와 중복 제거
    """
    # 1) 뉴스 언급 종목 (최근 7일, 중요도 2+, 제한 없음)
    query_news = (
        "SELECT arrayJoin(tickers) AS tk, count() AS cnt, "
        "countIf(sentiment='positive') AS pos, "
        "countIf(sentiment='negative') AS neg "
        "FROM trading.news "
        "WHERE published_at >= today() - 7 AND importance >= 2 AND length(tickers) > 0 "
        "GROUP BY tk ORDER BY cnt DESC LIMIT 200"
    )
    extra = {}
    try:
        resp = requests.get(CLICKHOUSE_URL, params={"query": query_news}, timeout=10)
        resp.raise_for_status()
        for line in resp.text.strip().splitlines():
            parts = line.split("\t")
            if len(parts) >= 1:
                tk = parts[0].strip()
                if tk and len(tk) == 6 and tk.isdigit() and tk not in CORE_WATCHLIST:
                    # STOCKS.csv에서 이름/마켓 조회
                    if stocks_db and tk in stocks_db:
                        name, suffix = stocks_db[tk]
                    else:
                        name, suffix = f"뉴스종목_{tk}", "KS"
                    extra[tk] = (name, suffix)
    except Exception as e:
        log.warning(f"  뉴스 동적 종목 조회 실패: {e}")

    # 2) DART 공시 언급 종목 (최근 3일)
    query_dart = (
        "SELECT stock_code AS tk "
        "FROM trading.dart_disclosure "
        "WHERE received_at >= today() - 3 AND importance >= 3 AND stock_code != '' "
        "GROUP BY tk"
    )
    try:
        resp = requests.get(CLICKHOUSE_URL, params={"query": query_dart}, timeout=10)
        resp.raise_for_status()
        for line in resp.text.strip().splitlines():
            tk = line.strip()
            if tk and len(tk) == 6 and tk.isdigit() and tk not in CORE_WATCHLIST and tk not in extra:
                if stocks_db and tk in stocks_db:
                    name, suffix = stocks_db[tk]
                else:
                    name, suffix = f"공시종목_{tk}", "KS"
                extra[tk] = (name, suffix)
    except Exception:
        pass

    return extra


def get_top_market_tickers(stocks_db: dict[str, tuple[str, str]], n: int = 50,
                           existing: set = None) -> dict:
    """STOCKS.csv 상위 N종목 추가 (시총순으로 이미 정렬됨)"""
    existing = existing or set()
    result = {}
    count = 0
    for code, (name, suffix) in stocks_db.items():
        if code in existing:
            continue
        # 우선주, 스팩 등 제외
        if name.endswith("우") or name.endswith("우B") or "스팩" in name:
            continue
        result[code] = (name, suffix)
        count += 1
        if count >= n:
            break
    return result


# ─── ClickHouse 저장 ──────────────────────────────────────────

def _esc(s: str) -> str:
    return (s or "").replace("\\", "\\\\").replace("'", "\\'")


def save_to_clickhouse(rows: list[dict]) -> int:
    if not rows:
        return 0

    values = []
    for r in rows:
        values.append(
            f"('{r['date']}', '{_esc(r['ticker'])}', '{_esc(r['ticker_name'])}', "
            f"'{_esc(r.get('market', 'KOSPI'))}', "
            f"{r['close']}, {r['change_pct']}, "
            f"{r['ma5']}, {r['ma20']}, {r['ma60']}, "
            f"{r['rsi14']}, {r['macd']}, {r['macd_signal']}, {r['macd_hist']}, "
            f"{r['bb_upper']}, {r['bb_middle']}, {r['bb_lower']}, {r['bb_pct']}, "
            f"{r['volume']}, {r['vol_ratio']}, "
            f"'{_esc(r['signal'])}', {r['signal_score']}, now())"
        )

    insert_sql = (
        "INSERT INTO trading.technical_signals "
        "(date, ticker, ticker_name, market, "
        "close_price, change_pct, "
        "ma5, ma20, ma60, "
        "rsi14, macd, macd_signal, macd_hist, "
        "bb_upper, bb_middle, bb_lower, bb_pct, "
        "volume, vol_ratio, "
        "signal, signal_score, updated_at) VALUES "
        + ",".join(values)
    )

    try:
        resp = requests.post(CLICKHOUSE_URL, data=insert_sql.encode("utf-8"), timeout=15)
        resp.raise_for_status()
        return len(rows)
    except Exception as e:
        log.error(f"ClickHouse INSERT 실패: {e}")
        return 0


# ─── 메인 ─────────────────────────────────────────────────────

def process_ticker(ticker: str, name: str, suffix: str, days: int) -> dict | None:
    """개별 종목 데이터 수집 + 지표 계산"""
    data = fetch_stock_data(ticker, suffix, days)
    if not data or len(data["closes"]) < 20:
        return None

    closes = data["closes"]
    volumes = data["volumes"]
    latest_date = data["dates"][-1]

    # 지표 계산
    ma5 = calc_sma(closes, 5) or 0
    ma20 = calc_sma(closes, 20) or 0
    ma60 = calc_sma(closes, 60) or 0
    rsi = calc_rsi(closes, 14)
    macd_val, macd_sig, macd_hist = calc_macd(closes, 12, 26, 9)
    bb_upper, bb_middle, bb_lower, bb_pct = calc_bollinger(closes, 20, 2.0)
    vol_ratio = calc_vol_ratio(volumes, 20)

    # 전일 대비
    if len(closes) >= 2 and closes[-2] > 0:
        change_pct = round((closes[-1] - closes[-2]) / closes[-2] * 100, 2)
    else:
        change_pct = 0.0

    # 종합 시그널
    score, signal = calc_signal_score(
        rsi, macd_hist, bb_pct, vol_ratio, ma5, ma20, closes[-1]
    )

    market = "KOSDAQ" if suffix == "KQ" else "KOSPI"

    return {
        "date": latest_date,
        "ticker": ticker,
        "ticker_name": name,
        "market": market,
        "close": closes[-1],
        "change_pct": change_pct,
        "ma5": round(ma5, 2),
        "ma20": round(ma20, 2),
        "ma60": round(ma60, 2),
        "rsi14": rsi if rsi is not None else 50,
        "macd": macd_val if macd_val is not None else 0,
        "macd_signal": macd_sig if macd_sig is not None else 0,
        "macd_hist": macd_hist if macd_hist is not None else 0,
        "bb_upper": bb_upper if bb_upper is not None else 0,
        "bb_middle": bb_middle if bb_middle is not None else 0,
        "bb_lower": bb_lower if bb_lower is not None else 0,
        "bb_pct": bb_pct if bb_pct is not None else 0.5,
        "volume": volumes[-1] if volumes else 0,
        "vol_ratio": vol_ratio,
        "signal": signal,
        "signal_score": score,
    }


def main():
    start = time.time()
    days = 90
    use_dynamic = False
    top_n = 0  # STOCKS.csv 상위 N종목 추가

    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--days" and i + 1 < len(args):
            days = int(args[i + 1])
        elif arg == "--dynamic":
            use_dynamic = True
        elif arg == "--top" and i + 1 < len(args):
            top_n = int(args[i + 1])

    log.info("=" * 60)
    log.info(f"기술적 지표 계산 (최근 {days}일 기반)")
    log.info("=" * 60)

    # STOCKS.csv 로드 (종목명/마켓 확인용)
    stocks_db = load_stocks_csv()

    # 워치리스트 구성
    watchlist = dict(CORE_WATCHLIST)
    core_cnt = len(watchlist)
    dyn_cnt = 0
    top_cnt = 0

    if use_dynamic:
        dyn = get_dynamic_tickers(stocks_db)
        if dyn:
            dyn_cnt = len(dyn)
            log.info(f"  뉴스/공시 동적 종목 추가: {dyn_cnt}종목")
            watchlist.update(dyn)

    if top_n > 0 and stocks_db:
        top_extra = get_top_market_tickers(stocks_db, top_n, set(watchlist.keys()))
        if top_extra:
            top_cnt = len(top_extra)
            log.info(f"  시총 상위 추가: {top_cnt}종목")
            watchlist.update(top_extra)

    total = len(watchlist)
    log.info(f"  총 {total}종목 분석 (핵심 {core_cnt} + 뉴스 {dyn_cnt} + 시총 {top_cnt})")

    # 오늘 날짜 기존 데이터 삭제 (중복 적재 방지)
    today_str = datetime.now().strftime("%Y-%m-%d")
    try:
        del_sql = f"DELETE FROM trading.technical_signals WHERE date = '{today_str}'"
        resp = requests.post(CLICKHOUSE_URL, data=del_sql.encode("utf-8"), timeout=10)
        resp.raise_for_status()
        log.info(f"  기존 기술지표 삭제 완료 (date={today_str})")
    except Exception as e:
        log.warning(f"  기존 데이터 삭제 실패 (무시): {e}")

    log.info("-" * 60)

    results = []
    success = 0
    fail = 0
    batch_size = 20  # 배치 단위로 ClickHouse 저장 (메모리 효율)
    batch_buf = []

    for idx, (ticker, (name, suffix)) in enumerate(watchlist.items(), 1):
        row = process_ticker(ticker, name, suffix, days)
        if row is None:
            log.warning(f"  [{idx}/{total}] {ticker} {name}: 데이터 부족, 스킵")
            fail += 1
            time.sleep(0.2)
            continue

        results.append(row)
        batch_buf.append(row)
        success += 1

        emoji = {"strong_buy": "🟢🟢", "buy": "🟢", "neutral": "⚪",
                 "sell": "🔴", "strong_sell": "🔴🔴"}
        log.info(
            f"  [{idx}/{total}] {ticker} {name:12s} | "
            f"{row['close']:>10,.0f}원 | RSI {row['rsi14']:5.1f} | "
            f"MACD_H {row['macd_hist']:+.2f} | BB {row['bb_pct']:.2f} | "
            f"Vol {row['vol_ratio']:.1f}x | "
            f"{emoji.get(row['signal'], '?')} {row['signal']}({row['signal_score']:+d})"
        )

        # 배치 저장
        if len(batch_buf) >= batch_size:
            save_to_clickhouse(batch_buf)
            batch_buf = []

        # yfinance 부하 방지 (0.25초로 최적화, 100종목 = ~25초)
        time.sleep(0.25)

    # 잔여 배치 저장
    if batch_buf:
        save_to_clickhouse(batch_buf)

    elapsed = time.time() - start
    log.info("=" * 60)
    log.info(f"완료: {success}성공, {fail}실패, 총 {total}종목 ({elapsed:.1f}초)")

    # 시그널 요약
    buy_cnt = sum(1 for r in results if r["signal_score"] >= 1)
    sell_cnt = sum(1 for r in results if r["signal_score"] <= -1)
    neutral_cnt = len(results) - buy_cnt - sell_cnt
    log.info(f"시그널: 🟢매수 {buy_cnt} | ⚪중립 {neutral_cnt} | 🔴매도 {sell_cnt}")

    strong_buys = sorted(
        [r for r in results if r["signal_score"] >= 3],
        key=lambda x: x["signal_score"], reverse=True
    )
    if strong_buys:
        log.info("▸ Strong Buy:")
        for r in strong_buys:
            log.info(f"  {r['ticker']} {r['ticker_name']} (score: {r['signal_score']:+d})")

    strong_sells = sorted(
        [r for r in results if r["signal_score"] <= -3],
        key=lambda x: x["signal_score"]
    )
    if strong_sells:
        log.info("▸ Strong Sell:")
        for r in strong_sells:
            log.info(f"  {r['ticker']} {r['ticker_name']} (score: {r['signal_score']:+d})")

    log.info("=" * 60)

    # ── 텔레그램 알림 ──
    tg_lines = [
        f"📊 <b>기술적 지표 계산 완료</b>",
        f"총 {success}종목 ({elapsed:.0f}초) | 핵심{core_cnt}+뉴스{dyn_cnt}+시총{top_cnt}",
        f"🟢매수 {buy_cnt} | ⚪중립 {neutral_cnt} | 🔴매도 {sell_cnt}",
    ]
    if strong_buys:
        names = ", ".join(f"{r['ticker_name']}({r['signal_score']:+d})" for r in strong_buys[:5])
        tg_lines.append(f"★ Strong Buy: {names}")
    if strong_sells:
        names = ", ".join(f"{r['ticker_name']}({r['signal_score']:+d})" for r in strong_sells[:5])
        tg_lines.append(f"⚠️ Strong Sell: {names}")
    try:
        tg_notify("\n".join(tg_lines))
    except Exception:
        pass


if __name__ == "__main__":
    main()
