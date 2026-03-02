#!/usr/bin/env python3
"""market_regime.py

ClickHouse의 기존 시장 데이터(지수, VIX, 환율, 뉴스)를 종합해
시장 레짐을 분류하고 저장.

OpenClaw gpt-5.2가 매매 판단 전에 시장 전체 컨텍스트를 한 번에 파악할 수 있게 한다.

레짐 분류:
  trend:         bull / bear / sideways (코스피 20일 이평 기준)
  volatility:    high / normal / low (VIX 기준)
  risk_appetite: risk_on / risk_off / neutral (VIX + DXY + 환율 조합)
  regime_label:  BULL_CALM, BULL_VOL, BEAR_CALM, BEAR_VOL, SIDEWAYS

사용법:
  python3 market_regime.py
"""
from __future__ import annotations

import os
import sys
import json
import time
import logging
import importlib.util
import re
from datetime import datetime
from pathlib import Path
from html import escape as html_escape

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()

try:
    import requests
except ImportError:
    from _requests_compat import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("market-regime")

CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "").strip()
if not CLICKHOUSE_URL:
    CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_HOST", "http://localhost:8123").strip()
if not CLICKHOUSE_URL:
    CLICKHOUSE_URL = "http://localhost:8123"
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "").strip()
CLICKHOUSE_PASS = os.environ.get("CLICKHOUSE_PASS", os.environ.get("CLICKHOUSE_PASSWORD", "")).strip()
CLICKHOUSE_AUTH = (CLICKHOUSE_USER, CLICKHOUSE_PASS) if CLICKHOUSE_USER else None


def _load_notify():
    candidates = [
        Path(__file__).resolve().parent / "telegram_notify.py",
        Path(__file__).resolve().parent.parent / "telegram_notify.py",
        Path.home() / ".openclaw" / "scripts" / "telegram_notify.py",
    ]
    for path in candidates:
        if not path.exists():
            continue
        spec = importlib.util.spec_from_file_location("telegram_notify", path)
        if not spec or not spec.loader:
            continue
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        notify = getattr(mod, "notify", None)
        if callable(notify):
            return notify
    return None


def ch_query(query: str) -> str | None:
    try:
        resp = requests.get(CLICKHOUSE_URL, params={"query": query}, timeout=10, auth=CLICKHOUSE_AUTH)
        resp.raise_for_status()
        return resp.text.strip()
    except Exception as e:
        log.warning(f"ClickHouse 쿼리 실패: {e}")
        return None


def ch_query_json(query: str) -> list[dict]:
    try:
        resp = requests.get(
            CLICKHOUSE_URL,
            params={"query": query, "default_format": "JSONEachRow"},
            timeout=10,
            auth=CLICKHOUSE_AUTH,
        )
        resp.raise_for_status()
        rows = []
        for line in resp.text.strip().splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows
    except Exception as e:
        log.warning(f"ClickHouse JSON 쿼리 실패: {e}")
        return []


def _table_has_column(table: str, column: str) -> bool:
    try:
        rows = ch_query_json(
            "SELECT count() AS c "
            "FROM system.columns "
            "WHERE database = 'trading' "
            f"AND table = '{table}' "
            f"AND name = '{column}'"
        )
        return bool(rows and int(rows[0].get("c", 0) or 0) > 0)
    except Exception:
        return False


# ─── 데이터 수집 ──────────────────────────────────────────────

def get_index_data(code: str, days: int = 30) -> list[dict]:
    """지수/VIX/DXY 최근 N일"""
    q = (
        f"SELECT date, close_price, change_pct "
        f"FROM trading.market_index "
        f"WHERE index_code = '{code}' "
        f"ORDER BY date DESC LIMIT {days}"
    )
    return ch_query_json(q)


def get_fx_data(pair: str, days: int = 30) -> list[dict]:
    q = (
        f"SELECT date, close_rate, change_pct "
        f"FROM trading.exchange_rate "
        f"WHERE currency_pair = '{pair}' "
        f"ORDER BY date DESC LIMIT {days}"
    )
    return ch_query_json(q)


def get_news_sentiment(days: int = 3) -> dict:
    """최근 N일 뉴스 감성 통계"""
    q = (
        f"SELECT sentiment, count() AS cnt "
        f"FROM trading.news "
        f"WHERE published_at >= today() - {days} "
        f"GROUP BY sentiment"
    )
    rows = ch_query_json(q)
    result = {"positive": 0, "negative": 0, "neutral": 0}
    for row in rows:
        s = row.get("sentiment", "neutral")
        result[s] = int(row.get("cnt", 0))
    return result


MACRO_TOPIC_KEYWORDS: dict[str, list[str]] = {
    "geopolitics": [
        "이란", "중동", "이스라엘", "전쟁", "공습", "미사일", "분쟁", "충돌", "휴전",
        "geopolitic", "war", "strike", "conflict",
    ],
    "war": [
        "전쟁", "공습", "폭격", "미사일", "교전", "war", "strike", "missile",
    ],
    "oil": [
        "원유", "유가", "브렌트", "wti", "opec", "석유", "호르무즈", "oil", "crude",
    ],
    "shipping": [
        "해운", "운임", "항로", "수에즈", "호르무즈", "물류", "shipping", "freight",
    ],
    "sanctions": [
        "제재", "관세", "수출통제", "엠바고", "금수", "sanction", "tariff", "export control",
    ],
}


def _detect_macro_topics(text: str) -> list[str]:
    t = (text or "").lower()
    topics: list[str] = []
    for topic, kws in MACRO_TOPIC_KEYWORDS.items():
        if any(k.lower() in t for k in kws):
            topics.append(topic)
    return topics


def get_macro_topic_snapshot(hours: int = 24, limit: int = 400) -> dict:
    """최근 중요 뉴스/이벤트에서 매크로 토픽을 추출한다.

    티커 매핑 유무와 무관하게 레짐 스트레스 플래그에 반영하기 위함.
    """
    h = max(6, int(hours))
    n = max(50, int(limit))
    rows = ch_query_json(
        f"""
WITH parseDateTimeBestEffortOrNull(toString(published_at)) AS p_ts
SELECT
    event_type,
    channels,
    thesis_path
FROM trading.news_event_frames
WHERE p_ts >= now() - INTERVAL {h} HOUR
  AND relevant = 1
ORDER BY p_ts DESC
LIMIT {n}
"""
    )
    topic_counter: dict[str, int] = {k: 0 for k in MACRO_TOPIC_KEYWORDS.keys()}
    topic_headline: dict[str, str] = {}
    for r in rows:
        event_type = str(r.get("event_type", "") or "")
        channels = r.get("channels", [])
        if not isinstance(channels, list):
            channels = []
        thesis_path = str(r.get("thesis_path", "") or "")
        text = f"{event_type} {' '.join([str(c) for c in channels])} {thesis_path}"
        topics = _detect_macro_topics(text)
        if not topics:
            continue
        title = event_type if event_type else thesis_path[:80]
        for topic in topics:
            topic_counter[topic] = topic_counter.get(topic, 0) + 1
            if topic not in topic_headline and title:
                topic_headline[topic] = title

    active_topics = [k for k, v in topic_counter.items() if int(v or 0) > 0]
    active_topics.sort(key=lambda x: topic_counter.get(x, 0), reverse=True)
    top = active_topics[:3]
    return {
        "topics": top,
        "counts": topic_counter,
        "headlines": {k: topic_headline.get(k, "") for k in top},
    }


def _sum_flow_rows(rows: list[dict]) -> dict:
    out = {
        "foreign": 0.0,
        "inst": 0.0,
        "foreign_1d": 0.0,
        "inst_1d": 0.0,
        "foreign_pos_days": 0,
        "inst_pos_days": 0,
        "n_days": 0,
    }
    if not rows:
        return out
    days_seen: set[str] = set()
    by_day: dict[str, dict[str, float]] = {}
    for r in rows:
        investor = str(r.get("investor_type", "")).strip().upper()
        d = str(r.get("trade_date", "")).strip()
        days_seen.add(d)
        v = float(r.get("net_buy_value_krw", 0) or 0)
        if d:
            day_row = by_day.setdefault(d, {"FOREIGN": 0.0, "INST": 0.0})
            if investor == "FOREIGN":
                day_row["FOREIGN"] += v
            elif investor in ("INST", "INSTITUTION"):
                day_row["INST"] += v
        if investor == "FOREIGN":
            out["foreign"] += v
            if v > 0:
                out["foreign_pos_days"] += 1
        elif investor in ("INST", "INSTITUTION"):
            out["inst"] += v
            if v > 0:
                out["inst_pos_days"] += 1
    clean_days = sorted([x for x in days_seen if x], reverse=True)
    out["n_days"] = len(clean_days)
    if clean_days:
        latest = clean_days[0]
        latest_row = by_day.get(latest, {})
        out["foreign_1d"] = float(latest_row.get("FOREIGN", 0.0) or 0.0)
        out["inst_1d"] = float(latest_row.get("INST", 0.0) or 0.0)
    return out


def get_market_flow_trend(days: int = 3) -> dict:
    """시장 수급 추세(외국인/기관) 조회.

    우선순위:
    1) market_flow_daily (market='ALL')
    2) market_flow_daily (KOSPI+KOSDAQ 합산)
    """
    d = max(1, int(days))
    rows_all = ch_query_json(
        f"""
SELECT
    trade_date,
    investor_type,
    net_buy_value_krw
FROM trading.market_flow_daily
WHERE market = 'ALL'
  AND investor_type IN ('FOREIGN', 'INST', 'INSTITUTION')
  AND trade_date IN (
    SELECT trade_date
    FROM trading.market_flow_daily
    WHERE market = 'ALL'
    ORDER BY trade_date DESC
    LIMIT {d}
  )
ORDER BY trade_date DESC, investor_type
"""
    )
    agg = _sum_flow_rows(rows_all)
    if agg["n_days"] > 0:
        agg["source"] = "market_flow_daily:ALL"
        return agg

    rows_mkt = ch_query_json(
        f"""
SELECT
    trade_date,
    investor_type,
    sum(net_buy_value_krw) AS net_buy_value_krw
FROM trading.market_flow_daily
WHERE market IN ('KOSPI', 'KOSDAQ')
  AND investor_type IN ('FOREIGN', 'INST', 'INSTITUTION')
  AND trade_date IN (
    SELECT DISTINCT trade_date
    FROM trading.market_flow_daily
    WHERE market IN ('KOSPI', 'KOSDAQ')
    ORDER BY trade_date DESC
    LIMIT {d}
  )
GROUP BY trade_date, investor_type
ORDER BY trade_date DESC, investor_type
"""
    )
    agg = _sum_flow_rows(rows_mkt)
    agg["source"] = "market_flow_daily:KOSPI+KOSDAQ"
    return agg


def _fmt_krw_jo(v: float) -> str:
    return f"{(float(v) / 1_000_000_000_000):+.2f}조"


def _build_flow_summary(flow: dict) -> str:
    n = int(flow.get("n_days", 0) or 0)
    if n <= 0:
        return "수급데이터 부족"
    fsum = _fmt_krw_jo(float(flow.get("foreign", 0.0) or 0.0))
    isum = _fmt_krw_jo(float(flow.get("inst", 0.0) or 0.0))
    f1d = _fmt_krw_jo(float(flow.get("foreign_1d", 0.0) or 0.0))
    i1d = _fmt_krw_jo(float(flow.get("inst_1d", 0.0) or 0.0))
    fpos = int(flow.get("foreign_pos_days", 0) or 0)
    ipos = int(flow.get("inst_pos_days", 0) or 0)
    return f"외인 {fsum}({fpos}/{n}일+,1일 {f1d}), 기관 {isum}({ipos}/{n}일+,1일 {i1d})"


def classify_action_posture(
    regime_label: str,
    vix: float,
    usdkrw: float,
    flow: dict,
    macro_topics: list[str] | None = None,
) -> tuple[str, list[str]]:
    """레짐 라벨과 별개로 실행 강도(posture)를 결정한다.

    stress 신호:
    - VIX >= 20
    - USD/KRW >= 1430
    - 외국인 1일 순매도 <= -2조
    """
    flags: list[str] = []
    if vix >= 20.0:
        flags.append("VIX>=20")
    if usdkrw >= 1430.0:
        flags.append("USDKRW>=1430")
    if float(flow.get("foreign_1d", 0.0) or 0.0) <= -2_000_000_000_000:
        flags.append("FOREIGN_1D<=-2T")
    macro_set = set(macro_topics or [])
    if "geopolitics" in macro_set or "war" in macro_set:
        flags.append("GEOPOLITICAL_RISK")
    if "oil" in macro_set:
        flags.append("OIL_SHOCK_RISK")
    if "shipping" in macro_set:
        flags.append("SHIPPING_DISRUPTION_RISK")
    if "sanctions" in macro_set:
        flags.append("SANCTIONS_RISK")

    stress_n = len(set(flags))
    base = "normal"
    if regime_label == "BULL_CALM":
        base = "aggressive"
    elif regime_label in ("BULL_VOL", "SIDEWAYS"):
        base = "normal"
    elif regime_label == "BEAR_CALM":
        base = "cautious"
    elif regime_label == "BEAR_VOL":
        base = "defensive"

    # 충돌 신호가 2개 이상이면 최소 1단계 감속, 3개면 방어 모드
    if stress_n >= 3:
        return "defensive", flags
    if stress_n >= 2:
        if base == "aggressive":
            return "cautious", flags
        if base == "normal":
            return "cautious", flags
        return base, flags
    return base, flags


# ─── 레짐 분류 로직 ───────────────────────────────────────────

def classify_trend(kospi_data: list[dict]) -> tuple[str, float, float]:
    """코스피 추세 분류"""
    if not kospi_data or len(kospi_data) < 5:
        return "sideways", 0, 0

    # 최신이 [0]
    current = float(kospi_data[0].get("close_price", 0))

    # 20일 평균
    prices = [float(d.get("close_price", 0)) for d in kospi_data[:20]]
    if not prices:
        return "sideways", current, 0

    ma20 = sum(prices) / len(prices)

    # 최근 5일 방향
    recent_5 = prices[:5] if len(prices) >= 5 else prices
    if len(recent_5) >= 2:
        direction = recent_5[0] - recent_5[-1]  # 양수면 상승
    else:
        direction = 0

    if current > ma20 * 1.01 and direction > 0:
        trend = "bull"
    elif current < ma20 * 0.99 and direction < 0:
        trend = "bear"
    else:
        trend = "sideways"

    return trend, current, ma20


def classify_volatility(vix_data: list[dict]) -> tuple[str, float]:
    """VIX 기반 변동성 분류"""
    if not vix_data:
        return "normal", 0

    vix = float(vix_data[0].get("close_price", 20))

    if vix >= 30:
        return "high", vix
    elif vix >= 20:
        return "normal", vix
    else:
        return "low", vix


def classify_risk_appetite(
    vix: float, dxy_data: list[dict], usdkrw_data: list[dict]
) -> tuple[str, float, float]:
    """리스크 선호도 분류"""
    dxy = float(dxy_data[0].get("close_price", 100)) if dxy_data else 100
    usdkrw = float(usdkrw_data[0].get("close_rate", 1300)) if usdkrw_data else 1300

    risk_score = 0

    # VIX: 낮으면 risk_on
    if vix < 15:
        risk_score += 2
    elif vix < 20:
        risk_score += 1
    elif vix > 30:
        risk_score -= 2
    elif vix > 25:
        risk_score -= 1

    # DXY: 강달러 = risk_off
    if dxy > 105:
        risk_score -= 1
    elif dxy < 100:
        risk_score += 1

    # USDKRW: 원화 약세 = risk_off
    if usdkrw > 1400:
        risk_score -= 1
    elif usdkrw < 1250:
        risk_score += 1

    if risk_score >= 2:
        appetite = "risk_on"
    elif risk_score <= -2:
        appetite = "risk_off"
    else:
        appetite = "neutral"

    return appetite, dxy, usdkrw


def classify_news_mood(sentiment: dict) -> str:
    total = sum(sentiment.values()) or 1
    pos_ratio = sentiment.get("positive", 0) / total
    neg_ratio = sentiment.get("negative", 0) / total

    if pos_ratio > 0.5:
        return "bullish"
    elif neg_ratio > 0.5:
        return "bearish"
    else:
        return "mixed"


def make_regime_label(trend: str, volatility: str) -> str:
    """종합 레짐 라벨"""
    if trend == "bull" and volatility in ("low", "normal"):
        return "BULL_CALM"
    elif trend == "bull" and volatility == "high":
        return "BULL_VOL"
    elif trend == "bear" and volatility in ("low", "normal"):
        return "BEAR_CALM"
    elif trend == "bear" and volatility == "high":
        return "BEAR_VOL"
    else:
        return "SIDEWAYS"


def make_summary(
    trend, volatility, appetite, vix, dxy, usdkrw, news_mood, kospi, kosdaq_close
) -> str:
    """한줄 요약 (OpenClaw이 읽기 좋게)"""
    parts = []

    trend_kr = {"bull": "상승추세", "bear": "하락추세", "sideways": "박스권"}
    parts.append(f"KOSPI {kospi:,.0f} ({trend_kr.get(trend, '?')})")

    if kosdaq_close:
        parts.append(f"KOSDAQ {kosdaq_close:,.0f}")

    vol_kr = {"high": "고변동", "normal": "보통", "low": "저변동"}
    parts.append(f"VIX {vix:.1f}({vol_kr.get(volatility, '?')})")

    parts.append(f"USD/KRW {usdkrw:,.0f}")

    appetite_kr = {"risk_on": "위험선호", "risk_off": "위험회피", "neutral": "중립"}
    parts.append(appetite_kr.get(appetite, "중립"))

    mood_kr = {"bullish": "뉴스긍정", "bearish": "뉴스부정", "mixed": "뉴스혼조"}
    parts.append(mood_kr.get(news_mood, "?"))

    return " | ".join(parts)


# ─── ClickHouse 저장 ──────────────────────────────────────────

def save_regime(data: dict) -> bool:
    def esc(s):
        return (s or "").replace("\\", "\\\\").replace("'", "\\'")

    # 같은 날짜 기존 데이터 삭제 (중복 방지)
    delete_sql = f"DELETE FROM trading.market_regime WHERE date = '{data['date']}'"
    try:
        resp = requests.post(CLICKHOUSE_URL, data=delete_sql.encode("utf-8"), timeout=10, auth=CLICKHOUSE_AUTH)
        resp.raise_for_status()
        log.info(f"기존 레짐 데이터 삭제 완료 (date={data['date']})")
    except Exception as e:
        log.warning(f"기존 데이터 삭제 실패 (무시하고 진행): {e}")

    # posture 확장 컬럼(호환성) 보장
    for alter_sql in (
        "ALTER TABLE trading.market_regime ADD COLUMN IF NOT EXISTS action_posture LowCardinality(String) DEFAULT 'normal'",
        "ALTER TABLE trading.market_regime ADD COLUMN IF NOT EXISTS stress_flags Array(String) DEFAULT []",
        "ALTER TABLE trading.market_regime ADD COLUMN IF NOT EXISTS guide_text String DEFAULT ''",
    ):
        try:
            _r = requests.post(CLICKHOUSE_URL, data=alter_sql.encode("utf-8"), timeout=10, auth=CLICKHOUSE_AUTH)
            _r.raise_for_status()
        except Exception:
            # 컬럼 생성 실패는 비치명(기존 스키마로 저장 시도)
            pass

    flags = data.get("stress_flags", []) if isinstance(data.get("stress_flags", []), list) else []
    flags_sql = "[" + ", ".join("'" + str(x).replace("\\", "\\\\").replace("'", "\\'") + "'" for x in flags) + "]"

    sql = (
        f"INSERT INTO trading.market_regime "
        f"(date, kospi_close, kospi_change_pct, kospi_ma20, "
        f"kosdaq_close, kosdaq_change_pct, vix_level, dxy_level, usdkrw, "
        f"trend, volatility, risk_appetite, regime_label, "
        f"news_positive, news_negative, news_neutral, news_mood, "
        f"summary, action_posture, stress_flags, guide_text, updated_at) VALUES "
        f"('{data['date']}', {data['kospi_close']}, {data['kospi_change_pct']}, {data['kospi_ma20']}, "
        f"{data['kosdaq_close']}, {data['kosdaq_change_pct']}, {data['vix']}, {data['dxy']}, {data['usdkrw']}, "
        f"'{esc(data['trend'])}', '{esc(data['volatility'])}', '{esc(data['appetite'])}', '{esc(data['regime'])}', "
        f"{data['news_pos']}, {data['news_neg']}, {data['news_neu']}, '{esc(data['news_mood'])}', "
        f"'{esc(data['summary'])}', '{esc(data.get('action_posture', 'normal'))}', {flags_sql}, '{esc(data.get('guide_text', ''))}', now())"
    )
    try:
        resp = requests.post(CLICKHOUSE_URL, data=sql.encode("utf-8"), timeout=10, auth=CLICKHOUSE_AUTH)
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error(f"저장 실패: {e}")
        return False


# ─── 메인 ─────────────────────────────────────────────────────

def main():
    start = time.time()
    log.info("=" * 60)
    log.info("시장 레짐 분류")
    log.info("=" * 60)

    # 1) 데이터 수집
    log.info("데이터 수집 중...")
    kospi_data = get_index_data("KOSPI", 30)
    kosdaq_data = get_index_data("KOSDAQ", 5)
    vix_data = get_index_data("VIX", 10)
    dxy_data = get_index_data("DXY", 10)
    usdkrw_data = get_fx_data("USDKRW", 10)
    news_sentiment = get_news_sentiment(3)
    flow_trend = get_market_flow_trend(3)
    macro_snapshot = get_macro_topic_snapshot(hours=int(os.environ.get("REGIME_MACRO_WINDOW_HOURS", "24")))

    # 2) 분류
    log.info("레짐 분류 중...")
    trend, kospi_close, kospi_ma20 = classify_trend(kospi_data)
    volatility, vix_level = classify_volatility(vix_data)
    appetite, dxy_level, usdkrw_level = classify_risk_appetite(
        vix_level, dxy_data, usdkrw_data
    )
    news_mood = classify_news_mood(news_sentiment)
    regime_label = make_regime_label(trend, volatility)
    posture, posture_flags = classify_action_posture(
        regime_label=regime_label,
        vix=vix_level,
        usdkrw=usdkrw_level,
        flow=flow_trend,
        macro_topics=list(macro_snapshot.get("topics", [])),
    )

    # 코스닥
    kosdaq_close = float(kosdaq_data[0].get("close_price", 0)) if kosdaq_data else 0
    kosdaq_pct = float(kosdaq_data[0].get("change_pct", 0)) if kosdaq_data else 0
    kospi_pct = float(kospi_data[0].get("change_pct", 0)) if kospi_data else 0

    summary = make_summary(
        trend, volatility, appetite, vix_level,
        dxy_level, usdkrw_level, news_mood, kospi_close, kosdaq_close
    )
    flow_summary = _build_flow_summary(flow_trend)
    macro_topics = list(macro_snapshot.get("topics", []))
    macro_txt = ",".join(macro_topics) if macro_topics else "-"
    summary = f"{summary} | 수급 {flow_summary} | macro {macro_txt}"

    today_str = kospi_data[0].get("date", datetime.now().strftime("%Y-%m-%d")) if kospi_data else datetime.now().strftime("%Y-%m-%d")

    # 3) 결과 출력
    log.info("-" * 60)
    log.info(f"  날짜:     {today_str}")
    log.info(f"  KOSPI:    {kospi_close:,.0f} (MA20: {kospi_ma20:,.0f})")
    log.info(f"  KOSDAQ:   {kosdaq_close:,.0f}")
    log.info(f"  VIX:      {vix_level:.1f}")
    log.info(f"  DXY:      {dxy_level:.1f}")
    log.info(f"  USD/KRW:  {usdkrw_level:,.0f}")
    log.info(f"  추세:     {trend}")
    log.info(f"  변동성:   {volatility}")
    log.info(f"  위험선호: {appetite}")
    log.info(
        "  수급(최근 %s일): 외인 %s (%s/%s일+), 기관 %s (%s/%s일+) [%s]",
        int(flow_trend.get("n_days", 0) or 0),
        _fmt_krw_jo(float(flow_trend.get("foreign", 0.0) or 0.0)),
        int(flow_trend.get("foreign_pos_days", 0) or 0),
        int(flow_trend.get("n_days", 0) or 0),
        _fmt_krw_jo(float(flow_trend.get("inst", 0.0) or 0.0)),
        int(flow_trend.get("inst_pos_days", 0) or 0),
        int(flow_trend.get("n_days", 0) or 0),
        str(flow_trend.get("source", "-")),
    )
    log.info(
        "  수급(1일): 외인 %s, 기관 %s",
        _fmt_krw_jo(float(flow_trend.get("foreign_1d", 0.0) or 0.0)),
        _fmt_krw_jo(float(flow_trend.get("inst_1d", 0.0) or 0.0)),
    )
    log.info(f"  레짐:     {regime_label}")
    log.info(f"  행동강도: {posture} (flags={','.join(posture_flags) if posture_flags else '-'})")
    if macro_topics:
        log.info("  매크로 토픽(24h): %s", ", ".join(macro_topics))
    log.info(f"  뉴스:     pos={news_sentiment['positive']} neg={news_sentiment['negative']} neu={news_sentiment['neutral']} → {news_mood}")
    log.info(f"  요약:     {summary}")
    log.info("-" * 60)

    # 매매 가이드라인 출력 (레짐 라벨 + 행동강도 분리)
    guide = {
        "BULL_CALM": "추세 우호. 선별 매수 검토.",
        "BULL_VOL": "분할 매수. 변동성 주의, 포지션 축소 고려.",
        "BEAR_CALM": "관망 또는 방어적 포지션. 현금 비중 확대.",
        "BEAR_VOL": "신규 매수 금지. 손절 규율 엄격 적용.",
        "SIDEWAYS": "박스권 하단 매수, 상단 매도. 단기 트레이딩.",
    }
    posture_guide = {
        "aggressive": "공격적 집행 가능(분할 진입 권장)",
        "normal": "표준 집행(추격보다 확인매수)",
        "cautious": "감속 집행(사이즈 축소·분할·확인 우선)",
        "defensive": "방어 모드(신규매수 최소화/보류)",
    }
    guide_text = guide.get(regime_label, "중립적 접근")
    posture_text = posture_guide.get(posture, "중립 집행")
    if posture_flags:
        guide_text = f"{guide_text} 단, 스트레스 신호({', '.join(posture_flags)})로 감속."

    # 4) 저장
    data = {
        "date": today_str,
        "kospi_close": kospi_close,
        "kospi_change_pct": kospi_pct,
        "kospi_ma20": round(kospi_ma20, 2),
        "kosdaq_close": kosdaq_close,
        "kosdaq_change_pct": kosdaq_pct,
        "vix": vix_level,
        "dxy": dxy_level,
        "usdkrw": usdkrw_level,
        "trend": trend,
        "volatility": volatility,
        "appetite": appetite,
        "regime": regime_label,
        "news_pos": news_sentiment["positive"],
        "news_neg": news_sentiment["negative"],
        "news_neu": news_sentiment["neutral"],
        "news_mood": news_mood,
        "summary": summary,
        "action_posture": posture,
        "stress_flags": posture_flags,
        "guide_text": posture_text,
    }

    if save_regime(data):
        log.info("ClickHouse 저장 완료")
    else:
        log.error("ClickHouse 저장 실패")

    elapsed = time.time() - start
    log.info("=" * 60)

    log.info(f"  가이드(레짐): {guide_text}")
    log.info(f"  가이드(행동): {posture_text}")
    log.info(f"  완료 ({elapsed:.1f}초)")
    log.info("=" * 60)

    # 텔레그램 알림
    try:
        notify = _load_notify()
        if not notify:
            raise RuntimeError("telegram_notify module not found")
        emoji = {"BULL_CALM": "🟢🌤", "BULL_VOL": "🟢⚡", "BEAR_CALM": "🔴🌤",
                 "BEAR_VOL": "🔴⚡", "SIDEWAYS": "⚪🔄"}.get(regime_label, "❓")
        flags_txt = ", ".join(posture_flags) if posture_flags else "-"
        safe_regime = html_escape(str(regime_label))
        safe_flow = html_escape(str(flow_summary))
        safe_posture = html_escape(str(posture))
        safe_flags = html_escape(flags_txt)
        safe_posture_text = html_escape(str(posture_text))
        notify(
            f"{emoji} <b>시장 레짐: {safe_regime}</b>\n"
            f"KOSPI {kospi_close:,.0f} | KOSDAQ {kosdaq_close:,.0f} | VIX {vix_level:.1f}\n"
            f"USD/KRW {usdkrw_level:,.0f} | DXY {dxy_level:.1f}\n"
            f"수급(최근 {int(flow_trend.get('n_days', 0) or 0)}일): {safe_flow}\n"
            f"행동강도: {safe_posture} ({safe_flags})\n"
            f"→ {safe_posture_text}"
        )
    except Exception as e:
        log.warning(f"텔레그램 전송 실패: {e}")


if __name__ == "__main__":
    main()
