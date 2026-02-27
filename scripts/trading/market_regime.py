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
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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

CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")


def ch_query(query: str) -> str | None:
    try:
        resp = requests.get(CLICKHOUSE_URL, params={"query": query}, timeout=10)
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
        resp = requests.post(CLICKHOUSE_URL, data=delete_sql.encode("utf-8"), timeout=10)
        resp.raise_for_status()
        log.info(f"기존 레짐 데이터 삭제 완료 (date={data['date']})")
    except Exception as e:
        log.warning(f"기존 데이터 삭제 실패 (무시하고 진행): {e}")

    sql = (
        f"INSERT INTO trading.market_regime "
        f"(date, kospi_close, kospi_change_pct, kospi_ma20, "
        f"kosdaq_close, kosdaq_change_pct, vix_level, dxy_level, usdkrw, "
        f"trend, volatility, risk_appetite, regime_label, "
        f"news_positive, news_negative, news_neutral, news_mood, "
        f"summary, updated_at) VALUES "
        f"('{data['date']}', {data['kospi_close']}, {data['kospi_change_pct']}, {data['kospi_ma20']}, "
        f"{data['kosdaq_close']}, {data['kosdaq_change_pct']}, {data['vix']}, {data['dxy']}, {data['usdkrw']}, "
        f"'{esc(data['trend'])}', '{esc(data['volatility'])}', '{esc(data['appetite'])}', '{esc(data['regime'])}', "
        f"{data['news_pos']}, {data['news_neg']}, {data['news_neu']}, '{esc(data['news_mood'])}', "
        f"'{esc(data['summary'])}', now())"
    )
    try:
        resp = requests.post(CLICKHOUSE_URL, data=sql.encode("utf-8"), timeout=10)
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

    # 2) 분류
    log.info("레짐 분류 중...")
    trend, kospi_close, kospi_ma20 = classify_trend(kospi_data)
    volatility, vix_level = classify_volatility(vix_data)
    appetite, dxy_level, usdkrw_level = classify_risk_appetite(
        vix_level, dxy_data, usdkrw_data
    )
    news_mood = classify_news_mood(news_sentiment)
    regime_label = make_regime_label(trend, volatility)

    # 코스닥
    kosdaq_close = float(kosdaq_data[0].get("close_price", 0)) if kosdaq_data else 0
    kosdaq_pct = float(kosdaq_data[0].get("change_pct", 0)) if kosdaq_data else 0
    kospi_pct = float(kospi_data[0].get("change_pct", 0)) if kospi_data else 0

    summary = make_summary(
        trend, volatility, appetite, vix_level,
        dxy_level, usdkrw_level, news_mood, kospi_close, kosdaq_close
    )

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
    log.info(f"  레짐:     {regime_label}")
    log.info(f"  뉴스:     pos={news_sentiment['positive']} neg={news_sentiment['negative']} neu={news_sentiment['neutral']} → {news_mood}")
    log.info(f"  요약:     {summary}")
    log.info("-" * 60)

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
    }

    if save_regime(data):
        log.info("ClickHouse 저장 완료")
    else:
        log.error("ClickHouse 저장 실패")

    elapsed = time.time() - start
    log.info("=" * 60)

    # 매매 가이드라인 출력
    guide = {
        "BULL_CALM": "적극적 매수 검토. 추세 따라가기.",
        "BULL_VOL": "분할 매수. 변동성 주의, 포지션 축소 고려.",
        "BEAR_CALM": "관망 또는 방어적 포지션. 현금 비중 확대.",
        "BEAR_VOL": "신규 매수 금지. 손절 규율 엄격 적용.",
        "SIDEWAYS": "박스권 하단 매수, 상단 매도. 단기 트레이딩.",
    }
    guide_text = guide.get(regime_label, '중립적 접근')
    log.info(f"  가이드: {guide_text}")
    log.info(f"  완료 ({elapsed:.1f}초)")
    log.info("=" * 60)

    # 텔레그램 알림
    try:
        from telegram_notify import notify
        emoji = {"BULL_CALM": "🟢🌤", "BULL_VOL": "🟢⚡", "BEAR_CALM": "🔴🌤",
                 "BEAR_VOL": "🔴⚡", "SIDEWAYS": "⚪🔄"}.get(regime_label, "❓")
        notify(
            f"{emoji} <b>시장 레짐: {regime_label}</b>\n"
            f"KOSPI {kospi_close:,.0f} | KOSDAQ {kosdaq_close:,.0f} | VIX {vix_level:.1f}\n"
            f"USD/KRW {usdkrw_level:,.0f} | DXY {dxy_level:.1f}\n"
            f"→ {guide_text}"
        )
    except Exception as e:
        log.warning(f"텔레그램 전송 실패: {e}")


if __name__ == "__main__":
    main()
