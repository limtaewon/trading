#!/usr/bin/env python3
"""
시장 브리핑 생성기: ClickHouse 데이터 → GPT-5.2 Strategist 입력용 마크다운

GPT-5.2가 매매 판단할 때 이 브리핑을 context로 받음.
OpenClaw agent의 pre-prompt 또는 tool로 활용.

사용법:
  python3 market_briefing.py              # 터미널 출력
  python3 market_briefing.py --json       # JSON 출력 (API 연동용)
  python3 market_briefing.py --save       # 파일 저장

출력 예시:
  ## 시장 브리핑 (2026-02-10 09:15)
  ### 지수
  코스피 2,650.32 (+0.8%) | 코스닥 845.21 (-0.3%)
  S&P500 6,120 (+0.5%) | 나스닥 19,850 (+0.9%)
  ...
"""

import os
import sys
import json
from datetime import datetime, timedelta

# ensure local imports work regardless of CWD (cron, manual run, etc.)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()

try:
    import requests
except ImportError:
    from _requests_compat import requests

from market_realtime import fetch_naver_realtime_indices, fetch_naver_usdkrw

CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")


def ch_query(sql):
    """ClickHouse 쿼리 → 텍스트"""
    try:
        resp = requests.get(CLICKHOUSE_URL, params={"query": sql}, timeout=10)
        resp.raise_for_status()
        return resp.text.strip()
    except Exception:
        return ""


def ch_json(sql):
    """ClickHouse 쿼리 → JSON"""
    try:
        resp = requests.get(
            CLICKHOUSE_URL,
            params={"query": sql + " FORMAT JSON"},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception:
        return []


def get_indices():
    """지수별 최신 데이터(코드별 max date 기준)."""
    rows = ch_json("""
        SELECT
            index_code,
            any(index_name) AS index_name,
            argMax(close_price, date) AS close_price,
            argMax(change_pct, date) AS change_pct,
            max(date) AS latest_date
        FROM trading.market_index
        WHERE date >= today() - 7
        GROUP BY index_code
        ORDER BY index_code
    """)
    return {r["index_code"]: r for r in rows} if rows else {}


def get_fx():
    """통화쌍별 최신 환율(코드별 max date 기준)."""
    rows = ch_json("""
        SELECT
            currency_pair,
            argMax(close_rate, date) AS close_rate,
            argMax(change_pct, date) AS change_pct,
            max(date) AS latest_date
        FROM trading.exchange_rate
        WHERE date >= today() - 7
        GROUP BY currency_pair
        ORDER BY currency_pair
    """)
    return {r["currency_pair"]: r for r in rows} if rows else {}


def get_rates():
    """최근 금리"""
    rows = ch_json("""
        SELECT rate_code, rate_name, rate_value, date
        FROM trading.interest_rate
        WHERE date >= today() - 7
        ORDER BY date DESC, rate_code
        LIMIT 30
    """)
    if not rows:
        return {}
    # 코드별 최신값
    result = {}
    for r in rows:
        if r["rate_code"] not in result:
            result[r["rate_code"]] = r
    return result


def get_commodities():
    """최근 원자재"""
    rows = ch_json("""
        SELECT commodity_code, commodity_name, close_price, change_pct, date
        FROM trading.commodity
        WHERE date >= today() - 3
        ORDER BY date DESC
        LIMIT 20
    """)
    if not rows:
        return {}
    latest_date = rows[0]["date"]
    return {r["commodity_code"]: r for r in rows if r["date"] == latest_date}


def get_investor_flow():
    """투자자별 매매동향"""
    rows = ch_json("""
        SELECT date, market, investor_type, net_amount
        FROM trading.investor_flow
        WHERE date >= today() - 5
        ORDER BY date DESC, market, investor_type
        LIMIT 30
    """)
    return rows


def get_important_news(limit=15):
    """중요 뉴스 (importance >= 3)"""
    rows = ch_json(f"""
        SELECT
            published_at, title, summary, importance,
            sentiment, impact_type, tickers
        FROM trading.news
        WHERE importance >= 3
          AND collected_at >= now() - INTERVAL 24 HOUR
        ORDER BY importance DESC, published_at DESC
        LIMIT {limit}
    """)
    return rows


def get_news_sentiment():
    """최근 24시간 뉴스 감성 요약"""
    rows = ch_json("""
        SELECT
            sentiment,
            count() AS cnt,
            avg(importance) AS avg_imp
        FROM trading.news
        WHERE collected_at >= now() - INTERVAL 24 HOUR
        GROUP BY sentiment
    """)
    return {r["sentiment"]: r for r in rows}


# ─── 브리핑 생성 ─────────────────────────────────────────────
def generate_briefing():
    """시장 브리핑 마크다운 생성"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"## 시장 브리핑 ({now})", ""]
    rt_indices = fetch_naver_realtime_indices(timeout_sec=8)
    rt_fx = fetch_naver_usdkrw(timeout_sec=8)

    # 1. 지수
    indices = get_indices()
    if indices:
        lines.append("### 주요 지수")
        for code in ["KOSPI", "KOSDAQ", "SPX", "NDX", "N225", "VIX", "DXY"]:
            idx = indices.get(code)
            if idx:
                show_price = idx["close_price"]
                show_pct = idx["change_pct"]
                stamp = idx.get("latest_date", "-")
                if code in ("KOSPI", "KOSDAQ") and code in rt_indices:
                    rt = rt_indices.get(code, {})
                    show_price = rt.get("price", show_price)
                    show_pct = rt.get("change_pct", show_pct)
                    stamp = "RT"
                try:
                    show_price = float(show_price)
                except Exception:
                    show_price = 0.0
                try:
                    show_pct = float(show_pct)
                except Exception:
                    show_pct = 0.0
                sign = "+" if show_pct >= 0 else ""
                lines.append(
                    f"- {idx['index_name']}: {show_price:,.2f} "
                    f"({sign}{show_pct:.1f}%) [{stamp}]"
                )
        lines.append("")

    # 2. 환율
    fx = get_fx()
    if fx:
        lines.append("### 환율")
        for code in ["USDKRW", "JPYKRW", "EURKRW"]:
            f = fx.get(code)
            if f or (code == "USDKRW" and rt_fx):
                show_rate = f["close_rate"] if f else 0.0
                show_pct = f["change_pct"] if f else 0.0
                stamp = f.get("latest_date", "-") if f else "-"
                if code == "USDKRW" and rt_fx:
                    show_rate = rt_fx.get("price", show_rate)
                    show_pct = rt_fx.get("change_pct", show_pct)
                    stamp = rt_fx.get("observed_at", "RT")
                try:
                    show_rate = float(show_rate)
                except Exception:
                    show_rate = 0.0
                try:
                    show_pct = float(show_pct)
                except Exception:
                    show_pct = 0.0
                sign = "+" if show_pct >= 0 else ""
                lines.append(
                    f"- {code}: {show_rate:,.2f} ({sign}{show_pct:.2f}%) [{stamp}]"
                )
        lines.append("")

    # 3. 금리
    rates = get_rates()
    if rates:
        lines.append("### 금리")
        for code in ["BOK_BASE", "KR_CD91", "KR_TB3Y", "KR_TB10Y"]:
            r = rates.get(code)
            if r:
                lines.append(f"- {r['rate_name']}: {r['rate_value']:.2f}%")
        lines.append("")

    # 4. 원자재
    commodities = get_commodities()
    if commodities:
        lines.append("### 원자재")
        for code in ["WTI", "GOLD", "COPPER"]:
            c = commodities.get(code)
            if c:
                sign = "+" if c["change_pct"] >= 0 else ""
                lines.append(
                    f"- {c['commodity_name']}: ${c['close_price']:,.2f} "
                    f"({sign}{c['change_pct']:.1f}%)"
                )
        lines.append("")

    # 5. 투자자 동향
    flows = get_investor_flow()
    if flows:
        lines.append("### 투자자 수급 (순매수, 백만원)")
        # 최신 날짜만
        latest = flows[0]["date"] if flows else ""
        for f in flows:
            if f["date"] == latest:
                sign = "+" if f["net_amount"] >= 0 else ""
                inv_kr = {"foreign": "외국인", "institution": "기관", "individual": "개인"}
                lines.append(
                    f"- {f['market']} {inv_kr.get(f['investor_type'], f['investor_type'])}: "
                    f"{sign}{f['net_amount']:,}"
                )
        lines.append("")

    # 6. 뉴스 감성
    sentiment = get_news_sentiment()
    if sentiment:
        total = sum(s.get("cnt", 0) for s in sentiment.values())
        pos = sentiment.get("positive", {}).get("cnt", 0)
        neg = sentiment.get("negative", {}).get("cnt", 0)
        lines.append("### 뉴스 센티먼트 (24h)")
        if total > 0:
            lines.append(
                f"- 긍정 {pos}건({pos/total*100:.0f}%) / "
                f"부정 {neg}건({neg/total*100:.0f}%) / "
                f"중립 {total-pos-neg}건"
            )
        lines.append("")

    # 7. 주요 뉴스
    news = get_important_news(10)
    if news:
        lines.append("### 주요 뉴스 (importance ≥ 3)")
        for n in news:
            imp = "★" * n["importance"]
            emoji = {"positive": "📈", "negative": "📉", "neutral": "➡️"}
            tickers = ", ".join(n.get("tickers", []))
            ticker_str = f" [{tickers}]" if tickers else ""
            lines.append(
                f"- {emoji.get(n['sentiment'], '?')} {imp} {n['summary']}{ticker_str}"
            )
        lines.append("")

    return "\n".join(lines)


def generate_json():
    """JSON 형식 (API 연동용)"""
    rt_indices = fetch_naver_realtime_indices(timeout_sec=8)
    rt_fx = fetch_naver_usdkrw(timeout_sec=8)
    return {
        "generated_at": datetime.now().isoformat(),
        "indices": get_indices(),
        "exchange_rates": get_fx(),
        "realtime": {
            "indices": rt_indices,
            "usdkrw": rt_fx,
        },
        "interest_rates": get_rates(),
        "commodities": get_commodities(),
        "investor_flow": get_investor_flow(),
        "news_sentiment": get_news_sentiment(),
        "important_news": get_important_news(10),
    }


def main():
    if "--json" in sys.argv:
        print(json.dumps(generate_json(), ensure_ascii=False, indent=2))
    elif "--save" in sys.argv:
        briefing = generate_briefing()
        filepath = os.path.expanduser("~/.openclaw/data/market_briefing.md")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(briefing)
        print(f"저장: {filepath}")
    else:
        print(generate_briefing())


if __name__ == "__main__":
    main()
