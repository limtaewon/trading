#!/usr/bin/env python3
"""morning_briefing.py - 매일 아침 08:00 시장 브리핑 → 텔레그램 전송

시장 레짐, 주요 지수, 매수/매도 후보, 뉴스 핵심을 텔레그램으로 보낸다.

크론: 0 8 * * 1-5 /usr/bin/python3 /Users/imtaewon/.openclaw/scripts/trading/morning_briefing.py >> /Users/imtaewon/.openclaw/logs/briefing.log 2>&1
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()
try:
    import requests
except ImportError:
    from _requests_compat import requests

from market_realtime import fetch_naver_realtime_indices, fetch_naver_usdkrw

CH_HOST = os.environ.get("CLICKHOUSE_HOST", "http://localhost:8123").strip()
CH_USER = os.environ.get("CLICKHOUSE_USER", "default").strip()
CH_PASSWORD = os.environ.get("CLICKHOUSE_PASS", os.environ.get("CLICKHOUSE_PASSWORD", "")).strip()

# 텔레그램 설정
TG_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()


def q(sql):
    try:
        params = {"query": sql, "default_format": "JSONEachRow"}
        if CH_USER:
            params["user"] = CH_USER
        if CH_PASSWORD:
            params["password"] = CH_PASSWORD
        r = requests.get(CH_HOST, params=params, timeout=10)
        r.raise_for_status()
        lines = [l for l in r.text.strip().splitlines() if l.strip()]
        return [json.loads(l) for l in lines]
    except Exception:
        return []


def send_telegram(text):
    """텔레그램으로 메시지 전송 (4096자 제한 처리)"""
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("  텔레그램 설정 미완료(TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID) - 전송 스킵")
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    # 텔레그램 메시지 최대 4096자, 넘으면 분할
    chunks = []
    while len(text) > 4000:
        # 줄바꿈 기준으로 분할
        cut = text.rfind("\n", 0, 4000)
        if cut == -1:
            cut = 4000
        chunks.append(text[:cut])
        text = text[cut:]
    chunks.append(text)

    for chunk in chunks:
        try:
            resp = requests.post(url, json={
                "chat_id": TG_CHAT_ID,
                "text": chunk,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }, timeout=10)
            if resp.status_code != 200:
                print(f"  텔레그램 전송 실패: {resp.status_code} {resp.text[:100]}")
        except Exception as e:
            print(f"  텔레그램 전송 오류: {e}")


def main():
    now = datetime.now()
    rt_indices = fetch_naver_realtime_indices(timeout_sec=8)
    rt_usdkrw = fetch_naver_usdkrw(timeout_sec=8)
    lines = []  # 텔레그램용 메시지 누적

    def p(text=""):
        print(text)
        lines.append(text)

    p(f"☀️ <b>모닝 브리핑</b> | {now.strftime('%Y-%m-%d %H:%M')}")
    p("")

    # ── 1. 시장 레짐 ──
    regime = q("SELECT * FROM trading.market_regime ORDER BY classified_at DESC LIMIT 1")
    if regime:
        r = regime[0]
        regime_label = r.get('regime', '?')
        emoji = {"BULL_CALM": "🟢🌤", "BULL_VOL": "🟢⚡", "BEAR_CALM": "🔴🌤",
                 "BEAR_VOL": "🔴⚡", "SIDEWAYS": "⚪🔄"}.get(regime_label, "❓")
        p(f"{emoji} <b>레짐: {regime_label}</b>")
        p(f"{r.get('summary', '?')}")
    else:
        regime_label = "UNKNOWN"
        p("❓ 레짐 데이터 없음")

    # ── 2. 주요 지수 ──
    indices = q("""
        SELECT index_code, close_price, change_pct
        FROM trading.market_index
        WHERE date = (SELECT max(date) FROM trading.market_index WHERE index_code = 'KOSPI')
          AND index_code IN ('KOSPI','KOSDAQ','SPX','NDX','VIX')
        ORDER BY index_code
    """)
    if indices:
        p("")
        p("📊 <b>주요 지수</b>")
        for idx in indices:
            code = idx.get("index_code", "")
            price = idx.get("close_price", 0)
            pct = idx.get("change_pct", 0)
            src = "DB"
            if code in ("KOSPI", "KOSDAQ") and code in rt_indices:
                rt = rt_indices.get(code, {})
                price = rt.get("price", price)
                pct = rt.get("change_pct", pct)
                src = "RT"
            try:
                price = float(price)
            except Exception:
                price = 0.0
            try:
                pct = float(pct)
            except Exception:
                pct = 0.0
            arrow = "▲" if pct > 0 else "▼" if pct < 0 else "─"
            sign = "+" if pct >= 0 else ""
            p(f"  {code:8s} {price:>,.0f} {arrow}{sign}{pct:.1f}% ({src})")

    # ── 3. 환율 ──
    fx = q("""
        SELECT currency_pair, close_rate, change_pct
        FROM trading.exchange_rate
        WHERE date = (SELECT max(date) FROM trading.exchange_rate)
          AND currency_pair = 'USDKRW'
        LIMIT 1
    """)
    if rt_usdkrw and rt_usdkrw.get("price") is not None:
        rate = float(rt_usdkrw.get("price") or 0)
        pct = float(rt_usdkrw.get("change_pct") or 0)
        p(f"  USD/KRW  {rate:>,.1f} ({'+' if pct >= 0 else ''}{pct:.2f}%) (RT)")
    elif fx:
        f = fx[0]
        pct = f.get('change_pct', 0)
        p(f"  USD/KRW  {f['close_rate']:>,.1f} ({'+' if pct >= 0 else ''}{pct:.2f}%) (DB)")

    # ── 4. 매수 후보 ──
    buy = q("""
        SELECT ticker, ticker_name, signal_score, rsi_14, volume_ratio, close_price
        FROM trading.technical_signals
        WHERE signal_score >= 1
        ORDER BY calculated_at DESC, signal_score DESC
        LIMIT 15
    """)

    # 뉴스 센티먼트
    news_sent = {}
    news_data = q("""
        SELECT
            arrayJoin(tickers) AS ticker,
            countIf(sentiment='positive') AS pos,
            countIf(sentiment='negative') AS neg,
            count() AS total
        FROM trading.news
        WHERE published_at > now() - INTERVAL 3 DAY AND importance >= 3
        GROUP BY ticker
        HAVING total >= 2
    """)
    for n in news_data:
        tk = n.get('ticker', '')
        pos = n.get('pos', 0)
        neg = n.get('neg', 0)
        if pos > neg * 2 and pos >= 2:
            news_sent[tk] = ("긍정", pos, neg)
        elif neg > pos:
            news_sent[tk] = ("부정", pos, neg)
        else:
            news_sent[tk] = ("중립", pos, neg)

    p("")
    p("🎯 <b>매수 후보</b>")
    if buy:
        for b in buy:
            tk = b.get('ticker', '')
            score = b.get('signal_score', 0)
            ns = news_sent.get(tk)
            if ns and ns[0] == "긍정" and score >= 2:
                star = "★"
            else:
                star = "  "
            news_str = f"뉴스{ns[0]}({ns[1]}/{ns[2]})" if ns else ""
            p(f"{star} {b.get('ticker_name','?')} {score:+.0f}점 RSI:{b.get('rsi_14',0):.0f} {b.get('close_price',0):,.0f}원 {news_str}")
        p("★ = 기술매수+뉴스긍정 = 최우선")
    else:
        p("  (매수 신호 종목 없음)")

    # ── 5. 매도 경고 ──
    sell = q("""
        SELECT ticker, ticker_name, signal_score, rsi_14
        FROM trading.technical_signals
        WHERE signal_score <= -2
        ORDER BY calculated_at DESC, signal_score ASC
        LIMIT 10
    """)
    if sell:
        p("")
        p("⚠️ <b>매도 경고</b>")
        for s in sell:
            p(f"  {s.get('ticker_name','?')} {s.get('signal_score',0):+.0f}점 RSI:{s.get('rsi_14',0):.0f}")

    # ── 6. 핫뉴스 ──
    hot = q("""
        SELECT title, importance, sentiment
        FROM trading.news
        WHERE importance >= 4 AND published_at > now() - INTERVAL 12 HOUR
        ORDER BY published_at DESC
        LIMIT 5
    """)
    if hot:
        p("")
        p("🔥 <b>핫뉴스</b>")
        for h in hot:
            emoji = "🟢" if h.get('sentiment') == 'positive' else "🔴" if h.get('sentiment') == 'negative' else "⚪"
            p(f"{emoji} {h.get('title','?')[:45]}")

    # ── 7. 전략 ──
    p("")
    p("📋 <b>오늘의 전략</b>")
    strategies = {
        "BULL_CALM": "적극적 매수 검토. ★ 종목 위주 진입",
        "BULL_VOL": "분할 매수. 변동성 높으니 몰빵 금지",
        "BEAR_CALM": "매수 자제. 현금 비중 확대",
        "BEAR_VOL": "🚨 신규 매수 금지! 방어 모드",
        "SIDEWAYS": "밴드 하단 매수, 상단 매도",
    }
    p(f"→ {strategies.get(regime_label, '데이터 확인 필요')}")

    top_buy = [b for b in (buy or []) if b.get('signal_score', 0) >= 2
               and news_sent.get(b.get('ticker', ''), ("",))[0] == "긍정"]
    if top_buy:
        names = ", ".join(b.get('ticker_name', '?') for b in top_buy[:3])
        p(f"→ 개별종목 우선: {names}")
        p(f"→ ETF 매수 불필요")
    elif buy:
        p("→ 매수 신호 있으나 뉴스 추가 확인 필요")
    else:
        if "BULL" in regime_label:
            p("→ 개별 후보 없음. ETF 소량 가능 (30% 이하)")
        else:
            p("→ 매수 후보 없음. 현금 보유")

    # ── 텔레그램 전송 ──
    msg = "\n".join(lines)
    print(f"\n--- 텔레그램 전송 ---")
    send_telegram(msg)
    print("--- 전송 완료 ---")


if __name__ == "__main__":
    main()
