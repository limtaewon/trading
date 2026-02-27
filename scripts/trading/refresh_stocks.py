#!/usr/bin/env python3
"""refresh_stocks.py - STOCKS.csv 및 krx_stocks.json 주간 갱신

KRX 전체 종목 마스터를 최신 상태로 유지한다.
신규 상장, 상폐, 종목명 변경을 반영하여
테마주 등 모든 종목을 빠짐없이 커버한다.

사용법:
  python3 ~/.openclaw/scripts/trading/refresh_stocks.py

크론 (매주 월요일 06:00):
  0 6 * * 1 python3 ~/.openclaw/scripts/trading/refresh_stocks.py >> ~/.openclaw/logs/refresh_stocks.log 2>&1
"""
from __future__ import annotations

import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path

WORKSPACE_CSV = Path.home() / ".openclaw" / "workspace" / "STOCKS.csv"
CACHE_JSON = Path.home() / ".openclaw" / "data" / "krx_stocks.json"


def fetch_krx_json_api():
    """KRX 정보데이터시스템 JSON API로 전체 종목 다운로드"""
    print("  KRX JSON API 시도...")
    try:
        import requests
        use_requests = True
    except ImportError:
        use_requests = False

    stocks = {}
    market_map = {}

    for mkt_id, label, market_name in [("STK", "KOSPI", "KOSPI"), ("KSQ", "KOSDAQ", "KOSDAQ")]:
        try:
            url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
            params = {
                "bld": "dbms/MDC/STAT/standard/MDCSTAT01901",
                "locale": "ko_KR",
                "mktId": mkt_id,
                "share": "1",
                "csvxls_isNo": "false",
            }
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader",
            }

            if use_requests:
                resp = requests.post(url, data=params, headers=headers, timeout=30)
                result = resp.json()
            else:
                from urllib.request import Request, urlopen
                encoded = "&".join(f"{k}={v}" for k, v in params.items()).encode()
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                req = Request(url, data=encoded, headers=headers)
                with urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode("utf-8"))

            items = result.get("OutBlock_1", [])
            count = 0
            for item in items:
                code = item.get("ISU_SRT_CD", "").strip()
                name = item.get("ISU_ABBRV", "").strip()
                if code and name and len(code) == 6 and code.isdigit():
                    stocks[name] = code
                    market_map[code] = market_name
                    count += 1
            print(f"    {label}: {count}종목")
        except Exception as e:
            print(f"    {label}: 실패 ({e})")

    return stocks, market_map


def fetch_financedatareader():
    """FinanceDataReader로 전체 종목 다운로드"""
    print("  FinanceDataReader 시도...")
    try:
        import FinanceDataReader as fdr
    except ImportError:
        print("    미설치")
        return {}, {}

    stocks = {}
    market_map = {}
    try:
        df = fdr.StockListing("KRX")
        for _, row in df.iterrows():
            code = str(row.get("Code", "")).strip()
            name = str(row.get("Name", "")).strip()
            market = str(row.get("Market", "")).strip()
            if code and name and len(code) == 6 and code.isdigit():
                stocks[name] = code
                market_map[code] = market or "KOSPI"
        print(f"    로드: {len(stocks)}종목")
    except Exception as e:
        print(f"    실패: {e}")

    return stocks, market_map


def fetch_pykrx():
    """pykrx로 전체 종목 다운로드"""
    print("  pykrx 시도...")
    try:
        from pykrx import stock
    except ImportError:
        print("    미설치")
        return {}, {}

    from datetime import timedelta
    stocks = {}
    market_map = {}

    for delta in range(0, 14):
        dt = (datetime.now() - timedelta(days=delta)).strftime("%Y%m%d")
        try:
            for mkt, label in [("KOSPI", "KOSPI"), ("KOSDAQ", "KOSDAQ")]:
                tickers = stock.get_market_ticker_list(dt, market=mkt)
                for t in tickers:
                    name = stock.get_market_ticker_name(t)
                    if name:
                        stocks[name] = t
                        market_map[t] = label
            if len(stocks) > 100:
                print(f"    날짜 {dt}: {len(stocks)}종목")
                return stocks, market_map
        except Exception:
            continue

    print("    실패")
    return stocks, market_map


def save_csv(stocks, market_map):
    """STOCKS.csv 저장"""
    WORKSPACE_CSV.parent.mkdir(parents=True, exist_ok=True)

    # 기존 market 정보 보존
    existing_markets = {}
    if WORKSPACE_CSV.exists():
        try:
            with open(WORKSPACE_CSV, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = row.get("Code", "").strip()
                    market = row.get("Market", "").strip()
                    if code and market:
                        existing_markets[code] = market
        except Exception:
            pass

    with open(WORKSPACE_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Code", "Name", "Market"])
        for name, code in sorted(stocks.items(), key=lambda x: x[1]):
            market = market_map.get(code) or existing_markets.get(code, "KOSPI")
            writer.writerow([code, name, market])

    print(f"  STOCKS.csv 저장: {len(stocks)}종목 → {WORKSPACE_CSV}")


def save_json(stocks):
    """krx_stocks.json 저장"""
    CACHE_JSON.parent.mkdir(parents=True, exist_ok=True)

    if CACHE_JSON.exists():
        backup = str(CACHE_JSON) + ".bak"
        os.rename(CACHE_JSON, backup)

    data = {
        "updated": datetime.now().isoformat(),
        "count": len(stocks),
        "stocks": stocks,
    }
    with open(CACHE_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"  krx_stocks.json 저장: {len(stocks)}종목 → {CACHE_JSON}")


def verify(stocks):
    """주요 종목 검증"""
    checks = {
        "삼성전자": "005930", "SK하이닉스": "000660",
        "현대차": "005380", "NAVER": "035420",
        "카카오": "035720", "기아": "000270",
    }
    ok = 0
    for name, expected in checks.items():
        if stocks.get(name) == expected:
            ok += 1
    print(f"  검증: {ok}/{len(checks)} 일치")
    return ok == len(checks)


def main():
    print("=" * 60)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] KRX 종목 마스터 갱신")
    print("=" * 60)

    stocks = {}
    market_map = {}
    threshold = 1000

    # 1순위: KRX JSON API (가장 정확, 실시간)
    stocks, market_map = fetch_krx_json_api()

    # 2순위: FinanceDataReader
    if len(stocks) < threshold:
        s, m = fetch_financedatareader()
        if len(s) > len(stocks):
            stocks, market_map = s, m

    # 3순위: pykrx
    if len(stocks) < threshold:
        s, m = fetch_pykrx()
        if len(s) > len(stocks):
            stocks, market_map = s, m

    # 4순위: 기존 STOCKS.csv 유지 (갱신 불가 시)
    if len(stocks) < threshold:
        print(f"\n  ⚠️  갱신 실패 ({len(stocks)}종목). 기존 STOCKS.csv 유지.")
        print("=" * 60)
        return

    # 저장
    save_csv(stocks, market_map)
    save_json(stocks)
    verify(stocks)

    print("=" * 60)


if __name__ == "__main__":
    main()
