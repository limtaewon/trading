#!/usr/bin/env python3
"""sync_normalized_flow_daily.py

feature_snapshot 기반 임시 수급 값을
Decision Operating Spec용 일별 정규화 테이블로 동기화한다.

생성/갱신 대상:
- trading.stock_flow_daily
- trading.market_flow_daily

선택 동기화:
- trading.investor_flow (legacy 호환)
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import sys
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen


def _log(msg: str) -> None:
    print(f"{dt.datetime.now().strftime('%H:%M:%S')} [flow-sync] {msg}", flush=True)


def _ch_url_and_headers() -> tuple[str, dict[str, str]]:
    host = os.getenv("CLICKHOUSE_HOST", "").strip()
    user = os.getenv("CLICKHOUSE_USER", "").strip()
    pw = os.getenv("CLICKHOUSE_PASS", "").strip()
    headers: dict[str, str] = {"Content-Type": "text/plain; charset=utf-8"}

    if host:
        if not user:
            user = "default"
        if not pw:
            pw = "trading"
        sep = "&" if "?" in host else "?"
        return f"{host}{sep}user={user}&password={pw}", headers

    url = os.getenv("CLICKHOUSE_URL", "http://localhost:8123").strip()
    sp = urlsplit(url)
    if sp.username is not None:
        auth = f"{sp.username}:{sp.password or ''}".encode("utf-8")
        headers["Authorization"] = "Basic " + base64.b64encode(auth).decode("ascii")
        netloc = sp.hostname or "localhost"
        if sp.port:
            netloc = f"{netloc}:{sp.port}"
        clean = urlunsplit((sp.scheme or "http", netloc, sp.path or "", sp.query, sp.fragment))
        return clean, headers

    return url, headers


def ch_execute(sql: str, timeout_sec: int = 60) -> None:
    url, headers = _ch_url_and_headers()
    req = Request(url, data=sql.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        _ = r.read()


def ch_scalar(sql: str, timeout_sec: int = 30) -> str:
    url, headers = _ch_url_and_headers()
    q = sql.strip() + "\nFORMAT TSV"
    req = Request(url, data=q.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        return r.read().decode("utf-8", errors="replace").strip()


def table_exists(name: str) -> bool:
    safe = name.replace("'", "\\'")
    q = (
        "SELECT count() "
        "FROM system.tables "
        "WHERE database = 'trading' "
        f"AND name = '{safe}'"
    )
    try:
        return int(ch_scalar(q) or "0") > 0
    except Exception:
        return False


def ensure_tables() -> None:
    ch_execute(
        """
CREATE TABLE IF NOT EXISTS trading.stock_flow_daily
(
    trade_date             Date,
    ticker                 String,
    market                 LowCardinality(String),
    investor_type          LowCardinality(String),
    net_buy_shares         Float64,
    net_buy_value_krw      Float64,
    foreign_ownership_pct  Float64 DEFAULT 0,
    traded_value_krw       Float64 DEFAULT 0,
    net_buy_pct_turnover   Float64 DEFAULT 0,
    source_session         LowCardinality(String) DEFAULT '',
    ingested_at            DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (trade_date, ticker, investor_type)
"""
    )
    ch_execute(
        """
CREATE TABLE IF NOT EXISTS trading.market_flow_daily
(
    trade_date               Date,
    market                   LowCardinality(String),
    investor_type            LowCardinality(String),
    net_buy_value_krw        Float64,
    market_traded_value_krw  Float64,
    net_buy_pct_turnover     Float64,
    n_tickers                UInt32,
    ingested_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (trade_date, market, investor_type)
"""
    )


def sync_stock_flow(days: int, sessions: list[str]) -> int:
    session_filter = ",".join("'" + s.replace("'", "\\'") + "'" for s in sessions if s)
    where_session = f"AND session IN ({session_filter})" if session_filter else ""
    ch_execute(f"DELETE FROM trading.stock_flow_daily WHERE trade_date >= today() - {int(days)}")
    sql = f"""
INSERT INTO trading.stock_flow_daily
(
    trade_date,
    ticker,
    market,
    investor_type,
    net_buy_shares,
    net_buy_value_krw,
    foreign_ownership_pct,
    traded_value_krw,
    net_buy_pct_turnover,
    source_session,
    ingested_at
)
WITH
base AS (
    SELECT
        toDate(ts) AS trade_date,
        symbol AS ticker,
        argMax(price, ts) AS close_price,
        greatest(
            max(toFloat64(liquidity_krw)),
            abs(sum(toFloat64(news_event_score))) * argMax(price, ts) * 5,
            abs(sum(toFloat64(inst_flow))) * argMax(price, ts) * 5
        ) AS traded_value_krw,
        argMax(session, ts) AS source_session,
        avg(toFloat64(foreign_flow)) AS foreign_ownership_pct,
        sum(toFloat64(news_event_score)) AS foreign_net_shares,
        sum(toFloat64(inst_flow)) AS inst_net_shares
    FROM trading.feature_snapshot
    WHERE ts >= now() - INTERVAL {int(days)} DAY
      AND match(symbol, '^[0-9]{{6}}$')
      {where_session}
    GROUP BY trade_date, ticker
),
tmap AS (
    SELECT
        ticker,
        argMax(market, date) AS market
    FROM trading.technical_signals
    WHERE date >= today() - 365
    GROUP BY ticker
)
SELECT
    b.trade_date,
    b.ticker,
    ifNull(t.market, 'UNKNOWN') AS market,
    'FOREIGN' AS investor_type,
    b.foreign_net_shares AS net_buy_shares,
    b.foreign_net_shares * b.close_price AS net_buy_value_krw,
    b.foreign_ownership_pct AS foreign_ownership_pct,
    b.traded_value_krw AS traded_value_krw,
    if(
        b.traded_value_krw > 0,
        ((b.foreign_net_shares * b.close_price) / b.traded_value_krw) * 100,
        0
    ) AS net_buy_pct_turnover,
    b.source_session AS source_session,
    now() AS ingested_at
FROM base b
LEFT JOIN tmap t ON t.ticker = b.ticker
UNION ALL
SELECT
    b.trade_date,
    b.ticker,
    ifNull(t.market, 'UNKNOWN') AS market,
    'INST' AS investor_type,
    b.inst_net_shares AS net_buy_shares,
    b.inst_net_shares * b.close_price AS net_buy_value_krw,
    0.0 AS foreign_ownership_pct,
    b.traded_value_krw AS traded_value_krw,
    if(
        b.traded_value_krw > 0,
        ((b.inst_net_shares * b.close_price) / b.traded_value_krw) * 100,
        0
    ) AS net_buy_pct_turnover,
    b.source_session AS source_session,
    now() AS ingested_at
FROM base b
LEFT JOIN tmap t ON t.ticker = b.ticker
"""
    ch_execute(sql, timeout_sec=180)
    n = int(ch_scalar(f"SELECT count() FROM trading.stock_flow_daily WHERE trade_date >= today() - {int(days)}") or "0")
    return n


def sync_market_flow_from_investor_flow(days: int) -> int:
    if not table_exists("investor_flow"):
        return 0
    rows = int(
        ch_scalar(
            f"""
SELECT count()
FROM trading.investor_flow
WHERE date >= today() - {int(days)}
"""
        )
        or "0"
    )
    if rows <= 0:
        return 0

    ch_execute(f"DELETE FROM trading.market_flow_daily WHERE trade_date >= today() - {int(days)}")
    sql = f"""
INSERT INTO trading.market_flow_daily
(
    trade_date,
    market,
    investor_type,
    net_buy_value_krw,
    market_traded_value_krw,
    net_buy_pct_turnover,
    n_tickers,
    ingested_at
)
WITH
base AS (
    SELECT
        date AS trade_date,
        market,
        multiIf(
            lower(investor_type) = 'foreign', 'FOREIGN',
            lower(investor_type) = 'institution', 'INST',
            lower(investor_type) = 'individual', 'RETAIL',
            upper(investor_type)
        ) AS investor_type,
        toFloat64(net_amount) * 1000000.0 AS net_buy_value_krw
    FROM trading.investor_flow
    WHERE date >= today() - {int(days)}
),
traded AS (
    SELECT
        trade_date,
        market,
        sum(traded_value_krw) AS traded_value_krw,
        toUInt32(uniqExact(ticker)) AS n_tickers
    FROM trading.stock_flow_daily
    WHERE trade_date >= today() - {int(days)}
      AND source_session = 'REGULAR'
    GROUP BY trade_date, market
)
SELECT
    b.trade_date AS trade_date,
    b.market AS market,
    b.investor_type AS investor_type,
    b.net_buy_value_krw AS net_buy_value_krw,
    ifNull(t.traded_value_krw, 0.0) AS market_traded_value_krw,
    if(ifNull(t.traded_value_krw, 0.0) > 0, (b.net_buy_value_krw / t.traded_value_krw) * 100, 0.0) AS net_buy_pct_turnover,
    ifNull(t.n_tickers, toUInt32(0)) AS n_tickers,
    now() AS ingested_at
FROM base b
LEFT JOIN traded t ON t.trade_date = b.trade_date AND t.market = b.market
UNION ALL
SELECT
    b.trade_date AS trade_date,
    'ALL' AS market,
    b.investor_type AS investor_type,
    sum(b.net_buy_value_krw) AS net_buy_value_krw,
    sum(ifNull(t.traded_value_krw, 0.0)) AS market_traded_value_krw,
    if(sum(ifNull(t.traded_value_krw, 0.0)) > 0, (sum(b.net_buy_value_krw) / sum(ifNull(t.traded_value_krw, 0.0))) * 100, 0.0) AS net_buy_pct_turnover,
    toUInt32(sum(ifNull(t.n_tickers, 0))) AS n_tickers,
    now() AS ingested_at
FROM base b
LEFT JOIN traded t ON t.trade_date = b.trade_date AND t.market = b.market
GROUP BY b.trade_date, b.investor_type
"""
    ch_execute(sql, timeout_sec=180)
    n = int(ch_scalar(f"SELECT count() FROM trading.market_flow_daily WHERE trade_date >= today() - {int(days)}") or "0")
    return n


def sync_market_flow(days: int) -> int:
    # 1순위: 공식 시장 수급(investor_flow)
    n_official = sync_market_flow_from_investor_flow(days)
    if n_official > 0:
        return n_official

    # 2순위: 종목 수급 합산(stock_flow_daily)
    ch_execute(f"DELETE FROM trading.market_flow_daily WHERE trade_date >= today() - {int(days)}")
    sql = f"""
INSERT INTO trading.market_flow_daily
(
    trade_date,
    market,
    investor_type,
    net_buy_value_krw,
    market_traded_value_krw,
    net_buy_pct_turnover,
    n_tickers,
    ingested_at
)
SELECT
    trade_date,
    market,
    investor_type,
    sum(s.net_buy_value_krw) AS net_buy_value_krw,
    sum(s.traded_value_krw) AS market_traded_value_krw,
    if(sum(s.traded_value_krw) > 0, (sum(s.net_buy_value_krw) / sum(s.traded_value_krw)) * 100, 0) AS net_buy_pct_turnover,
    toUInt32(uniqExact(ticker)) AS n_tickers,
    now() AS ingested_at
FROM trading.stock_flow_daily AS s
WHERE trade_date >= today() - {int(days)}
GROUP BY trade_date, market, investor_type
UNION ALL
SELECT
    trade_date,
    'ALL' AS market,
    investor_type,
    sum(s.net_buy_value_krw) AS net_buy_value_krw,
    sum(s.traded_value_krw) AS market_traded_value_krw,
    if(sum(s.traded_value_krw) > 0, (sum(s.net_buy_value_krw) / sum(s.traded_value_krw)) * 100, 0) AS net_buy_pct_turnover,
    toUInt32(uniqExact(ticker)) AS n_tickers,
    now() AS ingested_at
FROM trading.stock_flow_daily AS s
WHERE trade_date >= today() - {int(days)}
GROUP BY trade_date, investor_type
"""
    ch_execute(sql, timeout_sec=180)
    n = int(ch_scalar(f"SELECT count() FROM trading.market_flow_daily WHERE trade_date >= today() - {int(days)}") or "0")
    return n


def sync_legacy_investor_flow(days: int) -> int:
    if not table_exists("investor_flow"):
        return 0
    ch_execute(
        f"""
DELETE FROM trading.investor_flow
WHERE date >= today() - {int(days)}
  AND investor_type IN ('foreign', 'institution', 'individual')
"""
    )
    ch_execute(
        f"""
INSERT INTO trading.investor_flow
(
    date,
    collected_at,
    market,
    investor_type,
    buy_amount,
    sell_amount,
    net_amount
)
SELECT
    trade_date AS date,
    now() AS collected_at,
    market,
    multiIf(
        investor_type = 'FOREIGN', 'foreign',
        investor_type = 'INST', 'institution',
        investor_type = 'RETAIL', 'individual',
        lower(investor_type)
    ) AS investor_type,
    toInt64(0) AS buy_amount,
    toInt64(0) AS sell_amount,
    toInt64(round(net_buy_value_krw / 1000000.0, 0)) AS net_amount
FROM trading.market_flow_daily
WHERE trade_date >= today() - {int(days)}
  AND market IN ('KOSPI', 'KOSDAQ')
  AND investor_type IN ('FOREIGN', 'INST', 'RETAIL')
"""
    )
    n = int(
        ch_scalar(
            f"""
SELECT count()
FROM trading.investor_flow
WHERE date >= today() - {int(days)}
  AND investor_type IN ('foreign', 'institution', 'individual')
"""
        )
        or "0"
    )
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14, help="재적재 lookback 일수")
    ap.add_argument(
        "--sync-legacy-investor-flow",
        action="store_true",
        help="legacy trading.investor_flow를 함께 갱신",
    )
    ap.add_argument(
        "--sessions",
        default=os.getenv("STOCK_FLOW_SOURCE_SESSIONS", "REGULAR"),
        help="feature_snapshot에서 집계할 세션 CSV (기본: REGULAR)",
    )
    args = ap.parse_args()

    days = max(1, int(args.days))
    sessions = [s.strip().upper() for s in str(args.sessions or "").split(",") if s.strip()]
    if not sessions:
        sessions = ["REGULAR"]

    if not table_exists("feature_snapshot"):
        _log("feature_snapshot 테이블이 없어 중단")
        return 1

    try:
        ensure_tables()
        _log(f"정규화 수급 동기화 시작 (days={days}, sessions={','.join(sessions)})")
        stock_rows = sync_stock_flow(days, sessions=sessions)
        _log(f"stock_flow_daily rows={stock_rows}")
        market_rows = sync_market_flow(days)
        _log(f"market_flow_daily rows={market_rows}")
        legacy_rows = 0
        if args.sync_legacy_investor_flow:
            legacy_rows = sync_legacy_investor_flow(days)
            _log(f"investor_flow rows={legacy_rows} (legacy)")
        summary = {
            "ok": True,
            "days": days,
            "stock_flow_rows": stock_rows,
            "market_flow_rows": market_rows,
            "legacy_rows": legacy_rows,
        }
        print(json.dumps(summary, ensure_ascii=False), flush=True)
        return 0
    except Exception as e:
        _log(f"동기화 실패: {e}")
        print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False), flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
