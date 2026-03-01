#!/usr/bin/env python3
"""Backfill stock_flow_daily / market_flow_daily from technical_signals proxy.

When historical investor-flow sources are unavailable, this creates deterministic
proxy flow values so Stage2 can be replayed in strict backtests.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen
from urllib.error import HTTPError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()


def _ch_url_and_headers() -> tuple[str, dict[str, str]]:
    host = os.getenv("CLICKHOUSE_HOST", "").strip()
    user = os.getenv("CLICKHOUSE_USER", "").strip()
    pw = os.getenv("CLICKHOUSE_PASS", "").strip()
    headers: dict[str, str] = {"Content-Type": "text/plain; charset=utf-8"}
    if host:
        if not user:
            user = "default"
        if not pw:
            pw = os.getenv("CLICKHOUSE_PASSWORD", "trading")
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


def ch_execute(sql: str, timeout_sec: int = 180) -> None:
    url, headers = _ch_url_and_headers()
    req = Request(url, data=sql.encode("utf-8"), headers=headers, method="POST")
    try:
        with urlopen(req, timeout=timeout_sec) as r:
            _ = r.read()
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse HTTP {e.code}: {body}") from e


def ch_scalar(sql: str, timeout_sec: int = 60) -> str:
    url, headers = _ch_url_and_headers()
    q = sql.strip() + "\nFORMAT TSV"
    req = Request(url, data=q.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        return r.read().decode("utf-8", errors="replace").strip()


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill proxy flow tables from technical_signals")
    ap.add_argument("--start-date", default="2025-01-01")
    ap.add_argument("--end-date", default="2025-12-31")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    s = args.start_date
    e = args.end_date

    if args.overwrite:
        ch_execute(f"ALTER TABLE trading.stock_flow_daily DELETE WHERE trade_date >= '{s}' AND trade_date <= '{e}'")
        ch_execute(f"ALTER TABLE trading.market_flow_daily DELETE WHERE trade_date >= '{s}' AND trade_date <= '{e}'")

    # Stock-level proxy:
    # - traded_value_krw: close * volume
    # - flow pct: deterministic transform of signal_score (clamped)
    ch_execute(
        f"""
INSERT INTO trading.stock_flow_daily
(
  trade_date, ticker, market, investor_type,
  net_buy_shares, net_buy_value_krw, foreign_ownership_pct,
  traded_value_krw, net_buy_pct_turnover, source_session, ingested_at
)
SELECT
  date AS trade_date,
  ticker,
  market,
  investor_type,
  net_buy_shares,
  net_buy_value_krw,
  0.0 AS foreign_ownership_pct,
  traded_value_krw,
  if(traded_value_krw > 0, 100.0 * net_buy_value_krw / traded_value_krw, 0.0) AS net_buy_pct_turnover,
  'REGULAR' AS source_session,
  now() AS ingested_at
FROM
(
  SELECT
    date,
    ticker,
    market,
    close_price,
    volume,
    signal_score,
    greatest(toFloat64(close_price) * toFloat64(volume), 0.0) AS traded_value_krw,
    arrayJoin(['FOREIGN','INST']) AS investor_type,
    multiIf(
      investor_type='FOREIGN', greatest(-4.5, least(4.5, toFloat64(signal_score) * 0.9)),
      investor_type='INST',    greatest(-3.0, least(3.0, toFloat64(signal_score) * 0.6)),
      0.0
    ) AS flow_pct,
    traded_value_krw * flow_pct / 100.0 AS net_buy_value_krw,
    if(toFloat64(close_price) > 0, (traded_value_krw * flow_pct / 100.0) / toFloat64(close_price), 0.0) AS net_buy_shares
  FROM trading.technical_signals
  WHERE date >= '{s}' AND date <= '{e}'
    AND match(ticker, '^[0-9]{{6}}$')
    AND toFloat64(close_price) > 0
)
"""
    )

    # Market-level proxy derived from stock-level aggregates.
    ch_execute(
        f"""
INSERT INTO trading.market_flow_daily
(
  trade_date, market, investor_type, net_buy_value_krw,
  market_traded_value_krw, net_buy_pct_turnover, n_tickers, ingested_at,
  market_traded_value_krw_source, market_traded_value_krw_universe_n
)
SELECT
  trade_date,
  market,
  investor_type,
  net_buy_value_krw,
  market_traded_value_krw,
  if(market_traded_value_krw > 0, 100.0 * net_buy_value_krw / market_traded_value_krw, 0.0) AS net_buy_pct_turnover,
  n_tickers,
  now() AS ingested_at,
  'MARKET_TOTAL' AS market_traded_value_krw_source,
  n_tickers AS market_traded_value_krw_universe_n
FROM
(
  SELECT
    trade_date,
    market,
    investor_type,
    sum(net_buy_value_krw) AS net_buy_value_krw,
    sum(traded_value_krw) AS market_traded_value_krw,
    toUInt32(uniqExact(ticker)) AS n_tickers
  FROM trading.stock_flow_daily
  WHERE trade_date >= '{s}' AND trade_date <= '{e}'
  GROUP BY trade_date, market, investor_type
)
"""
    )

    ch_execute(
        f"""
INSERT INTO trading.market_flow_daily
(
  trade_date, market, investor_type, net_buy_value_krw,
  market_traded_value_krw, net_buy_pct_turnover, n_tickers, ingested_at,
  market_traded_value_krw_source, market_traded_value_krw_universe_n
)
SELECT
  trade_date,
  'ALL' AS market,
  investor_type,
  net_buy_value_krw,
  market_traded_value_krw,
  if(market_traded_value_krw > 0, 100.0 * net_buy_value_krw / market_traded_value_krw, 0.0) AS net_buy_pct_turnover,
  n_tickers,
  now() AS ingested_at,
  'MARKET_TOTAL' AS market_traded_value_krw_source,
  n_tickers AS market_traded_value_krw_universe_n
FROM
(
  SELECT
    trade_date,
    investor_type,
    sum(net_buy_value_krw) AS net_buy_value_krw,
    sum(traded_value_krw) AS market_traded_value_krw,
    toUInt32(uniqExact(ticker)) AS n_tickers
  FROM trading.stock_flow_daily
  WHERE trade_date >= '{s}' AND trade_date <= '{e}'
  GROUP BY trade_date, investor_type
)
"""
    )

    c_stock = ch_scalar(
        f"SELECT count() FROM trading.stock_flow_daily WHERE trade_date >= '{s}' AND trade_date <= '{e}'"
    )
    c_market = ch_scalar(
        f"SELECT count() FROM trading.market_flow_daily WHERE trade_date >= '{s}' AND trade_date <= '{e}'"
    )
    print(
        json.dumps(
            {
                "ok": True,
                "start_date": s,
                "end_date": e,
                "stock_flow_rows": int(float(c_stock or "0")),
                "market_flow_rows": int(float(c_market or "0")),
                "source": "PROXY_FROM_TECHNICAL_SIGNALS",
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
