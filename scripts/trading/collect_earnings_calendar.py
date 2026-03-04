#!/usr/bin/env python3
from __future__ import annotations

"""실적 이벤트 캘린더(Seed) 수집기.

현 단계에서는 DART 공시 + 뉴스 제목 기반으로 실적 관련 이벤트를 정규화해
trading.earnings_calendar에 적재한다.
"""

import argparse
import os
import re
from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import requests

from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()

EARN_RE = re.compile(r"(실적|잠정실적|영업이익|가이던스|실적발표)")


def _resolve_clickhouse() -> tuple[str, tuple[str, str] | None]:
    raw_url = (
        os.environ.get("CLICKHOUSE_URL", "").strip()
        or os.environ.get("CLICKHOUSE_HOST", "").strip()
        or "http://localhost:8123"
    )
    user = os.environ.get("CLICKHOUSE_USER", "").strip()
    pw = os.environ.get("CLICKHOUSE_PASS", os.environ.get("CLICKHOUSE_PASSWORD", "")).strip()
    sp = urlsplit(raw_url)
    q = dict(parse_qsl(sp.query, keep_blank_values=True))
    if not user:
        user = (sp.username or q.get("user") or "").strip()
    if not pw:
        pw = (sp.password or q.get("password") or "").strip()
    netloc = sp.hostname or "localhost"
    if sp.port:
        netloc = f"{netloc}:{sp.port}"
    raw_url = urlunsplit((sp.scheme or "http", netloc, "", "", ""))
    auth = (user, pw) if user else None
    return raw_url, auth


CH_URL, CH_AUTH = _resolve_clickhouse()
CH_DB = os.environ.get("CLICKHOUSE_DB", "trading").strip() or "trading"


def _sql_quote(v: Any) -> str:
    return "'" + str(v or "").replace("\\", "\\\\").replace("'", "\\'") + "'"


def ch_query(sql: str) -> list[dict[str, Any]]:
    resp = requests.post(
        CH_URL,
        params={"database": CH_DB, "default_format": "JSON"},
        data=(sql + "\n").encode("utf-8"),
        timeout=30,
        auth=CH_AUTH,
    )
    resp.raise_for_status()
    return (resp.json() or {}).get("data", [])


def ch_exec(sql: str) -> None:
    requests.post(
        CH_URL,
        params={"database": CH_DB},
        data=(sql + "\n").encode("utf-8"),
        timeout=30,
        auth=CH_AUTH,
    ).raise_for_status()


def ensure_table() -> None:
    ch_exec(
        """
CREATE TABLE IF NOT EXISTS trading.earnings_calendar
(
    event_date      Date,
    ticker          String,
    ticker_name     String,
    event_name      String,
    event_source    LowCardinality(String),
    importance      UInt8,
    sentiment_hint  LowCardinality(String),
    confidence      Float32,
    raw_ref         String,
    created_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (event_date, ticker, event_source)
"""
    )


def load_rows(days: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    dart_rows = ch_query(
        f"""
SELECT
  toDate(rcept_dt) AS event_date,
  stock_code AS ticker,
  corp_name AS ticker_name,
  report_nm,
  importance
FROM trading.dart_disclosure
WHERE rcept_dt >= today() - {max(1, int(days))}
  AND stock_code != ''
ORDER BY rcept_dt DESC
LIMIT 3000
"""
    )
    for r in dart_rows:
        title = str(r.get("report_nm") or "")
        if not EARN_RE.search(title):
            continue
        tk = str(r.get("ticker") or "").strip()
        if not re.fullmatch(r"\d{6}", tk):
            continue
        out.append(
            {
                "event_date": str(r.get("event_date") or ""),
                "ticker": tk,
                "ticker_name": str(r.get("ticker_name") or "").strip(),
                "event_name": title[:140],
                "event_source": "dart",
                "importance": int(r.get("importance") or 2),
                "sentiment_hint": "neutral",
                "confidence": 0.75,
                "raw_ref": title[:240],
            }
        )

    news_rows = ch_query(
        f"""
SELECT
  toDate(published_at) AS event_date,
  arrayJoin(tickers) AS ticker,
  title,
  importance,
  sentiment,
  source_url
FROM trading.news
WHERE published_at >= now() - INTERVAL {max(1, int(days))} DAY
  AND length(title) > 0
LIMIT 4000
"""
    )
    for r in news_rows:
        title = str(r.get("title") or "")
        if not EARN_RE.search(title):
            continue
        tk = str(r.get("ticker") or "").strip()
        if not re.fullmatch(r"\d{6}", tk):
            continue
        out.append(
            {
                "event_date": str(r.get("event_date") or ""),
                "ticker": tk,
                "ticker_name": "",
                "event_name": title[:140],
                "event_source": "news",
                "importance": int(r.get("importance") or 2),
                "sentiment_hint": str(r.get("sentiment") or "neutral"),
                "confidence": 0.45,
                "raw_ref": str(r.get("source_url") or "")[:240],
            }
        )
    return out


def insert_rows(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    values: list[str] = []
    for r in rows:
        values.append(
            "("
            f"toDate({_sql_quote(r['event_date'])}),"
            f"{_sql_quote(r['ticker'])},"
            f"{_sql_quote(r['ticker_name'])},"
            f"{_sql_quote(r['event_name'])},"
            f"{_sql_quote(r['event_source'])},"
            f"{int(r['importance'])},"
            f"{_sql_quote(r['sentiment_hint'])},"
            f"{float(r['confidence'])},"
            f"{_sql_quote(r['raw_ref'])},"
            "now()"
            ")"
        )
    sql = (
        "INSERT INTO trading.earnings_calendar "
        "(event_date, ticker, ticker_name, event_name, event_source, importance, sentiment_hint, confidence, raw_ref, created_at) VALUES "
        + ",".join(values)
    )
    ch_exec(sql)
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="실적 이벤트 캘린더(Seed) 수집")
    ap.add_argument("--days", type=int, default=45)
    args = ap.parse_args()

    ensure_table()
    rows = load_rows(args.days)
    n = insert_rows(rows)
    print(f"collect_earnings_calendar: scanned_days={args.days} inserted={n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
