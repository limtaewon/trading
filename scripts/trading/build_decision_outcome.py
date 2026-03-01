#!/usr/bin/env python3
"""Build post-decision outcomes for decision candidates.

For each decision_candidate row, compute forward return/drawdown metrics over
N trading days using trading.technical_signals close_price and store into
trading.decision_outcome.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import math
import os
import sys
from collections import defaultdict
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()


def _log(msg: str) -> None:
    print(f"{dt.datetime.now().strftime('%H:%M:%S')} [outcome] {msg}", flush=True)


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


def ch_select(sql: str, timeout_sec: int = 60) -> list[dict[str, Any]]:
    url, headers = _ch_url_and_headers()
    q = sql.strip() + "\nFORMAT JSON"
    req = Request(url, data=q.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        body = r.read().decode("utf-8", errors="replace")
    obj = json.loads(body)
    data = obj.get("data", [])
    return data if isinstance(data, list) else []


def ch_execute(sql: str, timeout_sec: int = 60) -> None:
    url, headers = _ch_url_and_headers()
    req = Request(url, data=sql.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        _ = r.read()


def ch_insert_json_each_row(table: str, rows: list[dict[str, Any]], timeout_sec: int = 120) -> None:
    if not rows:
        return
    url, headers = _ch_url_and_headers()
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    q = f"INSERT INTO {table} FORMAT JSONEachRow\n".encode("utf-8") + payload.encode("utf-8")
    req = Request(url, data=q, headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        _ = r.read()


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _sql_quote(s: str) -> str:
    return "'" + (s or "").replace("\\", "\\\\").replace("'", "\\'") + "'"


def _parse_date(v: Any) -> dt.date | None:
    s = str(v or "").strip()
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        return None


def _parse_horizons(raw: str) -> list[int]:
    out: list[int] = []
    for token in (raw or "").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            n = int(token)
            if n > 0:
                out.append(n)
        except Exception:
            continue
    uniq = sorted(set(out))
    return uniq if uniq else [1, 3, 5]


def _stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / float(len(values))
    var = sum((x - mean) ** 2 for x in values) / float(len(values) - 1)
    return math.sqrt(max(0.0, var))


def ensure_outcome_table() -> None:
    ch_execute(
        """
CREATE TABLE IF NOT EXISTS trading.decision_outcome
(
    decision_id             UUID,
    decision_time           DateTime,
    ticker                  String,
    action                  LowCardinality(String),
    horizon_days            UInt16,
    entry_date              Date,
    entry_price             Float64,
    exit_date               Date,
    exit_price              Float64,
    raw_return_pct          Float32,
    action_return_pct       Float32,
    max_drawdown_pct        Float32,
    max_runup_pct           Float32,
    realized_vol_pct        Float32,
    bars                    UInt16,
    resolved                UInt8,
    quality_code            LowCardinality(String),
    candidate_total_score   Float32,
    stage2_score            Float32,
    stage3_score            Float32,
    stage4_score            Float32,
    stage5_score            Float32,
    created_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (decision_time, decision_id, ticker, horizon_days)
"""
    )


def load_recent_decisions(lookback_days: int, limit_decisions: int) -> list[str]:
    rows = ch_select(
        f"""
SELECT toString(decision_id) AS decision_id
FROM trading.decision_run
WHERE decision_time >= now() - INTERVAL {max(1, lookback_days)} DAY
ORDER BY decision_time DESC
LIMIT {max(1, limit_decisions)}
"""
    )
    out: list[str] = []
    for r in rows:
        did = str(r.get("decision_id", "") or "").strip()
        if did:
            out.append(did)
    return out


def load_candidates(decision_ids: list[str]) -> list[dict[str, Any]]:
    if not decision_ids:
        return []
    id_list = ", ".join(f"toUUID({_sql_quote(did)})" for did in decision_ids)
    return ch_select(
        f"""
SELECT
    toString(c.decision_id) AS decision_id,
    r.decision_time,
    toDate(r.decision_time) AS decision_date,
    c.ticker,
    c.action,
    c.total_score,
    c.stage2_stock_flow_score,
    c.stage3_event_score,
    c.stage4_timing_score,
    c.stage5_risk_score
FROM trading.decision_candidate c
INNER JOIN trading.decision_run r USING (decision_id)
WHERE c.decision_id IN ({id_list})
  AND c.ticker != ''
ORDER BY r.decision_time DESC, c.ticker
"""
    )


def load_existing_outcomes(decision_ids: list[str], horizons: list[int]) -> set[tuple[str, str, int]]:
    if not decision_ids or not horizons:
        return set()
    id_list = ", ".join(f"toUUID({_sql_quote(did)})" for did in decision_ids)
    hz_list = ", ".join(str(int(h)) for h in horizons)
    rows = ch_select(
        f"""
SELECT
    toString(decision_id) AS decision_id,
    ticker,
    horizon_days
FROM trading.decision_outcome
WHERE decision_id IN ({id_list})
  AND horizon_days IN ({hz_list})
"""
    )
    return {
        (
            str(r.get("decision_id", "")).strip(),
            str(r.get("ticker", "")).strip(),
            _to_int(r.get("horizon_days"), 0),
        )
        for r in rows
    }


def load_price_series(
    tickers: list[str],
    min_date: dt.date,
) -> dict[str, list[tuple[dt.date, float]]]:
    out: dict[str, list[tuple[dt.date, float]]] = defaultdict(list)
    if not tickers:
        return out

    chunk_size = 150
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        tk_list = ", ".join(_sql_quote(t) for t in chunk)
        rows = ch_select(
            f"""
SELECT
    ticker,
    date,
    toFloat64(close_price) AS close_price
FROM trading.technical_signals
WHERE ticker IN ({tk_list})
  AND date >= toDate({_sql_quote(min_date.isoformat())}) - INTERVAL 7 DAY
ORDER BY ticker, date
"""
        )
        for r in rows:
            ticker = str(r.get("ticker", "") or "").strip()
            d = _parse_date(r.get("date"))
            px = _to_float(r.get("close_price"), 0.0)
            if not ticker or d is None or px <= 0:
                continue
            out[ticker].append((d, px))
    return out


def _compute_one(
    row: dict[str, Any],
    horizon: int,
    series: list[tuple[dt.date, float]],
    now_ts: str,
) -> dict[str, Any]:
    decision_id = str(row.get("decision_id", "") or "")
    decision_time = str(row.get("decision_time", now_ts))
    decision_date = _parse_date(row.get("decision_date"))
    ticker = str(row.get("ticker", "") or "")
    action = str(row.get("action", "") or "").upper()

    base = {
        "decision_id": decision_id,
        "decision_time": decision_time,
        "ticker": ticker,
        "action": action,
        "horizon_days": int(horizon),
        "entry_date": "1970-01-01",
        "entry_price": 0.0,
        "exit_date": "1970-01-01",
        "exit_price": 0.0,
        "raw_return_pct": 0.0,
        "action_return_pct": 0.0,
        "max_drawdown_pct": 0.0,
        "max_runup_pct": 0.0,
        "realized_vol_pct": 0.0,
        "bars": 0,
        "resolved": 0,
        "quality_code": "NO_SERIES",
        "candidate_total_score": round(_to_float(row.get("total_score"), 0.0), 4),
        "stage2_score": round(_to_float(row.get("stage2_stock_flow_score"), 0.0), 4),
        "stage3_score": round(_to_float(row.get("stage3_event_score"), 0.0), 4),
        "stage4_score": round(_to_float(row.get("stage4_timing_score"), 0.0), 4),
        "stage5_score": round(_to_float(row.get("stage5_risk_score"), 0.0), 4),
        "created_at": now_ts,
    }
    if decision_date is None:
        base["quality_code"] = "BAD_DECISION_DATE"
        return base
    if not series:
        base["quality_code"] = "NO_SERIES"
        return base

    entry_idx = -1
    for i, (d, px) in enumerate(series):
        if d >= decision_date and px > 0:
            entry_idx = i
            break
    if entry_idx < 0:
        base["quality_code"] = "NO_ENTRY_BAR"
        return base

    exit_idx = entry_idx + int(horizon)
    entry_date, entry_px = series[entry_idx]
    base["entry_date"] = entry_date.isoformat()
    base["entry_price"] = round(entry_px, 6)
    if exit_idx >= len(series):
        base["quality_code"] = "NO_EXIT_BAR"
        return base

    path = series[entry_idx : exit_idx + 1]
    closes = [p for _, p in path]
    if len(closes) < 2 or closes[0] <= 0:
        base["quality_code"] = "BAD_PATH"
        return base

    exit_date, exit_px = path[-1]
    raw_ret = (exit_px / closes[0] - 1.0) * 100.0
    action_ret = raw_ret if action not in {"SELL", "REDUCE"} else -raw_ret
    max_dd = (min(closes) / closes[0] - 1.0) * 100.0
    max_ru = (max(closes) / closes[0] - 1.0) * 100.0
    rets = [(closes[i] / closes[i - 1] - 1.0) for i in range(1, len(closes)) if closes[i - 1] > 0]
    vol_pct = _stdev(rets) * math.sqrt(252.0) * 100.0 if rets else 0.0

    base.update(
        {
            "exit_date": exit_date.isoformat(),
            "exit_price": round(exit_px, 6),
            "raw_return_pct": round(raw_ret, 4),
            "action_return_pct": round(action_ret, 4),
            "max_drawdown_pct": round(max_dd, 4),
            "max_runup_pct": round(max_ru, 4),
            "realized_vol_pct": round(vol_pct, 4),
            "bars": len(path) - 1,
            "resolved": 1,
            "quality_code": "RESOLVED",
        }
    )
    return base


def main() -> int:
    ap = argparse.ArgumentParser(description="Build trading.decision_outcome from decision_candidate + technical_signals.")
    ap.add_argument("--lookback-days", type=int, default=45, help="decision_run lookback days.")
    ap.add_argument("--limit-decisions", type=int, default=150, help="max decision_run count to process.")
    ap.add_argument("--horizons", default="1,3,5", help="comma-separated trading day horizons.")
    ap.add_argument("--overwrite", action="store_true", help="rebuild even if outcome row already exists.")
    ap.add_argument("--resolved-only", action="store_true", help="insert only resolved rows.")
    args = ap.parse_args()

    horizons = _parse_horizons(args.horizons)
    ensure_outcome_table()

    decision_ids = load_recent_decisions(args.lookback_days, args.limit_decisions)
    if not decision_ids:
        print(json.dumps({"ok": False, "error": "no decision_run rows found"}, ensure_ascii=False), flush=True)
        return 1

    candidates = load_candidates(decision_ids)
    if not candidates:
        print(json.dumps({"ok": False, "error": "no decision_candidate rows found"}, ensure_ascii=False), flush=True)
        return 1

    existing = set() if args.overwrite else load_existing_outcomes(decision_ids, horizons)
    min_date = None
    ticker_set: set[str] = set()
    for r in candidates:
        d = _parse_date(r.get("decision_date"))
        if d is not None and (min_date is None or d < min_date):
            min_date = d
        t = str(r.get("ticker", "") or "").strip()
        if t:
            ticker_set.add(t)
    if min_date is None:
        print(json.dumps({"ok": False, "error": "cannot determine min decision_date"}, ensure_ascii=False), flush=True)
        return 1

    price_map = load_price_series(sorted(ticker_set), min_date)
    now_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    rows_to_insert: list[dict[str, Any]] = []
    skipped_existing = 0
    unresolved = 0
    for row in candidates:
        did = str(row.get("decision_id", "") or "").strip()
        ticker = str(row.get("ticker", "") or "").strip()
        series = price_map.get(ticker, [])
        for h in horizons:
            key = (did, ticker, h)
            if key in existing:
                skipped_existing += 1
                continue
            out_row = _compute_one(row, h, series, now_ts)
            if _to_int(out_row.get("resolved"), 0) == 0:
                unresolved += 1
                if args.resolved_only:
                    continue
            rows_to_insert.append(out_row)

    ch_insert_json_each_row("trading.decision_outcome", rows_to_insert, timeout_sec=180)
    resolved_cnt = sum(1 for r in rows_to_insert if _to_int(r.get("resolved"), 0) == 1)
    _log(
        f"rows={len(rows_to_insert)} resolved={resolved_cnt} unresolved={unresolved} "
        f"skipped_existing={skipped_existing}"
    )
    print(
        json.dumps(
            {
                "ok": True,
                "rows": len(rows_to_insert),
                "resolved": resolved_cnt,
                "unresolved": unresolved,
                "skipped_existing": skipped_existing,
                "horizons": horizons,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
