#!/usr/bin/env python3
"""A/B report for trading.decision_outcome joined with decision_run metadata.

Usage examples:
  python3 scripts/trading/report_decision_outcome_ab.py --horizon 3 --lookback-days 30
  python3 scripts/trading/report_decision_outcome_ab.py \
    --a-from "2026-02-01 00:00:00" --a-to "2026-02-15 23:59:59" \
    --b-from "2026-02-16 00:00:00" --b-to "2026-03-02 23:59:59" \
    --horizon 3 --a-model-like "v1" --b-model-like "v2"
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import sys
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()


def _ch_url_and_headers() -> tuple[str, dict[str, str]]:
    host = os.getenv("CLICKHOUSE_HOST", "").strip()
    user = os.getenv("CLICKHOUSE_USER", "").strip()
    pw = os.getenv("CLICKHOUSE_PASS", os.getenv("CLICKHOUSE_PASSWORD", "")).strip()
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


def ch_select(sql: str, timeout_sec: int = 90) -> list[dict[str, Any]]:
    url, headers = _ch_url_and_headers()
    q = sql.strip() + "\nFORMAT JSON"
    req = Request(url, data=q.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        body = r.read().decode("utf-8", errors="replace")
    return json.loads(body).get("data", []) or []


def _sql_quote(s: str) -> str:
    return "'" + (s or "").replace("\\", "\\\\").replace("'", "\\'") + "'"


def _build_window_clause(
    d_from: str,
    d_to: str,
    model_like: str,
    source: str,
) -> str:
    clauses = [
        f"r.decision_time >= toDateTime({_sql_quote(d_from)})",
        f"r.decision_time <= toDateTime({_sql_quote(d_to)})",
    ]
    if model_like:
        clauses.append(f"positionCaseInsensitiveUTF8(toString(r.model_version), {_sql_quote(model_like)}) > 0")
    if source:
        clauses.append(f"positionCaseInsensitiveUTF8(toString(r.universe), {_sql_quote(source)}) > 0")
    return " AND ".join(clauses)


def _query_group_stats(where_clause: str, horizon: int) -> dict[str, Any]:
    rows = ch_select(
        f"""
SELECT
    count() AS total_n,
    countIf(o.resolved = 1) AS resolved_n,
    countIf(o.resolved = 0) AS unresolved_n,
    round(if(resolved_n = 0 OR NOT isFinite(avgIf(o.action_return_pct, o.resolved = 1)), 0.0, avgIf(o.action_return_pct, o.resolved = 1)), 4) AS avg_action_ret_pct,
    round(if(resolved_n = 0, 0.0, quantileExactIf(0.5)(o.action_return_pct, o.resolved = 1)), 4) AS p50_action_ret_pct,
    round(sumIf(o.action_return_pct, o.resolved = 1), 4) AS sum_action_ret_pct,
    countIf(o.resolved = 1 AND o.action_return_pct > 0) AS win_n,
    countIf(o.resolved = 1 AND o.action_return_pct <= 0) AS loss_n,
    round(if(resolved_n = 0, 0.0, win_n / resolved_n * 100.0), 2) AS win_rate_pct,
    round(if(resolved_n = 0 OR NOT isFinite(avgIf(o.max_drawdown_pct, o.resolved = 1)), 0.0, avgIf(o.max_drawdown_pct, o.resolved = 1)), 4) AS avg_mdd_pct,
    round(if(resolved_n = 0, 0.0, quantileExactIf(0.9)(abs(o.max_drawdown_pct), o.resolved = 1)), 4) AS p90_abs_mdd_pct,
    round(if(resolved_n = 0 OR NOT isFinite(avgIf(o.realized_vol_pct, o.resolved = 1)), 0.0, avgIf(o.realized_vol_pct, o.resolved = 1)), 4) AS avg_realized_vol_pct
FROM trading.decision_outcome o
INNER JOIN trading.decision_run r USING (decision_id)
WHERE {where_clause}
  AND o.horizon_days = {int(horizon)}
"""
    )
    if not rows:
        return {
            "total_n": 0,
            "resolved_n": 0,
            "unresolved_n": 0,
            "avg_action_ret_pct": 0.0,
            "p50_action_ret_pct": 0.0,
            "sum_action_ret_pct": 0.0,
            "win_n": 0,
            "loss_n": 0,
            "win_rate_pct": 0.0,
            "avg_mdd_pct": 0.0,
            "p90_abs_mdd_pct": 0.0,
            "avg_realized_vol_pct": 0.0,
        }
    return rows[0]


def _to_f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _auto_windows(lookback_days: int) -> tuple[str, str, str, str]:
    now = dt.datetime.now()
    start = now - dt.timedelta(days=max(2, lookback_days))
    mid = start + (now - start) / 2
    return (
        start.strftime("%Y-%m-%d %H:%M:%S"),
        mid.strftime("%Y-%m-%d %H:%M:%S"),
        mid.strftime("%Y-%m-%d %H:%M:%S"),
        now.strftime("%Y-%m-%d %H:%M:%S"),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="A/B report for decision_outcome")
    ap.add_argument("--lookback-days", type=int, default=30)
    ap.add_argument("--horizon", type=int, default=3)
    ap.add_argument("--a-from", default="")
    ap.add_argument("--a-to", default="")
    ap.add_argument("--b-from", default="")
    ap.add_argument("--b-to", default="")
    ap.add_argument("--a-label", default="A")
    ap.add_argument("--b-label", default="B")
    ap.add_argument("--a-model-like", default="")
    ap.add_argument("--b-model-like", default="")
    ap.add_argument("--universe-like", default="")
    args = ap.parse_args()

    if not (args.a_from and args.a_to and args.b_from and args.b_to):
        a_from, a_to, b_from, b_to = _auto_windows(args.lookback_days)
    else:
        a_from, a_to, b_from, b_to = args.a_from, args.a_to, args.b_from, args.b_to

    a_where = _build_window_clause(a_from, a_to, args.a_model_like, args.universe_like)
    b_where = _build_window_clause(b_from, b_to, args.b_model_like, args.universe_like)
    a = _query_group_stats(a_where, args.horizon)
    b = _query_group_stats(b_where, args.horizon)

    delta = {
        "avg_action_ret_pct": round(_to_f(b.get("avg_action_ret_pct")) - _to_f(a.get("avg_action_ret_pct")), 4),
        "sum_action_ret_pct": round(_to_f(b.get("sum_action_ret_pct")) - _to_f(a.get("sum_action_ret_pct")), 4),
        "win_rate_pct": round(_to_f(b.get("win_rate_pct")) - _to_f(a.get("win_rate_pct")), 2),
        "avg_mdd_pct": round(_to_f(b.get("avg_mdd_pct")) - _to_f(a.get("avg_mdd_pct")), 4),
        "avg_realized_vol_pct": round(_to_f(b.get("avg_realized_vol_pct")) - _to_f(a.get("avg_realized_vol_pct")), 4),
    }

    result = {
        "ok": True,
        "horizon_days": int(args.horizon),
        "a_window": {"label": args.a_label, "from": a_from, "to": a_to, "model_like": args.a_model_like, "stats": a},
        "b_window": {"label": args.b_label, "from": b_from, "to": b_to, "model_like": args.b_model_like, "stats": b},
        "delta_b_minus_a": delta,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
