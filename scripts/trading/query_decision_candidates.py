#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()

try:
    import requests
except ImportError:
    from _requests_compat import requests

EXECUTION_UNIVERSE_FILE = Path(__file__).with_name("execution_universe.json")
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "trading").strip() or "trading"


def load_name_fallbacks() -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        raw = json.loads(EXECUTION_UNIVERSE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return out
    if not isinstance(raw, dict):
        return out
    for rows in raw.values():
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or "").strip()
            name = str(row.get("ticker_name") or row.get("name") or "").strip()
            if ticker and name and ticker not in out:
                out[ticker] = name
    return out


NAME_FALLBACKS = load_name_fallbacks()


def _resolve_clickhouse() -> tuple[str, tuple[str, str] | None]:
    raw_url = (
        os.environ.get("CLICKHOUSE_URL", "").strip()
        or os.environ.get("CLICKHOUSE_HOST", "").strip()
        or "http://localhost:8123"
    )
    user = os.environ.get("CLICKHOUSE_USER", "").strip()
    pw = os.environ.get("CLICKHOUSE_PASS", os.environ.get("CLICKHOUSE_PASSWORD", "")).strip()
    sp = urlsplit(raw_url)
    if sp.username and not user:
        user = sp.username
        pw = sp.password or pw
    if sp.username:
        netloc = sp.hostname or "localhost"
        if sp.port:
            netloc = f"{netloc}:{sp.port}"
        raw_url = urlunsplit((sp.scheme or "http", netloc, sp.path or "", sp.query, sp.fragment))
    auth = (user, pw) if user else None
    return raw_url, auth


CLICKHOUSE_URL, CLICKHOUSE_AUTH = _resolve_clickhouse()


def ch_query(sql: str) -> list[dict]:
    resp = requests.post(
        CLICKHOUSE_URL,
        params={"database": CLICKHOUSE_DB, "default_format": "JSON"},
        data=(sql.strip().rstrip(";") + "\n").encode("utf-8"),
        timeout=30,
        auth=CLICKHOUSE_AUTH,
    )
    resp.raise_for_status()
    body = resp.json()
    rows = body.get("data", [])
    return rows if isinstance(rows, list) else []


def get_decision(decision_id: str = "") -> dict:
    did = str(decision_id or "").strip()
    if did:
        rows = ch_query(
            f"""
SELECT
    toString(decision_id) AS decision_id_str,
    toString(decision_time) AS decision_time_str,
    total_score,
    absolute_block_reason
FROM trading.decision_run
WHERE decision_id = '{did}'
ORDER BY decision_time DESC
LIMIT 1
"""
        )
        if not rows:
            return {}
        row = dict(rows[0])
        row["decision_id"] = str(row.get("decision_id_str") or "")
        row["decision_time"] = str(row.get("decision_time_str") or "")
        return row
    rows = ch_query(
        """
SELECT
    toString(decision_id) AS decision_id_str,
    toString(decision_time) AS decision_time_str,
    total_score,
    absolute_block_reason
FROM trading.decision_run
ORDER BY decision_time DESC
LIMIT 1
"""
    )
    if not rows:
        return {}
    row = dict(rows[0])
    row["decision_id"] = str(row.get("decision_id_str") or "")
    row["decision_time"] = str(row.get("decision_time_str") or "")
    return row


def get_candidates(decision_id: str, limit: int) -> list[dict]:
    did = str(decision_id or "").strip()
    if not did:
        return []
    rows = ch_query(
        f"""
SELECT
    c.ticker,
    nullIf(t.ticker_name, '') AS ticker_name,
    c.action,
    round(c.total_score, 2) AS total_score,
    arrayStringConcat(c.absolute_block_reason, ', ') AS absolute_block_reason,
    arrayStringConcat(c.stage5_fail_codes, ', ') AS fail_codes,
    c.liquidity_source,
    c.primary_cluster_id
FROM trading.decision_candidate c
LEFT JOIN
(
    SELECT ticker, any(ticker_name) AS ticker_name
    FROM trading.technical_signals
    GROUP BY ticker
) t USING (ticker)
WHERE c.decision_id = '{did}'
ORDER BY c.total_score DESC, c.ticker ASC
LIMIT {int(limit)}
"""
    )
    out: list[dict] = []
    for row in rows:
        ticker = str(row.get("ticker") or "")
        ticker_name = str(row.get("ticker_name") or "").strip() or NAME_FALLBACKS.get(ticker, "") or ticker
        out.append(
            {
                "ticker": ticker,
                "ticker_name": ticker_name,
                "action": str(row.get("action") or ""),
                "total_score": float(row.get("total_score") or 0.0),
                "absolute_block_reason": str(row.get("absolute_block_reason") or ""),
                "fail_codes": str(row.get("fail_codes") or ""),
                "liquidity_source": str(row.get("liquidity_source") or ""),
                "primary_cluster_id": str(row.get("primary_cluster_id") or ""),
            }
        )
    return out


def _stringify_reasons(value: object) -> str:
    if isinstance(value, list):
        return ", ".join(str(x).strip() for x in value if str(x).strip())
    return str(value or "").strip()


def main() -> int:
    ap = argparse.ArgumentParser(description="Stable direct query for latest decision candidates")
    ap.add_argument("--decision-id", default="", help="decision_id UUID; default uses latest decision_run")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    decision = get_decision(args.decision_id)
    decision_id = str(decision.get("decision_id") or "").strip()
    if not decision_id:
        print("decision not found", file=sys.stderr)
        return 1

    candidates = get_candidates(decision_id, max(1, int(args.limit)))
    result = {
        "decision_id": decision_id,
        "decision_time": str(decision.get("decision_time") or ""),
        "decision_total_score": decision.get("total_score"),
        "absolute_block_reason": _stringify_reasons(decision.get("absolute_block_reason")),
        "candidates": candidates,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    print(f"decision_id={result['decision_id']}")
    if result["decision_time"]:
        print(f"decision_time={result['decision_time']}")
    if result["decision_total_score"] is not None:
        print(f"decision_total_score={result['decision_total_score']}")
    if result["absolute_block_reason"]:
        print(f"absolute_block_reason={result['absolute_block_reason']}")
    print("")
    for idx, row in enumerate(candidates, 1):
        name = row["ticker_name"] or row["ticker"]
        action = row["action"] or "-"
        score = row["total_score"]
        print(f"{idx}. {name} ({row['ticker']})")
        print(f"   action: {action}")
        print(f"   total_score: {score:.2f}")
        if row["absolute_block_reason"]:
            print(f"   block_reason: {row['absolute_block_reason']}")
        if row["fail_codes"]:
            print(f"   fail_codes: {row['fail_codes']}")
        if row["liquidity_source"]:
            print(f"   liquidity_source: {row['liquidity_source']}")
        if row["primary_cluster_id"]:
            print(f"   cluster_id: {row['primary_cluster_id']}")
        print("")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
