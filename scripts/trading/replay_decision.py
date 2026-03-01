#!/usr/bin/env python3
"""Replay decision logs from stored decision artifacts.

This replay does not recompute market features from raw data. Instead, it
re-aggregates decision_candidate rows and validates consistency against the
original decision_run row.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import sys
import uuid
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()


def _log(msg: str) -> None:
    print(f"{dt.datetime.now().strftime('%H:%M:%S')} [replay] {msg}", flush=True)


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


def ch_insert_json_each_row(table: str, rows: list[dict[str, Any]], timeout_sec: int = 60) -> None:
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


def ensure_replay_table() -> None:
    ch_execute(
        """
CREATE TABLE IF NOT EXISTS trading.decision_replay
(
    replay_id                UUID,
    decision_id              UUID,
    source_decision_time     DateTime,
    replay_time              DateTime,
    horizon                  LowCardinality(String),
    universe                 LowCardinality(String),
    candidate_count          UInt16,
    orig_stage2_score        Float32,
    orig_stage3_score        Float32,
    orig_stage4_score        Float32,
    orig_stage5_score        Float32,
    orig_total_score         Float32,
    recalc_stage2_score      Float32,
    recalc_stage3_score      Float32,
    recalc_stage4_score      Float32,
    recalc_stage5_score      Float32,
    recalc_total_score       Float32,
    diff_stage2_score        Float32,
    diff_stage3_score        Float32,
    diff_stage4_score        Float32,
    diff_stage5_score        Float32,
    diff_total_score         Float32,
    buy_count                UInt16,
    hold_count               UInt16,
    reduce_count             UInt16,
    replay_status            LowCardinality(String),
    replay_reason_codes      Array(String) DEFAULT [],
    detail_json              String DEFAULT '{}',
    created_at               DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (source_decision_time, decision_id, replay_time)
"""
    )


def load_target_runs(decision_id: str, lookback_days: int, limit: int) -> list[dict[str, Any]]:
    if decision_id:
        return ch_select(
            f"""
SELECT
    toString(decision_id) AS decision_id,
    decision_time,
    horizon,
    universe,
    stage2_score,
    stage3_score,
    stage4_score,
    stage5_score,
    total_score,
    stage3_pass,
    stage4_pass,
    stage5_pass
FROM trading.decision_run
WHERE decision_id = toUUID({_sql_quote(decision_id)})
LIMIT 1
"""
        )
    return ch_select(
        f"""
SELECT
    toString(decision_id) AS decision_id,
    decision_time,
    horizon,
    universe,
    stage2_score,
    stage3_score,
    stage4_score,
    stage5_score,
    total_score,
    stage3_pass,
    stage4_pass,
    stage5_pass
FROM trading.decision_run
WHERE decision_time >= now() - INTERVAL {max(1, lookback_days)} DAY
ORDER BY decision_time DESC
LIMIT {max(1, limit)}
"""
    )


def load_candidates(decision_id: str) -> list[dict[str, Any]]:
    return ch_select(
        f"""
SELECT
    action,
    stage2_stock_flow_score,
    stage3_event_score,
    stage4_timing_score,
    stage5_risk_score,
    total_score
FROM trading.decision_candidate
WHERE decision_id = toUUID({_sql_quote(decision_id)})
"""
    )


def _avg(rows: list[dict[str, Any]], key: str) -> float:
    if not rows:
        return 0.0
    return sum(_to_float(r.get(key), 0.0) for r in rows) / float(len(rows))


def replay_one(run_row: dict[str, Any], tolerance: float) -> dict[str, Any]:
    did = str(run_row.get("decision_id", "") or "")
    candidates = load_candidates(did)
    now_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    recalc_s2 = round(_avg(candidates, "stage2_stock_flow_score"), 4)
    recalc_s3 = round(_avg(candidates, "stage3_event_score"), 4)
    recalc_s4 = round(_avg(candidates, "stage4_timing_score"), 4)
    recalc_s5 = round(_avg(candidates, "stage5_risk_score"), 4)
    recalc_total = round(_avg(candidates, "total_score"), 4)

    orig_s2 = _to_float(run_row.get("stage2_score"), 0.0)
    orig_s3 = _to_float(run_row.get("stage3_score"), 0.0)
    orig_s4 = _to_float(run_row.get("stage4_score"), 0.0)
    orig_s5 = _to_float(run_row.get("stage5_score"), 0.0)
    orig_total = _to_float(run_row.get("total_score"), 0.0)

    diff_s2 = round(recalc_s2 - orig_s2, 4)
    diff_s3 = round(recalc_s3 - orig_s3, 4)
    diff_s4 = round(recalc_s4 - orig_s4, 4)
    diff_s5 = round(recalc_s5 - orig_s5, 4)
    diff_total = round(recalc_total - orig_total, 4)

    buy_count = sum(1 for r in candidates if str(r.get("action", "")).upper() == "BUY")
    hold_count = sum(1 for r in candidates if str(r.get("action", "")).upper() == "HOLD")
    reduce_count = sum(1 for r in candidates if str(r.get("action", "")).upper() == "REDUCE")

    reason_codes: list[str] = []
    if not candidates:
        reason_codes.append("NO_CANDIDATES")
    if abs(diff_s2) > tolerance:
        reason_codes.append("STAGE2_DIFF_OOB")
    if abs(diff_s3) > tolerance:
        reason_codes.append("STAGE3_DIFF_OOB")
    if abs(diff_s4) > tolerance:
        reason_codes.append("STAGE4_DIFF_OOB")
    if abs(diff_s5) > tolerance:
        reason_codes.append("STAGE5_DIFF_OOB")
    if abs(diff_total) > tolerance:
        reason_codes.append("TOTAL_DIFF_OOB")
    if _to_int(run_row.get("stage3_pass"), 0) == 1 and orig_s3 < 50.0:
        reason_codes.append("STAGE3_PASS_INCONSISTENT")
    if _to_int(run_row.get("stage4_pass"), 0) == 1 and orig_s4 < 55.0:
        reason_codes.append("STAGE4_PASS_INCONSISTENT")
    if _to_int(run_row.get("stage5_pass"), 0) == 1 and orig_s5 < 60.0:
        reason_codes.append("STAGE5_PASS_INCONSISTENT")

    status = "PASS" if not reason_codes else "FAIL"

    detail = {
        "tolerance": tolerance,
        "candidate_count": len(candidates),
        "action_counts": {"buy": buy_count, "hold": hold_count, "reduce": reduce_count},
    }

    return {
        "replay_id": str(uuid.uuid4()),
        "decision_id": did,
        "source_decision_time": str(run_row.get("decision_time", now_ts)),
        "replay_time": now_ts,
        "horizon": str(run_row.get("horizon", "")),
        "universe": str(run_row.get("universe", "")),
        "candidate_count": len(candidates),
        "orig_stage2_score": round(orig_s2, 4),
        "orig_stage3_score": round(orig_s3, 4),
        "orig_stage4_score": round(orig_s4, 4),
        "orig_stage5_score": round(orig_s5, 4),
        "orig_total_score": round(orig_total, 4),
        "recalc_stage2_score": recalc_s2,
        "recalc_stage3_score": recalc_s3,
        "recalc_stage4_score": recalc_s4,
        "recalc_stage5_score": recalc_s5,
        "recalc_total_score": recalc_total,
        "diff_stage2_score": diff_s2,
        "diff_stage3_score": diff_s3,
        "diff_stage4_score": diff_s4,
        "diff_stage5_score": diff_s5,
        "diff_total_score": diff_total,
        "buy_count": buy_count,
        "hold_count": hold_count,
        "reduce_count": reduce_count,
        "replay_status": status,
        "replay_reason_codes": reason_codes,
        "detail_json": json.dumps(detail, ensure_ascii=False),
        "created_at": now_ts,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Replay decision_run consistency from decision_candidate artifacts.")
    ap.add_argument("--decision-id", default="", help="Target decision_id(UUID). If omitted, run bulk replay.")
    ap.add_argument("--lookback-days", type=int, default=14, help="Bulk mode lookback window.")
    ap.add_argument("--limit", type=int, default=20, help="Bulk mode decision_run count limit.")
    ap.add_argument("--tolerance", type=float, default=0.1, help="Score diff tolerance.")
    args = ap.parse_args()

    ensure_replay_table()
    runs = load_target_runs(args.decision_id.strip(), args.lookback_days, args.limit)
    if not runs:
        print(json.dumps({"ok": False, "error": "no decision_run rows found"}, ensure_ascii=False), flush=True)
        return 1

    rows = [replay_one(r, max(0.0, args.tolerance)) for r in runs]
    ch_insert_json_each_row("trading.decision_replay", rows, timeout_sec=120)

    pass_cnt = sum(1 for r in rows if str(r.get("replay_status")) == "PASS")
    fail_cnt = len(rows) - pass_cnt
    _log(f"replayed={len(rows)} pass={pass_cnt} fail={fail_cnt}")
    print(
        json.dumps(
            {
                "ok": True,
                "replayed": len(rows),
                "pass": pass_cnt,
                "fail": fail_cnt,
                "decision_ids": [r.get("decision_id", "") for r in rows[:10]],
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
