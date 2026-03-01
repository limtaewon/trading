#!/usr/bin/env python3
"""interest_watchlist 스냅샷/런 메타 retention prune 전용 잡.

refresh_interest_watchlist.py에서 분리된 운영 정리 작업이다.
"""

from __future__ import annotations

import argparse
import os
from typing import Any

import requests

CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "").strip() or os.environ.get("CLICKHOUSE_HOST", "http://localhost:8123").strip() or "http://localhost:8123"
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "").strip()
CLICKHOUSE_PASS = os.environ.get("CLICKHOUSE_PASS", os.environ.get("CLICKHOUSE_PASSWORD", "")).strip()
CLICKHOUSE_AUTH = (CLICKHOUSE_USER, CLICKHOUSE_PASS) if CLICKHOUSE_USER else None


def _ch_query(sql: str) -> list[dict[str, Any]]:
    resp = requests.post(
        CLICKHOUSE_URL,
        params={"default_format": "JSON"},
        data=(sql + "\n").encode("utf-8"),
        timeout=90,
        auth=CLICKHOUSE_AUTH,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def _ch_execute(sql: str) -> None:
    resp = requests.post(
        CLICKHOUSE_URL,
        data=(sql + "\n").encode("utf-8"),
        timeout=120,
        auth=CLICKHOUSE_AUTH,
    )
    resp.raise_for_status()


def _table_exists(name: str) -> bool:
    rows = _ch_query(
        "SELECT count() AS c FROM system.tables "
        f"WHERE database='trading' AND name='{name}'"
    )
    return bool(rows and int(rows[0].get("c", 0) or 0) > 0)


def _count_old_rows(table: str, col: str, days: int) -> int:
    rows = _ch_query(
        f"SELECT count() AS c FROM trading.{table} "
        f"WHERE {col} < now() - INTERVAL {int(days)} DAY"
    )
    return int(rows[0].get("c", 0) or 0) if rows else 0


def _prune(table: str, col: str, days: int, dry_run: bool) -> tuple[int, int]:
    old_rows = _count_old_rows(table, col, days)
    if old_rows <= 0 or dry_run:
        return old_rows, 0
    _ch_execute(
        f"ALTER TABLE trading.{table} "
        f"DELETE WHERE {col} < now() - INTERVAL {int(days)} DAY"
    )
    return old_rows, old_rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--retention-days", type=int, default=max(7, int(os.environ.get("WATCHLIST_RETENTION_DAYS", "21"))))
    ap.add_argument("--run-retention-days", type=int, default=max(14, int(os.environ.get("WATCHLIST_RUN_RETENTION_DAYS", "45"))))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    retention_days = max(7, int(args.retention_days))
    run_retention_days = max(retention_days, int(args.run_retention_days))

    if not _table_exists("interest_watchlist"):
        print("watchlist_prune skip: table trading.interest_watchlist not found")
        return 0

    watch_old, watch_deleted = _prune("interest_watchlist", "ts", retention_days, args.dry_run)

    runs_old = 0
    runs_deleted = 0
    if _table_exists("interest_watchlist_runs"):
        runs_old, runs_deleted = _prune("interest_watchlist_runs", "ts", run_retention_days, args.dry_run)

    mode = "dry-run" if args.dry_run else "execute"
    print(
        "watchlist_prune "
        f"mode={mode} "
        f"watchlist_old_rows={watch_old} watchlist_deleted={watch_deleted} retention_days={retention_days} "
        f"runs_old_rows={runs_old} runs_deleted={runs_deleted} run_retention_days={run_retention_days}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
