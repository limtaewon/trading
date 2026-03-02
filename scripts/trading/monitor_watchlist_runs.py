#!/usr/bin/env python3
"""interest_watchlist_runs 기반 watchlist 생성 헬스 모니터.

- 최신 run의 상태/삽입행수/신선도 점검
- 이상 시 텔레그램 알림
- 동일 알림 스팸 방지를 위해 상태 파일로 cooldown 관리
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

import requests
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()

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

STATE_FILE = Path.home() / ".openclaw" / "data" / "watchlist_runs_health.json"


def _now_local() -> datetime:
    return datetime.now()


def _parse_ts(v: str) -> datetime | None:
    s = str(v or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


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


def _table_exists(name: str) -> bool:
    rows = _ch_query(
        "SELECT count() AS c FROM system.tables "
        f"WHERE database='trading' AND name='{name}'"
    )
    return bool(rows and int(rows[0].get("c", 0) or 0) > 0)


def _load_state() -> dict[str, Any]:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_state(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_notify() -> Callable[[str], Any] | None:
    candidates = [
        Path(__file__).resolve().parent / "telegram_notify.py",
        Path(__file__).resolve().parent.parent / "telegram_notify.py",
        Path.home() / ".openclaw" / "scripts" / "telegram_notify.py",
    ]
    for path in candidates:
        if not path.exists():
            continue
        spec = importlib.util.spec_from_file_location("telegram_notify", path)
        if not spec or not spec.loader:
            continue
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        notify = getattr(mod, "notify", None)
        if callable(notify):
            return notify
    return None


def _query_latest_runs(sources: list[str], limit: int = 1) -> list[dict[str, Any]]:
    source_list = ",".join("'" + s.replace("'", "\\'") + "'" for s in sources)
    return _ch_query(
        "SELECT run_id, ts, source, status, limit_n, inserted_rows, min_expected_rows, "
        "llm_enabled, llm_rows, llm_error "
        "FROM trading.interest_watchlist_runs "
        f"WHERE source IN ({source_list}) "
        "ORDER BY ts DESC "
        f"LIMIT {max(1, int(limit))}"
    )


def _parse_sources(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in str(raw or "").split(","):
        src = token.strip()
        if not src or src in seen:
            continue
        seen.add(src)
        out.append(src)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=os.environ.get("WATCHLIST_ACTIVE_SOURCE", "enrich_data"))
    ap.add_argument("--stale-minutes", type=int, default=max(60, int(os.environ.get("WATCHLIST_RUN_STALE_MINUTES", "390"))))
    ap.add_argument("--min-health-ratio", type=float, default=max(0.1, min(1.0, float(os.environ.get("WATCHLIST_MIN_HEALTH_RATIO", "0.8")))))
    ap.add_argument("--cooldown-minutes", type=int, default=max(5, int(os.environ.get("WATCHLIST_RUN_ALERT_COOLDOWN_MINUTES", "120"))))
    ap.add_argument("--fail-on-alert", action="store_true")
    args = ap.parse_args()

    sources = _parse_sources(args.source)
    if not sources:
        sources = ["enrich_data"]
    source_label = ",".join(sources)
    stale_minutes = max(30, int(args.stale_minutes))
    min_health_ratio = max(0.1, min(1.0, float(args.min_health_ratio)))
    cooldown_minutes = max(5, int(args.cooldown_minutes))

    if not _table_exists("interest_watchlist_runs"):
        print("watchlist_run_health status=skip reason=table_missing")
        return 0

    rows = _query_latest_runs(sources, limit=1)
    now = _now_local()

    status = "ok"
    reason = "-"
    detail = "-"

    if not rows:
        status = "alert"
        reason = "no_run"
        detail = f"source={source_label}"
    else:
        r = rows[0]
        run_ts = _parse_ts(str(r.get("ts", "")))
        age_minutes = int((now - run_ts).total_seconds() // 60) if run_ts else 10**9
        run_status = str(r.get("status", "")).strip() or "unknown"
        inserted = int(r.get("inserted_rows", 0) or 0)
        min_expected = int(r.get("min_expected_rows", 0) or 0)
        limit_n = int(r.get("limit_n", 0) or 0)
        llm_enabled = int(r.get("llm_enabled", 0) or 0) == 1
        llm_rows = int(r.get("llm_rows", 0) or 0)
        llm_error = str(r.get("llm_error", "") or "").strip()

        floor = max(1, min_expected if min_expected > 0 else int(round(max(1, limit_n) * min_health_ratio)))

        if age_minutes > stale_minutes:
            status = "alert"
            reason = "stale"
            detail = f"age={age_minutes}m>{stale_minutes}m source={source_label}"
        elif run_status != "ok":
            status = "alert"
            reason = "partial"
            detail = f"run_status={run_status} inserted={inserted} min_expected={floor} source={source_label}"
        elif inserted < floor:
            status = "alert"
            reason = "insufficient_rows"
            detail = f"inserted={inserted} min_expected={floor} source={source_label}"
        elif llm_enabled and llm_rows <= 0:
            status = "warn"
            reason = "llm_empty"
            detail = f"llm_rows=0 llm_error={llm_error or '-'} source={source_label}"
        else:
            detail = (
                f"source={source_label} run_status={run_status} inserted={inserted} "
                f"min_expected={floor} age={age_minutes}m"
            )

    out_line = f"watchlist_run_health status={status} reason={reason} detail={detail}"
    print(out_line)

    notify = _load_notify()
    state = _load_state()
    last_key = str(state.get("last_alert_key", ""))
    last_alert_ts = _parse_ts(str(state.get("last_alert_ts", "")))
    last_status = str(state.get("last_status", "ok"))

    alert_key = f"{status}:{reason}:{detail}"
    should_alert = status in {"alert", "warn"}
    in_cooldown = False
    if should_alert and last_alert_ts:
        in_cooldown = ((now - last_alert_ts).total_seconds() < cooldown_minutes * 60)

    if should_alert and notify and (alert_key != last_key or not in_cooldown):
        notify(
            "[watchlist-run-health] "
            f"status={status}\n"
            f"reason={reason}\n"
            f"detail={detail}"
        )
        state["last_alert_key"] = alert_key
        state["last_alert_ts"] = now.strftime("%Y-%m-%d %H:%M:%S")

    if status == "ok" and last_status in {"alert", "warn"} and notify:
        notify(
            "[watchlist-run-health] status=recovered\n"
            f"detail={detail}"
        )

    state["last_status"] = status
    state["last_reason"] = reason
    state["last_checked_ts"] = now.strftime("%Y-%m-%d %H:%M:%S")
    _save_state(state)

    if status == "alert" and args.fail_on_alert:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
