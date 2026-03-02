#!/usr/bin/env python3
"""뉴스 파이프라인 헬스 체크.

점검 항목:
1) market_regime 최신성
2) news 최근 입력량
3) news_cluster_state 최신성
4) news_event_frames explain_ready 생성량
5) hidden_relation_signals 최신성
6) interest_watchlist_runs 최신 run 상태
"""

from __future__ import annotations

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
STATE_FILE = Path.home() / ".openclaw" / "data" / "news_pipeline_health.json"


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


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _to_str(v: Any) -> str:
    return str(v or "").strip()


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


def main() -> int:
    stale_regime_min = max(60, _to_int(os.environ.get("NEWS_PIPELINE_STALE_REGIME_MIN", "1500"), 1500))
    stale_cluster_min = max(30, _to_int(os.environ.get("NEWS_PIPELINE_STALE_CLUSTER_MIN", "180"), 180))
    stale_relation_min = max(30, _to_int(os.environ.get("NEWS_PIPELINE_STALE_RELATION_MIN", "180"), 180))
    min_news_3h = max(0, _to_int(os.environ.get("NEWS_PIPELINE_MIN_NEWS_3H", "5"), 5))
    min_frames_explain_6h = max(0, _to_int(os.environ.get("NEWS_PIPELINE_MIN_FRAMES_EXPLAIN_6H", "3"), 3))
    notify_enabled = os.environ.get("NEWS_PIPELINE_HEALTH_NOTIFY", "0").strip() in {"1", "true", "TRUE", "yes", "YES"}
    cooldown_min = max(5, _to_int(os.environ.get("NEWS_PIPELINE_HEALTH_COOLDOWN_MIN", "120"), 120))

    checks: dict[str, Any] = {}
    fail: list[str] = []
    warn: list[str] = []

    # 1) market_regime freshness
    if _table_exists("market_regime"):
        rows = _ch_query("SELECT if(count()=0, 99999, greatest(dateDiff('minute', max(updated_at), now()), 0)) AS age_min FROM trading.market_regime")
        age = _to_int((rows[0] if rows else {}).get("age_min"), 99999)
        checks["market_regime_age_min"] = age
        if age > stale_regime_min:
            fail.append(f"market_regime_stale({age}m)")
    else:
        fail.append("market_regime_missing")

    # 2) news ingest
    if _table_exists("news"):
        rows = _ch_query("SELECT count() AS c FROM trading.news WHERE published_at >= now() - INTERVAL 3 HOUR")
        news_3h = _to_int((rows[0] if rows else {}).get("c"), 0)
        checks["news_rows_3h"] = news_3h
        if news_3h < min_news_3h:
            warn.append(f"news_rows_low({news_3h}/3h)")
    else:
        fail.append("news_missing")

    # 3) cluster freshness
    if _table_exists("news_cluster_state"):
        rows = _ch_query("SELECT if(count()=0, 99999, greatest(dateDiff('minute', max(asof_ts), now()), 0)) AS age_min FROM trading.news_cluster_state")
        age = _to_int((rows[0] if rows else {}).get("age_min"), 99999)
        checks["cluster_age_min"] = age
        if age > stale_cluster_min:
            warn.append(f"cluster_stale({age}m)")
    else:
        fail.append("news_cluster_state_missing")

    # 4) event frame explain_ready
    if _table_exists("news_event_frames"):
        rows = _ch_query(
            """
SELECT countIf(relevant=1 AND thesis_path!='' AND evidence_json!='[]') AS c
FROM trading.news_event_frames
WHERE published_at >= now() - INTERVAL 6 HOUR
"""
        )
        c = _to_int((rows[0] if rows else {}).get("c"), 0)
        checks["event_frames_explain_ready_6h"] = c
        if c < min_frames_explain_6h:
            warn.append(f"event_frames_explain_low({c}/6h)")
    else:
        fail.append("news_event_frames_missing")

    # 5) hidden relation freshness
    if _table_exists("hidden_relation_signals"):
        rows = _ch_query("SELECT if(count()=0, 99999, greatest(dateDiff('minute', max(asof_ts), now()), 0)) AS age_min FROM trading.hidden_relation_signals")
        age = _to_int((rows[0] if rows else {}).get("age_min"), 99999)
        checks["relation_age_min"] = age
        if age > stale_relation_min:
            warn.append(f"relation_stale({age}m)")
    else:
        fail.append("hidden_relation_signals_missing")

    # 6) watchlist latest run health
    if _table_exists("interest_watchlist_runs"):
        src = os.environ.get("WATCHLIST_ACTIVE_SOURCE", "enrich_data").strip()
        src_list = [s.strip() for s in src.split(",") if s.strip()]
        in_sql = ",".join("'" + s.replace("'", "\\'") + "'" for s in src_list) if src_list else "'enrich_data'"
        rows = _ch_query(
            f"""
SELECT status, inserted_rows, min_expected_rows, toString(ts) AS ts
FROM trading.interest_watchlist_runs
WHERE source IN ({in_sql})
ORDER BY ts DESC
LIMIT 1
"""
        )
        if rows:
            r = rows[0]
            status = _to_str(r.get("status")) or "unknown"
            inserted = _to_int(r.get("inserted_rows"), 0)
            min_expected = max(1, _to_int(r.get("min_expected_rows"), 1))
            checks["watchlist_status"] = status
            checks["watchlist_inserted"] = inserted
            checks["watchlist_min_expected"] = min_expected
            if status != "ok" or inserted < min_expected:
                warn.append(f"watchlist_unhealthy(status={status},rows={inserted}/{min_expected})")
        else:
            warn.append("watchlist_run_missing")
    else:
        warn.append("watchlist_run_table_missing")

    status = "ok"
    if fail:
        status = "alert"
    elif warn:
        status = "warn"

    payload = {
        "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": status,
        "fail": fail,
        "warn": warn,
        "checks": checks,
    }
    print(json.dumps(payload, ensure_ascii=False))

    if notify_enabled:
        notify = _load_notify()
        if notify:
            state = _load_state()
            last_key = _to_str(state.get("last_alert_key"))
            last_ts_s = _to_str(state.get("last_alert_ts"))
            now = datetime.now()
            in_cooldown = False
            if last_ts_s:
                try:
                    last_ts = datetime.strptime(last_ts_s, "%Y-%m-%d %H:%M:%S")
                    in_cooldown = (now - last_ts).total_seconds() < cooldown_min * 60
                except Exception:
                    pass
            key = f"{status}|{'/'.join(fail)}|{'/'.join(warn)}"
            if status in {"alert", "warn"} and (key != last_key or not in_cooldown):
                notify(
                    "[news-pipeline-health]\n"
                    f"status={status}\n"
                    f"fail={', '.join(fail) if fail else '-'}\n"
                    f"warn={', '.join(warn) if warn else '-'}"
                )
                state["last_alert_key"] = key
                state["last_alert_ts"] = now.strftime("%Y-%m-%d %H:%M:%S")
            state["last_status"] = status
            _save_state(state)
    return 0 if status != "alert" else 2


if __name__ == "__main__":
    raise SystemExit(main())
