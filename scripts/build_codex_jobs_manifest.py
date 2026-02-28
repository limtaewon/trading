#!/usr/bin/env python3
"""build_codex_jobs_manifest.py

OpenClaw jobs + 기존 데이터 수집 crontab 작업을 하나의 codex_jobs.json으로 통합한다.
"""
from __future__ import annotations

import json
from pathlib import Path

HOME = Path.home()
BASE = HOME / ".openclaw"
JOBS_FILE = BASE / "cron" / "jobs.json"
OUT_FILE = BASE / "cron" / "codex_jobs.json"


def load_openclaw_jobs() -> list[dict]:
    if not JOBS_FILE.exists():
        return []
    data = json.loads(JOBS_FILE.read_text(encoding="utf-8"))
    jobs = data.get("jobs", [])
    out: list[dict] = []
    for j in jobs:
        name = j.get("name")
        schedule = j.get("schedule", {})
        payload = j.get("payload", {})
        if not name or not schedule or not payload:
            continue
        out.append(
            {
                "name": name,
                "enabled": True,
                "schedule": {
                    "expr": schedule.get("expr", ""),
                    "tz": schedule.get("tz", "Asia/Seoul"),
                },
                "payload": payload,
                "source": "openclaw-jobs",
            }
        )
    return out


def data_jobs() -> list[dict]:
    env = 'source "$HOME/.openclaw/cron/codex_env.sh"'
    trading_scripts = "$HOME/.openclaw/scripts/trading"
    base_scripts = "$HOME/.openclaw/scripts"
    logs = "$HOME/.openclaw/logs"
    py = "/usr/bin/python3"
    collect_expr = "*/15 9-15 * * 1-5"
    cluster_expr = "*/20 9-15 * * 1-5"
    cluster_window_hours = "144"
    cluster_threshold = "0.48"
    cluster_limit = "10000"
    return [
        {
            "name": "data-news-morning",
            "enabled": True,
            "schedule": {"expr": "0 7 * * 1-5", "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && {py} {trading_scripts}/collect_news.py morning >> {logs}/news-collector.log 2>&1',
            },
            "source": "legacy-crontab",
        },
        {
            "name": "data-news-weekend-hourly",
            "enabled": True,
            "schedule": {"expr": "0 * * * 0,6", "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && {py} {trading_scripts}/collect_news.py morning >> {logs}/news-collector.log 2>&1',
            },
            "source": "legacy-crontab",
        },
        {
            "name": "data-news-trading-hourly",
            "enabled": True,
            "schedule": {"expr": collect_expr, "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && {py} {trading_scripts}/collect_news.py trading >> {logs}/news-collector.log 2>&1',
            },
            "source": "legacy-crontab",
        },
        {
            "name": "data-news-monitor-5m",
            "enabled": True,
            "schedule": {"expr": "*/5 9-15 * * 1-5", "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && {py} {trading_scripts}/monitor_news.py >> {logs}/news-monitor.log 2>&1',
            },
            "source": "legacy-crontab",
        },
        {
            "name": "data-news-cluster-hourly",
            "enabled": True,
            "schedule": {"expr": cluster_expr, "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && {py} {trading_scripts}/cluster_news.py --window-hours {cluster_window_hours} --threshold {cluster_threshold} --limit {cluster_limit} --min-size 2 >> {logs}/news-cluster.log 2>&1',
            },
            "source": "legacy-crontab",
        },
        {
            "name": "data-enrich-pre-market",
            "enabled": True,
            "schedule": {"expr": "30 7 * * 1-5", "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && bash {trading_scripts}/enrich_data.sh >> {logs}/enrich.log 2>&1',
            },
            "source": "legacy-crontab",
        },
        {
            "name": "data-enrich-midday-quick",
            "enabled": True,
            "schedule": {"expr": "0 12 * * 1-5", "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && bash {trading_scripts}/enrich_data.sh --quick >> {logs}/enrich.log 2>&1',
            },
            "source": "legacy-crontab",
        },
        {
            "name": "position-manager-20m",
            "enabled": True,
            "schedule": {"expr": "*/20 9-15 * * 1-5", "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && {py} {trading_scripts}/manage_positions.py --execute >> {logs}/position-manager.log 2>&1',
            },
            "source": "legacy-crontab",
        },
        {
            "name": "data-refresh-stocks-weekly",
            "enabled": True,
            "schedule": {"expr": "0 6 * * 1", "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && {py} {trading_scripts}/refresh_stocks.py >> {logs}/refresh_stocks.log 2>&1',
            },
            "source": "legacy-crontab",
        },
        {
            "name": "data-market-10m",
            "enabled": True,
            "schedule": {"expr": "*/10 9-16 * * 1-5", "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && {py} {trading_scripts}/collect_market_data.py >> {logs}/market_data.log 2>&1',
            },
            "source": "legacy-crontab",
        },
        {
            "name": "data-morning-briefing-0800",
            "enabled": True,
            "schedule": {"expr": "0 8 * * 1-5", "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && {py} {trading_scripts}/morning_briefing.py >> {logs}/briefing.log 2>&1',
            },
            "source": "legacy-crontab",
        },
        {
            "name": "bybit-futures-ws-private-1m",
            "enabled": True,
            "schedule": {"expr": "* * * * *", "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && bash {base_scripts}/bybit_futures_cron.sh ws >> {logs}/bybit-futures-ws.log 2>&1',
            },
            "source": "bybit-futures-template",
        },
        {
            "name": "bybit-futures-reconcile-5m",
            "enabled": True,
            "schedule": {"expr": "*/5 * * * *", "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && bash {base_scripts}/bybit_futures_cron.sh reconcile >> {logs}/bybit-futures-reconcile.log 2>&1',
            },
            "source": "bybit-futures-template",
        },
        {
            "name": "bybit-futures-decision-1m",
            "enabled": True,
            "schedule": {"expr": "* * * * *", "tz": "Asia/Seoul"},
            "payload": {
                "kind": "command",
                "command": f'{env} && bash {base_scripts}/bybit_futures_cron.sh decision >> {logs}/bybit-futures-decision.log 2>&1',
            },
            "source": "bybit-futures-template",
        },
    ]


def main() -> int:
    jobs = load_openclaw_jobs()
    jobs.extend(data_jobs())

    dedup: dict[str, dict] = {}
    for job in jobs:
        if not isinstance(job, dict):
            continue
        name = str(job.get("name", "")).strip()
        if not name:
            continue
        dedup[name] = job
    jobs = list(dedup.values())

    merged = {
        "version": 1,
        "generated_by": "build_codex_jobs_manifest.py",
        "jobs": jobs,
    }

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {OUT_FILE} ({len(jobs)} jobs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
