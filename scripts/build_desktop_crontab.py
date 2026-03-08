#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


HOME = Path.home()
REPO_ROOT = Path(__file__).resolve().parents[1]
CODEX_JOBS_FILE = REPO_ROOT / "cron" / "codex_jobs.json"
OUT_FILE = HOME / ".openclaw" / "cron" / "desktop_trading.crontab"
REPO_OUT_FILE = REPO_ROOT / "cron" / "desktop_trading.crontab"


HEADER = """SHELL=/bin/bash
PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

# Generated from trading/cron/codex_jobs.json
# Install manually when macOS permits:
#   crontab ~/.openclaw/cron/desktop_trading.crontab
#
# Note:
# - macOS cron parses schedules in the Mac's local timezone.
# - It does not honor CRON_TZ for schedule evaluation.
# - Keep the desktop timezone set to Asia/Seoul before installing this file.
"""


def main() -> int:
    obj = json.loads(CODEX_JOBS_FILE.read_text(encoding="utf-8"))
    jobs = obj.get("jobs", [])
    lines: list[str] = [HEADER.rstrip(), ""]
    for job in jobs:
        if not isinstance(job, dict):
            continue
        if job.get("enabled", True) is False:
            continue
        if job.get("source") != "legacy-crontab":
            continue
        payload = job.get("payload", {})
        if not isinstance(payload, dict) or payload.get("kind") != "command":
            continue
        expr = str(job.get("schedule", {}).get("expr", "") or "").strip()
        tz = str(job.get("schedule", {}).get("tz", "") or "").strip()
        command = str(payload.get("command", "") or "").strip()
        name = str(job.get("name", "") or "").strip()
        if not expr or not command or not name:
            continue
        if tz and tz != "Asia/Seoul":
            raise SystemExit(
                f"unsupported timezone for macOS cron export: {name} uses {tz!r}"
            )
        lines.append(f"# {name}")
        lines.append(f"{expr} {command}")
        lines.append("")

    text = "\n".join(lines).rstrip() + "\n"
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(text, encoding="utf-8")
    REPO_OUT_FILE.write_text(text, encoding="utf-8")
    print(f"wrote {OUT_FILE} and {REPO_OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
