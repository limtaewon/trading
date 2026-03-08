#!/usr/bin/env python3
"""morning_briefing.py

Telegram owner morning digest.

shared report payload를 기반으로 execution mode / stress / policy / watch 요약을 보낸다.
"""
from __future__ import annotations

import argparse
import html
import json
from pathlib import Path

from env_bootstrap import bootstrap_openclaw_env
from report_payload_builder import build_owner_payload, save_payload_state
from report_renderer_telegram_owner import render_owner_morning_message

bootstrap_openclaw_env()

from telegram_notify import notify, notify_plain


STATE_FILE = Path.home() / ".openclaw" / "state" / "reporting" / "telegram_owner_morning.json"


def main() -> int:
    ap = argparse.ArgumentParser(description="Send Telegram owner morning digest")
    ap.add_argument("--dry-run", action="store_true", help="print only")
    ap.add_argument("--dump-payload", default="", help="optional JSON payload output path")
    args = ap.parse_args()

    payload = build_owner_payload(
        report_type="telegram_owner_ops",
        top_candidates=3,
        previous_payload_path=STATE_FILE,
    )
    message = render_owner_morning_message(payload)

    print(message)
    if args.dump_payload:
        dump_path = Path(str(args.dump_payload)).expanduser()
        dump_path.parent.mkdir(parents=True, exist_ok=True)
        dump_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.dry_run:
        return 0

    ok = notify(message)
    if not ok:
        plain = html.unescape(message.replace("<b>", "").replace("</b>", ""))
        notify_plain(plain)

    save_payload_state(payload, STATE_FILE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
