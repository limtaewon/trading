#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path

from env_bootstrap import bootstrap_openclaw_env
from public_daily_report_llm import render_public_report
from public_report_delivery import deliver_public_report
from public_weekly_payload import build_public_weekly_payload

bootstrap_openclaw_env()


STATE_FILE = Path.home() / ".openclaw" / "state" / "reporting" / "telegram_public_weekly_outlook.json"
PROMPT_FILE = Path(__file__).resolve().parent / "prompts" / "report_telegram_public_weekly_outlook_prompt.txt"


def _default_as_of() -> date:
    today = datetime.now().date()
    return today if today.weekday() >= 4 else (today - timedelta(days=today.weekday() + 3))


def main() -> int:
    ap = argparse.ArgumentParser(description="Send public Telegram weekly outlook")
    ap.add_argument("--as-of", default="", help="기준일 YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    as_of = date.fromisoformat(args.as_of) if str(args.as_of).strip() else _default_as_of()
    payload = build_public_weekly_payload(as_of=as_of, report_type="weekly_outlook")
    message, err = render_public_report(
        payload=payload,
        timeout_sec=120,
        prompt_path=str(PROMPT_FILE),
        prompt_env_key="TELEGRAM_PUBLIC_WEEKLY_OUTLOOK_PROMPT_FILE",
        model_env_key="TELEGRAM_PUBLIC_WEEKLY_OUTLOOK_LLM_MODEL",
    )
    if not message:
        raise RuntimeError(f"weekly outlook llm failed: {err}")
    print(message)
    if args.dry_run:
        return 0
    ok, errs = deliver_public_report(message)
    if errs:
        print("[WARN] " + " | ".join(errs))
    if ok:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
