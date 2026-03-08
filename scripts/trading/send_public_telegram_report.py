#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from env_bootstrap import bootstrap_openclaw_env
from public_report_delivery import deliver_public_report
from report_payload_builder import build_public_payload, save_payload_state
from public_daily_report_llm import render_public_daily_report
from report_renderer_telegram_public import render_public_market_message

bootstrap_openclaw_env()


STATE_FILE = Path.home() / ".openclaw" / "state" / "reporting" / "telegram_public_market.json"


def main() -> int:
    ap = argparse.ArgumentParser(description="Send public Telegram market explanation")
    ap.add_argument("--dry-run", action="store_true", help="print only")
    ap.add_argument("--top-candidates", type=int, default=2)
    ap.add_argument("--decision-id", default="")
    ap.add_argument("--dump-payload", default="", help="optional JSON payload output path")
    args = ap.parse_args()

    payload = build_public_payload(
        top_candidates=max(1, int(args.top_candidates)),
        previous_payload_path=STATE_FILE,
        decision_id_override=str(args.decision_id).strip(),
    )
    message, llm_err = render_public_daily_report(payload)
    if not message:
        message = render_public_market_message(payload)
        if not args.dry_run:
            print(f"[WARN] public daily LLM fallback: {llm_err}")
    print(message)

    if args.dump_payload:
        dump_path = Path(str(args.dump_payload)).expanduser()
        dump_path.parent.mkdir(parents=True, exist_ok=True)
        dump_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.dry_run:
        return 0

    ok, errs = deliver_public_report(message)
    if errs:
        print("[WARN] " + " | ".join(errs))
    if ok:
        save_payload_state(payload, STATE_FILE)
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
