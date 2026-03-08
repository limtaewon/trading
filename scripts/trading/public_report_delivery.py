#!/usr/bin/env python3
from __future__ import annotations

import os
from typing import Any

try:
    import requests
except ImportError:
    from _requests_compat import requests

from telegram_notify import notify_public_plain


def resolve_dooray_webhooks() -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in (
        os.environ.get("DOORAY_WEBHOOK_URL", "").strip(),
        os.environ.get("DOORAY_WEBHOOK_URL_EXTRA", "").strip(),
    ):
        if raw and raw not in seen:
            out.append(raw)
            seen.add(raw)
    extra = os.environ.get("DOORAY_WEBHOOK_URLS", "").strip()
    if extra:
        for token in extra.split(","):
            url = str(token or "").strip()
            if url and url not in seen:
                out.append(url)
                seen.add(url)
    return out


def post_text_to_webhooks(webhooks: list[str], text: str, timeout_sec: int = 10) -> tuple[int, list[str]]:
    ok = 0
    errs: list[str] = []
    for i, url in enumerate(webhooks, 1):
        try:
            resp = requests.post(url, json={"text": text}, timeout=timeout_sec)
            resp.raise_for_status()
            ok += 1
        except Exception as e:
            errs.append(f"[dooray:{i}] {type(e).__name__}: {e}")
    return ok, errs


def deliver_public_report(text: str) -> tuple[bool, list[str]]:
    errs: list[str] = []
    tg_ok = bool(notify_public_plain(text))
    if not tg_ok:
        errs.append("[telegram] send failed")

    webhooks = resolve_dooray_webhooks()
    dooray_ok = True
    if webhooks:
        ok_count, dooray_errs = post_text_to_webhooks(webhooks, text, timeout_sec=10)
        dooray_ok = ok_count > 0 and len(dooray_errs) < len(webhooks)
        errs.extend(dooray_errs)
    else:
        errs.append("[dooray] no webhook configured")
        dooray_ok = False

    return tg_ok and dooray_ok, errs
