#!/usr/bin/env python3
"""공용 텔레그램 전송 모듈 (env 기반).

우선순위:
1) TG_BOT_TOKEN / TG_CHAT_ID
2) TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
3) 미설정 시 전송 스킵 (오류 대신 안내 로그)
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()

try:
    import requests
except ImportError:
    from _requests_compat import requests


def _resolve_creds() -> tuple[str, str]:
    token = (
        os.environ.get("TG_BOT_TOKEN", "").strip()
        or os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    )
    chat_id = (
        os.environ.get("TG_CHAT_ID", "").strip()
        or os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    )
    return token, chat_id


def _load_shared_notify():
    shared = Path.home() / ".openclaw" / "scripts" / "telegram_notify.py"
    if not shared.exists():
        return None
    try:
        spec = importlib.util.spec_from_file_location("openclaw_telegram_notify", shared)
        if not spec or not spec.loader:
            return None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        fn = getattr(mod, "notify", None)
        return fn if callable(fn) else None
    except Exception:
        return None


def _send(text: str, parse_mode: str | None = "HTML") -> bool:
    if not str(text or "").strip():
        return True
    token, chat_id = _resolve_creds()
    if not token or not chat_id:
        shared_notify = _load_shared_notify()
        if shared_notify:
            return bool(shared_notify(str(text)))
        print("[TG] skip: TG_BOT_TOKEN/TG_CHAT_ID not configured")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    chunks: list[str] = []
    msg = str(text)
    while len(msg) > 4000:
        cut = msg.rfind("\n", 0, 4000)
        if cut <= 0:
            cut = 4000
        chunks.append(msg[:cut])
        msg = msg[cut:]
    chunks.append(msg)

    ok = True
    for chunk in chunks:
        payload = {
            "chat_id": chat_id,
            "text": chunk,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code != 200:
                print(f"[TG] send failed: {resp.status_code} {resp.text[:240]}")
                ok = False
        except Exception as e:
            print(f"[TG] error: {e}")
            ok = False
    return ok


def notify(text: str) -> bool:
    return _send(text, "HTML")


def alert(text: str) -> bool:
    return _send(text, "HTML")


def notify_plain(text: str) -> bool:
    return _send(text, None)


if __name__ == "__main__":
    msg = sys.argv[1] if len(sys.argv) > 1 else "telegram notify test"
    print("sent" if notify(msg) else "failed")
