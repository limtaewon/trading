#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

from codex_exec_guard import run_codex_cached
from llm_model_config import resolve_model


PROMPT_FILE = Path(__file__).resolve().parent / "prompts" / "report_telegram_public_daily_longform_prompt.txt"


def _load_prompt(path_override: str = "", env_key: str = "TELEGRAM_PUBLIC_DAILY_PROMPT_FILE", default_path: Path = PROMPT_FILE) -> str:
    env_file = os.environ.get(env_key, "").strip()
    path = Path(path_override).expanduser() if path_override else (Path(env_file).expanduser() if env_file else default_path)
    txt = path.read_text(encoding="utf-8").strip()
    if not txt:
        raise RuntimeError(f"prompt empty: {path}")
    return txt


def _parse_first_json_object(raw: str) -> dict[str, Any] | None:
    txt = str(raw or "").strip()
    if not txt:
        return None
    try:
        obj = json.loads(txt)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def render_public_report(
    payload: dict[str, Any],
    timeout_sec: int = 110,
    prompt_path: str = "",
    prompt_env_key: str = "TELEGRAM_PUBLIC_DAILY_PROMPT_FILE",
    model_env_key: str = "TELEGRAM_PUBLIC_DAILY_LLM_MODEL",
) -> tuple[str, str]:
    codex_bin = os.getenv("CODEX_BIN", os.getenv("OPENCLAW_BIN", "openclaw")).strip() or "openclaw"
    resolved = shutil.which(codex_bin) or codex_bin
    if not shutil.which(resolved) and not Path(resolved).exists():
        return "", f"codex_not_found:{resolved}"

    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "report_text": {"type": "string"},
        },
        "required": ["report_text"],
    }
    prompt = _load_prompt(path_override=prompt_path, env_key=prompt_env_key).rstrip() + "\n\n[INPUT_JSON]\n" + json.dumps(payload, ensure_ascii=False, indent=2)

    schema_path = ""
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as sf:
            schema_path = sf.name
            json.dump(schema, sf, ensure_ascii=False)
        raw = run_codex_cached(
            prompt=prompt,
            codex_bin=resolved,
            model=resolve_model(model_env_key, "CODEX_MODEL"),
            workdir=None,
            timeout_sec=max(40, int(timeout_sec)),
            base_args=["--skip-git-repo-check", "--full-auto"],
            output_schema_path=schema_path,
            cache_dir=os.getenv("CODEX_EXEC_CACHE_DIR", os.path.expanduser("~/.openclaw/cache/codex-exec")),
            cache_ttl_sec=int(os.getenv("TELEGRAM_PUBLIC_DAILY_CACHE_TTL", os.getenv("CODEX_EXEC_CACHE_TTL", "180"))),
            cache_lock_wait_sec=int(os.getenv("CODEX_EXEC_CACHE_LOCK_WAIT", "20")),
        )
        obj = _parse_first_json_object(raw)
        if not obj:
            return "", "llm_json_parse_failed"
        text = str(obj.get("report_text") or "").strip()
        if not text:
            return "", "llm_empty_report_text"
        return text, ""
    except Exception as e:
        return "", f"llm_error:{type(e).__name__}:{e}"
    finally:
        if schema_path:
            try:
                Path(schema_path).unlink(missing_ok=True)
            except Exception:
                pass


def render_public_daily_report(payload: dict[str, Any], timeout_sec: int = 110) -> tuple[str, str]:
    return render_public_report(payload=payload, timeout_sec=timeout_sec)
