#!/usr/bin/env python3
"""OpenClaw agent execution helper with cache + lock.

기존 codex exec fallback 경로는 제거하고 openclaw agent 단일 경로만 유지한다.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Optional, Sequence


DEFAULT_CACHE_TTL_SEC = int(os.environ.get("CODEX_EXEC_CACHE_TTL", "0"))
DEFAULT_CACHE_DIR = os.path.expanduser(os.environ.get("CODEX_EXEC_CACHE_DIR", "~/.openclaw/cache/codex-exec"))
DEFAULT_CACHE_LOCK_WAIT = int(os.environ.get("CODEX_EXEC_CACHE_LOCK_WAIT", "20"))


def _resolve_openclaw_bin() -> Optional[str]:
    candidate = os.environ.get("OPENCLAW_BIN", "")
    if candidate:
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
        found = shutil.which(candidate)
        if found:
            return found

    for cand in (
        "/usr/local/bin/openclaw",
        "/usr/bin/openclaw",
        os.path.expanduser("~/.npm-global/bin/openclaw"),
        "/opt/homebrew/bin/openclaw",
        "openclaw",
    ):
        found = shutil.which(cand)
        if found:
            return found
    return None


def _normalize_model_name(model: str) -> str:
    m = (model or "").strip()
    aliases = {
        "openai/gpt-5.2": "gpt-5.3-codex-spark",
        "openai/gpt-5.3": "gpt-5.3-codex-spark",
        "gpt": "gpt-5.3-codex-spark",
        "openai-codex/gpt-5.2": "gpt-5.3-codex-spark",
        "openai-codex/gpt-5.3": "gpt-5.3-codex-spark",
        "openai-codex/gpt-5.3-codex-spark": "gpt-5.3-codex-spark",
    }
    return aliases.get(m, m)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _schema_signature(path: Optional[str]) -> str:
    if not path:
        return ""
    try:
        data = Path(path).read_bytes()
    except Exception:
        return _sha256_text(str(path))
    return hashlib.sha256(data).hexdigest()


def _cache_key(
    prompt: str,
    openclaw_bin: str,
    model: str,
    base_args: Sequence[str],
    workdir: str,
    output_schema_path: Optional[str] = None,
) -> str:
    payload = {
        "backend": "openclaw",
        "openclaw_bin": str(Path(openclaw_bin).resolve()) if openclaw_bin else "",
        "model": model or "",
        "workdir": workdir or "",
        "base_args": list(base_args),
        "schema": _schema_signature(output_schema_path),
        "prompt": prompt,
    }
    return _sha256_text(json.dumps(payload, sort_keys=True, ensure_ascii=False))


def _cache_file(cache_root: Path, key: str) -> Path:
    return cache_root / f"{key}.json"


def _read_cache(cache_path: Path, ttl_sec: int) -> Optional[str]:
    if ttl_sec <= 0 or not cache_path.exists():
        return None
    try:
        meta = json.loads(cache_path.read_text(encoding="utf-8"))
        if not isinstance(meta, dict):
            return None
        created = float(meta.get("created_at", 0.0))
        if ttl_sec > 0 and (time.time() - created) > ttl_sec:
            return None
        output = meta.get("output")
        if isinstance(output, str) and output.strip():
            return output
    except Exception:
        return None
    return None


def _write_cache(cache_path: Path, output: str) -> None:
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps({"created_at": time.time(), "output": output}, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass


def _extract_openclaw_reply(raw: str) -> str:
    txt = (raw or "").strip()
    if not txt:
        return ""
    try:
        obj = json.loads(txt)
    except Exception:
        return txt
    if not isinstance(obj, dict):
        return txt

    result = obj.get("result")
    if isinstance(result, dict):
        payloads = result.get("payloads")
        if isinstance(payloads, list) and payloads:
            first = payloads[0]
            if isinstance(first, dict):
                t = first.get("text")
                if isinstance(t, str) and t.strip():
                    return t.strip()
        for k in ("output", "summary"):
            v = result.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

    return txt


def _inject_schema_hint(prompt: str, output_schema_path: Optional[str]) -> str:
    if not output_schema_path:
        return prompt
    try:
        schema_text = Path(output_schema_path).read_text(encoding="utf-8")
    except Exception:
        return prompt

    hint = (
        "\n\n[OUTPUT_SCHEMA]\n"
        "아래 JSON Schema를 가능한 엄격하게 준수해 응답하라.\n"
        f"{schema_text}\n"
    )
    return prompt + hint


def _run_openclaw_agent(
    prompt: str,
    timeout_sec: int,
    session_id: str,
    base_args: Sequence[str],
    workdir: Optional[str] = None,
) -> str:
    resolved_bin = _resolve_openclaw_bin()
    if not resolved_bin:
        raise RuntimeError("openclaw binary not found")

    cmd = [resolved_bin, "agent", "--json", "--session-id", session_id, "--message", prompt]
    if any(a == "--local" for a in base_args):
        cmd.append("--local")
    agent_id = os.environ.get("OPENCLAW_AGENT_ID", "").strip()
    if agent_id:
        cmd.extend(["--agent", agent_id])
    thinking = os.environ.get("OPENCLAW_AGENT_THINKING", "").strip()
    if thinking:
        cmd.extend(["--thinking", thinking])

    run = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        timeout=timeout_sec + 20,
        check=False,
        cwd=workdir or None,
    )
    if run.returncode != 0:
        err = (run.stderr or run.stdout or "").strip()
        raise RuntimeError(f"openclaw agent exited {run.returncode}: {err[:400]}")
    raw = (run.stdout or "").strip()
    if not raw:
        raise RuntimeError("openclaw empty output")
    output = _extract_openclaw_reply(raw)
    if not output:
        raise RuntimeError("openclaw parsed empty output")
    return output


@contextlib.contextmanager
def _file_lock(lock_path: Path, wait_sec: int = DEFAULT_CACHE_LOCK_WAIT):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = open(lock_path, "a+")
    acquired = False
    start = time.time()
    try:
        while True:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except BlockingIOError:
                if wait_sec > 0 and (time.time() - start) >= wait_sec:
                    break
                time.sleep(0.2)
        yield acquired
    finally:
        if acquired:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
        try:
            lock_file.close()
        except Exception:
            pass


def run_codex_cached(
    *,
    prompt: str,
    codex_bin: str,
    model: Optional[str],
    workdir: Optional[str],
    timeout_sec: int,
    base_args: Sequence[str],
    output_schema_path: Optional[str] = None,
    cache_dir: Optional[str] = None,
    cache_ttl_sec: int = DEFAULT_CACHE_TTL_SEC,
    cache_lock_wait_sec: int = DEFAULT_CACHE_LOCK_WAIT,
) -> str:
    # codex_bin 파라미터는 하위호환 유지용. 실행은 openclaw agent만 사용한다.
    _ = codex_bin

    resolved_openclaw_bin = _resolve_openclaw_bin()
    if not resolved_openclaw_bin:
        raise RuntimeError("openclaw binary not found")

    normalized_model = _normalize_model_name(model or "")
    base_args_list: List[str] = [*(base_args or [])]

    final_prompt = _inject_schema_hint(prompt, output_schema_path)

    key = _cache_key(
        prompt=final_prompt,
        openclaw_bin=resolved_openclaw_bin,
        model=normalized_model,
        base_args=base_args_list,
        workdir=workdir or "",
        output_schema_path=output_schema_path,
    )

    root = Path(cache_dir or DEFAULT_CACHE_DIR)
    cache_root = root / "cache"
    lock_root = root / "locks"
    cache_file = _cache_file(cache_root, key)

    if cache_ttl_sec > 0:
        cached = _read_cache(cache_file, ttl_sec=cache_ttl_sec)
        if cached is not None:
            return cached

    output = ""
    with _file_lock(lock_root / f"{key}.lock", wait_sec=cache_lock_wait_sec):
        if cache_ttl_sec > 0:
            cached = _read_cache(cache_file, ttl_sec=cache_ttl_sec)
            if cached is not None:
                output = cached
            else:
                session_id = (
                    os.environ.get("OPENCLAW_SESSION_ID", "").strip()
                    or os.environ.get("OPENCLAW_AGENT_SESSION_ID", "").strip()
                    or "openclaw-codex-bridge"
                )
                output = _run_openclaw_agent(
                    prompt=final_prompt,
                    timeout_sec=timeout_sec,
                    session_id=session_id,
                    base_args=base_args_list,
                    workdir=workdir or None,
                )
                _write_cache(cache_file, output)
        else:
            session_id = (
                os.environ.get("OPENCLAW_SESSION_ID", "").strip()
                or os.environ.get("OPENCLAW_AGENT_SESSION_ID", "").strip()
                or "openclaw-codex-bridge"
            )
            output = _run_openclaw_agent(
                prompt=final_prompt,
                timeout_sec=timeout_sec,
                session_id=session_id,
                base_args=base_args_list,
                workdir=workdir or None,
            )

    return output
