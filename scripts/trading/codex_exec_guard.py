#!/usr/bin/env python3
"""OpenClaw agent execution helper with cache + lock.

기본 경로는 openclaw agent이며, 컨텍스트 초과/세션 만료 등 복구 가능 오류에서는
재시도 후 codex exec 폴백을 선택적으로 사용한다.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Sequence

from llm_model_config import infer_fallback_model, is_spark_like_model, normalize_model_name


DEFAULT_CACHE_TTL_SEC = int(os.environ.get("CODEX_EXEC_CACHE_TTL", "0"))
DEFAULT_CACHE_DIR = os.path.expanduser(os.environ.get("CODEX_EXEC_CACHE_DIR", "~/.openclaw/cache/codex-exec"))
DEFAULT_CACHE_LOCK_WAIT = int(os.environ.get("CODEX_EXEC_CACHE_LOCK_WAIT", "20"))
DEFAULT_OPENCLAW_RETRY_MAX = max(0, int(os.environ.get("OPENCLAW_AGENT_RETRY_MAX", "2")))
DEFAULT_OPENCLAW_RETRY_DELAY_SEC = max(0.0, float(os.environ.get("OPENCLAW_AGENT_RETRY_DELAY_SEC", "1.0")))
DEFAULT_ENABLE_CODEX_EXEC_FALLBACK = os.environ.get("ENABLE_CODEX_EXEC_FALLBACK", "0") == "1"
DEFAULT_CODEX_FALLBACK_ON_ANY_ERROR = os.environ.get("CODEX_FALLBACK_ON_ANY_ERROR", "0") == "1"
DEFAULT_SPARK_FALLBACK_ON_ANY_ERROR = os.environ.get("CODEX_SPARK_FALLBACK_ON_ANY_ERROR", "0") == "1"


RECOVERABLE_OPENCLAW_ERROR_PATTERNS = (
    "ctx max",
    "context max",
    "context limit",
    "context length",
    "context overflow",
    "maximum context",
    "prompt too large",
    "token limit",
    "too many tokens",
    "conversation too long",
    "session expired",
    "session has expired",
    "session not found",
    "invalid session",
    "stale session",
    "429",
    "rate limit",
    "too many requests",
    "quota exceeded",
)


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


def _normalize_fallback_model(model: str) -> str:
    override = os.environ.get("CODEX_FALLBACK_MODEL", "").strip()
    if override:
        return override
    return infer_fallback_model(model)


def _is_recoverable_openclaw_error(msg: str) -> bool:
    text = (msg or "").lower()
    if not text:
        return False
    return any(pat in text for pat in RECOVERABLE_OPENCLAW_ERROR_PATTERNS)


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


def _cache_key_direct_codex(
    prompt: str,
    codex_bin: str,
    model: str,
    workdir: str,
    output_schema_path: Optional[str] = None,
) -> str:
    payload = {
        "backend": "codex_exec",
        "codex_bin": str(Path(codex_bin).resolve()) if codex_bin and Path(codex_bin).exists() else codex_bin,
        "model": model or "",
        "workdir": workdir or "",
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


def _resolve_session_id(prompt: str) -> str:
    explicit = (
        os.environ.get("OPENCLAW_SESSION_ID", "").strip()
        or os.environ.get("OPENCLAW_AGENT_SESSION_ID", "").strip()
    )
    if explicit:
        return explicit

    mode = os.environ.get("OPENCLAW_SESSION_MODE", "ephemeral").strip().lower()
    if mode in {"shared", "sticky", "fixed"}:
        return "openclaw-codex-bridge"

    prefix = os.environ.get("OPENCLAW_SESSION_PREFIX", "openclaw-codex-bridge").strip() or "openclaw-codex-bridge"
    nonce = hashlib.sha1(f"{os.getpid()}:{time.time_ns()}:{len(prompt)}".encode("utf-8")).hexdigest()[:12]
    return f"{prefix}-{nonce}"


def _resolve_codex_fallback_bin(codex_bin: str) -> Optional[str]:
    preferred = os.environ.get("CODEX_FALLBACK_BIN", "").strip()
    candidates: list[str] = []
    if preferred:
        candidates.append(preferred)
    if codex_bin and "openclaw" not in codex_bin.lower():
        candidates.append(codex_bin)
    candidates.extend(
        [
            "/opt/homebrew/bin/codex",
            "/usr/local/bin/codex",
            "/usr/bin/codex",
            "codex",
        ]
    )
    for cand in candidates:
        if os.path.isfile(cand) and os.access(cand, os.X_OK):
            return cand
        found = shutil.which(cand)
        if found:
            return found
    return None


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
    if _is_recoverable_openclaw_error(output):
        raise RuntimeError(f"openclaw recoverable output: {output[:400]}")
    return output


def _run_codex_exec_fallback(
    *,
    prompt: str,
    codex_bin: str,
    model: str,
    workdir: Optional[str],
    timeout_sec: int,
    output_schema_path: Optional[str] = None,
) -> str:
    resolved_bin = _resolve_codex_fallback_bin(codex_bin)
    if not resolved_bin:
        raise RuntimeError("codex exec fallback binary not found")

    with tempfile.NamedTemporaryFile(prefix="codex_last_", suffix=".txt", delete=False) as fp:
        out_path = fp.name

    base_cmd = [
        resolved_bin,
        "exec",
        "--skip-git-repo-check",
        "--dangerously-bypass-approvals-and-sandbox",
        "--output-last-message",
        out_path,
    ]
    fallback_model = _normalize_fallback_model(model)
    if fallback_model:
        base_cmd.extend(["--model", fallback_model])

    def _run_once(use_schema: bool) -> str:
        try:
            Path(out_path).write_text("", encoding="utf-8")
        except Exception:
            pass
        cmd = [*base_cmd]
        if use_schema and output_schema_path and Path(output_schema_path).exists():
            cmd.extend(["--output-schema", output_schema_path])
        cmd.append("-")
        run = subprocess.run(
            cmd,
            input=prompt,
            text=True,
            capture_output=True,
            timeout=max(timeout_sec, int(os.environ.get("CODEX_FALLBACK_TIMEOUT_SEC", "240"))),
            check=False,
            cwd=workdir or None,
        )
        if run.returncode != 0:
            err = (run.stderr or run.stdout or "").strip()
            low = err.lower()
            if "invalid_json_schema" in low or "invalid schema for response_format" in low:
                raise RuntimeError("codex exec fallback invalid_json_schema")
            raise RuntimeError(f"codex exec fallback exited {run.returncode}: {err[:500]}")
        output = ""
        try:
            output = Path(out_path).read_text(encoding="utf-8").strip()
        except Exception:
            output = ""
        if not output:
            output = (run.stdout or "").strip()
        if not output:
            raise RuntimeError("codex exec fallback empty output")
        return output

    try:
        return _run_once(use_schema=True)
    except Exception as exc:
        msg = str(exc).lower()
        schema_err = "invalid_json_schema" in msg or "invalid schema for response_format" in msg
        if output_schema_path and schema_err:
            return _run_once(use_schema=False)
        raise
    finally:
        try:
            Path(out_path).unlink(missing_ok=True)
        except Exception:
            pass


def _run_with_recovery(
    *,
    prompt: str,
    codex_bin: str,
    model: str,
    workdir: Optional[str],
    timeout_sec: int,
    base_args: Sequence[str],
    output_schema_path: Optional[str],
) -> str:
    max_retry = DEFAULT_OPENCLAW_RETRY_MAX
    delay_sec = DEFAULT_OPENCLAW_RETRY_DELAY_SEC
    last_err = ""
    for attempt in range(max_retry + 1):
        session_id = _resolve_session_id(prompt)
        try:
            return _run_openclaw_agent(
                prompt=prompt,
                timeout_sec=timeout_sec,
                session_id=session_id,
                base_args=base_args,
                workdir=workdir,
            )
        except Exception as exc:
            last_err = str(exc)
            recoverable = _is_recoverable_openclaw_error(last_err)
            if recoverable and attempt < max_retry:
                if delay_sec > 0:
                    time.sleep(delay_sec)
                continue
            break

    fallback_on_any = DEFAULT_CODEX_FALLBACK_ON_ANY_ERROR or (
        DEFAULT_SPARK_FALLBACK_ON_ANY_ERROR and is_spark_like_model(model)
    )
    fallback_allowed = DEFAULT_ENABLE_CODEX_EXEC_FALLBACK and (
        fallback_on_any or _is_recoverable_openclaw_error(last_err)
    )
    if fallback_allowed:
        return _run_codex_exec_fallback(
            prompt=prompt,
            codex_bin=codex_bin,
            model=model,
            workdir=workdir,
            timeout_sec=timeout_sec,
            output_schema_path=output_schema_path,
        )
    raise RuntimeError(last_err or "openclaw agent failed")


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
    resolved_openclaw_bin = _resolve_openclaw_bin()
    if not resolved_openclaw_bin:
        if DEFAULT_ENABLE_CODEX_EXEC_FALLBACK:
            return _run_codex_exec_fallback(
                prompt=_inject_schema_hint(prompt, output_schema_path),
                codex_bin=codex_bin,
                model=normalize_model_name(model or ""),
                workdir=workdir or None,
                timeout_sec=timeout_sec,
                output_schema_path=output_schema_path,
            )
        raise RuntimeError("openclaw binary not found")

    normalized_model = normalize_model_name(model or "")
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
                session_id = _resolve_session_id(final_prompt)
                _ = session_id
                output = _run_with_recovery(
                    prompt=final_prompt,
                    codex_bin=codex_bin,
                    model=normalized_model,
                    workdir=workdir or None,
                    timeout_sec=timeout_sec,
                    base_args=base_args_list,
                    output_schema_path=output_schema_path,
                )
                _write_cache(cache_file, output)
        else:
            output = _run_with_recovery(
                prompt=final_prompt,
                codex_bin=codex_bin,
                model=normalized_model,
                workdir=workdir or None,
                timeout_sec=timeout_sec,
                base_args=base_args_list,
                output_schema_path=output_schema_path,
            )

    return output


def run_codex_exec_cached(
    *,
    prompt: str,
    codex_bin: str,
    model: Optional[str],
    workdir: Optional[str],
    timeout_sec: int,
    output_schema_path: Optional[str] = None,
    cache_dir: Optional[str] = None,
    cache_ttl_sec: int = DEFAULT_CACHE_TTL_SEC,
    cache_lock_wait_sec: int = DEFAULT_CACHE_LOCK_WAIT,
) -> str:
    normalized_model = normalize_model_name(model or "")
    final_prompt = _inject_schema_hint(prompt, output_schema_path)

    key = _cache_key_direct_codex(
        prompt=final_prompt,
        codex_bin=codex_bin,
        model=normalized_model,
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
                output = _run_codex_exec_fallback(
                    prompt=final_prompt,
                    codex_bin=codex_bin,
                    model=normalized_model,
                    workdir=workdir or None,
                    timeout_sec=timeout_sec,
                    output_schema_path=output_schema_path,
                )
                _write_cache(cache_file, output)
        else:
            output = _run_codex_exec_fallback(
                prompt=final_prompt,
                codex_bin=codex_bin,
                model=normalized_model,
                workdir=workdir or None,
                timeout_sec=timeout_sec,
                output_schema_path=output_schema_path,
            )

    return output
