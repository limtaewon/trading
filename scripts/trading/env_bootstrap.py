#!/usr/bin/env python3
"""Runtime env bootstrap for trading scripts.

Loads OpenClaw env files so manual execution has the same credentials
as cron-router execution.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse, urlunparse


def _parse_env_line(line: str) -> tuple[str, str] | None:
    s = line.strip()
    if not s or s.startswith("#") or "=" not in s:
        return None
    if s.startswith("export "):
        s = s[len("export ") :].strip()
        if "=" not in s:
            return None
    k, v = s.split("=", 1)
    key = k.strip()
    if not key:
        return None
    val = v.strip()
    if len(val) >= 2 and ((val[0] == "'" and val[-1] == "'") or (val[0] == '"' and val[-1] == '"')):
        val = val[1:-1]
    return key, val


def _load_env_file(path: Path, override: bool) -> int:
    if not path.exists() or not path.is_file():
        return 0
    loaded = 0
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            parsed = _parse_env_line(raw)
            if not parsed:
                continue
            k, v = parsed
            if override or not os.environ.get(k):
                os.environ[k] = v
                loaded += 1
    except Exception:
        return loaded
    return loaded


def bootstrap_openclaw_env(override: bool = False) -> int:
    """Load ~/.openclaw env files. Returns number of keys loaded."""
    home = Path.home()
    explicit = os.getenv("OPENCLAW_ENV_FILE", "").strip()
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit).expanduser())
    candidates.extend(
        [
            home / ".openclaw" / ".env.trading",
            home / ".openclaw" / ".env",
        ]
    )

    total = 0
    seen: set[Path] = set()
    for p in candidates:
        rp = p.resolve() if p.exists() else p
        if rp in seen:
            continue
        seen.add(rp)
        total += _load_env_file(p, override=override)

    # Normalize ClickHouse auth aliases across legacy scripts.
    ch_pass = os.environ.get("CLICKHOUSE_PASS", "")
    ch_password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    if ch_pass and not ch_password:
        os.environ["CLICKHOUSE_PASSWORD"] = ch_pass
    elif ch_password and not ch_pass:
        os.environ["CLICKHOUSE_PASS"] = ch_password

    # Normalize ClickHouse host/url/auth defaults to prevent intermittent auth failures.
    ch_host = (os.environ.get("CLICKHOUSE_HOST", "") or "").strip()
    ch_url = (os.environ.get("CLICKHOUSE_URL", "") or "").strip()
    ch_user = (os.environ.get("CLICKHOUSE_USER", "") or "").strip() or "default"
    ch_pass2 = (
        (os.environ.get("CLICKHOUSE_PASS", "") or "").strip()
        or (os.environ.get("CLICKHOUSE_PASSWORD", "") or "").strip()
        or "trading"
    )

    os.environ["CLICKHOUSE_USER"] = ch_user
    os.environ["CLICKHOUSE_PASS"] = ch_pass2
    os.environ["CLICKHOUSE_PASSWORD"] = ch_pass2

    base = ch_url or ch_host or "http://localhost:8123"
    p = urlparse(base)
    scheme = p.scheme or "http"
    hostname = p.hostname or "localhost"
    netloc = hostname
    if p.port:
        netloc = f"{hostname}:{p.port}"

    # Keep explicit URL userinfo if present; otherwise compose from USER/PASS.
    if p.username is not None:
        userinfo = f"{p.username}:{p.password or ''}@"
        netloc = f"{userinfo}{netloc}"
    else:
        userinfo = f"{ch_user}:{ch_pass2}@"
        netloc = f"{userinfo}{netloc}"

    path = p.path or "/"
    if not path.startswith("/"):
        path = "/" + path
    if path == "":
        path = "/"
    normalized_url = urlunparse((scheme, netloc, path, p.params, p.query, p.fragment))
    normalized_host = urlunparse((scheme, f"{hostname}:{p.port}" if p.port else hostname, path, p.params, p.query, p.fragment))
    os.environ["CLICKHOUSE_URL"] = normalized_url
    os.environ["CLICKHOUSE_HOST"] = normalized_host

    # Global LLM fallback policy:
    # if primary model is spark/codex-openai family and fallback model is unset,
    # enforce gpt-5.3-codex as the first fallback target.
    codex_model = (os.environ.get("CODEX_MODEL", "") or "").strip()
    fallback_model = (os.environ.get("CODEX_FALLBACK_MODEL", "") or "").strip()
    if not fallback_model:
        if ("codex-spark" in codex_model) or codex_model.startswith("openai-codex/"):
            os.environ["CODEX_FALLBACK_MODEL"] = "gpt-5.3-codex"
        elif not codex_model:
            os.environ["CODEX_FALLBACK_MODEL"] = "gpt-5.3-codex"

    return total
