#!/usr/bin/env python3
"""Shared LLM model defaults and normalization helpers for trading scripts."""

from __future__ import annotations

import os


DEFAULT_PRIMARY_MODEL = "gpt-5.4"
DEFAULT_FALLBACK_MODEL = "gpt-5.4"


def default_primary_model() -> str:
    for key in ("OPENCLAW_PRIMARY_MODEL", "OPENCLAW_MODEL_DEFAULT"):
        value = (os.getenv(key, "") or "").strip()
        if value:
            return value
    return DEFAULT_PRIMARY_MODEL


def normalize_model_name(model: str) -> str:
    primary = default_primary_model()
    m = (model or "").strip()
    aliases = {
        "gpt": primary,
        "openai/gpt-5.2": primary,
        "openai/gpt-5.3": primary,
        "openai/gpt-5.4": primary,
        "gpt-5.3-codex-spark": primary,
        "gpt-5.4-codex-spark": primary,
        "gpt-5.4-codex": primary,
        "openai-codex/gpt-5.2": primary,
        "openai-codex/gpt-5.3": primary,
        "openai-codex/gpt-5.4": primary,
        "openai-codex/gpt-5.3-codex-spark": primary,
        "openai-codex/gpt-5.4-codex-spark": primary,
        "gpt-5-codex": primary,
    }
    return aliases.get(m, m)


def infer_fallback_model(primary_model: str = "") -> str:
    override = (os.getenv("OPENCLAW_FALLBACK_MODEL", "") or "").strip()
    if override:
        return override
    primary = normalize_model_name(primary_model or "")
    if not primary:
        return DEFAULT_FALLBACK_MODEL
    if "codex-spark" in primary or primary.startswith("openai-codex/"):
        return DEFAULT_FALLBACK_MODEL
    return primary


def resolve_model(*env_keys: str, default: str = "") -> str:
    for key in env_keys:
        value = (os.getenv(key, "") or "").strip()
        if value:
            return normalize_model_name(value)
    if default:
        return normalize_model_name(default)
    return default_primary_model()


def is_spark_like_model(model: str) -> bool:
    m = normalize_model_name(model).lower()
    if not m:
        return False
    return ("codex-spark" in m) or m.startswith("openai-codex/")
