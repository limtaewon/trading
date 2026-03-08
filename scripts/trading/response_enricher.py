#!/usr/bin/env python3
"""Shared enrichment for trading responses before validation/execution."""
from __future__ import annotations

from copy import deepcopy
from typing import Any


def _mode_name(state: dict[str, Any] | None) -> str:
    if not isinstance(state, dict):
        return "normal"
    mode = str(state.get("execution_mode", "normal") or "normal").strip().lower()
    return mode if mode in {"normal", "shock", "recovery", "close_only"} else "normal"


def _infer_strategy_family(order: dict[str, Any], mode: str) -> str:
    action = str(order.get("action", "") or "").upper()
    existing = str(order.get("strategy_family", "") or "").strip()
    if existing:
        return existing
    event_type = str(order.get("event_type", "") or "").strip().lower()
    if action == "SELL":
        if event_type in {"hard_exit", "dynamic_exit"}:
            return "forced_exit"
        return "core_defensive"
    if mode == "shock":
        return "shock_hedge"
    if mode == "recovery":
        return "shock_rebound"
    return "stock_selection"


def _infer_order_role(order: dict[str, Any], family: str) -> str:
    action = str(order.get("action", "") or "").upper()
    existing = str(order.get("order_role", "") or "").strip()
    if existing:
        return existing
    if action == "SELL":
        if family == "forced_exit":
            return "forced_exit"
        return "reduce"
    if family in {"shock_hedge", "index_etf"}:
        return "hedge"
    return "new_entry"


def _infer_priority(order: dict[str, Any], family: str) -> int:
    raw = order.get("priority")
    if isinstance(raw, int) and not isinstance(raw, bool):
        return max(1, min(10, raw))
    action = str(order.get("action", "") or "").upper()
    if action == "SELL" and family == "forced_exit":
        return 9
    if action == "SELL":
        return 7
    if family in {"shock_hedge", "index_etf"}:
        return 6
    return 5


def enrich_trading_response(
    data: Any,
    *,
    execution_mode_state: dict[str, Any] | None = None,
    source_label: str = "brain",
) -> Any:
    if not isinstance(data, dict):
        return data
    state = execution_mode_state if isinstance(execution_mode_state, dict) else {}
    mode = _mode_name(state)
    allowed_universe = str(state.get("allowed_universe", "watchlist") or "watchlist")
    close_only = bool(state.get("close_only", mode == "close_only"))

    enriched = deepcopy(data)
    enriched.setdefault("execution_mode", mode)
    enriched.setdefault("allowed_universe", allowed_universe)
    enriched.setdefault("playbook_summary", f"{source_label}:{mode}:{allowed_universe}")

    orders = enriched.get("orders", [])
    if not isinstance(orders, list):
        return enriched

    for idx, raw in enumerate(orders):
        if not isinstance(raw, dict):
            continue
        family = _infer_strategy_family(raw, mode)
        raw.setdefault("strategy_family", family)
        raw.setdefault("playbook_id", f"{source_label}_{mode}")
        raw.setdefault("priority", _infer_priority(raw, family))
        raw.setdefault("order_role", _infer_order_role(raw, family))
        raw.setdefault("close_only", close_only)
        horizon = str(raw.get("time_horizon", "") or "").strip()
        if horizon:
            raw.setdefault("expected_holding_window", horizon)
        elif raw.get("action") == "SELL":
            raw.setdefault("expected_holding_window", "intraday")
        orders[idx] = raw
    enriched["orders"] = orders
    return enriched
