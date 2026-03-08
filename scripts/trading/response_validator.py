#!/usr/bin/env python3
"""Shared validation for trading LLM responses.

The main brain path and executor both consume the same response object.
This module keeps fail-closed validation logic in one place so the two
entrypoints cannot drift quietly.
"""
from __future__ import annotations

import re
from typing import Any

REGIME_ACTIONS = {"aggressive", "normal", "cautious", "defensive"}
ORDER_ACTIONS = {"BUY", "SELL"}
ORDER_TYPES = {"LIMIT", "MARKET"}
EVENT_HORIZONS = {"intraday", "1d", "1-3d", "1w", "1-2w", "2w+"}
REQUIRED_ROOT_KEYS = {
    "timestamp",
    "market_assessment",
    "regime_action",
    "orders",
    "risk_targets",
    "watch_list",
    "portfolio_advice",
    "self_evaluation",
    "next_focus",
}
ALLOWED_ROOT_KEYS = set(REQUIRED_ROOT_KEYS) | {"execution_mode", "allowed_universe", "playbook_summary", "missing_fields"}
ORDER_KEYS = {
    "action",
    "ticker",
    "ticker_name",
    "quantity",
    "order_type",
    "price",
    "confidence",
    "reasoning",
    "event_signature",
    "strategy_family",
    "playbook_id",
    "priority",
    "order_role",
    "close_only",
    "expected_holding_window",
    "event_type",
    "time_horizon",
    "lag_hours",
    "channels",
    "thesis_path",
    "evidence_refs",
    "evidence_urls",
    "invalidation",
}
RISK_TARGET_KEYS = {
    "ticker",
    "ticker_name",
    "take_profit_pct",
    "stop_loss_pct",
    "confidence",
    "time_horizon",
    "reasoning",
    "invalidation",
}
WATCH_ITEM_KEYS = {"ticker", "ticker_name", "reason"}
EXECUTION_MODES = {"normal", "shock", "recovery", "close_only"}


def _is_nonempty_text(v: Any) -> bool:
    return isinstance(v, str) and bool(v.strip())


def _is_six_digit_ticker(v: Any) -> bool:
    return isinstance(v, str) and bool(re.fullmatch(r"\d{6}", v.strip()))


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _append(errors: list[str], msg: str, limit: int) -> None:
    if len(errors) < limit:
        errors.append(msg)


def validate_trading_response(
    data: Any,
    *,
    execution_mode_state: dict[str, Any] | None = None,
    error_limit: int = 50,
) -> list[str]:
    errors: list[str] = []

    if not isinstance(data, dict):
        return ["root must be an object"]

    missing = [k for k in REQUIRED_ROOT_KEYS if k not in data]
    if missing:
        _append(errors, f"missing root keys: {', '.join(sorted(missing))}", error_limit)

    extra = sorted(set(data.keys()) - ALLOWED_ROOT_KEYS)
    if extra:
        _append(errors, f"unexpected root keys: {', '.join(extra)}", error_limit)

    if not _is_nonempty_text(data.get("timestamp")):
        _append(errors, "timestamp must be a non-empty string", error_limit)
    if not _is_nonempty_text(data.get("market_assessment")):
        _append(errors, "market_assessment must be a non-empty string", error_limit)
    execution_mode = data.get("execution_mode")
    if execution_mode is not None and execution_mode not in EXECUTION_MODES:
        _append(errors, f"execution_mode must be one of {sorted(EXECUTION_MODES)}", error_limit)
    allowed_universe = data.get("allowed_universe")
    if allowed_universe is not None and allowed_universe not in {"watchlist", "shock_core", "recovery_core"}:
        _append(errors, "allowed_universe must be one of ['recovery_core', 'shock_core', 'watchlist']", error_limit)
    playbook_summary = data.get("playbook_summary")
    if playbook_summary is not None and not _is_nonempty_text(playbook_summary):
        _append(errors, "playbook_summary must be a non-empty string when present", error_limit)

    regime_action = data.get("regime_action")
    if regime_action not in REGIME_ACTIONS:
        _append(errors, f"regime_action must be one of {sorted(REGIME_ACTIONS)}", error_limit)

    for key in ("portfolio_advice", "self_evaluation", "next_focus"):
        if not _is_nonempty_text(data.get(key)):
            _append(errors, f"{key} must be a non-empty string", error_limit)

    orders = data.get("orders")
    if not isinstance(orders, list):
        _append(errors, "orders must be an array", error_limit)
        orders = []
    state = execution_mode_state if isinstance(execution_mode_state, dict) else {}
    effective_mode = str(state.get("execution_mode", execution_mode or "normal") or "normal")
    if effective_mode not in EXECUTION_MODES:
        effective_mode = "normal"
    effective_close_only = bool(state.get("close_only", effective_mode == "close_only"))
    allowed_tickers_raw = state.get("allowed_tickers", [])
    allowed_tickers = {
        str(v).strip()
        for v in allowed_tickers_raw
        if isinstance(v, (str, int)) and _is_six_digit_ticker(str(v).strip())
    }
    for idx, order in enumerate(orders):
        prefix = f"orders[{idx}]"
        if not isinstance(order, dict):
            _append(errors, f"{prefix} must be an object", error_limit)
            continue
        extra = sorted(set(order.keys()) - ORDER_KEYS)
        if extra:
            _append(errors, f"{prefix} unexpected keys: {', '.join(extra)}", error_limit)
        for key in ("action", "ticker", "ticker_name", "quantity", "order_type", "confidence", "reasoning"):
            if key not in order:
                _append(errors, f"{prefix}.{key} is required", error_limit)
        if order.get("action") not in ORDER_ACTIONS:
            _append(errors, f"{prefix}.action must be BUY or SELL", error_limit)
        if not _is_six_digit_ticker(order.get("ticker")):
            _append(errors, f"{prefix}.ticker must be 6 digits", error_limit)
        if not _is_nonempty_text(order.get("ticker_name")):
            _append(errors, f"{prefix}.ticker_name must be non-empty", error_limit)
        quantity = order.get("quantity")
        if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity < 1:
            _append(errors, f"{prefix}.quantity must be an integer >= 1", error_limit)
        if order.get("order_type") not in ORDER_TYPES:
            _append(errors, f"{prefix}.order_type must be LIMIT or MARKET", error_limit)
        price = order.get("price")
        if "price" in order and not _is_number(price):
            _append(errors, f"{prefix}.price must be numeric when present", error_limit)
        if order.get("order_type") == "LIMIT":
            if not _is_number(price) or float(price) <= 0:
                _append(errors, f"{prefix}.price must be > 0 for LIMIT orders", error_limit)
        confidence = order.get("confidence")
        if not _is_number(confidence) or float(confidence) < 0 or float(confidence) > 1:
            _append(errors, f"{prefix}.confidence must be between 0 and 1", error_limit)
        if not _is_nonempty_text(order.get("reasoning")):
            _append(errors, f"{prefix}.reasoning must be non-empty", error_limit)
        if "time_horizon" in order and order.get("time_horizon") not in EVENT_HORIZONS:
            _append(errors, f"{prefix}.time_horizon must be one of {sorted(EVENT_HORIZONS)}", error_limit)
        if "expected_holding_window" in order and order.get("expected_holding_window") not in EVENT_HORIZONS:
            _append(errors, f"{prefix}.expected_holding_window must be one of {sorted(EVENT_HORIZONS)}", error_limit)
        if "lag_hours" in order:
            lag = order.get("lag_hours")
            if not isinstance(lag, int) or isinstance(lag, bool) or lag < 0 or lag > 336:
                _append(errors, f"{prefix}.lag_hours must be an integer between 0 and 336", error_limit)
        if "evidence_refs" in order and not isinstance(order.get("evidence_refs"), list):
            _append(errors, f"{prefix}.evidence_refs must be an array", error_limit)
        if "evidence_urls" in order and not isinstance(order.get("evidence_urls"), list):
            _append(errors, f"{prefix}.evidence_urls must be an array", error_limit)
        if "priority" in order:
            pr = order.get("priority")
            if not isinstance(pr, int) or isinstance(pr, bool) or pr < 1 or pr > 10:
                _append(errors, f"{prefix}.priority must be an integer between 1 and 10", error_limit)
        if "order_role" in order and order.get("order_role") not in {"new_entry", "reduce", "forced_exit", "hedge"}:
            _append(errors, f"{prefix}.order_role has invalid value", error_limit)
        if "close_only" in order and not isinstance(order.get("close_only"), bool):
            _append(errors, f"{prefix}.close_only must be boolean", error_limit)
        if "strategy_family" in order and order.get("strategy_family") not in {
            "stock_selection",
            "index_etf",
            "shock_hedge",
            "shock_rebound",
            "core_defensive",
            "forced_exit",
        }:
            _append(errors, f"{prefix}.strategy_family has invalid value", error_limit)
        strategy_family = str(order.get("strategy_family", "") or "")
        action = str(order.get("action", "") or "")
        ticker = str(order.get("ticker", "") or "")
        order_close_only = bool(order.get("close_only", False))
        if strategy_family == "forced_exit" and action != "SELL":
            _append(errors, f"{prefix}.forced_exit must use SELL action", error_limit)
        if order.get("order_role") == "forced_exit" and action != "SELL":
            _append(errors, f"{prefix}.order_role forced_exit must use SELL action", error_limit)
        if (effective_close_only or order_close_only) and action != "SELL":
            _append(errors, f"{prefix} close_only mode cannot contain BUY", error_limit)
        if effective_mode == "shock" and action == "BUY" and strategy_family not in {"shock_hedge", "index_etf"}:
            _append(errors, f"{prefix} BUY must use shock_hedge or index_etf in shock mode", error_limit)
        if effective_mode == "recovery" and action == "BUY" and strategy_family not in {"shock_rebound", "index_etf", "core_defensive"}:
            _append(errors, f"{prefix} BUY has invalid strategy_family for recovery mode", error_limit)
        if effective_mode in {"shock", "recovery"} and action == "BUY" and allowed_tickers and ticker not in allowed_tickers:
            _append(errors, f"{prefix}.ticker is outside allowed_tickers for {effective_mode} mode", error_limit)

        if order.get("action") == "BUY":
            if order.get("time_horizon") not in EVENT_HORIZONS:
                _append(errors, f"{prefix}.time_horizon is required for BUY", error_limit)
            if not _is_nonempty_text(order.get("thesis_path")):
                _append(errors, f"{prefix}.thesis_path is required for BUY", error_limit)
            refs = order.get("evidence_refs")
            urls = order.get("evidence_urls")
            refs_ok = isinstance(refs, list) and len(refs) > 0
            urls_ok = isinstance(urls, list) and len(urls) > 0
            if not (refs_ok or urls_ok):
                _append(errors, f"{prefix} BUY requires evidence_refs or evidence_urls", error_limit)

    risk_targets = data.get("risk_targets")
    if not isinstance(risk_targets, list):
        _append(errors, "risk_targets must be an array", error_limit)
        risk_targets = []
    for idx, row in enumerate(risk_targets):
        prefix = f"risk_targets[{idx}]"
        if not isinstance(row, dict):
            _append(errors, f"{prefix} must be an object", error_limit)
            continue
        extra = sorted(set(row.keys()) - RISK_TARGET_KEYS)
        if extra:
            _append(errors, f"{prefix} unexpected keys: {', '.join(extra)}", error_limit)
        for key in ("ticker", "ticker_name", "take_profit_pct", "stop_loss_pct", "confidence", "time_horizon", "reasoning"):
            if key not in row:
                _append(errors, f"{prefix}.{key} is required", error_limit)
        if not _is_six_digit_ticker(row.get("ticker")):
            _append(errors, f"{prefix}.ticker must be 6 digits", error_limit)
        if not _is_nonempty_text(row.get("ticker_name")):
            _append(errors, f"{prefix}.ticker_name must be non-empty", error_limit)
        tp = row.get("take_profit_pct")
        sl = row.get("stop_loss_pct")
        if not _is_number(tp) or float(tp) <= 0 or float(tp) > 0.6:
            _append(errors, f"{prefix}.take_profit_pct must be > 0 and <= 0.6", error_limit)
        if not _is_number(sl) or float(sl) >= 0 or float(sl) < -0.6:
            _append(errors, f"{prefix}.stop_loss_pct must be < 0 and >= -0.6", error_limit)
        conf = row.get("confidence")
        if not _is_number(conf) or float(conf) < 0 or float(conf) > 1:
            _append(errors, f"{prefix}.confidence must be between 0 and 1", error_limit)
        if row.get("time_horizon") not in EVENT_HORIZONS:
            _append(errors, f"{prefix}.time_horizon must be one of {sorted(EVENT_HORIZONS)}", error_limit)
        if not _is_nonempty_text(row.get("reasoning")):
            _append(errors, f"{prefix}.reasoning must be non-empty", error_limit)

    watch_list = data.get("watch_list")
    if not isinstance(watch_list, list):
        _append(errors, "watch_list must be an array", error_limit)
        watch_list = []
    for idx, row in enumerate(watch_list):
        prefix = f"watch_list[{idx}]"
        if not isinstance(row, dict):
            _append(errors, f"{prefix} must be an object", error_limit)
            continue
        extra = sorted(set(row.keys()) - WATCH_ITEM_KEYS)
        if extra:
            _append(errors, f"{prefix} unexpected keys: {', '.join(extra)}", error_limit)
        if not _is_six_digit_ticker(row.get("ticker")):
            _append(errors, f"{prefix}.ticker must be 6 digits", error_limit)
        if not _is_nonempty_text(row.get("ticker_name")):
            _append(errors, f"{prefix}.ticker_name must be non-empty", error_limit)
        if not _is_nonempty_text(row.get("reason")):
            _append(errors, f"{prefix}.reason must be non-empty", error_limit)

    return errors
