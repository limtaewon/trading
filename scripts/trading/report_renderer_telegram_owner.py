#!/usr/bin/env python3
from __future__ import annotations

import html
from typing import Any


MODE_LABELS = {
    "normal": "normal",
    "defensive": "defensive",
    "shock": "shock",
    "recovery": "recovery",
    "close_only": "close_only",
}

SESSION_LABELS = {
    "WEEKEND_CLOSED": "weekend_closed",
    "PREMARKET": "premarket",
    "REGULAR": "regular",
    "AFTERMARKET": "aftermarket",
}


def _ctx(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    return value if isinstance(value, dict) else {}


def _short_list(items: list[str], max_items: int = 3, empty: str = "-") -> str:
    vals = [str(x).strip() for x in items if str(x).strip()]
    if not vals:
        return empty
    if len(vals) <= max_items:
        return ", ".join(vals)
    return ", ".join(vals[:max_items]) + f" +{len(vals) - max_items}"


def _fmt_signed_pct(value: float) -> str:
    return f"{value:+.2f}%"


def _fmt_num(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}"


def _candidate_summary(candidate: dict[str, Any]) -> str:
    name = str(candidate.get("name") or candidate.get("ticker") or "-").strip()
    action = str(candidate.get("current_action") or "HOLD").strip().upper()
    grade = str(candidate.get("signal_grade") or "-").strip()
    return f"{name}({action}/{grade})"


def render_owner_morning_message(payload: dict[str, Any]) -> str:
    market_context = _ctx(payload, "market_context")
    mode_context = _ctx(payload, "mode_context")
    candidate_context = _ctx(payload, "candidate_context")
    change_context = _ctx(payload, "change_context")
    guidance_context = _ctx(payload, "guidance_context")
    ops_context = _ctx(payload, "ops_context")

    index = _ctx(market_context, "index")
    macro = _ctx(market_context, "macro")
    market_stress = _ctx(market_context, "market_stress")
    market_phase = _ctx(market_context, "market_phase")
    metrics = _ctx(ops_context, "metrics")
    mode_triggers = _ctx(mode_context, "mode_change_triggers")
    top_candidates = candidate_context.get("top_candidates", [])
    changed = change_context.get("what_changed_today", [])
    bullish = mode_triggers.get("bullish_reenable", [])

    mode = MODE_LABELS.get(str(mode_context.get("execution_mode") or "normal"), str(mode_context.get("execution_mode") or "normal"))
    posture = str(mode_context.get("action_posture") or "normal")
    session = SESSION_LABELS.get(str(mode_context.get("session") or "UNKNOWN"), str(mode_context.get("session") or "UNKNOWN").lower())

    lines: list[str] = []
    lines.append("📌 <b>Owner Morning Digest</b>")
    lines.append(f"- mode: <b>{html.escape(mode)}</b> / posture: {html.escape(posture)} / session: {html.escape(session)}")
    lines.append(f"- phase: {html.escape(str(market_phase.get('label') or 'unknown'))} / stress: {html.escape(str(market_stress.get('stress_level') or 'unknown'))}")
    lines.append(
        "- market: "
        f"KOSPI {_fmt_num(float(index.get('kospi') or 0), 2)} ({_fmt_signed_pct(float(index.get('kospi_change_pct') or 0))}) | "
        f"KOSDAQ {_fmt_num(float(index.get('kosdaq') or 0), 2)} ({_fmt_signed_pct(float(index.get('kosdaq_change_pct') or 0))})"
    )
    lines.append(f"- macro: VIX {_fmt_num(float(macro.get('vix') or 0), 2)} / USDKRW {_fmt_num(float(macro.get('usdkrw') or 0), 1)}")
    lines.append(f"- policy: allow {html.escape(_short_list(list(mode_context.get('allowed_actions') or []), 2, '-'))}")
    lines.append(f"- policy: block {html.escape(_short_list(list(mode_context.get('blocked_actions') or []), 2, '-'))}")
    lines.append(f"- conclusion: {html.escape(str(guidance_context.get('one_line_conclusion') or mode_context.get('one_line_policy') or '-'))}")
    if isinstance(top_candidates, list) and top_candidates:
        lines.append(f"- watch: {html.escape(', '.join(_candidate_summary(c) for c in top_candidates[:3] if isinstance(c, dict)))}")
    if isinstance(changed, list) and changed:
        lines.append(f"- change: {html.escape(_short_list([str(x) for x in changed], 2, '-'))}")
    if isinstance(bullish, list) and bullish:
        lines.append(f"- what changes my mind: {html.escape(_short_list([str(x) for x in bullish], 2, '-'))}")
    lines.append(
        "- ops: "
        f"news3h {int(metrics.get('news_rows_3h') or 0)} / "
        f"cluster {int(metrics.get('cluster_age_min') or 0)}m / "
        f"relation {int(metrics.get('relation_age_min') or 0)}m / "
        f"pending_exit {int(metrics.get('pending_exit_count') or 0)}"
    )
    return "\n".join(lines)


def render_owner_execution_message(payload: dict[str, Any]) -> str:
    mode_context = _ctx(payload, "mode_context")
    execution_context = _ctx(payload, "execution_context")
    portfolio_delta = _ctx(execution_context, "portfolio_delta")
    today_orders = _ctx(execution_context, "today_orders")

    skip_rows = execution_context.get("skipped_orders", [])
    top_skip = "-"
    if isinstance(skip_rows, list) and skip_rows:
        first = skip_rows[0] if isinstance(skip_rows[0], dict) else {}
        top_skip = str(first.get("skip_reason") or "-")

    cash_before = portfolio_delta.get("cash_ratio_before")
    cash_after = portfolio_delta.get("cash_ratio_after")
    if cash_before is None and cash_after is None:
        cash_line = "- portfolio: cash -"
    elif cash_before is None:
        cash_line = f"- portfolio: cash after {float(cash_after) * 100:.0f}%"
    elif cash_after is None:
        cash_line = f"- portfolio: cash before {float(cash_before) * 100:.0f}%"
    else:
        cash_line = f"- portfolio: cash {float(cash_before) * 100:.0f}% -> {float(cash_after) * 100:.0f}%"

    lines = [
        "📌 <b>Execution Report</b>",
        f"- mode: <b>{html.escape(str(mode_context.get('execution_mode') or 'normal'))}</b>",
        f"- attempted: {int(today_orders.get('attempted') or 0)} / executed: {int(today_orders.get('executed') or 0)} / skipped: {int(today_orders.get('skipped') or 0)}",
        f"- top skip: {html.escape(top_skip)}",
        cash_line,
        f"- risk: {html.escape(str(mode_context.get('one_line_policy') or '-'))}",
    ]
    return "\n".join(lines)
