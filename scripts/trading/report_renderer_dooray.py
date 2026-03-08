#!/usr/bin/env python3
from __future__ import annotations

from typing import Any


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


def _candidate_line(candidate: dict[str, Any]) -> str:
    name = str(candidate.get("name") or candidate.get("ticker") or "-").strip()
    grade = str(candidate.get("signal_grade") or "-").strip()
    action = str(candidate.get("current_action") or "HOLD").upper()
    thesis = str(candidate.get("thesis") or "근거 요약 부족").strip()
    reason = str(candidate.get("action_reason") or "정책 기준 설명 부족").strip()
    return f"- {name} | {action}/{grade} | {thesis} | {reason}"


def _event_line(event: dict[str, Any]) -> str:
    title = str(event.get("title") or "-").strip()
    impact = str(event.get("market_impact") or "market").strip()
    summary = str(event.get("summary") or "").strip()
    if summary:
        return f"- {title} ({impact}) | {summary}"
    return f"- {title} ({impact})"


def render_dooray_internal_message(payload: dict[str, Any]) -> str:
    market_context = _ctx(payload, "market_context")
    mode_context = _ctx(payload, "mode_context")
    event_context = _ctx(payload, "event_context")
    candidate_context = _ctx(payload, "candidate_context")
    change_context = _ctx(payload, "change_context")
    guidance_context = _ctx(payload, "guidance_context")
    ops_context = _ctx(payload, "ops_context")

    index = _ctx(market_context, "index")
    macro = _ctx(market_context, "macro")
    market_stress = _ctx(market_context, "market_stress")
    market_phase = _ctx(market_context, "market_phase")
    mode_triggers = _ctx(mode_context, "mode_change_triggers")
    metrics = _ctx(ops_context, "metrics")

    lines: list[str] = []
    lines.append("[오늘 모드]")
    lines.append(
        f"- execution_mode: {mode_context.get('execution_mode', '-')}"
        f" / posture: {mode_context.get('action_posture', '-')}"
        f" / session: {mode_context.get('session', '-')}"
    )
    lines.append(f"- 핵심 리스크: {_short_list(list(market_stress.get('stress_flags') or []), 4, '-')}")
    lines.append(f"- 허용: {_short_list(list(mode_context.get('allowed_actions') or []), 4, '-')}")
    lines.append(f"- 금지: {_short_list(list(mode_context.get('blocked_actions') or []), 4, '-')}")

    lines.append("")
    lines.append("[어제 대비 바뀐 점]")
    changed = change_context.get("what_changed_today", [])
    if isinstance(changed, list) and changed:
        for item in changed[:3]:
            lines.append(f"- {item}")
    else:
        lines.append("- 변화 요약 없음")
    lines.append(f"- 의미: {change_context.get('why_it_matters', '-')}")

    lines.append("")
    lines.append("[시장 상황]")
    lines.append(f"- KOSPI {_fmt_num(float(index.get('kospi') or 0), 2)} ({_fmt_signed_pct(float(index.get('kospi_change_pct') or 0))})")
    lines.append(f"- KOSDAQ {_fmt_num(float(index.get('kosdaq') or 0), 2)} ({_fmt_signed_pct(float(index.get('kosdaq_change_pct') or 0))})")
    lines.append(f"- VIX {_fmt_num(float(macro.get('vix') or 0), 2)} / USDKRW {_fmt_num(float(macro.get('usdkrw') or 0), 1)}")
    lines.append(f"- 해석: {market_phase.get('summary', '-')}")

    lines.append("")
    lines.append("[핵심 이벤트]")
    top_events = event_context.get("top_events", [])
    if isinstance(top_events, list) and top_events:
        for event in top_events[:3]:
            if isinstance(event, dict):
                lines.append(_event_line(event))
    else:
        lines.append("- 중요 이벤트 데이터 부족")

    lines.append("")
    lines.append("[후보 종목]")
    top_candidates = candidate_context.get("top_candidates", [])
    if isinstance(top_candidates, list) and top_candidates:
        for candidate in top_candidates[:5]:
            if isinstance(candidate, dict):
                lines.append(_candidate_line(candidate))
    else:
        lines.append("- 후보 종목 데이터 부족")

    lines.append("")
    lines.append("[오늘 실행 원칙]")
    for principle in list(guidance_context.get("today_principles") or [])[:3]:
        lines.append(f"- {principle}")
    bullish = mode_triggers.get("bullish_reenable", [])
    if isinstance(bullish, list) and bullish:
        lines.append(f"- 모드 전환 조건: {_short_list([str(x) for x in bullish], 3, '-')}")
    lines.append(f"- 결론: {guidance_context.get('one_line_conclusion', '-')}")
    lines.append(
        f"- 운영 메모: news3h={int(metrics.get('news_rows_3h') or 0)}, "
        f"cluster_age={int(metrics.get('cluster_age_min') or 0)}m, "
        f"relation_age={int(metrics.get('relation_age_min') or 0)}m"
    )
    return "\n".join(lines).strip()
