#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
from typing import Any


def _ctx(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    return value if isinstance(value, dict) else {}


def _fmt_num(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}"


def _fmt_pct(value: float) -> str:
    return f"{value:+.2f}%"


def _short_list(items: list[str], max_items: int = 2, empty: str = "-") -> str:
    vals = [str(x).strip() for x in items if str(x).strip()]
    if not vals:
        return empty
    if len(vals) <= max_items:
        return ", ".join(vals)
    return ", ".join(vals[:max_items]) + f" 외 {len(vals) - max_items}"


def _mode_label(mode: str) -> str:
    labels = {
        "normal": "중립 모드",
        "defensive": "방어 모드",
        "shock": "충격 대응 모드",
        "recovery": "회복 확인 모드",
        "close_only": "관망 모드",
    }
    return labels.get(str(mode or "").strip().lower(), "중립 모드")


def _allowed_actions_text(actions: list[str]) -> str:
    joined = set(str(x).strip().lower() for x in actions if str(x).strip())
    if {"hold", "reduce", "risk_off_rebalance"} & joined:
        return "보유 종목 관리와 리스크 축소"
    if {"selective_add", "core_reentry"} & joined:
        return "코어 종목 중심의 제한적 대응"
    if {"selective_buy", "watchlist_based_entry"} & joined:
        return "선별적인 관찰과 제한적 진입"
    return "보수적인 대응"


def _blocked_actions_text(actions: list[str]) -> str:
    joined = set(str(x).strip().lower() for x in actions if str(x).strip())
    if "new_buy" in joined or "aggressive_add" in joined:
        return "공격적인 신규 매수"
    if "theme_chasing" in joined:
        return "테마 추격 매수"
    return "과도한 추격 대응"


def _candidate_public_reason(candidate: dict[str, Any]) -> str:
    relation = candidate.get("relation")
    if isinstance(relation, dict):
        summary = str(relation.get("summary") or "").strip()
        quality = float(relation.get("quality") or 0)
        effective = float(relation.get("effective_score") or 0)
        events = int(float(relation.get("support_events") or 0))
        clusters = int(float(relation.get("support_clusters") or 0))
        if summary and quality >= 0.65 and effective >= 2.0:
            return f"내부 연관 데이터에서 반복 확인된 이유는 {summary}"
        if effective >= 4.0:
            return f"내부 연관 신호가 강하게 잡히고 있고, 이벤트 {events}건과 클러스터 {clusters}건에서 반복 확인됩니다."
    thesis = str(candidate.get("thesis") or "").strip()
    flow = candidate.get("flow")
    technical = candidate.get("technical")
    notes: list[str] = []
    if thesis:
        if "수급 유입" in thesis:
            notes.append("수급 관심이 이어지고 있습니다")
        elif "수급 약세" in thesis:
            notes.append("수급은 아직 강하지 않습니다")
        elif "기술 buy" in thesis:
            notes.append("기술 흐름은 상대적으로 양호합니다")
        else:
            notes.append(thesis)
    if isinstance(flow, dict):
        foreign = float(flow.get("foreign") or 0)
        inst = float(flow.get("institution") or 0)
        if foreign > 0 or inst > 0:
            notes.append("수급은 일부 유입 신호가 있습니다")
        elif foreign < 0 or inst < 0:
            notes.append("수급은 아직 엇갈립니다")
    if isinstance(technical, dict):
        signal = str(technical.get("signal") or "").strip().lower()
        if signal == "buy":
            notes.append("기술 흐름은 나쁘지 않습니다")
    if not notes:
        return "시장의 관심 축에 들어와 있지만 아직 확인이 더 필요합니다."
    text = notes[0]
    if not text.endswith("."):
        text += "."
    return text


def _humanize_trigger(trigger: str) -> str:
    text = str(trigger or "").strip()
    mapping = {
        "VIX<20": "변동성이 더 낮아지는지",
        "USDKRW<1430": "환율이 1,430원 아래로 안정되는지",
        "지정학 리스크 완화 확인": "지정학 리스크가 완화되는지",
        "유가 충격 완화 확인": "유가 부담이 진정되는지",
    }
    return mapping.get(text, text)


def _humanize_flag(flag: str) -> str:
    mapping = {
        "VIX>=20": "변동성이 높은 상태",
        "USDKRW>=1430": "환율 부담이 높은 상태",
        "GEOPOLITICAL_RISK": "지정학 리스크",
        "OIL_SHOCK_RISK": "유가 충격 리스크",
        "SHIPPING_DISRUPTION_RISK": "물류 차질 리스크",
    }
    return mapping.get(str(flag or "").strip(), str(flag or "").strip())


def _stress_risk_text(market_context: dict[str, Any]) -> str:
    market_stress = _ctx(market_context, "market_stress")
    flags = [str(x).strip() for x in list(market_stress.get("stress_flags") or []) if str(x).strip()]
    if "GEOPOLITICAL_RISK" in flags and "OIL_SHOCK_RISK" in flags:
        return "지정학 변수와 유가 부담이 동시에 남아 있어, 반등이 나와도 아직 리스크오프 구조가 완전히 끝났다고 보긴 어렵습니다."
    if "USDKRW>=1430" in flags or "VIX>=20" in flags:
        return "변동성과 환율이 높은 상태라 종목별 신호보다 시장 전체 리스크 관리가 더 중요한 구간입니다."
    return "전역 리스크는 상대적으로 완만하지만, 추세가 완전히 정리됐다고 단정하기는 이릅니다."


def _generated_date(payload: dict[str, Any]) -> str:
    raw = str(payload.get("generated_at") or "").strip()
    if not raw:
        return "오늘"
    try:
        dt = datetime.fromisoformat(raw)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return raw[:10] or "오늘"


def _event_summary(event_context: dict[str, Any], market_context: dict[str, Any]) -> str:
    top_events = event_context.get("top_events", [])
    if isinstance(top_events, list) and top_events:
        first = top_events[0] if isinstance(top_events[0], dict) else {}
        title = str(first.get("title") or "").strip()
        summary = str(first.get("summary") or "").strip()
        if title:
            return title
        if summary:
            return summary
    market_stress = _ctx(market_context, "market_stress")
    flags = [str(x).strip() for x in list(market_stress.get("stress_flags") or []) if str(x).strip()]
    if "GEOPOLITICAL_RISK" in flags and "OIL_SHOCK_RISK" in flags:
        return "지정학 리스크와 유가 부담이 함께 시장 변동성을 자극하고 있습니다."
    if flags:
        return "오늘은 단일 종목 이슈보다 변동성과 환율 흐름이 더 중요합니다."
    return "오늘은 뚜렷한 단일 이벤트보다 시장 전반의 안정 여부가 더 중요합니다."


def render_public_market_message(payload: dict[str, Any]) -> str:
    market_context = _ctx(payload, "market_context")
    mode_context = _ctx(payload, "mode_context")
    event_context = _ctx(payload, "event_context")
    candidate_context = _ctx(payload, "candidate_context")
    relation_context = _ctx(payload, "relation_context")
    guidance_context = _ctx(payload, "guidance_context")

    index = _ctx(market_context, "index")
    macro = _ctx(market_context, "macro")
    market_phase = _ctx(market_context, "market_phase")
    top_candidates = candidate_context.get("top_candidates", [])
    top_relations = relation_context.get("top_signals", [])
    triggers = _ctx(mode_context, "mode_change_triggers").get("bullish_reenable", [])
    report_date = _generated_date(payload)
    risk_text = _stress_risk_text(market_context)
    market_stress = _ctx(market_context, "market_stress")
    stress_flags = [_humanize_flag(str(x)) for x in list(market_stress.get("stress_flags") or []) if str(x).strip()]

    watch_lines: list[str] = []
    if isinstance(top_candidates, list):
        for candidate in top_candidates[:2]:
            if not isinstance(candidate, dict):
                continue
            name = str(candidate.get("name") or candidate.get("ticker") or "-").strip()
            reason = _candidate_public_reason(candidate)
            watch_lines.append(f"- {name}: {reason} 지금은 바로 추격하기보다 흐름이 이어지는지 확인이 먼저입니다.")
            watch_lines.append("")
    if not watch_lines and isinstance(top_relations, list):
        for relation in top_relations[:2]:
            if not isinstance(relation, dict):
                continue
            name = str(relation.get("name") or relation.get("ticker") or "-").strip()
            why = str(relation.get("why_candidate") or "").strip()
            if why:
                watch_lines.append(f"- {name}: {why} 지금은 신규 진입보다 확인과 관찰이 우선입니다.")
                watch_lines.append("")

    headline = str(market_phase.get("summary") or "시장 해석 데이터가 부족합니다.").strip()
    event_line = _event_summary(event_context, market_context)
    mode_label = _mode_label(str(mode_context.get("execution_mode") or "normal"))
    allowed_text = _allowed_actions_text(list(mode_context.get("allowed_actions") or []))
    blocked_text = _blocked_actions_text(list(mode_context.get("blocked_actions") or []))
    trigger_lines = [_humanize_trigger(str(x)) for x in list(triggers or []) if str(x).strip()]
    conclusion = str(guidance_context.get("one_line_conclusion") or "-").strip()

    lines = [
        f"# {report_date} 일간 시장 설명",
        "",
        "## 한줄 요약",
        f"오늘 시장은 {headline}",
        "",
        "## 오늘 핵심 숫자",
        f"- 코스피: {_fmt_num(float(index.get('kospi') or 0), 2)} ({_fmt_pct(float(index.get('kospi_change_pct') or 0))})",
        f"- 코스닥: {_fmt_num(float(index.get('kosdaq') or 0), 2)} ({_fmt_pct(float(index.get('kosdaq_change_pct') or 0))})",
        f"- USD/KRW: {_fmt_num(float(macro.get('usdkrw') or 0), 1)}",
        f"- VIX: {_fmt_num(float(macro.get('vix') or 0), 2)}",
        "",
        "## 시장을 움직인 요인",
        f"- 핵심 이벤트: {event_line}",
        f"- 변동성/환율: VIX와 환율이 모두 높은 상태라 위험자산 선호가 완전히 회복됐다고 보긴 어렵습니다.",
        f"- 시스템 시각: {risk_text}",
        "",
        "## 오늘 시장 해석",
        f"- 오늘 반등이나 지수 회복이 나오더라도, 지금 시장은 추세 복귀보다 변동성 소화 구간으로 보는 편이 더 안전합니다.",
        f"- 특히 {_short_list(stress_flags, 3, '전역 스트레스 신호')}가 동시에 남아 있어, 종목별 강세 신호가 있어도 시장 전체 리스크가 더 중요합니다.",
        f"- 그래서 오늘은 강한 방향성 베팅보다 확인된 흐름만 선별적으로 보는 접근이 맞습니다.",
        "",
        "## 시스템 대응",
        f"- 현재 시스템은 {mode_label}를 유지하고 있습니다.",
        f"- 우선 행동: {allowed_text}",
        f"- 제한 행동: {blocked_text}",
        f"- 요약하면 {conclusion}",
    ]
    if watch_lines:
        while watch_lines and not str(watch_lines[-1]).strip():
            watch_lines.pop()
        lines.extend(["", "## 관찰 종목"] + watch_lines)
    lines.extend(
        [
            "",
            "## 오늘 체크포인트",
            f"- 가장 중요한 포인트: {conclusion}",
        ]
    )
    if trigger_lines:
        for item in trigger_lines[:3]:
            lines.append(f"- 전략 전환 조건: {item}")
    return "\n".join(lines).strip()
