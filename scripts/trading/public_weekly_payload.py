#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from web_market_signals import fetch_web_market_signals
from weekly_market_report import (
    DEFENSIVE_NAMES,
    TECH_REBOUND,
    WAR_WINNERS,
    by_code,
    get_fx_rows,
    get_latest_decision_run,
    get_latest_watchlist,
    get_market_rows,
    get_news_rows,
    get_regime_rows,
    get_top_decisions,
    load_stock_names,
    monday_of_week,
    friday_of_week,
    pct_change,
    summarize_actions,
)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _build_market_snapshot(start_d: date, end_d: date) -> dict[str, Any]:
    market_rows = get_market_rows(start_d, end_d)
    fx_rows = get_fx_rows(start_d, end_d)
    market_map = by_code(market_rows, "index_code")

    def first_last_price(rows: list[dict[str, Any]], field: str) -> tuple[float | None, float | None]:
        if not rows:
            return None, None
        return float(rows[0].get(field) or 0), float(rows[-1].get(field) or 0)

    kospi_start, kospi_end = first_last_price(market_map.get("KOSPI", []), "close_price")
    kosdaq_start, kosdaq_end = first_last_price(market_map.get("KOSDAQ", []), "close_price")
    vix_start, vix_end = first_last_price(market_map.get("VIX", []), "close_price")
    usdkrw_start = float(fx_rows[0].get("close_rate") or 0) if fx_rows else None
    usdkrw_end = float(fx_rows[-1].get("close_rate") or 0) if fx_rows else None

    return {
        "kospi": {"start": kospi_start, "end": kospi_end, "pct": pct_change(kospi_start, kospi_end)},
        "kosdaq": {"start": kosdaq_start, "end": kosdaq_end, "pct": pct_change(kosdaq_start, kosdaq_end)},
        "vix": {"start": vix_start, "end": vix_end, "pct": pct_change(vix_start, vix_end)},
        "usdkrw": {"start": usdkrw_start, "end": usdkrw_end, "pct": pct_change(usdkrw_start, usdkrw_end)},
    }


def _build_watchlist_block(as_of: date) -> dict[str, Any]:
    stock_names = load_stock_names()
    latest_watchlist = get_latest_watchlist(as_of)
    latest_run = get_latest_decision_run(as_of)
    top_decisions = get_top_decisions(str(latest_run.get("decision_id") or ""))

    watch_items = []
    for row in latest_watchlist[:8]:
        code = str(row.get("ticker") or "").strip()
        watch_items.append(
            {
                "ticker": code,
                "name": stock_names.get(code, code),
                "action": str(row.get("action") or "").strip(),
                "rank": int(float(row.get("rank") or 0)),
            }
        )

    top_buy_items = []
    for row in top_decisions[:8]:
        code = str(row.get("ticker") or "").strip()
        top_buy_items.append(
            {
                "ticker": code,
                "name": stock_names.get(code, code),
                "action": str(row.get("action") or "").strip(),
                "score": _to_float(row.get("total_score")),
                "target_weight_pct": _to_float(row.get("target_weight")) * 100.0,
            }
        )

    decision_debug = {}
    try:
        decision_debug = json.loads(str(latest_run.get("stage_debug_json") or "{}"))
    except Exception:
        decision_debug = {}

    return {
        "latest_watchlist": watch_items,
        "top_decisions": top_buy_items,
        "decision_run": {
            "decision_id": str(latest_run.get("decision_id") or ""),
            "decision_time": str(latest_run.get("decision_time") or ""),
            "stage_debug": decision_debug,
        },
    }


def _stress_flag_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return []


def _infer_theme(stress_flags: list[str], news_rows: list[dict[str, Any]], web_signals: list[dict[str, Any]]) -> dict[str, Any]:
    joined = " ".join(str(r.get("summary") or "") for r in news_rows[:6]).lower()
    topics = [str(x.get("topic") or "").strip() for x in web_signals]
    if "GEOPOLITICAL_RISK" in stress_flags or "OIL_SHOCK_RISK" in stress_flags:
        return {
            "label": "geopolitics_oil_fx",
            "summary": "지정학 리스크, 유가, 환율이 하나의 전역 리스크 축으로 결합된 상태입니다.",
        }
    if "USDKRW>=1430" in stress_flags:
        return {
            "label": "fx_stress",
            "summary": "환율 부담이 위험자산 할인율을 높이는 구간으로 보는 것이 맞습니다.",
        }
    if "VIX>=20" in stress_flags:
        return {
            "label": "volatility_stress",
            "summary": "변동성 확대가 시장 방향보다 리스크 관리 우선순위를 높이는 구간입니다.",
        }
    if "서킷브레이커" in joined or "korea_market" in topics:
        return {
            "label": "panic_repricing",
            "summary": "급락 이후 가격 재조정과 유동성 확인이 동시에 필요한 구간입니다.",
        }
    return {
        "label": "mixed",
        "summary": "단일 테마보다 복합 리스크가 겹쳐 있는 장세로 보는 것이 적절합니다.",
    }


def _build_mode_context(
    action_posture: str,
    hard_riskoff: bool,
    stress_flags: list[str],
    stage0: dict[str, Any],
    stage5: dict[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    if "VIX>=20" in stress_flags:
        reasons.append("변동성 지표가 높아 시장이 아직 안정을 회복했다고 보기 어렵습니다.")
    if "USDKRW>=1430" in stress_flags:
        reasons.append("환율 부담이 유지돼 외국인 수급과 밸류에이션 부담이 동시에 남아 있습니다.")
    if "GEOPOLITICAL_RISK" in stress_flags:
        reasons.append("지정학 리스크가 뉴스 흐름과 심리를 반복적으로 자극하고 있습니다.")
    if "OIL_SHOCK_RISK" in stress_flags:
        reasons.append("유가 급등 리스크가 인플레이션과 비용 부담을 다시 키울 수 있습니다.")
    if hard_riskoff and not reasons:
        reasons.append("시장 레벨 하드 리스크오프 조건이 유지되고 있습니다.")

    allowed_actions = [
        "현금 여력과 포지션 집중도 우선 점검",
        "기존 보유 종목의 리스크 관리",
        "확인된 신호에 한한 단계적 접근",
    ]
    blocked_actions = [
        "급락 직후 감정적 추격 진입",
        "반등 하루만 보고 레버리지 확대",
        "변동성 완화 확인 전 공격적 신규 확대",
    ]
    what_changes = [
        "VIX가 다시 낮아지며 변동성 스트레스가 완화되는지",
        "USD/KRW가 1430 아래로 안정되는지",
        "지정학·유가 뉴스 강도가 약해지는지",
    ]
    freshness = {
        "freshness_score": _to_float(stage0.get("freshness_score")),
        "null_ratio": _to_float(stage0.get("null_ratio")),
        "liquidity_snapshot_coverage_pct": _to_float(((stage5.get("health") or {}).get("liquidity_snapshot_coverage_pct"))),
    }
    return {
        "action_posture": action_posture,
        "hard_riskoff": hard_riskoff,
        "stress_flags": stress_flags,
        "mode_reason": reasons,
        "allowed_actions": allowed_actions,
        "blocked_actions": blocked_actions,
        "what_changes_my_mind": what_changes,
        "freshness": freshness,
        "one_line_policy": "수익 추격보다 손실 통제와 신호 확인을 우선하는 주간으로 보시는 것이 맞습니다.",
    }


def _build_event_context(news_rows: list[dict[str, Any]], web_signals: list[dict[str, Any]], theme: dict[str, Any]) -> dict[str, Any]:
    top_clusters = []
    seen = set()
    for row in news_rows:
        summary = str(row.get("summary") or "").strip()
        if not summary or summary in seen:
            continue
        seen.add(summary)
        top_clusters.append(
            {
                "summary": summary,
                "n_news": int(float(row.get("n_news") or 0)),
                "importance_max": int(float(row.get("importance_max") or 0)),
                "asof_ts": str(row.get("asof_ts") or ""),
            }
        )
        if len(top_clusters) >= 4:
            break

    web_items = []
    for item in web_signals[:4]:
        web_items.append(
            {
                "topic": str(item.get("topic") or ""),
                "title": str(item.get("title") or ""),
                "source_name": str(item.get("source_name") or ""),
                "published_at": str(item.get("published_at") or ""),
            }
        )
    return {
        "dominant_theme": theme,
        "top_clusters": top_clusters,
        "web_supporting_signals": web_items,
    }


def _classify_watch_reason(code: str) -> tuple[str, str]:
    if code in WAR_WINNERS:
        return "war_defense", "전쟁·유가 리스크가 지속될 때 상대적으로 방어력이 확인되는 축입니다."
    if code in DEFENSIVE_NAMES:
        return "defensive", "변동성 확대 구간에서 상대적으로 버티는 방어 성격을 기대할 수 있습니다."
    if code in TECH_REBOUND:
        return "tech_rebound", "환율과 변동성이 진정될 때 반등 민감도가 높은 축입니다."
    return "general", "시장 변동성과 뉴스 민감도를 함께 점검할 필요가 있는 종목입니다."


def _enrich_watch_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for item in items:
        code = str(item.get("ticker") or "").strip()
        bucket, reason = _classify_watch_reason(code)
        enriched = dict(item)
        enriched["bucket"] = bucket
        enriched["reason"] = reason
        out.append(enriched)
    return out


def _build_action_context(
    stress_flags: list[str],
    latest_watchlist: list[dict[str, Any]],
    top_decisions: list[dict[str, Any]],
    stock_names: dict[str, str],
) -> dict[str, Any]:
    lines = summarize_actions(stress_flags, latest_watchlist, top_decisions, stock_names)
    portfolio_ops = lines[:2]
    what_to_watch = [
        "지수보다 VIX, USD/KRW, 지정학/유가 뉴스 강도의 동행 여부",
        "시스템 posture와 스트레스 플래그의 완화 여부",
        "유동성 스냅샷과 데이터 신선도가 유지되는지",
    ]
    avoid_actions = [
        "단일 뉴스만 보고 방향을 단정하는 추격 대응",
        "낙폭과대 논리만으로 비중을 급격히 키우는 행동",
        "방어 규율이 풀리지 않았는데 포지션 속도를 높이는 행동",
    ]
    return {
        "portfolio_operations": portfolio_ops,
        "what_to_watch_first": what_to_watch,
        "avoid_actions": avoid_actions,
        "execution_summary": lines,
    }


def _build_scenario_context(
    action_posture: str,
    hard_riskoff: bool,
    stress_flags: list[str],
) -> dict[str, Any]:
    base = [
        "기본 전제는 높은 변동성 속 방어적 운영 지속입니다.",
        "기술적 반등이 나오더라도 재확인 조정 가능성을 함께 열어두는 편이 안전합니다.",
        "신규 공격적 확대보다 기존 포지션 리스크 관리와 유동성 점검이 우선입니다.",
    ]
    risk = [
        "주말 사이 지정학 이슈가 악화되면 월요일 장 초반 갭다운과 변동성 재확대가 먼저 나올 수 있습니다.",
        "유가와 환율이 동시에 추가 상승하면 기술주와 경기민감주 반등은 더 늦어질 수 있습니다.",
    ]
    recovery = [
        "변동성과 환율이 동시에 진정되면 주중 중반부터 낙폭과대 반등 시도가 이어질 수 있습니다.",
        "다만 그 경우에도 일방향 추세 복귀보다는 확인 후 단계적 대응이 더 적절합니다.",
    ]
    if not hard_riskoff and action_posture != "defensive":
        base[0] = "기본 전제는 반등 시도와 재확인 조정이 병행되는 중립 구간입니다."
    if "GEOPOLITICAL_RISK" not in stress_flags:
        risk = [line for line in risk if "지정학" not in line]
    return {
        "base": base,
        "risk": risk,
        "recovery": recovery,
    }


def build_public_weekly_payload(as_of: date, report_type: str) -> dict[str, Any]:
    week_start = monday_of_week(as_of)
    week_end = friday_of_week(as_of)
    prev_friday = week_start - timedelta(days=3)
    next_week_start = week_start + timedelta(days=7)
    next_week_end = next_week_start + timedelta(days=4)

    regime_rows = get_regime_rows(week_start, week_end)
    news_rows = get_news_rows(week_start, week_end)
    latest_regime = regime_rows[-1] if regime_rows else {}
    watch_block = _build_watchlist_block(week_end)
    stage1 = (((watch_block.get("decision_run") or {}).get("stage_debug") or {}).get("stage1") or {})
    stage0 = (((watch_block.get("decision_run") or {}).get("stage_debug") or {}).get("stage0") or {})
    stage5 = (((watch_block.get("decision_run") or {}).get("stage_debug") or {}).get("stage5") or {})
    web_signals = fetch_web_market_signals(limit=10, timeout_sec=6)
    stress_flags = _stress_flag_list(stage1.get("stress_flags") or latest_regime.get("stress_flags") or [])
    action_posture = str(stage1.get("action_posture") or latest_regime.get("action_posture") or "")
    hard_riskoff = bool(stage1.get("hard_riskoff"))
    stock_names = load_stock_names()
    theme = _infer_theme(stress_flags, news_rows, web_signals)
    latest_watchlist = watch_block.get("latest_watchlist") or []
    top_decisions = watch_block.get("top_decisions") or []
    mode_context = _build_mode_context(action_posture, hard_riskoff, stress_flags, stage0, stage5)
    event_context = _build_event_context(news_rows, web_signals, theme)
    action_context = _build_action_context(stress_flags, latest_watchlist, top_decisions, stock_names)
    scenario_context = _build_scenario_context(action_posture, hard_riskoff, stress_flags)
    watch_context = dict(watch_block)
    watch_context["latest_watchlist"] = _enrich_watch_items(latest_watchlist)
    watch_context["top_decisions"] = _enrich_watch_items(top_decisions)

    return {
        "report_type": report_type,
        "audience": "public",
        "generated_at": date.today().isoformat(),
        "as_of": as_of.isoformat(),
        "week_context": {
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "next_week_start": next_week_start.isoformat(),
            "next_week_end": next_week_end.isoformat(),
            "market_snapshot": _build_market_snapshot(prev_friday, week_end),
            "latest_regime": {
                "summary": str(latest_regime.get("summary") or "").strip(),
                "action_posture": str(latest_regime.get("action_posture") or "").strip(),
                "stress_flags": stress_flags,
            },
            "hard_riskoff": hard_riskoff,
            "action_posture": action_posture,
            "news_rows": news_rows,
            "web_market_signals": web_signals,
        },
        "mode_context": mode_context,
        "event_context": event_context,
        "scenario_context": scenario_context,
        "action_context": action_context,
        "watch_context": watch_context,
    }
