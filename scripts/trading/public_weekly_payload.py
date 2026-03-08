#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from web_market_signals import fetch_web_market_signals
from weekly_market_report import (
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
    web_signals = fetch_web_market_signals(limit=10, timeout_sec=6)

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
                "stress_flags": latest_regime.get("stress_flags") or [],
            },
            "hard_riskoff": bool(stage1.get("hard_riskoff")),
            "action_posture": str(stage1.get("action_posture") or latest_regime.get("action_posture") or ""),
            "news_rows": news_rows,
            "web_market_signals": web_signals,
        },
        "watch_context": watch_block,
    }
