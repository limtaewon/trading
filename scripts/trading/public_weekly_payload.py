#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from web_market_signals import fetch_google_news_signals, fetch_web_market_signals
from weekly_market_report import (
    DEFENSIVE_NAMES,
    TECH_REBOUND,
    WAR_WINNERS,
    by_code,
    ch_query,
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


WEEKLY_DISCOVERY_QUERIES: list[tuple[str, str]] = [
    ("macro", "미국 CPI PCE FOMC 고용 GDP 물가지표 금리 일정 이번주 when:10d"),
    ("macro", "미국 연준 금리 국채금리 달러인덱스 이번주 when:10d"),
    ("policy", "한국 정부 정책 시장안정 금융지원 규제 세제 증시 when:10d"),
    ("geopolitics", "중동 전쟁 이란 이스라엘 중국 대만 제재 관세 when:10d"),
    ("energy", "원유 유가 브렌트 WTI 호르무즈 LNG 해운 운임 when:10d"),
    ("fx_rates", "원달러 환율 달러인덱스 DXY 국채금리 외환 when:10d"),
    ("korea_market", "코스피 코스닥 수급 공매도 서킷브레이커 증시 전망 when:10d"),
    ("sector", "반도체 HBM AI 배터리 조선 방산 전력인프라 실적 전망 when:10d"),
]

EVENT_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "macro_calendar": ["cpi", "pce", "fomc", "고용", "물가지표", "지표", "일정", "발표", "gdp", "inflation"],
    "policy": ["정책", "시장안정", "금융지원", "규제", "부양", "세제", "재정", "프로그램", "정부", "대통령"],
    "geopolitics": ["전쟁", "분쟁", "공습", "이란", "이스라엘", "중동", "대만", "제재", "관세", "conflict", "war"],
    "energy_shipping": ["유가", "원유", "브렌트", "wti", "호르무즈", "lpg", "lng", "운임", "해운", "shipping", "oil"],
    "fx_rates": ["환율", "원달러", "달러", "dxy", "국채금리", "수익률", "외환", "yield", "rates"],
    "korea_market": ["코스피", "코스닥", "서킷브레이커", "수급", "증시", "공매도", "kospi", "kosdaq"],
    "sector_theme": ["반도체", "hbm", "ai", "배터리", "조선", "방산", "전력", "인프라", "실적", "earnings"],
}

EVENT_CATEGORY_LABELS: dict[str, str] = {
    "macro_calendar": "거시 일정·물가지표",
    "policy": "정책·시장안정 조치",
    "geopolitics": "지정학 리스크",
    "energy_shipping": "유가·에너지·해운",
    "fx_rates": "환율·금리",
    "korea_market": "국내 증시 구조",
    "sector_theme": "섹터·테마",
    "general": "기타 시장 변수",
}

EVENT_CHECKPOINT_MAP: dict[str, str] = {
    "macro_calendar": "다음 주 예정된 물가·금리·고용 등 거시 일정이 시장 변동성을 다시 키우는지",
    "policy": "정책 발표가 실제 수급 안정과 심리 회복으로 이어지는지",
    "geopolitics": "지정학 리스크 헤드라인 강도가 완화되는지",
    "energy_shipping": "유가·해운·원자재 충격이 환율과 함께 진정되는지",
    "fx_rates": "환율과 금리가 동반 안정되는지",
    "korea_market": "국내 수급·서킷브레이커급 변동성이 진정되는지",
    "sector_theme": "시장 전체보다 특정 섹터만 과열/왜곡되는지",
    "general": "단일 이벤트보다 복합 리스크가 커지는지",
}


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _join_str_list(value: Any, sep: str = ", ") -> str:
    if isinstance(value, list):
        parts = [str(v).strip() for v in value if str(v).strip()]
        return sep.join(parts)
    if value is None:
        return ""
    return str(value).strip()


def _relation_strength_label(score: float) -> str:
    if score >= 7.0:
        return "매우 강한 편"
    if score >= 4.0:
        return "강한 편"
    if score >= 2.0:
        return "보통 이상"
    return "초기 단계"


def _relation_quality_label(quality: float) -> str:
    if quality >= 0.85:
        return "신뢰도 높음"
    if quality >= 0.65:
        return "신뢰도 양호"
    if quality >= 0.45:
        return "신뢰도 보통"
    return "신뢰도 낮음"


def _relation_freshness_label(source_at: str, cluster_at: str) -> str:
    ts = str(source_at or "").strip() or str(cluster_at or "").strip()
    if not ts:
        return "최신성 정보 부족"
    return f"근거 최신 시각 {ts}"


def _relation_bias_text(bias: str) -> str:
    value = str(bias or "").strip().lower()
    if value == "positive":
        return "긍정 전이"
    if value == "negative":
        return "부정 전이"
    return "중립 전이"


def _relation_channel_text(channels: str) -> str:
    mapping = {
        "sentiment": "시장 심리",
        "risk": "리스크 인식",
        "valuation": "밸류에이션",
        "demand": "수요",
        "supply": "공급",
        "liquidity": "유동성",
        "policy": "정책",
        "revenue": "실적",
        "cost": "비용",
    }
    items = [mapping.get(part.strip(), part.strip()) for part in str(channels or "").split(",") if part.strip()]
    if not items:
        return ""
    deduped: list[str] = []
    for item in items:
        if item not in deduped:
            deduped.append(item)
    return ", ".join(deduped[:4])


def _relation_channel_phrase(channel: str, bias: str) -> str:
    positive = {
        "시장 심리": "투자심리 개선",
        "리스크 인식": "위험회피 완화",
        "밸류에이션": "밸류 재평가",
        "수요": "수요 기대 확대",
        "공급": "공급 여건 개선",
        "유동성": "수급 여건 개선",
        "정책": "정책 기대 강화",
        "실적": "실적 개선 기대",
        "비용": "비용 부담 완화",
    }
    negative = {
        "시장 심리": "투자심리 악화",
        "리스크 인식": "위험회피 확대",
        "밸류에이션": "밸류 부담 재평가",
        "수요": "수요 둔화 우려",
        "공급": "공급 차질 우려",
        "유동성": "수급 위축 가능성",
        "정책": "정책 불확실성",
        "실적": "실적 둔화 우려",
        "비용": "비용 부담 확대",
    }
    neutral = {
        "시장 심리": "투자심리 변화",
        "리스크 인식": "리스크 인식 변화",
        "밸류에이션": "밸류 재조정",
        "수요": "수요 변화",
        "공급": "공급 변화",
        "유동성": "수급 변화",
        "정책": "정책 변수",
        "실적": "실적 변수",
        "비용": "비용 변수",
    }
    bias_key = str(bias or "").strip().lower()
    if bias_key == "positive":
        return positive.get(channel, channel)
    if bias_key == "negative":
        return negative.get(channel, channel)
    return neutral.get(channel, channel)


def _relation_channel_reason(channels: str, bias: str) -> str:
    raw_items = [part.strip() for part in _relation_channel_text(channels).split(",") if part.strip()]
    items = [_relation_channel_phrase(item, bias) for item in raw_items]
    if not items:
        return ""
    if len(items) == 1:
        tail = f"{items[0]} 요인이 중심으로 작동했습니다"
    elif len(items) == 2:
        tail = f"{items[0]}, {items[1]} 요인이 함께 작동했습니다"
    else:
        tail = f"{items[0]}, {items[1]}, {items[2]} 요인이 함께 작동했습니다"
    bias_text = _relation_bias_text(bias)
    return f"{bias_text} 관점에서는 {tail}."


def _relation_support_text(events: int, clusters: int) -> str:
    parts: list[str] = []
    if events > 0:
        parts.append(f"이벤트 {events}건")
    if clusters > 0:
        parts.append(f"클러스터 {clusters}건")
    if not parts:
        return "근거 축적은 아직 제한적입니다."
    return ", ".join(parts) + "에서 반복 확인됐습니다."


def _relation_why_candidate(
    name: str,
    bias: str,
    strength_label: str,
    quality_label: str,
    support_text: str,
    reason_summary: str,
    channels: str,
    freshness_text: str,
) -> str:
    bias_text = _relation_bias_text(bias)
    channel_text = _relation_channel_text(channels)
    channel_reason = _relation_channel_reason(channels, bias)
    if reason_summary:
        tail = f" 이 판단은 {bias_text} 축에서 {support_text}"
        if channel_reason:
            tail += f" {channel_reason}"
        if freshness_text:
            tail += f" {freshness_text}."
        return f"{reason_summary}{tail}"
    base = f"{name}은(는) {bias_text} 축에서 {support_text}"
    if channel_reason:
        base += f" {channel_reason}"
    elif channel_text:
        base += f" 특히 {channel_text} 요인이 함께 반영된 후보입니다."
    else:
        base += f" 내부 연관 전이 신호 강도는 {strength_label}, 근거는 {quality_label}입니다."
    if freshness_text:
        base += f" {freshness_text}."
    return base


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


def _text_blob(*parts: Any) -> str:
    return " ".join(str(p or "").strip() for p in parts if str(p or "").strip())


def _map_seed_topic(topic: str) -> str:
    return {
        "macro": "macro_calendar",
        "policy": "policy",
        "geopolitics": "geopolitics",
        "energy": "energy_shipping",
        "fx_rates": "fx_rates",
        "korea_market": "korea_market",
        "sector": "sector_theme",
    }.get(str(topic or "").strip(), "general")


def _infer_event_category(text: str, default: str = "general") -> str:
    low = str(text or "").lower()
    for category, kws in EVENT_CATEGORY_KEYWORDS.items():
        if any(kw.lower() in low for kw in kws):
            return category
    return default


def _get_relation_signal_rows(limit: int = 40, min_abs_score: float = 0.18) -> list[dict[str, Any]]:
    rows = ch_query(f"""
SELECT
  ticker,
  ticker_name,
  toString(asof_ts) AS asof_ts_s,
  total_relation_score,
  relation_quality,
  relation_bias,
  support_events,
  support_clusters,
  abs(total_relation_score) * (0.5 + 0.5 * ifNull(relation_quality, 0.5)) AS effective_relation_score,
  top_channels
FROM trading.hidden_relation_signals
WHERE asof_ts = (SELECT max(asof_ts) FROM trading.hidden_relation_signals)
  AND abs(total_relation_score) >= {float(min_abs_score)}
ORDER BY effective_relation_score DESC, support_events DESC, support_clusters DESC
LIMIT {int(limit)}
FORMAT JSON
""")
    for row in rows:
        row["asof_ts"] = str(row.get("asof_ts_s", "") or "")
        row["top_channels_str"] = _join_str_list(row.get("top_channels"))
    return rows


def _get_relation_reasoning_rows(limit: int = 30, min_confidence: float = 0.32) -> list[dict[str, Any]]:
    rows = ch_query(f"""
SELECT
  ticker,
  ticker_name,
  toString(asof_ts) AS asof_ts_s,
  confidence,
  summary,
  time_horizon,
  source_cluster,
  source_max_published_at,
  source_max_cluster_asof_ts,
  relation_quality,
  support_events,
  support_clusters,
  effective_relation_score,
  evidence_titles
FROM trading.hidden_relation_reasoning
WHERE toFloat64OrZero(toString(confidence)) >= {float(min_confidence)}
ORDER BY asof_ts DESC, effective_relation_score DESC
LIMIT {int(limit)}
FORMAT JSON
""")
    for row in rows:
        row["asof_ts"] = str(row.get("asof_ts_s", "") or "")
        row["evidence_titles_str"] = _join_str_list(row.get("evidence_titles"))
    return rows


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


def _headline_summary(item: dict[str, Any]) -> str:
    source = str(item.get("source_name") or "").strip()
    title = str(item.get("title") or "").strip()
    if source and title:
        return f"{source}: {title}"
    return title or source


def _build_external_research(
    next_week_start: date,
    next_week_end: date,
    news_rows: list[dict[str, Any]],
    latest_watchlist: list[dict[str, Any]],
    top_decisions: list[dict[str, Any]],
    stress_flags: list[str],
) -> dict[str, Any]:
    stock_names = load_stock_names()
    web_rows = fetch_google_news_signals(
        WEEKLY_DISCOVERY_QUERIES,
        limit=28,
        timeout_sec=7,
        per_query=4,
    )
    relation_rows = _get_relation_signal_rows(limit=40, min_abs_score=0.16)
    reasoning_rows = _get_relation_reasoning_rows(limit=30, min_confidence=0.32)

    watch_codes = {str(r.get("ticker") or "").strip() for r in latest_watchlist if str(r.get("ticker") or "").strip()}
    watch_codes.update({str(r.get("ticker") or "").strip() for r in top_decisions if str(r.get("ticker") or "").strip()})

    buckets: dict[str, dict[str, Any]] = {}

    def ensure_bucket(category: str) -> dict[str, Any]:
        bucket = buckets.get(category)
        if bucket is None:
            bucket = {
                "category": category,
                "label": EVENT_CATEGORY_LABELS.get(category, EVENT_CATEGORY_LABELS["general"]),
                "web_hits": 0,
                "cluster_hits": 0,
                "cluster_news_total": 0,
                "cluster_importance_max": 0,
                "relation_hits": 0,
                "relation_score_sum": 0.0,
                "relation_effective_sum": 0.0,
                "relation_quality_sum": 0.0,
                "reasoning_hits": 0,
                "reasoning_conf_sum": 0.0,
                "reasoning_quality_sum": 0.0,
                "source_names": set(),
                "headlines": [],
                "cluster_summaries": [],
                "relation_tickers": set(),
                "reasoning_summaries": [],
                "watch_overlap": set(),
                "freshest_relation_source_at": "",
            }
            buckets[category] = bucket
        return bucket

    for row in web_rows:
        seed = _map_seed_topic(str(row.get("topic") or ""))
        category = _infer_event_category(_text_blob(row.get("title"), row.get("source_name")), seed)
        bucket = ensure_bucket(category)
        bucket["web_hits"] += 1
        src = str(row.get("source_name") or "").strip()
        if src:
            bucket["source_names"].add(src)
        title = str(row.get("title") or "").strip()
        if title and title not in bucket["headlines"]:
            bucket["headlines"].append(title)

    for row in news_rows:
        summary = str(row.get("summary") or "").strip()
        if not summary:
            continue
        category = _infer_event_category(summary, "general")
        bucket = ensure_bucket(category)
        bucket["cluster_hits"] += 1
        bucket["cluster_news_total"] += int(float(row.get("n_news") or 0))
        bucket["cluster_importance_max"] = max(bucket["cluster_importance_max"], int(float(row.get("importance_max") or 0)))
        if summary not in bucket["cluster_summaries"]:
            bucket["cluster_summaries"].append(summary)

    for row in relation_rows:
        code = str(row.get("ticker") or "").strip()
        blob = _text_blob(row.get("ticker_name"), row.get("top_channels_str"), row.get("relation_bias"))
        category = _infer_event_category(blob, "sector_theme")
        bucket = ensure_bucket(category)
        bucket["relation_hits"] += 1
        bucket["relation_score_sum"] += abs(_to_float(row.get("total_relation_score")))
        bucket["relation_effective_sum"] += _to_float(row.get("effective_relation_score"))
        bucket["relation_quality_sum"] += _to_float(row.get("relation_quality"), 0.5)
        if code.isdigit() and len(code) == 6:
            bucket["relation_tickers"].add(code)
            if code in watch_codes:
                bucket["watch_overlap"].add(code)

    for row in reasoning_rows:
        code = str(row.get("ticker") or "").strip()
        blob = _text_blob(row.get("summary"), row.get("source_cluster"), row.get("evidence_titles_str"))
        category = _infer_event_category(blob, "sector_theme")
        bucket = ensure_bucket(category)
        bucket["reasoning_hits"] += 1
        bucket["reasoning_conf_sum"] += _to_float(row.get("confidence"))
        bucket["reasoning_quality_sum"] += _to_float(row.get("relation_quality"), 0.5)
        summary = str(row.get("summary") or "").strip()
        if summary and summary not in bucket["reasoning_summaries"]:
            bucket["reasoning_summaries"].append(summary)
        source_max = str(row.get("source_max_published_at") or "").strip()
        if source_max and source_max > bucket["freshest_relation_source_at"]:
            bucket["freshest_relation_source_at"] = source_max
        if code.isdigit() and len(code) == 6:
            bucket["relation_tickers"].add(code)
            if code in watch_codes:
                bucket["watch_overlap"].add(code)

    ranked: list[dict[str, Any]] = []
    for category, bucket in buckets.items():
        source_count = len(bucket["source_names"])
        watch_overlap = len(bucket["watch_overlap"])
        stress_bonus = 0.0
        if category == "geopolitics" and "GEOPOLITICAL_RISK" in stress_flags:
            stress_bonus += 2.0
        if category == "energy_shipping" and "OIL_SHOCK_RISK" in stress_flags:
            stress_bonus += 2.0
        if category == "fx_rates" and "USDKRW>=1430" in stress_flags:
            stress_bonus += 1.8
        if category == "korea_market" and "VIX>=20" in stress_flags:
            stress_bonus += 1.4
        if category == "macro_calendar":
            stress_bonus += 0.8
        internal_component = (
            bucket["cluster_hits"] * 3.0
            + min(bucket["cluster_news_total"] / 6.0, 6.0)
            + bucket["cluster_importance_max"] * 1.5
        )
        relation_component = min(
            bucket["relation_hits"] * 0.7
            + min(bucket["relation_score_sum"], 4.0)
            + min(bucket["relation_effective_sum"] / 3.0, 4.0)
            + min(bucket["relation_quality_sum"], 2.0)
            + bucket["reasoning_hits"] * 0.7
            + min(bucket["reasoning_conf_sum"], 2.0)
            + min(bucket["reasoning_quality_sum"], 1.5),
            9.0,
        )
        external_component = bucket["web_hits"] * 0.9 + min(source_count, 3) * 0.7
        score = internal_component + relation_component + external_component + min(watch_overlap, 3) * 0.8 + stress_bonus
        if bucket["cluster_hits"] == 0:
            score *= 0.72
        if bucket["cluster_hits"] == 0 and bucket["web_hits"] == 0:
            score *= 0.55
        why: list[str] = []
        if bucket["cluster_hits"] > 0:
            why.append("internal_news_clusters")
        if bucket["relation_hits"] > 0 or bucket["reasoning_hits"] > 0:
            why.append("relation_linkage")
        if source_count >= 2:
            why.append("multiple_external_sources")
        if watch_overlap > 0:
            why.append("watchlist_relevance")
        if stress_bonus > 0:
            why.append("active_stress_alignment")
        rep_title = bucket["headlines"][0] if bucket["headlines"] else (bucket["cluster_summaries"][0] if bucket["cluster_summaries"] else bucket["label"])
        ranked.append(
            {
                "category": category,
                "label": bucket["label"],
                "score": round(score, 2),
                "representative_title": rep_title,
                "why_important": why,
                "checkpoint": EVENT_CHECKPOINT_MAP.get(category, EVENT_CHECKPOINT_MAP["general"]),
                "web_hits": bucket["web_hits"],
                "cluster_hits": bucket["cluster_hits"],
                "relation_hits": bucket["relation_hits"] + bucket["reasoning_hits"],
                "relation_quality_avg": round(
                    (
                        bucket["relation_quality_sum"] + bucket["reasoning_quality_sum"]
                    ) / max(bucket["relation_hits"] + bucket["reasoning_hits"], 1),
                    3,
                ),
                "effective_relation_score_sum": round(bucket["relation_effective_sum"], 3),
                "watch_overlap_count": watch_overlap,
                "headlines": bucket["headlines"][:3],
                "cluster_summaries": bucket["cluster_summaries"][:2],
                "relation_summaries": bucket["reasoning_summaries"][:2],
                "related_tickers": sorted(bucket["relation_tickers"])[:6],
                "related_ticker_names": [stock_names.get(code, code) for code in sorted(bucket["relation_tickers"])[:6]],
                "freshest_relation_source_at": bucket["freshest_relation_source_at"],
            }
        )

    ranked.sort(key=lambda x: (float(x.get("score") or 0.0), int(x.get("cluster_hits") or 0), int(x.get("relation_hits") or 0)), reverse=True)
    top_events = ranked[:5]

    checkpoints: list[str] = []
    for item in top_events:
        cp = str(item.get("checkpoint") or "").strip()
        if cp and cp not in checkpoints:
            checkpoints.append(cp)

    source_lines: list[str] = []
    for item in top_events:
        for title in item.get("headlines") or []:
            line = str(title or "").strip()
            if line and line not in source_lines:
                source_lines.append(line)

    return {
        "top_events": top_events,
        "checkpoints": checkpoints[:5] or [
            "변동성·환율·핵심 이벤트가 같은 방향으로 움직이는지",
            "내부 뉴스와 외부 헤드라인이 같은 리스크를 가리키는지",
        ],
        "source_lines": source_lines[:10],
        "web_signal_count": len(web_rows),
        "relation_signal_count": len(relation_rows),
        "relation_reasoning_count": len(reasoning_rows),
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


def _build_relation_context(
    relation_rows: list[dict[str, Any]],
    reasoning_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    reasoning_map = {str(r.get("ticker") or "").strip(): r for r in reasoning_rows}
    top_signals: list[dict[str, Any]] = []
    latest_reasoning_asof = ""
    for row in reasoning_rows:
        ts = str(row.get("asof_ts") or "").strip()
        if ts and ts > latest_reasoning_asof:
            latest_reasoning_asof = ts

    for row in relation_rows[:8]:
        code = str(row.get("ticker") or "").strip()
        reason = reasoning_map.get(code) or {}
        effective = round(_to_float(row.get("effective_relation_score")), 3)
        quality = round(_to_float(row.get("relation_quality"), 0.5), 3)
        support_events = int(_to_float(row.get("support_events")))
        support_clusters = int(_to_float(row.get("support_clusters")))
        bias = str(row.get("relation_bias") or "").strip()
        summary = str(reason.get("summary") or "").strip()
        source_max_published_at = str(reason.get("source_max_published_at") or "").strip()
        source_max_cluster_asof_ts = str(reason.get("source_max_cluster_asof_ts") or "").strip()
        channels = str(row.get("top_channels_str") or "").strip()
        strength_label = _relation_strength_label(effective)
        quality_label = _relation_quality_label(quality)
        support_text = _relation_support_text(support_events, support_clusters)
        freshness_text = _relation_freshness_label(source_max_published_at, source_max_cluster_asof_ts)
        channel_reason = _relation_channel_reason(channels, bias)
        top_signals.append(
            {
                "ticker": code,
                "name": str(row.get("ticker_name") or "").strip(),
                "effective_relation_score": effective,
                "relation_quality": quality,
                "relation_bias": bias,
                "support_events": support_events,
                "support_clusters": support_clusters,
                "channels": channels,
                "reason_summary": summary,
                "reason_confidence": round(_to_float(reason.get("confidence"), 0.0), 3),
                "source_max_published_at": source_max_published_at,
                "source_max_cluster_asof_ts": source_max_cluster_asof_ts,
                "strength_label": strength_label,
                "quality_label": quality_label,
                "bias_text": _relation_bias_text(bias),
                "support_text": support_text,
                "freshness_text": freshness_text,
                "channel_reason": channel_reason,
                "why_candidate": _relation_why_candidate(
                    str(row.get("ticker_name") or code).strip(),
                    bias=bias,
                    strength_label=strength_label,
                    quality_label=quality_label,
                    support_text=support_text,
                    reason_summary=summary,
                    channels=channels,
                    freshness_text=freshness_text,
                ),
            }
        )

    return {
        "top_signals": top_signals,
        "latest_reasoning_asof": latest_reasoning_asof,
        "rows_signals": len(relation_rows),
        "rows_reasonings": len(reasoning_rows),
    }


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
    external_research = _build_external_research(
        next_week_start,
        next_week_end,
        news_rows=news_rows,
        latest_watchlist=latest_watchlist,
        top_decisions=top_decisions,
        stress_flags=stress_flags,
    )
    relation_context = _build_relation_context(
        _get_relation_signal_rows(limit=20, min_abs_score=0.16),
        _get_relation_reasoning_rows(limit=20, min_confidence=0.32),
    )

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
        "relation_context": relation_context,
        "external_research": external_research,
        "scenario_context": scenario_context,
        "action_context": action_context,
        "watch_context": watch_context,
    }
