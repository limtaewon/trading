#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from env_bootstrap import bootstrap_openclaw_env
from market_realtime import fetch_naver_realtime_indices, fetch_naver_usdkrw
from web_market_signals import fetch_web_market_signals

bootstrap_openclaw_env()

try:
    import requests
except ImportError:
    from _requests_compat import requests


HOME = Path.home()
BASE = HOME / ".openclaw"
EXECUTION_MODE_FILE = BASE / "state" / "market_execution_mode.json"
DEFAULT_STATE_DIR = BASE / "state" / "reporting"
DEFAULT_PREV_STATE_FILE = DEFAULT_STATE_DIR / "telegram_owner_morning.json"
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "trading").strip() or "trading"
WATCHLIST_SOURCE = os.environ.get("WATCHLIST_ACTIVE_SOURCE", "enrich_data").strip() or "enrich_data"


def _resolve_clickhouse() -> tuple[str, tuple[str, str] | None]:
    raw = (
        os.environ.get("CLICKHOUSE_URL", "").strip()
        or os.environ.get("CLICKHOUSE_HOST", "").strip()
        or "http://localhost:8123"
    )
    user = os.environ.get("CLICKHOUSE_USER", "").strip()
    password = os.environ.get("CLICKHOUSE_PASS", os.environ.get("CLICKHOUSE_PASSWORD", "")).strip()

    parsed = urlsplit(raw)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query_dict = dict(query_pairs)
    if not user:
        user = (parsed.username or query_dict.get("user") or "").strip()
    if not password:
        password = (parsed.password or query_dict.get("password") or "").strip()

    netloc = parsed.netloc or parsed.path
    if "@" in netloc:
        netloc = netloc.split("@", 1)[1]
    clean_pairs = [(k, v) for (k, v) in query_pairs if k.lower() not in {"user", "password"}]
    clean_query = urlencode(clean_pairs, doseq=True)
    clean_url = urlunsplit((parsed.scheme or "http", netloc, parsed.path if parsed.netloc else "", clean_query, parsed.fragment))

    if not user:
        user = "default"
    auth = (user, password) if user else None
    return clean_url, auth


CLICKHOUSE_URL, CLICKHOUSE_AUTH = _resolve_clickhouse()


def _now_kst() -> datetime:
    return datetime.now().astimezone()


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _to_str(v: Any, default: str = "") -> str:
    s = str(v or "").strip()
    return s if s else default


def _public_name(v: Any, fallback: str = "해당 종목") -> str:
    s = str(v or "").strip()
    if not s or s.isdigit():
        return fallback
    return s


def _parse_json_obj(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    text = _to_str(raw)
    if not text:
        return {}
    try:
        obj = json.loads(text)
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _load_json_file(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def _save_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def save_payload_state(payload: dict[str, Any], path: Path | None = None) -> None:
    _save_json_file(path or DEFAULT_PREV_STATE_FILE, payload)


def _dedupe(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _split_flags(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return _dedupe([_to_str(x) for x in raw if _to_str(x)])
    text = _to_str(raw)
    if not text:
        return []
    return _dedupe([part.strip() for part in text.split(",") if part.strip()])


def _score_band(score: float) -> str:
    if score >= 75:
        return "A"
    if score >= 68:
        return "B"
    if score >= 60:
        return "C"
    return "D"


def _invert_flag_to_trigger(flag: str) -> str:
    special = {
        "GEOPOLITICAL_RISK": "지정학 리스크 완화 확인",
        "OIL_SHOCK_RISK": "유가 충격 완화 확인",
        "SHIPPING_DISRUPTION_RISK": "해운 차질 완화 확인",
        "ACTION_POSTURE_DEFENSIVE": "방어 posture 해제 확인",
        "HARD_RISK_OFF": "전역 위험차단 해제 확인",
    }
    if flag in special:
        return special[flag]
    for op_from, op_to in ((">=", "<"), ("<=", ">"), (">", "<="), ("<", ">=")):
        if op_from in flag:
            left, right = [part.strip() for part in flag.split(op_from, 1)]
            if left and right:
                return f"{left}{op_to}{right}"
    return f"{flag} 완화 확인"


def _normalize_ticker_field(row: dict[str, Any]) -> str:
    for key in ("ticker", "c.ticker"):
        value = _to_str(row.get(key))
        if value:
            return value
    return ""


def ch_query(sql: str) -> list[dict[str, Any]]:
    try:
        response = requests.post(
            CLICKHOUSE_URL,
            params={"database": CLICKHOUSE_DB, "default_format": "JSON"},
            data=(sql.strip() + "\n").encode("utf-8"),
            timeout=45,
            auth=CLICKHOUSE_AUTH,
        )
        response.raise_for_status()
        obj = response.json()
        data = obj.get("data", [])
        return data if isinstance(data, list) else []
    except Exception:
        return []


def ch_scalar(sql: str, key: str, default: Any) -> Any:
    rows = ch_query(sql)
    if not rows:
        return default
    return rows[0].get(key, default)


def load_execution_mode_state() -> dict[str, Any]:
    default = {
        "execution_mode": "normal",
        "action_posture": "normal",
        "session_id": "UNKNOWN",
        "stress_flags": [],
        "mode_reason_codes": [],
        "allowed_universe": "watchlist",
        "allowed_tickers": [],
        "hard_riskoff": False,
        "latest_decision_id": "",
    }
    raw = _load_json_file(EXECUTION_MODE_FILE, default)
    return raw if isinstance(raw, dict) else default


def get_regime_rows(limit: int = 2) -> list[dict[str, Any]]:
    return ch_query(
        f"""
SELECT
  date,
  kospi_close,
  kospi_change_pct,
  kosdaq_close,
  kosdaq_change_pct,
  vix_level,
  usdkrw,
  regime_label,
  summary,
  updated_at,
  ifNull(action_posture, 'normal') AS action_posture,
  stress_flags,
  ifNull(guide_text, '') AS guide_text
FROM trading.market_regime
ORDER BY date DESC, updated_at DESC
LIMIT {max(1, int(limit))}
"""
    )


def get_latest_decision_run(preferred_decision_id: str = "") -> dict[str, Any]:
    where = f"WHERE decision_id = '{preferred_decision_id}'" if preferred_decision_id else ""
    rows = ch_query(
        f"""
SELECT
  toString(decision_id) AS decision_id,
  toString(decision_time) AS decision_time,
  total_score,
  absolute_block_reason,
  stage_debug_json
FROM trading.decision_run
{where}
ORDER BY decision_time DESC
LIMIT 1
"""
    )
    if rows:
        return rows[0]
    if preferred_decision_id:
        return get_latest_decision_run("")
    return {}


def get_candidate_rows(decision_id: str, limit: int = 3) -> list[dict[str, Any]]:
    if not decision_id:
        return []
    return ch_query(
        f"""
WITH
  latest_tech AS (SELECT max(date) AS d FROM trading.technical_signals),
  latest_rel AS (SELECT max(asof_ts) AS ts FROM trading.hidden_relation_signals),
  latest_reasoning AS (
    SELECT
      ticker,
      argMax(summary, asof_ts) AS relation_summary,
      argMax(toFloat64(confidence), asof_ts) AS relation_confidence,
      argMax(source_max_published_at, asof_ts) AS relation_source_max_published_at,
      argMax(source_max_cluster_asof_ts, asof_ts) AS relation_source_max_cluster_asof_ts
    FROM trading.hidden_relation_reasoning
    GROUP BY ticker
  )
SELECT
  c.ticker AS ticker,
  any(ts.ticker_name) AS ticker_name,
  c.action,
  c.total_score,
  any(ts.signal) AS signal,
  max(toFloat64(ts.rsi14)) AS rsi14,
  max(toFloat64(ts.vol_ratio)) AS vol_ratio,
  max(toFloat64(ts.close_price)) AS close_price,
  max(toFloat64(ts.ma20)) AS ma20,
  max(toFloat64(ts.ma60)) AS ma60,
  max(toFloat64(ifNull(hrs.total_relation_score, 0))) AS rel_score,
  max(toFloat64(ifNull(hrs.relation_quality, 0.5))) AS relation_quality,
  max(toFloat64(ifNull(hrs.support_events, 0))) AS support_events,
  max(toFloat64(ifNull(hrs.support_clusters, 0))) AS support_clusters,
  max(toFloat64(abs(ifNull(hrs.total_relation_score, 0)) * (0.5 + 0.5 * ifNull(hrs.relation_quality, 0.5)))) AS effective_relation_score,
  any(ifNull(hrs.relation_bias, 'neutral')) AS rel_bias,
  any(ifNull(hrs.top_channels, [])) AS top_channels,
  argMax(toFloat64(vfs.foreign_net_flow), vfs.ts) AS foreign_flow,
  argMax(toFloat64(vfs.inst_net_flow), vfs.ts) AS inst_flow,
  any(ifNull(lr.relation_summary, '')) AS relation_summary,
  any(toFloat64(ifNull(lr.relation_confidence, 0))) AS relation_confidence,
  any(ifNull(lr.relation_source_max_published_at, '')) AS relation_source_max_published_at,
  any(ifNull(lr.relation_source_max_cluster_asof_ts, '')) AS relation_source_max_cluster_asof_ts,
  any(c.absolute_block_reason) AS absolute_block_reason
FROM trading.decision_candidate c
LEFT JOIN trading.technical_signals ts
  ON ts.ticker = c.ticker
 AND ts.date = (SELECT d FROM latest_tech)
LEFT JOIN trading.hidden_relation_signals hrs
  ON hrs.ticker = c.ticker
 AND hrs.asof_ts = (SELECT ts FROM latest_rel)
LEFT JOIN latest_reasoning lr
  ON lr.ticker = c.ticker
LEFT JOIN trading.v_feature_snapshot vfs
  ON vfs.symbol = c.ticker
 AND vfs.ts >= now() - INTERVAL 2 DAY
WHERE c.decision_id = '{decision_id}'
GROUP BY c.ticker, c.action, c.total_score
ORDER BY c.total_score DESC
LIMIT {max(1, int(limit))}
"""
    )


def get_relation_rows(limit: int = 8, min_abs_score: float = 0.16) -> list[dict[str, Any]]:
    return ch_query(
        f"""
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
LIMIT {max(1, int(limit))}
"""
    )


def get_relation_reasonings(limit: int = 8, min_confidence: float = 0.30) -> list[dict[str, Any]]:
    return ch_query(
        f"""
SELECT
  ticker,
  ticker_name,
  toString(asof_ts) AS asof_ts_s,
  confidence,
  summary,
  source_max_published_at,
  source_max_cluster_asof_ts,
  relation_quality,
  support_events,
  support_clusters,
  effective_relation_score
FROM trading.hidden_relation_reasoning
WHERE toFloat64OrZero(toString(confidence)) >= {float(min_confidence)}
ORDER BY asof_ts DESC, effective_relation_score DESC
LIMIT {max(1, int(limit))}
"""
    )


def get_recent_events(limit: int = 3) -> list[dict[str, Any]]:
    rows = ch_query(
        f"""
SELECT
  title,
  importance,
  sentiment,
  impact_type,
  toString(published_at) AS published_at
FROM trading.news
WHERE importance >= 4
  AND published_at >= now() - INTERVAL 48 HOUR
ORDER BY published_at DESC
LIMIT {max(1, int(limit))}
"""
    )
    events: list[dict[str, Any]] = []
    for row in rows:
        title = _to_str(row.get("title"))
        if not title:
            continue
        impact = _to_str(row.get("impact_type"), "market")
        market_impact = {
            "macro": "risk_off",
            "market": "risk_off",
            "sector": "sector_rotation",
            "stock": "stock_specific",
        }.get(impact, "market")
        events.append(
            {
                "title": title,
                "category": impact,
                "market_impact": market_impact,
                "importance": _to_int(row.get("importance"), 0),
                "summary": f"{impact} 축 이벤트, sentiment={_to_str(row.get('sentiment'), 'neutral')}",
                "published_at": _to_str(row.get("published_at")),
            }
        )
    return events


def get_ops_context() -> dict[str, Any]:
    news_rows_3h = _to_int(ch_scalar("SELECT count() AS c FROM trading.news WHERE published_at >= now() - INTERVAL 3 HOUR", "c", 0), 0)
    cluster_age_min = _to_int(
        ch_scalar(
            "SELECT if(count()=0, 99999, greatest(dateDiff('minute', max(asof_ts), now()), 0)) AS age_min FROM trading.news_cluster_state",
            "age_min",
            99999,
        ),
        99999,
    )
    relation_age_min = _to_int(
        ch_scalar(
            "SELECT if(count()=0, 99999, greatest(dateDiff('minute', max(asof_ts), now()), 0)) AS age_min FROM trading.hidden_relation_signals",
            "age_min",
            99999,
        ),
        99999,
    )
    watchlist_rows = ch_query(
        f"""
SELECT status, inserted_rows, min_expected_rows, toString(ts) AS ts
FROM trading.interest_watchlist_runs
WHERE source = '{WATCHLIST_SOURCE}'
ORDER BY ts DESC
LIMIT 1
"""
    )
    watchlist_status = "missing"
    confidence_notes: list[str] = []
    alerts: list[str] = []
    if watchlist_rows:
        row = watchlist_rows[0]
        watchlist_status = _to_str(row.get("status"), "unknown")
        inserted = _to_int(row.get("inserted_rows"), 0)
        min_expected = _to_int(row.get("min_expected_rows"), 0)
        if watchlist_status == "ok" and inserted >= max(1, min_expected):
            confidence_notes.append("watchlist run 정상")
        else:
            alerts.append(f"watchlist={watchlist_status} ({inserted}/{min_expected})")
    if cluster_age_min <= 180:
        confidence_notes.append(f"cluster freshness {cluster_age_min}m")
    else:
        alerts.append(f"cluster stale {cluster_age_min}m")
    if relation_age_min <= 180:
        confidence_notes.append(f"relation freshness {relation_age_min}m")
    else:
        alerts.append(f"relation stale {relation_age_min}m")

    pending_exit_items = _load_json_file(BASE / "state" / "pending_exit_orders.json", [])
    pending_exit_count = len(pending_exit_items) if isinstance(pending_exit_items, list) else 0
    if pending_exit_count > 0:
        alerts.append(f"pending_exit_queue={pending_exit_count}")

    return {
        "data_health": {
            "market_data_fresh": True,
            "watchlist_fresh": watchlist_status == "ok",
            "news_pipeline_fresh": news_rows_3h > 0,
            "execution_mode_fresh": EXECUTION_MODE_FILE.exists(),
        },
        "alerts": alerts,
        "confidence_notes": confidence_notes,
        "metrics": {
            "news_rows_3h": news_rows_3h,
            "cluster_age_min": cluster_age_min,
            "relation_age_min": relation_age_min,
            "pending_exit_count": pending_exit_count,
            "watchlist_status": watchlist_status,
        },
    }


def _build_market_phase(snapshot: dict[str, Any], stress_flags: list[str], action_posture: str) -> dict[str, str]:
    kospi_pct = _to_float(((snapshot.get("index") or {}).get("kospi_change_pct")), 0.0)
    kosdaq_pct = _to_float(((snapshot.get("index") or {}).get("kosdaq_change_pct")), 0.0)
    stress_score = len(stress_flags)
    if stress_score >= 3 and (kospi_pct > 0 or kosdaq_pct > 0):
        label = "high_volatility_rebound"
        summary = "지수 반등이 나왔지만 전역 리스크 신호가 여전히 높은 상태"
    elif stress_score >= 3:
        label = "risk_off_decline"
        summary = "리스크오프 압력이 강하고 방어적 해석이 우선인 구간"
    elif action_posture == "defensive":
        label = "defensive_transition"
        summary = "지수 방향보다 방어 posture 유지 여부가 더 중요한 구간"
    else:
        label = "normal_rotation"
        summary = "전역 리스크보다 섹터/종목 선별이 상대적으로 중요한 구간"
    return {"label": label, "summary": summary}


def _effective_mode(mode_state: dict[str, Any], regime: dict[str, Any], stage_debug: dict[str, Any]) -> tuple[str, str, bool]:
    raw_mode = _to_str(mode_state.get("execution_mode"), "normal").lower()
    stage1 = stage_debug.get("stage1") if isinstance(stage_debug.get("stage1"), dict) else {}
    posture = _to_str(stage1.get("action_posture") or regime.get("action_posture") or mode_state.get("action_posture"), "normal").lower()
    hard_riskoff = bool(mode_state.get("hard_riskoff") or stage1.get("hard_riskoff"))
    effective_mode = raw_mode
    if raw_mode not in {"shock", "recovery", "close_only"} and (hard_riskoff or posture == "defensive"):
        effective_mode = "defensive"
    return effective_mode, posture, hard_riskoff


def _build_mode_context(mode_state: dict[str, Any], regime: dict[str, Any], stage_debug: dict[str, Any]) -> dict[str, Any]:
    effective_mode, posture, hard_riskoff = _effective_mode(mode_state, regime, stage_debug)
    mode_reason = _split_flags(mode_state.get("mode_reason_codes"))
    if not mode_reason:
        mode_reason = _split_flags(stage_debug.get("mode", {}).get("reason_codes")) if isinstance(stage_debug.get("mode"), dict) else []
    if not mode_reason and _to_str(regime.get("guide_text")):
        mode_reason = [_to_str(regime.get("guide_text"))]

    if effective_mode == "close_only":
        allowed_actions = ["hold", "reduce", "risk_off_rebalance"]
        blocked_actions = ["new_buy", "aggressive_add", "theme_chasing"]
    elif effective_mode == "shock":
        allowed_actions = ["hold", "reduce", "shock_core_only"]
        blocked_actions = ["new_buy", "theme_chasing", "aggressive_add"]
    elif effective_mode == "recovery":
        allowed_actions = ["hold", "selective_add", "core_reentry"]
        blocked_actions = ["theme_chasing", "full_risk_on"]
    elif effective_mode == "defensive":
        allowed_actions = ["hold", "reduce", "risk_off_rebalance"]
        blocked_actions = ["new_buy", "aggressive_add", "theme_chasing"]
    else:
        allowed_actions = ["hold", "selective_buy", "watchlist_based_entry"]
        blocked_actions = ["theme_chasing"]

    if _to_str(mode_state.get("session_id")) == "WEEKEND_CLOSED":
        blocked_actions = _dedupe(blocked_actions + ["market_execution_closed"])

    bullish_reenable = [_invert_flag_to_trigger(flag) for flag in (_split_flags(mode_state.get("stress_flags")) or _split_flags(regime.get("stress_flags")))]
    guidance = {
        "defensive": "현재는 신규매수보다 리스크 축소와 현금 관리가 우선",
        "shock": "지금은 shock core와 리스크 통제가 우선",
        "recovery": "회복 초기라 코어 자산 중심의 제한적 대응이 우선",
        "close_only": "신규 진입보다 보유 리스크 정리가 우선",
        "normal": "watchlist 기반 선별 대응이 가능한 구간",
    }
    return {
        "execution_mode": effective_mode,
        "system_mode_raw": _to_str(mode_state.get("execution_mode"), "normal"),
        "action_posture": posture,
        "mode_reason": mode_reason,
        "allowed_actions": allowed_actions,
        "blocked_actions": blocked_actions,
        "mode_change_triggers": {
            "bullish_reenable": _dedupe(bullish_reenable),
            "further_defensive": [],
        },
        "one_line_policy": guidance.get(effective_mode, guidance["normal"]),
        "hard_riskoff": hard_riskoff,
        "session": _to_str(mode_state.get("session_id"), "UNKNOWN"),
        "allowed_universe": _to_str(mode_state.get("allowed_universe"), "watchlist"),
    }


def _candidate_reason(row: dict[str, Any]) -> str:
    parts: list[str] = []
    signal = _to_str(row.get("signal"))
    if signal and signal != "neutral":
        parts.append("기술 흐름이 양호합니다")
    effective_relation_score = _to_float(row.get("effective_relation_score"))
    relation_quality = _to_float(row.get("relation_quality"), 0.5)
    support_events = _to_int(row.get("support_events"), 0)
    support_clusters = _to_int(row.get("support_clusters"), 0)
    relation_summary = _to_str(row.get("relation_summary"))
    channel_reason = _relation_channel_reason(row.get("channels") or [], _to_str(row.get("relation_bias"), "neutral"))
    if effective_relation_score >= 4.0 and relation_quality >= 0.65:
        parts.append(f"내부 연관 전이 신호가 강하고, 이벤트 {support_events}건과 클러스터 {support_clusters}건에서 반복 확인됐습니다")
    elif effective_relation_score >= 2.0:
        parts.append("내부 연관 전이 신호가 보통 이상으로 확인됩니다")
    if relation_summary:
        parts.append(relation_summary)
    elif channel_reason:
        parts.append(channel_reason)
    foreign_flow = _to_float(row.get("flow", {}).get("foreign"))
    inst_flow = _to_float(row.get("flow", {}).get("institution"))
    if foreign_flow > 0 or inst_flow > 0:
        parts.append("수급은 일부 유입 신호가 있습니다")
    elif foreign_flow < 0 or inst_flow < 0:
        parts.append("수급은 아직 엇갈리거나 약한 편입니다")
    return " ".join(parts[:2]) or "후보 근거 요약이 아직 제한적입니다."


def _candidate_action_reason(row: dict[str, Any], mode_context: dict[str, Any]) -> str:
    abs_blocks = [_to_str(x) for x in row.get("absolute_block_reason", []) if _to_str(x)]
    current_action = _to_str(row.get("current_action"), "HOLD")
    if abs_blocks:
        return f"absolute block: {', '.join(abs_blocks[:2])}"
    if current_action != "BUY":
        return f"현재 액션은 {current_action}"
    if "new_buy" in mode_context.get("blocked_actions", []):
        return "시장 전역 방어 모드로 신규매수 제한"
    return "추가 확인 전 관찰 우선"


def _build_candidate_context(candidate_rows: list[dict[str, Any]], mode_context: dict[str, Any]) -> dict[str, Any]:
    top_candidates: list[dict[str, Any]] = []
    for row in candidate_rows:
        score = _to_float(row.get("total_score"))
        item = {
            "ticker": _normalize_ticker_field(row),
            "name": _public_name(row.get("ticker_name")),
            "sector_bucket": _to_str(row.get("rel_bias"), "neutral"),
            "decision_score": score,
            "signal_grade": _score_band(score),
            "technical": {
                "signal": _to_str(row.get("signal"), "neutral"),
                "rsi": _to_float(row.get("rsi14")),
                "volume_ratio": _to_float(row.get("vol_ratio")),
                "close_price": _to_float(row.get("close_price")),
                "ma20": _to_float(row.get("ma20")),
                "ma60": _to_float(row.get("ma60")),
            },
            "flow": {
                "foreign": _to_float(row.get("foreign_flow")),
                "institution": _to_float(row.get("inst_flow")),
            },
            "relation": {
                "effective_score": _to_float(row.get("effective_relation_score")),
                "quality": _to_float(row.get("relation_quality"), 0.5),
                "support_events": _to_int(row.get("support_events"), 0),
                "support_clusters": _to_int(row.get("support_clusters"), 0),
                "bias": _to_str(row.get("rel_bias"), "neutral"),
                "summary": _to_str(row.get("relation_summary")),
                "confidence": _to_float(row.get("relation_confidence")),
                "source_max_published_at": _to_str(row.get("relation_source_max_published_at")),
                "source_max_cluster_asof_ts": _to_str(row.get("relation_source_max_cluster_asof_ts")),
                "channels": row.get("top_channels") or [],
            },
            "thesis": _candidate_reason(
                {
                    "signal": row.get("signal"),
                    "effective_relation_score": _to_float(row.get("effective_relation_score")),
                    "relation_quality": _to_float(row.get("relation_quality"), 0.5),
                    "support_events": _to_int(row.get("support_events"), 0),
                    "support_clusters": _to_int(row.get("support_clusters"), 0),
                    "relation_summary": _to_str(row.get("relation_summary")),
                    "channels": row.get("top_channels") or [],
                    "relation_bias": _to_str(row.get("rel_bias"), "neutral"),
                    "flow": {
                        "foreign": _to_float(row.get("foreign_flow")),
                        "institution": _to_float(row.get("inst_flow")),
                    },
                }
            ),
            "current_action": _to_str(row.get("action"), "HOLD").upper(),
            "absolute_block_reason": row.get("absolute_block_reason") or [],
        }
        item["action_reason"] = _candidate_action_reason(item, mode_context)
        item["invalidation"] = "추세 약화 또는 전역 리스크 악화"
        item["reentry_condition"] = "execution_mode 완화 + 추세 유지 확인"
        top_candidates.append(item)

    avoid_list: list[dict[str, Any]] = []
    for row in top_candidates:
        if row.get("technical", {}).get("signal") == "sell":
            avoid_list.append(
                {
                    "ticker": row.get("ticker"),
                    "name": row.get("name"),
                    "reason": "기술 신호가 아직 약함",
                }
            )
    return {
        "selection_policy": mode_context.get("one_line_policy", "현재 정책 기준 따름"),
        "top_candidates": top_candidates,
        "avoid_list": avoid_list[:2],
    }


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


def _relation_bias_text(bias: str) -> str:
    value = _to_str(bias).lower()
    if value == "positive":
        return "긍정 전이"
    if value == "negative":
        return "부정 전이"
    return "중립 전이"


def _relation_channel_text(channels: Any) -> str:
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
    raw_items = channels if isinstance(channels, list) else str(channels or "").split(",")
    items = [mapping.get(_to_str(item), _to_str(item)) for item in raw_items if _to_str(item)]
    deduped: list[str] = []
    for item in items:
        if item and item not in deduped:
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
    bias_key = _to_str(bias).lower()
    if bias_key == "positive":
        return positive.get(channel, channel)
    if bias_key == "negative":
        return negative.get(channel, channel)
    return neutral.get(channel, channel)


def _relation_channel_reason(channels: Any, bias: str) -> str:
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
    return f"{_relation_bias_text(bias)} 관점에서는 {tail}."


def _relation_support_text(events: int, clusters: int) -> str:
    parts: list[str] = []
    if events > 0:
        parts.append(f"이벤트 {events}건")
    if clusters > 0:
        parts.append(f"클러스터 {clusters}건")
    if not parts:
        return "근거 축적이 아직 제한적입니다."
    return ", ".join(parts) + "에서 반복 확인됐습니다."


def _relation_freshness_text(source_at: str, cluster_at: str) -> str:
    ts = _to_str(source_at) or _to_str(cluster_at)
    return f"근거 최신 시각 {ts}" if ts else "최신성 정보 부족"


def _build_relation_context(
    relation_rows: list[dict[str, Any]],
    reasoning_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    reasoning_map = {_to_str(r.get("ticker")): r for r in reasoning_rows}
    top_signals: list[dict[str, Any]] = []
    latest_reasoning_asof = ""
    for row in reasoning_rows:
        ts = _to_str(row.get("asof_ts_s"))
        if ts and ts > latest_reasoning_asof:
            latest_reasoning_asof = ts

    for row in relation_rows[:8]:
        code = _to_str(row.get("ticker"))
        reason = reasoning_map.get(code) or {}
        effective = _to_float(row.get("effective_relation_score"))
        quality = _to_float(row.get("relation_quality"), 0.5)
        support_events = _to_int(row.get("support_events"), 0)
        support_clusters = _to_int(row.get("support_clusters"), 0)
        summary = _to_str(reason.get("summary"))
        bias = _to_str(row.get("relation_bias"), "neutral")
        strength_label = _relation_strength_label(effective)
        quality_label = _relation_quality_label(quality)
        support_text = _relation_support_text(support_events, support_clusters)
        freshness_text = _relation_freshness_text(
            _to_str(reason.get("source_max_published_at")),
            _to_str(reason.get("source_max_cluster_asof_ts")),
        )
        name = _public_name(row.get("ticker_name"))
        channels = row.get("top_channels") or []
        channel_text = _relation_channel_text(channels)
        channel_reason = _relation_channel_reason(channels, bias)
        if summary:
            why_candidate = (
                f"{summary} {_relation_bias_text(bias)} 축에서 {support_text} "
                + (f"{channel_reason} " if channel_reason else "")
                + f"{freshness_text}."
            ).strip()
        else:
            why_candidate = (
                f"{name}은(는) {_relation_bias_text(bias)} 축에서 {support_text} "
                + (f"{channel_reason} " if channel_reason else "")
                + (f"특히 {channel_text} 요인이 함께 반영됩니다. " if (not channel_reason and channel_text) else "")
                + f"내부 연관 전이 신호는 {strength_label}, 근거는 {quality_label}입니다."
            ).strip()
        top_signals.append(
            {
                "ticker": code,
                "name": name,
                "effective_relation_score": round(effective, 3),
                "relation_quality": round(quality, 3),
                "relation_bias": bias,
                "support_events": support_events,
                "support_clusters": support_clusters,
                "reason_summary": summary,
                "reason_confidence": _to_float(reason.get("confidence")),
                "source_max_published_at": _to_str(reason.get("source_max_published_at")),
                "source_max_cluster_asof_ts": _to_str(reason.get("source_max_cluster_asof_ts")),
                "strength_label": strength_label,
                "quality_label": quality_label,
                "bias_text": _relation_bias_text(bias),
                "support_text": support_text,
                "freshness_text": freshness_text,
                "channel_reason": channel_reason,
                "channel_text": channel_text,
                "why_candidate": why_candidate,
            }
        )

    return {
        "top_signals": top_signals,
        "latest_reasoning_asof": latest_reasoning_asof,
        "rows_signals": len(relation_rows),
        "rows_reasonings": len(reasoning_rows),
    }


def _build_market_context(regime: dict[str, Any], stage_debug: dict[str, Any], effective_mode: str, posture: str) -> dict[str, Any]:
    rt_indices = fetch_naver_realtime_indices(timeout_sec=8)
    rt_usdkrw = fetch_naver_usdkrw(timeout_sec=8)
    stage2 = stage_debug.get("stage2") if isinstance(stage_debug.get("stage2"), dict) else {}
    stress_flags = _split_flags(regime.get("stress_flags"))

    kospi_value = _to_float(regime.get("kospi_close"))
    kospi_pct = _to_float(regime.get("kospi_change_pct"))
    kosdaq_value = _to_float(regime.get("kosdaq_close"))
    kosdaq_pct = _to_float(regime.get("kosdaq_change_pct"))
    usdkrw = _to_float(regime.get("usdkrw"))

    if isinstance(rt_indices.get("KOSPI"), dict):
        kospi_value = _to_float(rt_indices["KOSPI"].get("price"), kospi_value)
        kospi_pct = _to_float(rt_indices["KOSPI"].get("change_pct"), kospi_pct)
    if isinstance(rt_indices.get("KOSDAQ"), dict):
        kosdaq_value = _to_float(rt_indices["KOSDAQ"].get("price"), kosdaq_value)
        kosdaq_pct = _to_float(rt_indices["KOSDAQ"].get("change_pct"), kosdaq_pct)
    if rt_usdkrw:
        usdkrw = _to_float(rt_usdkrw.get("price"), usdkrw)

    snapshot = {
        "index": {
            "kospi": kospi_value,
            "kosdaq": kosdaq_value,
            "kospi_change_pct": kospi_pct,
            "kosdaq_change_pct": kosdaq_pct,
        },
        "macro": {
            "vix": _to_float(regime.get("vix_level")),
            "usdkrw": usdkrw,
            "oil_price": 0.0,
        },
        "market_stress": {
            "stress_score": len(stress_flags),
            "stress_flags": stress_flags,
            "stress_level": "high" if len(stress_flags) >= 3 else "normal",
        },
        "flow": {
            "foreign_5d": _to_float(stage2.get("foreign_net_krw_5d")),
            "institution_5d": _to_float(stage2.get("inst_net_krw_5d")),
            "shock_level": _to_str(stage2.get("shock_level"), "UNKNOWN").lower(),
        },
    }
    snapshot["market_phase"] = _build_market_phase(snapshot, stress_flags, posture)
    snapshot["market_phase"]["mode_hint"] = effective_mode
    return snapshot


def _build_execution_context(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "today_orders": {
            "attempted": 0,
            "executed": 0,
            "skipped": 0,
        },
        "executed_orders": [],
        "skipped_orders": [],
        "portfolio_delta": {},
        "decision_id": _to_str(run.get("decision_id")),
        "decision_time": _to_str(run.get("decision_time")),
        "absolute_block_reason": run.get("absolute_block_reason") or [],
    }


def _build_change_context(current_payload: dict[str, Any], previous_payload: dict[str, Any]) -> dict[str, Any]:
    prev_mode = _to_str(((previous_payload.get("mode_context") or {}).get("execution_mode")), _to_str((current_payload.get("mode_context") or {}).get("execution_mode"), "normal"))
    now_mode = _to_str(((current_payload.get("mode_context") or {}).get("execution_mode")), "normal")
    prev_stress_score = _to_int((((previous_payload.get("market_context") or {}).get("market_stress") or {}).get("stress_score")), 0)
    now_stress_score = _to_int((((current_payload.get("market_context") or {}).get("market_stress") or {}).get("stress_score")), 0)
    prev_theme = _to_str(((previous_payload.get("event_context") or {}).get("dominant_theme")), "")
    now_theme = _to_str(((current_payload.get("event_context") or {}).get("dominant_theme")), "")

    if not previous_payload:
        what_changed = ["첫 리포트 생성"]
        why_it_matters = "첫 기준 상태 저장"
    else:
        what_changed = []
        if prev_mode != now_mode:
            what_changed.append(f"execution_mode {prev_mode} -> {now_mode}")
        if prev_stress_score != now_stress_score:
            what_changed.append(f"stress_score {prev_stress_score} -> {now_stress_score}")
        if prev_theme and now_theme and prev_theme != now_theme:
            what_changed.append(f"dominant_theme {prev_theme} -> {now_theme}")
        if not what_changed:
            what_changed.append("정책/핵심 테마 변화 없음")
        why_it_matters = ((current_payload.get("market_context") or {}).get("market_phase") or {}).get("summary") or "시장 해석 변화 없음"

    return {
        "from_previous_report": {
            "execution_mode": {"before": prev_mode, "after": now_mode},
            "stress_score": {"before": prev_stress_score, "after": now_stress_score},
            "dominant_theme": {"before": prev_theme, "after": now_theme},
        },
        "what_changed_today": what_changed,
        "why_it_matters": _to_str(why_it_matters),
    }


def _build_guidance_context(mode_context: dict[str, Any], market_context: dict[str, Any]) -> dict[str, Any]:
    execution_mode = _to_str(mode_context.get("execution_mode"), "normal")
    bullish = ((mode_context.get("mode_change_triggers") or {}).get("bullish_reenable") or [])
    watch = [
        "USD/KRW 안정 여부",
        "VIX 재하락 여부",
        "지정학 리스크 완화 여부",
    ]
    if execution_mode in {"defensive", "close_only"}:
        today_principles = [
            "신규매수 금지",
            "추격매수 금지",
            "체결 필요 시 리스크 축소 목적만 허용",
        ]
    elif execution_mode == "shock":
        today_principles = [
            "shock core 외 신규진입 금지",
            "현금 및 방어자산 우선",
            "고변동 종목 추격 금지",
        ]
    elif execution_mode == "recovery":
        today_principles = [
            "코어 자산 중심 제한적 재진입",
            "전면 risk-on 전환 금지",
            "확인된 추세만 선별 대응",
        ]
    else:
        today_principles = [
            "watchlist 기반 선별 대응",
            "테마 추격 금지",
            "리스크 대비 보상 비율 우선",
        ]
    return {
        "today_principles": today_principles,
        "what_to_watch": watch,
        "what_changes_my_mind": bullish,
        "one_line_conclusion": _to_str(mode_context.get("one_line_policy")) or _to_str(((market_context.get("market_phase") or {}).get("summary"))),
    }


def _build_event_context(events: list[dict[str, Any]]) -> dict[str, Any]:
    dominant_theme = ""
    if events:
        top_categories = [_to_str(e.get("category"), "market") for e in events[:3]]
        dominant_theme = "_".join(_dedupe(top_categories[:3]))
    web_signals = fetch_web_market_signals(limit=8, timeout_sec=6)
    return {
        "top_events": events,
        "dominant_theme": dominant_theme or "market_general",
        "theme_summary": events[0]["summary"] if events else "주요 이벤트 데이터 부족",
        "web_market_signals": web_signals,
    }


def build_report_payload(
    report_type: str,
    audience: str,
    top_candidates: int = 3,
    previous_payload_path: Path | None = None,
    decision_id_override: str = "",
) -> dict[str, Any]:
    mode_state = load_execution_mode_state()
    regime_rows = get_regime_rows(limit=2)
    regime = regime_rows[0] if regime_rows else {}
    preferred_decision_id = _to_str(decision_id_override) or _to_str(mode_state.get("latest_decision_id"))
    run = get_latest_decision_run(preferred_decision_id)
    stage_debug = _parse_json_obj(run.get("stage_debug_json"))

    effective_mode, posture, _ = _effective_mode(mode_state, regime, stage_debug)
    mode_context = _build_mode_context(mode_state, regime, stage_debug)
    market_context = _build_market_context(regime, stage_debug, effective_mode, posture)
    events = get_recent_events(limit=3)
    event_context = _build_event_context(events)
    candidate_context = _build_candidate_context(get_candidate_rows(_to_str(run.get("decision_id")), limit=top_candidates), mode_context)
    execution_context = _build_execution_context(run)
    ops_context = get_ops_context()

    payload: dict[str, Any] = {
        "report_type": report_type,
        "audience": audience,
        "generated_at": _now_kst().isoformat(timespec="seconds"),
        "as_of": _now_kst().isoformat(timespec="seconds"),
        "market_context": market_context,
        "mode_context": mode_context,
        "event_context": event_context,
        "candidate_context": candidate_context,
        "relation_context": _build_relation_context(
            get_relation_rows(limit=max(6, top_candidates * 3), min_abs_score=0.16),
            get_relation_reasonings(limit=max(6, top_candidates * 3), min_confidence=0.30),
        ),
        "execution_context": execution_context,
        "guidance_context": _build_guidance_context(mode_context, market_context),
        "ops_context": ops_context,
    }

    previous_payload = _load_json_file(previous_payload_path or DEFAULT_PREV_STATE_FILE, {})
    payload["change_context"] = _build_change_context(payload, previous_payload if isinstance(previous_payload, dict) else {})
    return payload


def build_internal_payload(
    top_candidates: int = 5,
    previous_payload_path: Path | None = None,
    decision_id_override: str = "",
) -> dict[str, Any]:
    return build_report_payload(
        report_type="dooray_internal",
        audience="internal",
        top_candidates=top_candidates,
        previous_payload_path=previous_payload_path,
        decision_id_override=decision_id_override,
    )


def build_owner_payload(
    report_type: str = "telegram_owner_ops",
    top_candidates: int = 3,
    previous_payload_path: Path | None = None,
    decision_id_override: str = "",
) -> dict[str, Any]:
    return build_report_payload(
        report_type=report_type,
        audience="owner",
        top_candidates=top_candidates,
        previous_payload_path=previous_payload_path,
        decision_id_override=decision_id_override,
    )


def build_public_payload(
    top_candidates: int = 2,
    previous_payload_path: Path | None = None,
    decision_id_override: str = "",
) -> dict[str, Any]:
    return build_report_payload(
        report_type="telegram_public",
        audience="public",
        top_candidates=top_candidates,
        previous_payload_path=previous_payload_path,
        decision_id_override=decision_id_override,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Build shared trading report payload")
    ap.add_argument("--report-type", default="telegram_owner_ops")
    ap.add_argument("--audience", default="owner")
    ap.add_argument("--top-candidates", type=int, default=3)
    ap.add_argument("--previous-payload", default="")
    ap.add_argument("--decision-id", default="")
    ap.add_argument("--pretty", action="store_true")
    args = ap.parse_args()

    prev_path = Path(args.previous_payload).expanduser() if str(args.previous_payload).strip() else None
    payload = build_report_payload(
        report_type=str(args.report_type).strip(),
        audience=str(args.audience).strip(),
        top_candidates=max(1, int(args.top_candidates)),
        previous_payload_path=prev_path,
        decision_id_override=str(args.decision_id).strip(),
    )
    if args.pretty:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
