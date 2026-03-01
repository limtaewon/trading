#!/usr/bin/env python3
"""decision_operating_pipeline.py

Decision Operating Spec (P0) 실행기.
- Stage 0~5 점수 계산
- absolute block 판정
- trading.decision_run / trading.decision_candidate 적재

주문 실행은 하지 않으며, 판단 로그만 기록한다.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import re
import uuid
from collections import Counter
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()


def _log(msg: str) -> None:
    print(f"{dt.datetime.now().strftime('%H:%M:%S')} [decision-p0] {msg}", flush=True)


def _ch_url_and_headers() -> tuple[str, dict[str, str]]:
    host = os.getenv("CLICKHOUSE_HOST", "").strip()
    user = os.getenv("CLICKHOUSE_USER", "").strip()
    pw = os.getenv("CLICKHOUSE_PASS", os.getenv("CLICKHOUSE_PASSWORD", "")).strip()
    headers: dict[str, str] = {"Content-Type": "text/plain; charset=utf-8"}

    if host:
        if not user:
            user = "default"
        if not pw:
            pw = "trading"
        sep = "&" if "?" in host else "?"
        return f"{host}{sep}user={user}&password={pw}", headers

    url = os.getenv("CLICKHOUSE_URL", "http://localhost:8123").strip()
    if not user:
        user = "default"
    if not pw:
        pw = "trading"
    sp = urlsplit(url)
    if sp.username is not None:
        auth = f"{sp.username}:{sp.password or ''}".encode("utf-8")
        headers["Authorization"] = "Basic " + base64.b64encode(auth).decode("ascii")
        netloc = sp.hostname or "localhost"
        if sp.port:
            netloc = f"{netloc}:{sp.port}"
        clean = urlunsplit((sp.scheme or "http", netloc, sp.path or "", sp.query, sp.fragment))
        return clean, headers

    # query string에 user/password가 없으면 기본 인증 파라미터를 덧붙인다.
    q_pairs = parse_qsl(sp.query, keep_blank_values=True)
    q_keys = {k.lower() for k, _ in q_pairs}
    if "user" not in q_keys:
        q_pairs.append(("user", user))
    if "password" not in q_keys:
        q_pairs.append(("password", pw))
    new_query = urlencode(q_pairs, doseq=True)
    with_auth = urlunsplit((sp.scheme or "http", sp.netloc or "localhost:8123", sp.path or "", new_query, sp.fragment))
    return with_auth, headers


def ch_select(sql: str, timeout_sec: int = 60) -> list[dict[str, Any]]:
    url, headers = _ch_url_and_headers()
    q = sql.strip() + "\nFORMAT JSON"
    req = Request(url, data=q.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        body = r.read().decode("utf-8", errors="replace")
    obj = json.loads(body)
    data = obj.get("data", [])
    return data if isinstance(data, list) else []


def ch_scalar(sql: str, timeout_sec: int = 30) -> str:
    url, headers = _ch_url_and_headers()
    q = sql.strip() + "\nFORMAT TSV"
    req = Request(url, data=q.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        return r.read().decode("utf-8", errors="replace").strip()


def ch_execute(sql: str, timeout_sec: int = 60) -> None:
    url, headers = _ch_url_and_headers()
    req = Request(url, data=sql.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        _ = r.read()


def ch_insert_json_each_row(table: str, rows: list[dict[str, Any]], timeout_sec: int = 120) -> None:
    if not rows:
        return
    url, headers = _ch_url_and_headers()
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    q = f"INSERT INTO {table} FORMAT JSONEachRow\n".encode("utf-8") + payload.encode("utf-8")
    req = Request(url, data=q, headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        _ = r.read()


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


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _is_ticker(s: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", s or ""))


def table_exists(name: str) -> bool:
    safe = name.replace("'", "\\'")
    q = (
        "SELECT count() "
        "FROM system.tables "
        "WHERE database = 'trading' "
        f"AND name = '{safe}'"
    )
    try:
        return int(ch_scalar(q) or "0") > 0
    except Exception:
        return False


def column_exists(table: str, column: str) -> bool:
    safe_t = table.replace("'", "\\'")
    safe_c = column.replace("'", "\\'")
    q = (
        "SELECT count() "
        "FROM system.columns "
        "WHERE database = 'trading' "
        f"AND table = '{safe_t}' "
        f"AND name = '{safe_c}'"
    )
    try:
        return int(ch_scalar(q) or "0") > 0
    except Exception:
        return False


def ensure_decision_tables() -> None:
    ch_execute(
        """
CREATE TABLE IF NOT EXISTS trading.decision_run
(
    decision_id             UUID,
    decision_time           DateTime,
    horizon                 LowCardinality(String),
    universe                LowCardinality(String),
    stage0_pass             UInt8,
    stage0_score            Float32,
    stage1_pass             UInt8,
    stage1_score            Float32,
    stage2_pass             UInt8,
    stage2_score            Float32,
    stage3_pass             UInt8,
    stage3_score            Float32,
    stage4_pass             UInt8,
    stage4_score            Float32,
    stage5_pass             UInt8,
    stage5_score            Float32,
    total_score             Float32,
    penalty_score           Float32 DEFAULT 0,
    absolute_block_reason   Array(String) DEFAULT [],
    data_freshness_json     String DEFAULT '{}',
    stage_debug_json        String DEFAULT '{}',
    model_version           String DEFAULT 'decision-operating-spec-p0',
    prompt_hash             String DEFAULT '',
    created_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (decision_time, decision_id)
"""
    )
    ch_execute(
        """
CREATE TABLE IF NOT EXISTS trading.decision_candidate
(
    decision_id                 UUID,
    ticker                      String,
    action                      LowCardinality(String),
    target_weight               Float32 DEFAULT 0,
    stage1_score                Float32 DEFAULT 0,
    stage2_stock_flow_score     Float32 DEFAULT 0,
    stage3_event_score          Float32 DEFAULT 0,
    stage4_timing_score         Float32 DEFAULT 0,
    stage5_risk_score           Float32 DEFAULT 0,
    stage5_fail_codes           Array(String) DEFAULT [],
    stage5_exec_multiplier      Float32 DEFAULT 1,
    stage3_evidence_count       UInt16 DEFAULT 0,
    stage3_score_capped         UInt8 DEFAULT 0,
    total_score                 Float32 DEFAULT 0,
    absolute_block_reason       Array(String) DEFAULT [],
    primary_cluster_id          String DEFAULT '',
    primary_event_frame_id      String DEFAULT '',
    primary_reasoning_id        String DEFAULT '',
    explanation_codes           Array(String) DEFAULT [],
    created_at                  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (decision_id, ticker)
"""
    )
    # 기존 테이블 호환
    ch_execute("ALTER TABLE trading.decision_run ADD COLUMN IF NOT EXISTS stage_debug_json String DEFAULT '{}'")
    ch_execute(
        "ALTER TABLE trading.decision_candidate ADD COLUMN IF NOT EXISTS stage5_fail_codes Array(String) DEFAULT []"
    )
    ch_execute(
        "ALTER TABLE trading.decision_candidate ADD COLUMN IF NOT EXISTS stage5_exec_multiplier Float32 DEFAULT 1"
    )
    ch_execute(
        "ALTER TABLE trading.decision_candidate ADD COLUMN IF NOT EXISTS stage3_evidence_count UInt16 DEFAULT 0"
    )
    ch_execute(
        "ALTER TABLE trading.decision_candidate ADD COLUMN IF NOT EXISTS stage3_score_capped UInt8 DEFAULT 0"
    )


@dataclass
class Stage0Result:
    passed: bool
    score: float
    freshness_ok: bool
    null_ok: bool
    outlier_score: float
    time_alignment_ok: bool
    freshness_map: dict[str, int]
    freshness_score: float
    null_ratio: float


def compute_stage0() -> Stage0Result:
    feature_lookback_hours = max(24, int(os.getenv("STAGE0_FEATURE_LOOKBACK_HOURS", "72")))
    freshness_cfg = {
        "feature_snapshot": (
            "SELECT if(count()=0, 99999, greatest(dateDiff('minute', max(ts), now()), 0)) AS stale FROM trading.feature_snapshot",
            int(os.getenv("STAGE0_MAX_STALE_FEATURE_MIN", "30")),
            True,
        ),
        "technical_signals": (
            "SELECT if(count()=0, 99999, greatest(dateDiff('minute', max(updated_at), now()), 0)) AS stale FROM trading.technical_signals",
            int(os.getenv("STAGE0_MAX_STALE_TECH_MIN", "1500")),
            True,
        ),
        "market_regime": (
            "SELECT if(count()=0, 99999, greatest(dateDiff('minute', max(updated_at), now()), 0)) AS stale FROM trading.market_regime",
            int(os.getenv("STAGE0_MAX_STALE_REGIME_MIN", "1500")),
            True,
        ),
        "market_index": (
            "SELECT if(count()=0, 99999, greatest(dateDiff('minute', max(collected_at), now()), 0)) AS stale FROM trading.market_index",
            int(os.getenv("STAGE0_MAX_STALE_MARKET_INDEX_MIN", "1500")),
            False,
        ),
        "exchange_rate": (
            "SELECT if(count()=0, 99999, greatest(dateDiff('minute', max(collected_at), now()), 0)) AS stale FROM trading.exchange_rate",
            int(os.getenv("STAGE0_MAX_STALE_FX_MIN", "1500")),
            False,
        ),
        "interest_rate": (
            "SELECT if(count()=0, 99999, greatest(dateDiff('minute', max(collected_at), now()), 0)) AS stale FROM trading.interest_rate",
            int(os.getenv("STAGE0_MAX_STALE_RATE_MIN", "2880")),
            False,
        ),
        "news_clusters": (
            "SELECT if(count()=0, 99999, greatest(dateDiff('minute', max(asof_ts), now()), 0)) AS stale FROM trading.news_clusters",
            int(os.getenv("STAGE0_MAX_STALE_NEWS_CLUSTER_MIN", "1440")),
            False,
        ),
        "dart_disclosure": (
            "SELECT if(count()=0, 99999, greatest(dateDiff('minute', max(collected_at), now()), 0)) AS stale FROM trading.dart_disclosure",
            int(os.getenv("STAGE0_MAX_STALE_DART_MIN", "4320")),
            False,
        ),
    }

    freshness_map: dict[str, int] = {}
    freshness_fail = 0
    core_fail = 0

    for table, (sql, limit_min, is_core) in freshness_cfg.items():
        if not table_exists(table):
            stale = 99999
        else:
            try:
                stale = _to_int(ch_scalar(sql), 99999)
            except Exception:
                stale = 99999
        freshness_map[table] = stale
        if stale > limit_min:
            freshness_fail += 1
            if is_core:
                core_fail += 1

    freshness_score = float(max(0, 100 - 25 * freshness_fail))
    freshness_ok = core_fail == 0

    null_ratio_feature = 1.0
    null_ratio_tech = 1.0
    if table_exists("feature_snapshot"):
        try:
            null_ratio_feature = _to_float(
                ch_scalar(
                    f"""
SELECT
    if(
        count() = 0,
        1.0,
        sum(if(symbol = '' OR toFloat64(price) <= 0, 1, 0)) / count()
    )
FROM trading.feature_snapshot
WHERE ts >= now() - INTERVAL {feature_lookback_hours} HOUR
  AND session = 'REGULAR'
"""
                ),
                1.0,
            )
        except Exception:
            null_ratio_feature = 1.0
    if table_exists("technical_signals"):
        try:
            null_ratio_tech = _to_float(
                ch_scalar(
                    """
SELECT
    if(
        count() = 0,
        1.0,
        sum(if(ticker = '' OR toFloat64(close_price) <= 0 OR rsi14 < 0 OR rsi14 > 100, 1, 0)) / count()
    )
FROM trading.technical_signals
WHERE date = (SELECT max(date) FROM trading.technical_signals)
"""
                ),
                1.0,
            )
        except Exception:
            null_ratio_tech = 1.0

    null_ratio = max(null_ratio_feature, null_ratio_tech)
    null_ok = null_ratio <= 0.01
    null_score = float(max(0, 100 - int(null_ratio * 100 * 50)))

    outlier_count = 10
    if table_exists("feature_snapshot"):
        try:
            outlier_count = _to_int(
                ch_scalar(
                    f"""
SELECT countIf(
    abs(toFloat64(news_event_score)) > 100000000
    OR abs(toFloat64(inst_flow)) > 100000000
    OR toFloat64(price) <= 0
    OR toFloat64(price) > 5000000
)
FROM trading.feature_snapshot
WHERE ts >= now() - INTERVAL {feature_lookback_hours} HOUR
  AND session = 'REGULAR'
"""
                ),
                10,
            )
        except Exception:
            outlier_count = 10
    outlier_score = float(max(0, 100 - min(5, outlier_count) * 20))

    time_alignment_ok = True
    if table_exists("news_event_frames"):
        try:
            future_cnt = _to_int(
                ch_scalar(
                    """
SELECT count()
FROM trading.news_event_frames
WHERE collected_at >= now() - INTERVAL 7 DAY
  AND published_at > collected_at + INTERVAL 10 MINUTE
"""
                ),
                0,
            )
            time_alignment_ok = future_cnt == 0
        except Exception:
            time_alignment_ok = False

    stage0_pass = freshness_ok and null_ok and time_alignment_ok
    stage0_score = float(min(freshness_score, null_score, outlier_score))
    return Stage0Result(
        passed=stage0_pass,
        score=stage0_score,
        freshness_ok=freshness_ok,
        null_ok=null_ok,
        outlier_score=outlier_score,
        time_alignment_ok=time_alignment_ok,
        freshness_map=freshness_map,
        freshness_score=freshness_score,
        null_ratio=null_ratio,
    )


@dataclass
class Stage1Result:
    score: float
    passed_for_buy: bool
    hard_riskoff: bool
    action_posture: str
    stress_flags: str


def _pct_change_from_rows(rows: list[dict[str, Any]], key: str, span: int) -> float:
    if len(rows) < span + 1:
        return 0.0
    latest = _to_float(rows[0].get(key), 0.0)
    old = _to_float(rows[span].get(key), 0.0)
    if old == 0:
        return 0.0
    return (latest - old) / old * 100.0


def compute_stage1() -> Stage1Result:
    trend = "sideways"
    volatility = "normal"
    risk_appetite = "neutral"
    vix_level = 0.0
    action_posture = "normal"
    stress_flags = ""

    if table_exists("market_regime"):
        try:
            rows = ch_select(
                """
SELECT trend, volatility, risk_appetite, vix_level,
       ifNull(action_posture, 'normal') AS action_posture,
       ifNull(arrayStringConcat(stress_flags, ', '), '') AS stress_flags
FROM trading.market_regime
ORDER BY date DESC
LIMIT 1
"""
            )
            if rows:
                r = rows[0]
                trend = str(r.get("trend", "sideways") or "sideways").lower()
                volatility = str(r.get("volatility", "normal") or "normal").lower()
                risk_appetite = str(r.get("risk_appetite", "neutral") or "neutral").lower()
                vix_level = _to_float(r.get("vix_level"), 0.0)
                action_posture = str(r.get("action_posture", "normal") or "normal").lower()
                stress_flags = str(r.get("stress_flags", "") or "").strip()
        except Exception:
            pass

    usd_rows: list[dict[str, Any]] = []
    kospi_rows: list[dict[str, Any]] = []
    rate_rows: list[dict[str, Any]] = []
    try:
        if table_exists("exchange_rate"):
            usd_rows = ch_select(
                """
SELECT date, close_rate
FROM trading.exchange_rate
WHERE currency_pair = 'USDKRW'
ORDER BY date DESC
LIMIT 6
"""
            )
    except Exception:
        usd_rows = []
    try:
        if table_exists("market_index"):
            kospi_rows = ch_select(
                """
SELECT date, close_price
FROM trading.market_index
WHERE index_code = 'KOSPI'
ORDER BY date DESC
LIMIT 8
"""
            )
    except Exception:
        kospi_rows = []
    try:
        if table_exists("interest_rate"):
            rate_rows = ch_select(
                """
SELECT date, rate_value
FROM trading.interest_rate
WHERE rate_code IN ('KR_TB10Y', 'KOR_10Y', 'KTB10Y')
ORDER BY date DESC
LIMIT 8
"""
            )
    except Exception:
        rate_rows = []

    usdkrw_3d = _pct_change_from_rows(usd_rows, "close_rate", 3)
    kospi_5d = _pct_change_from_rows(kospi_rows, "close_price", 5)
    rate_5d_bp = 0.0
    if len(rate_rows) >= 6:
        latest = _to_float(rate_rows[0].get("rate_value"), 0.0)
        old = _to_float(rate_rows[5].get("rate_value"), 0.0)
        rate_5d_bp = (latest - old) * 100.0

    trend_score = 20.0
    if trend == "bull":
        trend_score = 35.0
    elif trend == "bear":
        trend_score = 8.0

    vol_score = 20.0
    if vix_level > 0:
        if vix_level <= 22:
            vol_score = 30.0
        elif vix_level <= 28:
            vol_score = 20.0
        elif vix_level <= 35:
            vol_score = 10.0
        else:
            vol_score = 0.0
    else:
        if volatility == "low":
            vol_score = 28.0
        elif volatility == "high":
            vol_score = 8.0

    fx_score = 20.0
    if usdkrw_3d > 2.0:
        fx_score = 0.0
    elif usdkrw_3d > 1.0:
        fx_score = 10.0
    elif usdkrw_3d > 0.3:
        fx_score = 15.0

    rates_score = 10.0
    if rate_5d_bp > 20:
        rates_score = 0.0
    elif rate_5d_bp > 10:
        rates_score = 5.0

    appetite_bonus = 0.0
    if risk_appetite == "risk_on":
        appetite_bonus = 5.0
    elif risk_appetite == "risk_off":
        appetite_bonus = -5.0

    score = _clamp(trend_score + vol_score + fx_score + rates_score + appetite_bonus, 0, 100)
    if action_posture == "aggressive":
        score = _clamp(score + 3.0, 0, 100)
    elif action_posture == "cautious":
        score = _clamp(score - 8.0, 0, 100)
    elif action_posture == "defensive":
        score = _clamp(score - 15.0, 0, 100)

    hard_riskoff = bool(
        (vix_level > 35 and vix_level > 0)
        or (usdkrw_3d > 2.0)
        or (kospi_5d < -3.5)
    )
    if action_posture == "defensive":
        hard_riskoff = True
    passed = score >= 55 and not hard_riskoff
    return Stage1Result(
        score=float(round(score, 2)),
        passed_for_buy=passed,
        hard_riskoff=hard_riskoff,
        action_posture=action_posture,
        stress_flags=stress_flags,
    )


def load_universe(universe: str, limit: int) -> list[dict[str, str]]:
    lim = max(1, int(limit))
    if universe != "watchlist":
        _log(f"universe={universe} 요청 감지: decision 유니버스는 watchlist로 강제합니다")
    active_source_raw = os.getenv("WATCHLIST_ACTIVE_SOURCE", "enrich_data").strip()
    active_sources = [s.strip() for s in active_source_raw.split(",") if s.strip()]
    source_filter = ""
    if active_sources:
        safe_sources = []
        for src in active_sources:
            safe_sources.append("'" + src.replace("\\", "\\\\").replace("'", "\\'") + "'")
        source_filter = f" AND source IN ({', '.join(safe_sources)})"
    if table_exists("interest_watchlist"):
        try:
            # run 메타가 있으면 최신 "정상" 스냅샷(run_id)을 우선 채택
            if table_exists("interest_watchlist_runs"):
                rows = ch_select(
                    f"""
WITH latest_run AS (
    SELECT run_id
    FROM trading.interest_watchlist_runs
    WHERE toDate(ts) >= today() - 3
      {source_filter}
      AND status = 'ok'
      AND inserted_rows >= greatest(1, toUInt32(ifNull(min_expected_rows, 1)))
    ORDER BY ts DESC
    LIMIT 1
)
SELECT
    ticker,
    anyLast(ticker_name) AS ticker_name,
    min(toInt32(ifNull(rank, 0))) AS rank_ord,
    max(toFloat64(ifNull(context_score, 0))) AS context_score
FROM trading.interest_watchlist
WHERE decision_id = (SELECT run_id FROM latest_run)
  {source_filter}
GROUP BY ticker
HAVING match(ticker, '^[0-9]{{6}}$')
ORDER BY rank_ord ASC, context_score DESC, ticker ASC
LIMIT {lim}
"""
                )
                out = [{"ticker": str(r.get("ticker", "")), "ticker_name": str(r.get("ticker_name", ""))} for r in rows]
                out = [r for r in out if _is_ticker(r["ticker"])]
                if out:
                    return out

            rows = ch_select(
                f"""
WITH latest_ts AS (
    SELECT ts
    FROM trading.interest_watchlist
    WHERE toDate(ts) >= today() - 3
      {source_filter}
    ORDER BY ts DESC
    LIMIT 1
)
SELECT
    ticker,
    anyLast(ticker_name) AS ticker_name,
    min(toInt32(ifNull(rank, 0))) AS rank_ord,
    max(toFloat64(ifNull(context_score, 0))) AS context_score
FROM trading.interest_watchlist
WHERE ts = (SELECT ts FROM latest_ts)
  {source_filter}
GROUP BY ticker
HAVING match(ticker, '^[0-9]{{6}}$')
ORDER BY rank_ord ASC, context_score DESC, ticker ASC
LIMIT {lim}
"""
            )
            out = [{"ticker": str(r.get("ticker", "")), "ticker_name": str(r.get("ticker_name", ""))} for r in rows]
            out = [r for r in out if _is_ticker(r["ticker"])]
            if out:
                return out
            if active_sources:
                _log(f"interest_watchlist source 필터({','.join(active_sources)}) 결과가 비어있음")
        except Exception:
            pass
    _log("interest_watchlist 데이터가 없어 후보를 비웁니다(technical/feature fallback 비활성)")
    return []


def _score_market_flow_ratio(pct: float, mode: str) -> float:
    if mode == "foreign":
        if pct >= 0.8:
            return 25.0
        if pct >= 0.3:
            return 15.0
        if pct > -0.3:
            return 8.0
        return 0.0
    if pct >= 0.8:
        return 15.0
    if pct >= 0.3:
        return 10.0
    if pct > -0.3:
        return 5.0
    return 0.0


def _score_stock_flow_ratio(pct: float, mode: str) -> float:
    if mode == "foreign":
        if pct >= 6.0:
            return 30.0
        if pct >= 2.0:
            return 20.0
        if pct >= 0.0:
            return 10.0
        return 0.0
    if pct >= 4.0:
        return 15.0
    if pct >= 1.0:
        return 10.0
    if pct >= 0.0:
        return 5.0
    return 0.0


@dataclass
class Stage2MarketResult:
    score: float
    valid: bool
    flags: list[str]
    source: str
    universe_n: int
    coverage_ratio: float
    flow_conf: float
    shock_level: str
    shock_abs_ratio_pct: float
    raw_shock_abs_ratio_pct: float
    foreign_adj_pct: float
    inst_adj_pct: float
    foreign_net_krw: float
    foreign_traded_krw: float
    foreign_pct: float
    inst_net_krw: float
    inst_traded_krw: float
    inst_pct: float


def _sanity_check_market_flow(
    net_buy_krw: float,
    traded_krw: float,
    universe_n: int,
    source: str,
) -> tuple[bool, float, list[str]]:
    flags: list[str] = []
    if traded_krw <= 0:
        flags.append("DENOM_ZERO_OR_NULL")
        return False, 0.0, flags

    ratio = (net_buy_krw / traded_krw) * 100.0
    if abs(ratio) > 20.0:
        flags.append(f"DENOM_SANITY_RATIO_OOB={ratio:.2f}%")
    if source != "MARKET_TOTAL":
        flags.append(f"DENOM_SOURCE={source or 'UNKNOWN'}")
    ok = ("DENOM_ZERO_OR_NULL" not in flags) and not any(f.startswith("DENOM_SANITY_RATIO_OOB=") for f in flags)
    return ok, ratio, flags


def compute_stage2_market_score() -> Stage2MarketResult:
    if not table_exists("market_flow_daily"):
        return Stage2MarketResult(
            score=0.0,
            valid=False,
            flags=["FLOW_TABLE_MISSING", "FLOW_DENOM_INVALID"],
            source="MISSING",
            universe_n=0,
            coverage_ratio=0.0,
            flow_conf=0.0,
            shock_level="UNKNOWN",
            shock_abs_ratio_pct=0.0,
            raw_shock_abs_ratio_pct=0.0,
            foreign_adj_pct=0.0,
            inst_adj_pct=0.0,
            foreign_net_krw=0.0,
            foreign_traded_krw=0.0,
            foreign_pct=0.0,
            inst_net_krw=0.0,
            inst_traded_krw=0.0,
            inst_pct=0.0,
        )

    source_col = "market_traded_value_krw_source" if column_exists("market_flow_daily", "market_traded_value_krw_source") else ""
    uni_col = (
        "market_traded_value_krw_universe_n"
        if column_exists("market_flow_daily", "market_traded_value_krw_universe_n")
        else ("n_tickers" if column_exists("market_flow_daily", "n_tickers") else "")
    )
    source_expr = f"anyHeavy({source_col}) AS denom_source" if source_col else "'UNKNOWN' AS denom_source"
    uni_expr = f"toUInt32(max({uni_col})) AS universe_n" if uni_col else "toUInt32(0) AS universe_n"

    rows = ch_select(
        f"""
SELECT
    investor_type,
    sum(net_buy_value_krw) AS net_buy_value_krw,
    sum(market_traded_value_krw) AS market_traded_value_krw,
    {source_expr},
    {uni_expr}
FROM trading.market_flow_daily
WHERE trade_date >= today() - 5
  AND market = 'ALL'
GROUP BY investor_type
"""
    )
    if not rows:
        rows = ch_select(
            f"""
SELECT
    investor_type,
    sum(net_buy_value_krw) AS net_buy_value_krw,
    sum(market_traded_value_krw) AS market_traded_value_krw,
    {source_expr},
    {uni_expr}
FROM trading.market_flow_daily
WHERE trade_date >= today() - 5
  AND market IN ('KOSPI', 'KOSDAQ')
GROUP BY investor_type
"""
        )
    if not rows:
        return Stage2MarketResult(
            score=0.0,
            valid=False,
            flags=["FLOW_EMPTY", "FLOW_DENOM_INVALID"],
            source="UNKNOWN",
            universe_n=0,
            coverage_ratio=0.0,
            flow_conf=0.0,
            shock_level="UNKNOWN",
            shock_abs_ratio_pct=0.0,
            raw_shock_abs_ratio_pct=0.0,
            foreign_adj_pct=0.0,
            inst_adj_pct=0.0,
            foreign_net_krw=0.0,
            foreign_traded_krw=0.0,
            foreign_pct=0.0,
            inst_net_krw=0.0,
            inst_traded_krw=0.0,
            inst_pct=0.0,
        )

    source_values = {str(r.get("denom_source", "") or "UNKNOWN").upper() for r in rows}
    source = source_values.pop() if len(source_values) == 1 else "MIXED"
    universe_n = max(_to_int(r.get("universe_n"), 0) for r in rows)
    expected_universe_n = max(1, _to_int(os.getenv("STAGE2_EXPECTED_UNIVERSE_N", "2000"), 2000))
    if source == "MARKET_TOTAL" and universe_n <= 0:
        coverage_ratio = 1.0
    else:
        coverage_ratio = _clamp(universe_n / float(expected_universe_n), 0.0, 1.0)
    flow_conf = coverage_ratio
    if source != "MARKET_TOTAL":
        # 소스가 시장 전체가 아니면 신뢰도를 낮춰 쇼크 과대판정을 완화.
        flow_conf *= 0.5

    foreign_net = 0.0
    foreign_traded = 0.0
    foreign_pct = 0.0
    foreign_adj_pct = 0.0
    inst_net = 0.0
    inst_traded = 0.0
    inst_pct = 0.0
    inst_adj_pct = 0.0
    flags: list[str] = []
    valid = True

    for r in rows:
        inv = str(r.get("investor_type", "")).upper()
        net = _to_float(r.get("net_buy_value_krw"), 0.0)
        traded = _to_float(r.get("market_traded_value_krw"), 0.0)
        pct = (net / traded * 100.0) if traded > 0 else 0.0
        adj_pct = pct * flow_conf
        if inv in {"FOREIGN", "INST"}:
            ok, pct, sanity_flags = _sanity_check_market_flow(net, traded, universe_n, source)
            if not ok:
                valid = False
                flags.extend(sanity_flags)
            else:
                flags.extend([f for f in sanity_flags if f.startswith("DENOM_SOURCE=")])
        if inv == "FOREIGN":
            foreign_net = net
            foreign_traded = traded
            foreign_pct = pct
            foreign_adj_pct = adj_pct
        if inv == "INST":
            inst_net = net
            inst_traded = traded
            inst_pct = pct
            inst_adj_pct = adj_pct

    score = 0.0
    raw_shock_abs = max(abs(foreign_pct), abs(inst_pct))
    shock_abs = max(abs(foreign_adj_pct), abs(inst_adj_pct))
    shock_level = "PASS"
    if shock_abs > 12.0:
        shock_level = "EXTREME"
    elif shock_abs > 8.0:
        shock_level = "ALERT"
    elif shock_abs > 3.0:
        shock_level = "WARN"
    if valid:
        score = _score_market_flow_ratio(foreign_adj_pct, "foreign") + _score_market_flow_ratio(inst_adj_pct, "inst")
        if shock_level == "WARN":
            flags.append("FLOW_SHOCK_WARN")
        elif shock_level == "ALERT":
            flags.append("FLOW_SHOCK_ALERT")
            score *= 0.6
        elif shock_level == "EXTREME":
            flags.append("FLOW_SHOCK_EXTREME")
            score *= 0.35
    else:
        flags.append("FLOW_DENOM_INVALID")
        shock_level = "UNKNOWN"

    return Stage2MarketResult(
        score=float(round(score, 2)),
        valid=valid,
        flags=sorted(set(flags)),
        source=source,
        universe_n=universe_n,
        coverage_ratio=round(coverage_ratio, 4),
        flow_conf=round(flow_conf, 4),
        shock_level=shock_level,
        shock_abs_ratio_pct=round(shock_abs, 4),
        raw_shock_abs_ratio_pct=round(raw_shock_abs, 4),
        foreign_adj_pct=round(foreign_adj_pct, 4),
        inst_adj_pct=round(inst_adj_pct, 4),
        foreign_net_krw=foreign_net,
        foreign_traded_krw=foreign_traded,
        foreign_pct=foreign_pct,
        inst_net_krw=inst_net,
        inst_traded_krw=inst_traded,
        inst_pct=inst_pct,
    )


def load_stage_maps() -> dict[str, dict[str, Any]]:
    maps: dict[str, dict[str, Any]] = {
        "flow": {},
        "flow_fallback": {},
        "event": {},
        "event_market": {},
        "cluster": {},
        "dart": {},
        "tech": {},
        "risk": {},
        "reasoning": {},
    }

    if table_exists("stock_flow_daily"):
        rows = ch_select(
            """
SELECT
    ticker,
    sumIf(net_buy_value_krw, investor_type = 'FOREIGN') AS foreign_net_value_3d,
    sumIf(net_buy_value_krw, investor_type = 'INST') AS inst_net_value_3d,
    sumIf(traded_value_krw, investor_type = 'FOREIGN') AS traded_value_3d,
    sumIf(net_buy_pct_turnover, investor_type = 'FOREIGN') AS foreign_net_pct_3d,
    sumIf(net_buy_pct_turnover, investor_type = 'INST') AS inst_net_pct_3d,
    countIf(investor_type = 'FOREIGN' AND net_buy_value_krw > 0) AS foreign_pos_days,
    countIf(investor_type = 'INST' AND net_buy_value_krw > 0) AS inst_pos_days
FROM trading.stock_flow_daily
WHERE trade_date >= today() - 3
GROUP BY ticker
HAVING match(ticker, '^[0-9]{6}$')
"""
        )
        maps["flow"] = {str(r.get("ticker")): r for r in rows}
        fallback_rows = ch_select(
            """
SELECT
    ticker,
    toString(max(trade_date)) AS last_flow_date,
    argMaxIf(net_buy_value_krw, trade_date, investor_type = 'FOREIGN') AS foreign_net_last,
    argMaxIf(net_buy_value_krw, trade_date, investor_type = 'INST') AS inst_net_last,
    argMax(traded_value_krw, trade_date) AS traded_last
FROM trading.stock_flow_daily
WHERE trade_date >= today() - 12
GROUP BY ticker
HAVING match(ticker, '^[0-9]{6}$')
"""
        )
        maps["flow_fallback"] = {str(r.get("ticker")): r for r in fallback_rows}

    if table_exists("news_event_frames"):
        try:
            rows = ch_select(
                """
SELECT
    ticker,
    argMax(frame_id_s, published_at) AS frame_id,
    max(importance_val) AS importance_max,
    avg(importance_val) AS importance_avg,
    count() AS event_cnt,
    countIf(thesis_path != '' AND evidence_json != '[]') AS explain_ready_cnt,
    anyHeavy(event_type) AS event_type
FROM
(
    SELECT
        arrayJoin(tickers) AS ticker,
        published_at,
        toString(frame_id) AS frame_id_s,
        toFloat64(importance) AS importance_val,
        thesis_path,
        evidence_json,
        event_type
    FROM trading.news_event_frames
    WHERE published_at >= now() - INTERVAL 3 DAY
      AND relevant = 1
)
WHERE match(ticker, '^[0-9]{6}$')
GROUP BY ticker
"""
            )
        except Exception:
            rows = ch_select(
                """
SELECT
    ticker,
    argMax(frame_id_s, published_at) AS frame_id,
    max(importance_val) AS importance_max,
    avg(importance_val) AS importance_avg,
    count() AS event_cnt,
    count() AS explain_ready_cnt,
    anyHeavy(event_type) AS event_type
FROM
(
    SELECT
        arrayJoin(tickers) AS ticker,
        published_at,
        toString(frame_id) AS frame_id_s,
        toFloat64(importance) AS importance_val,
        event_type
    FROM trading.news_event_frames
    WHERE published_at >= now() - INTERVAL 3 DAY
      AND relevant = 1
)
WHERE match(ticker, '^[0-9]{6}$')
GROUP BY ticker
"""
            )
        maps["event"] = {str(r.get("ticker")): r for r in rows}
        market_rows = ch_select(
            """
SELECT
    avg(toFloat64(importance)) AS importance_avg,
    max(toFloat64(importance)) AS importance_max,
    count() AS event_cnt,
    countIf(thesis_path != '' AND evidence_json != '[]') AS explain_ready_cnt
FROM trading.news_event_frames
WHERE published_at >= now() - INTERVAL 3 DAY
  AND relevant = 1
"""
        )
        maps["event_market"] = {"ALL": market_rows[0]} if market_rows else {}

    if table_exists("news_cluster_state"):
        rows = ch_select(
            """
SELECT
    ticker,
    argMax(cluster_id, asof_ts) AS cluster_id,
    argMax(state_label, asof_ts) AS state_label,
    max(toFloat64(importance_max)) AS importance_max
FROM
(
    SELECT
        arrayJoin(top_tickers) AS ticker,
        cluster_id,
        state_label,
        importance_max,
        asof_ts
    FROM trading.news_cluster_state
    WHERE asof_ts >= now() - INTERVAL 3 DAY
)
WHERE match(ticker, '^[0-9]{6}$')
GROUP BY ticker
"""
        )
        maps["cluster"] = {str(r.get("ticker")): r for r in rows}

    if table_exists("dart_disclosure"):
        rows = ch_select(
            """
SELECT
    stock_code AS ticker,
    max(toUInt8(importance)) AS importance_max,
    argMax(report_nm, rcept_dt) AS report_nm
FROM trading.dart_disclosure
WHERE rcept_dt >= today() - 30
  AND match(stock_code, '^[0-9]{6}$')
GROUP BY stock_code
"""
        )
        maps["dart"] = {str(r.get("ticker")): r for r in rows}

    if table_exists("technical_signals"):
        rows = ch_select(
            """
SELECT
    ticker,
    any(ticker_name) AS ticker_name,
    max(close_price) AS close_price,
    max(ma20) AS ma20,
    max(ma60) AS ma60,
    max(rsi14) AS rsi14,
    max(vol_ratio) AS vol_ratio,
    max(signal_score) AS signal_score,
    max(bb_pct) AS bb_pct
FROM trading.technical_signals
WHERE date = (SELECT max(date) FROM trading.technical_signals)
GROUP BY ticker
HAVING match(ticker, '^[0-9]{6}$')
"""
        )
        maps["tech"] = {str(r.get("ticker")): r for r in rows}

    if table_exists("feature_snapshot"):
        rows = ch_select(
            """
SELECT
    symbol AS ticker,
    argMaxIf(liquidity_krw, ts, session = 'REGULAR') AS liquidity_krw,
    argMaxIf(spread_bp, ts, session = 'REGULAR') AS spread_bp
FROM trading.feature_snapshot
WHERE ts >= now() - INTERVAL 5 DAY
  AND match(symbol, '^[0-9]{6}$')
GROUP BY symbol
"""
        )
        maps["risk"] = {str(r.get("ticker")): r for r in rows}

    if table_exists("stock_flow_daily"):
        rows = ch_select(
            """
SELECT
    ticker,
    avg(day_traded_value_krw) AS adv20_traded_value_krw
FROM
(
    SELECT
        trade_date,
        ticker,
        max(traded_value_krw) AS day_traded_value_krw
    FROM trading.stock_flow_daily
    WHERE trade_date >= today() - 20
      AND source_session = 'REGULAR'
      AND match(ticker, '^[0-9]{6}$')
    GROUP BY trade_date, ticker
)
GROUP BY ticker
"""
        )
        for r in rows:
            ticker = str(r.get("ticker") or "")
            if not ticker:
                continue
            adv20 = _to_float(r.get("adv20_traded_value_krw"), 0.0)
            if ticker not in maps["risk"]:
                maps["risk"][ticker] = {"ticker": ticker, "liquidity_krw": adv20, "spread_bp": 0.0}
                continue
            if _to_float(maps["risk"][ticker].get("liquidity_krw"), 0.0) <= 0 and adv20 > 0:
                maps["risk"][ticker]["liquidity_krw"] = adv20

    if table_exists("hidden_relation_reasoning"):
        rows = ch_select(
            """
SELECT
    ticker,
    toString(max(asof_ts)) AS reasoning_id
FROM trading.hidden_relation_reasoning
WHERE match(ticker, '^[0-9]{6}$')
GROUP BY ticker
"""
        )
        maps["reasoning"] = {str(r.get("ticker")): r for r in rows}
    return maps


def _is_dart_redflag(report_nm: str, importance_max: float) -> bool:
    text = (report_nm or "").lower()
    if importance_max < 4:
        return False
    keywords = [
        "유상증자",
        "전환사채",
        "신주인수권",
        "관리종목",
        "상장폐지",
        "거래정지",
        "횡령",
        "배임",
        "파산",
        "회생",
        "감사의견",
        "의견거절",
        "한정",
    ]
    return any(k.lower() in text for k in keywords)


def _is_event_redflag(event_type: str, importance_max: float) -> bool:
    if importance_max < 4:
        return False
    t = (event_type or "").lower()
    red = {"fraud", "regulatory", "suspension", "delisting", "bankruptcy"}
    return t in red


def _cluster_state_score(state: str) -> float:
    s = (state or "").lower()
    if s == "reinforcing":
        return 35.0
    if s == "emerging":
        return 20.0
    if s == "reversing":
        return -15.0
    if s == "stable":
        return 10.0
    if s == "decaying":
        return 5.0
    return 5.0


def _mode_state_path() -> str:
    return os.getenv(
        "DECISION_MODE_STATE_FILE",
        os.path.expanduser("~/.openclaw/data/decision_mode_state.json"),
    )


def _load_mode_state() -> dict[str, Any]:
    path = _mode_state_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
            if isinstance(obj, dict):
                return obj
    except Exception:
        pass
    return {}


def _save_mode_state(state: dict[str, Any]) -> None:
    path = _mode_state_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        _log(f"mode state save failed: {exc}")


def _resolve_mode(base_mode: str, state: dict[str, Any], today: dt.date) -> tuple[str, bool]:
    base = (base_mode or "strict").lower().strip()
    if base not in {"strict", "balanced", "neutral"}:
        base = "strict"
    override_mode = str(state.get("override_mode", "") or "").lower().strip()
    override_until = str(state.get("override_until", "") or "").strip()
    if override_mode in {"strict", "balanced", "neutral"} and override_until:
        try:
            until_d = dt.date.fromisoformat(override_until)
            if today <= until_d:
                return override_mode, True
        except Exception:
            pass
    return base, False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", default="INTRADAY", choices=["INTRADAY", "D1_3", "W1_2"])
    ap.add_argument("--universe", default="watchlist", choices=["watchlist", "all"])
    ap.add_argument("--limit", type=int, default=30)
    ap.add_argument("--mode", default=os.getenv("DECISION_MODE", "strict"), choices=["strict", "balanced", "neutral"])
    ap.add_argument("--model-version", default="decision-operating-spec-p0")
    args = ap.parse_args()

    ensure_decision_tables()
    today = dt.date.today()
    mode_state = _load_mode_state()
    mode, mode_overridden = _resolve_mode(args.mode, mode_state, today)

    mode_cfg = {
        "strict": {"buy_threshold": 70.0, "stage2_min": 50.0, "s2_unknown_s3": 70.0, "s2_unknown_s4": 65.0},
        "balanced": {"buy_threshold": 65.0, "stage2_min": 45.0, "s2_unknown_s3": 65.0, "s2_unknown_s4": 60.0},
        "neutral": {"buy_threshold": 60.0, "stage2_min": 40.0, "s2_unknown_s3": 60.0, "s2_unknown_s4": 58.0},
    }[mode]
    if args.universe != "watchlist":
        _log(f"--universe {args.universe} 무시: watchlist로 강제")
        args.universe = "watchlist"
    rsi_overheat_block_enabled = os.getenv("ENABLE_RSI_OVERHEAT_BLOCK", "0").strip() in {"1", "true", "TRUE", "yes", "YES"}
    prefilter_liquidity_krw = max(0.0, _to_float(os.getenv("PREFILTER_LIQUIDITY_KRW", "1000000000"), 1_000_000_000.0))

    stage0 = compute_stage0()
    stage1 = compute_stage1()
    market_stage2 = compute_stage2_market_score()
    market_stage2_score = market_stage2.score
    maps = load_stage_maps()
    # Stage2 종목 수급을 티커별로 분리하기 위해 3일 수급비율 정규화(0~1)를 선계산
    flow_norm: dict[str, float] = {}
    flow_values: list[tuple[str, float]] = []
    for tk, row in maps.get("flow", {}).items():
        f_net = _to_float(row.get("foreign_net_value_3d"), 0.0)
        i_net = _to_float(row.get("inst_net_value_3d"), 0.0)
        traded = _to_float(row.get("traded_value_3d"), 0.0)
        if traded > 0:
            flow_values.append((tk, (f_net + i_net) / traded))
    if flow_values:
        vals = [v for _, v in flow_values]
        mn = min(vals)
        mx = max(vals)
        for tk, v in flow_values:
            if mx > mn:
                flow_norm[tk] = _clamp((v - mn) / (mx - mn), 0.0, 1.0)
            else:
                flow_norm[tk] = 0.5
    tickers = load_universe(args.universe, args.limit)

    if not tickers:
        _log("유니버스가 비어있어 decision_run만 기록")

    weights = {
        "INTRADAY": {"s1": 20.0, "s2": 25.0, "s3": 30.0, "s4": 20.0, "s5": 5.0},
        "D1_3": {"s1": 25.0, "s2": 30.0, "s3": 25.0, "s4": 15.0, "s5": 5.0},
        "W1_2": {"s1": 30.0, "s2": 25.0, "s3": 20.0, "s4": 15.0, "s5": 10.0},
    }[args.horizon]

    decision_id = str(uuid.uuid4())
    now_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_abs_blocks: list[str] = []
    if not stage0.passed:
        run_abs_blocks.append("STAGE0_FAIL")
    if stage1.hard_riskoff:
        run_abs_blocks.append("HARD_RISK_OFF")
    if market_stage2.shock_level == "EXTREME":
        run_abs_blocks.append("FLOW_SHOCK_EXTREME")

    candidates: list[dict[str, Any]] = []
    s2_scores: list[float] = []
    s3_scores: list[float] = []
    s4_scores: list[float] = []
    s5_scores: list[float] = []
    total_scores: list[float] = []
    stage5_fail_counter: Counter[str] = Counter()
    stage5_exec_zero_counter: Counter[str] = Counter()
    flow_unknown_count = 0
    prefilter_low_liquidity_count = 0

    for item in tickers:
        ticker = item["ticker"]
        ticker_name = item.get("ticker_name", "")
        abs_blocks = list(run_abs_blocks)
        explain_codes: list[str] = []
        pre_risk = maps["risk"].get(ticker, {})
        pre_liquidity = _to_float(pre_risk.get("liquidity_krw"), 0.0)
        if prefilter_liquidity_krw > 0 and pre_liquidity > 0 and pre_liquidity < prefilter_liquidity_krw:
            prefilter_low_liquidity_count += 1
            continue

        flow = maps["flow"].get(ticker, {})
        flow_fb = maps["flow_fallback"].get(ticker, {})
        foreign_net = _to_float(flow.get("foreign_net_value_3d"), 0.0)
        inst_net = _to_float(flow.get("inst_net_value_3d"), 0.0)
        traded = _to_float(flow.get("traded_value_3d"), 0.0)
        foreign_pct = (foreign_net / traded * 100.0) if traded > 0 else 0.0
        inst_pct = (inst_net / traded * 100.0) if traded > 0 else 0.0
        foreign_pos_days = _to_int(flow.get("foreign_pos_days"), 0)
        inst_pos_days = _to_int(flow.get("inst_pos_days"), 0)
        flow_persistence = max(foreign_pos_days, inst_pos_days)
        flow_unknown = False
        s2_penalty = 0.0
        s2_stock = 0.0
        if traded <= 0:
            last_flow_date = str(flow_fb.get("last_flow_date", "") or "")
            fb_foreign = _to_float(flow_fb.get("foreign_net_last"), 0.0)
            fb_inst = _to_float(flow_fb.get("inst_net_last"), 0.0)
            fb_traded = _to_float(flow_fb.get("traded_last"), 0.0)
            stale_days = 999
            if last_flow_date:
                try:
                    stale_days = max(0, (today - dt.date.fromisoformat(last_flow_date)).days)
                except Exception:
                    stale_days = 999
            if fb_traded > 0 and stale_days <= 2:
                decay = 1.0
                foreign_net = fb_foreign * decay
                inst_net = fb_inst * decay
                traded = fb_traded
                explain_codes.append("FLOW_STALE_OK")
            elif fb_traded > 0 and stale_days <= 5:
                decay = 0.7
                foreign_net = fb_foreign * decay
                inst_net = fb_inst * decay
                traded = fb_traded
                s2_penalty += 5.0
                explain_codes.append("FLOW_STALE_DECAY")
            elif fb_traded > 0 and stale_days <= 10:
                flow_unknown = True
                s2_penalty += 12.0
                explain_codes.append("FLOW_STALE_TOO_OLD")
            else:
                flow_unknown = True
                s2_penalty += 15.0
                explain_codes.append("FLOW_MISSING")
            if flow_unknown:
                flow_unknown_count += 1
                s2_stock = 15.0
                foreign_pct = 0.0
                inst_pct = 0.0
            else:
                foreign_pct = (foreign_net / traded * 100.0) if traded > 0 else 0.0
                inst_pct = (inst_net / traded * 100.0) if traded > 0 else 0.0
        else:
            net_ratio_3d = (foreign_net + inst_net) / traded
            norm_ratio = _to_float(flow_norm.get(ticker), 0.0)
            persistence = _clamp((foreign_pos_days + inst_pos_days) / 3.0, 0.0, 1.0)
            # 0~40: net_ratio + persistence + cross-sectional normalization
            s2_stock = _clamp(50.0 * net_ratio_3d + 10.0 * persistence + 20.0 * norm_ratio, 0.0, 40.0)

        stage2_score = _clamp(market_stage2_score + s2_stock - s2_penalty, 0, 100)
        distribution_block = foreign_pct <= -6.0 and inst_pct <= -3.0
        # Stage2는 보조지표. 실행 차단은 EXTREME 충격에서만 적용.
        stage2_pass = market_stage2.shock_level != "EXTREME"
        if not market_stage2.valid:
            explain_codes.append("FLOW_DENOM_INVALID")
        if market_stage2.shock_level == "WARN":
            explain_codes.append("FLOW_SHOCK_WARN")
        if market_stage2.shock_level == "ALERT":
            explain_codes.append("FLOW_SHOCK_ALERT")
        if market_stage2.shock_level == "EXTREME":
            explain_codes.append("FLOW_SHOCK_EXTREME")
        if distribution_block:
            explain_codes.append("FLOW_DISTRIBUTION_WARN")
        if foreign_pct >= 2.0:
            explain_codes.append("FOREIGN_ACCUM_3D")
        if inst_pct >= 1.0:
            explain_codes.append("INST_ACCUM_3D")

        event = maps["event"].get(ticker, {})
        event_market = maps["event_market"].get("ALL", {})
        cluster = maps["cluster"].get(ticker, {})
        dart = maps["dart"].get(ticker, {})
        event_imp_avg = _to_float(event.get("importance_avg"), 0.0)
        event_imp_max = _to_float(event.get("importance_max"), 0.0)
        event_cnt = _to_int(event.get("event_cnt"), 0)
        explain_ready_cnt = _to_int(event.get("explain_ready_cnt"), 0)
        event_type = str(event.get("event_type", "") or "")
        cluster_state = str(cluster.get("state_label", "") or "")
        cluster_imp_max = _to_float(cluster.get("importance_max"), 0.0)
        # TE: ticker evidence (0~60)
        te_score = _clamp(10.0 * min(explain_ready_cnt, 3) + 10.0 * min(event_imp_max, 5.0), 0.0, 60.0)
        # CE: cluster evidence (0~30)
        state_label = cluster_state.lower()
        if state_label == "reinforcing":
            ce_state = 30.0
        elif state_label == "emerging":
            ce_state = 18.0
        elif state_label == "stable":
            ce_state = 10.0
        elif state_label == "decaying":
            ce_state = 5.0
        elif state_label == "reversing":
            ce_state = -10.0
        else:
            ce_state = 0.0
        ce_score = _clamp(ce_state + (5.0 if cluster_imp_max >= 4.0 else 0.0), 0.0, 30.0)
        # ME: market-wide evidence (0~20)
        m_imp_max = _to_float(event_market.get("importance_max"), 0.0)
        m_explain = _to_int(event_market.get("explain_ready_cnt"), 0)
        if m_imp_max >= 4.0 and m_explain >= 10:
            me_score = 20.0 if (cluster_state or event_cnt > 0) else 10.0
        elif m_imp_max >= 3.0 and m_explain >= 5:
            me_score = 10.0 if (cluster_state or event_cnt > 0) else 5.0
        else:
            me_score = 0.0
        stage3_score = _clamp(te_score + ce_score + me_score, 0, 100)
        stage3_score_capped = False
        redflag = False
        if _is_event_redflag(event_type, event_imp_max):
            redflag = True
            abs_blocks.append("EVENT_REDFLAG")
        dart_imp = _to_float(dart.get("importance_max"), 0.0)
        dart_nm = str(dart.get("report_nm", "") or "")
        if _is_dart_redflag(dart_nm, dart_imp):
            redflag = True
            abs_blocks.append("DART_REDFLAG")
        stage3_size_multiplier = 1.0
        if mode == "strict":
            # strict: TE 우선, TE 부재시 CE 고강도 + 타이밍 강도 필요
            stage3_pass = (not redflag) and (
                (te_score >= 20.0 and stage3_score >= 50.0)
                or (te_score <= 0.0 and ce_score >= 23.0 and cluster_imp_max >= 4.0)
            )
            if te_score <= 0.0 and stage3_pass:
                stage3_size_multiplier = 0.5
                explain_codes.append("STAGE3_HYBRID_CE")
        elif mode == "balanced":
            stage3_pass = (not redflag) and (stage3_score >= 50.0 or (te_score <= 0.0 and ce_score >= 18.0 and stage3_score >= 45.0))
            if te_score <= 0.0 and stage3_pass:
                stage3_size_multiplier = 0.4
                explain_codes.append("STAGE3_HYBRID_CE")
        else:
            stage3_pass = (not redflag) and stage3_score >= 45.0
            if te_score <= 0.0 and stage3_pass:
                stage3_size_multiplier = 0.5
        if stage3_score >= 60:
            explain_codes.append("NEWS_CLUSTER_SUPPORT")
        if te_score <= 0:
            stage3_score_capped = True
            explain_codes.append("NO_TICKER_EVIDENCE")

        tech = maps["tech"].get(ticker, {})
        if not ticker_name:
            ticker_name = str(tech.get("ticker_name", "") or "")
        close = _to_float(tech.get("close_price"), 0.0)
        ma20 = _to_float(tech.get("ma20"), 0.0)
        ma60 = _to_float(tech.get("ma60"), 0.0)
        rsi = _to_float(tech.get("rsi14"), 50.0)
        vol_ratio = _to_float(tech.get("vol_ratio"), 1.0)
        signal_score = _to_float(tech.get("signal_score"), 0.0)
        bb_pct = _to_float(tech.get("bb_pct"), 0.5)

        trend_score = 5.0
        if close > ma20 > ma60 > 0:
            trend_score = 40.0
        elif ma20 > ma60 > 0:
            trend_score = 28.0
        elif close > ma20 > 0:
            trend_score = 18.0

        mom_score = 4.0
        if 45 <= rsi <= 65:
            mom_score = 20.0
        elif 35 <= rsi <= 70:
            mom_score = 12.0
        if signal_score >= 3:
            mom_score += 5.0
        elif signal_score >= 1:
            mom_score += 3.0
        elif signal_score <= -2:
            mom_score -= 5.0
        mom_score = _clamp(mom_score, 0, 25)

        vol_score = 15.0 if 0.2 <= bb_pct <= 0.8 else 8.0
        vol_conf_score = 3.0
        if vol_ratio >= 1.2:
            vol_conf_score = 20.0
        elif vol_ratio >= 1.0:
            vol_conf_score = 15.0
        elif vol_ratio >= 0.7:
            vol_conf_score = 8.0

        stage4_score = _clamp(trend_score + mom_score + vol_score + vol_conf_score, 0, 100)
        regime = "RISK_OFF" if stage1.hard_riskoff else ("RISK_ON" if stage1.score >= 70 else "NEUTRAL")
        p1 = close > ma20 > ma60 > 0 and vol_ratio >= 1.0 and rsi <= 72
        p1_riskoff = close > ma20 > ma60 > 0 and vol_ratio >= 1.2 and rsi <= 68
        p2 = ma20 > ma60 > 0 and (ma20 * 0.975 <= close <= ma20 * 1.005) and (35 <= rsi <= 65)
        p3 = signal_score >= 3 and vol_ratio >= 1.3 and rsi <= 75 and close >= ma20 > 0
        if mode == "strict":
            if regime == "RISK_ON":
                stage4_pass = stage4_score >= 55 and (p1 or p2 or p3)
            elif regime == "NEUTRAL":
                stage4_pass = stage4_score >= 58 and (p1 or p2)
            else:
                stage4_pass = stage4_score >= 65 and p1_riskoff
        elif mode == "balanced":
            if regime == "RISK_ON":
                stage4_pass = stage4_score >= 52 and (p1 or p2 or p3)
            elif regime == "NEUTRAL":
                stage4_pass = stage4_score >= 55 and (p1 or p2)
            else:
                stage4_pass = stage4_score >= 62 and p1_riskoff
        else:
            if regime == "RISK_ON":
                stage4_pass = stage4_score >= 50 and (p1 or p2 or p3)
            elif regime == "NEUTRAL":
                stage4_pass = stage4_score >= 53 and (p1 or p2)
            else:
                stage4_pass = stage4_score >= 60 and p1_riskoff
        if rsi > 75 and rsi_overheat_block_enabled:
            abs_blocks.append("TECH_OVERHEAT_RSI")
        if close > ma20 > ma60 > 0:
            explain_codes.append("TREND_STRUCTURE_UP")
        if flow_unknown:
            explain_codes.append("FLOW_UNKNOWN_REFERENCE")

        risk = maps["risk"].get(ticker, {})
        liquidity = _to_float(risk.get("liquidity_krw"), 0.0)
        spread_bp = _to_float(risk.get("spread_bp"), 0.0)
        stage5_fail_codes: list[str] = []
        stage5_exec_multiplier = 1.0
        if liquidity >= 5_000_000_000:
            stage5_score = 100.0
            stage5_exec_multiplier = 1.0
        elif liquidity >= 1_000_000_000:
            stage5_score = 80.0
            stage5_exec_multiplier = 0.7
        elif liquidity >= 500_000_000:
            stage5_score = 60.0
            stage5_exec_multiplier = 0.4
        elif liquidity >= 300_000_000:
            stage5_score = 40.0
            stage5_exec_multiplier = 0.2
        else:
            stage5_score = 20.0
            stage5_exec_multiplier = 0.0
            stage5_fail_codes.append("LOW_LIQUIDITY")
            abs_blocks.append("LOW_LIQUIDITY")
        if spread_bp > 80:
            stage5_score = 0.0
            stage5_fail_codes.append("SPREAD_TOO_WIDE")
            stage5_exec_multiplier = 0.0
            abs_blocks.append("SPREAD_TOO_WIDE")
        elif spread_bp > 50:
            stage5_score = max(0.0, stage5_score - 15.0)
            stage5_fail_codes.append("SPREAD_WIDE")
            stage5_exec_multiplier = min(stage5_exec_multiplier, 0.6)
        # Step5는 기본적으로 사이징 엔진: 하드 차단은 극단 저유동/초광스프레드만 적용.
        stage5_pass = stage5_exec_multiplier > 0.0
        if not stage5_pass:
            if "LOW_LIQUIDITY" not in stage5_fail_codes and "LOW_LIQUIDITY_CONDITIONAL" not in stage5_fail_codes:
                stage5_fail_codes.append("EXEC_BLOCKED")
        else:
            explain_codes.append("LIQUIDITY_OK")
        if stage5_fail_codes:
            stage5_fail_counter.update(stage5_fail_codes)
            if stage5_exec_multiplier <= 0.0:
                stage5_exec_zero_counter.update(stage5_fail_codes)

        penalty = 0.0
        if redflag:
            penalty += 30.0
        if distribution_block:
            penalty += 15.0
        if market_stage2.shock_level == "ALERT":
            penalty += 10.0
        if market_stage2.shock_level == "EXTREME":
            penalty += 20.0
        if rsi > 75:
            penalty += 10.0

        total = (
            weights["s1"] * stage1.score
            + weights["s2"] * stage2_score
            + weights["s3"] * stage3_score
            + weights["s4"] * stage4_score
            + weights["s5"] * stage5_score
        ) / 100.0
        total = _clamp(total - penalty, 0, 100)

        # BUY 하드게이트: Stage0/1/2(EXTREME only)/5
        # Stage3/4는 점수와 설명에는 반영하되 차단 게이트로는 사용하지 않는다.
        all_pass = stage0.passed and stage1.passed_for_buy and stage2_pass and stage5_pass
        has_block = len(abs_blocks) > 0
        action = "HOLD"
        if not has_block and all_pass and total >= mode_cfg["buy_threshold"]:
            action = "BUY"
        elif total <= 35:
            action = "REDUCE"

        target_weight = 0.0
        if action == "BUY":
            buy_mult = _clamp((stage1.score - 45.0) / 25.0, 0.0, 1.0)
            if market_stage2.shock_level == "EXTREME":
                m_shock = 0.0
            elif market_stage2.shock_level == "ALERT":
                m_shock = 0.35
            elif market_stage2.shock_level == "WARN":
                m_shock = 0.70
            else:
                m_shock = 1.0
            base = 0.03 + max(0.0, total - mode_cfg["buy_threshold"]) / 200.0
            target_weight = _clamp(base * buy_mult * m_shock * stage5_exec_multiplier * stage3_size_multiplier, 0.0, 0.10)

        if not explain_codes:
            explain_codes.append("WAIT_SIGNAL")

        candidates.append(
            {
                "decision_id": decision_id,
                "ticker": ticker,
                "action": action,
                "target_weight": round(target_weight, 4),
                "stage1_score": round(stage1.score, 2),
                "stage2_stock_flow_score": round(stage2_score, 2),
                "stage3_event_score": round(stage3_score, 2),
                "stage4_timing_score": round(stage4_score, 2),
                "stage5_risk_score": round(stage5_score, 2),
                "stage5_fail_codes": sorted(set(stage5_fail_codes)),
                "stage5_exec_multiplier": round(stage5_exec_multiplier, 2),
                "stage3_evidence_count": max(0, explain_ready_cnt),
                "stage3_score_capped": 1 if stage3_score_capped else 0,
                "total_score": round(total, 2),
                "absolute_block_reason": sorted(set(abs_blocks)),
                "primary_cluster_id": str(cluster.get("cluster_id", "") or ""),
                "primary_event_frame_id": str(event.get("frame_id", "") or ""),
                "primary_reasoning_id": str((maps["reasoning"].get(ticker, {}) or {}).get("reasoning_id", "") or ""),
                "explanation_codes": explain_codes[:8],
                "created_at": now_ts,
            }
        )

        s2_scores.append(stage2_score)
        s3_scores.append(stage3_score)
        s4_scores.append(stage4_score)
        s5_scores.append(stage5_score)
        total_scores.append(total)

    stage2_run = round(sum(s2_scores) / len(s2_scores), 2) if s2_scores else round(market_stage2_score, 2)
    stage3_run = round(sum(s3_scores) / len(s3_scores), 2) if s3_scores else 0.0
    stage4_run = round(sum(s4_scores) / len(s4_scores), 2) if s4_scores else 0.0
    stage5_run = round(sum(s5_scores) / len(s5_scores), 2) if s5_scores else 0.0
    total_run = round(sum(total_scores) / len(total_scores), 2) if total_scores else 0.0
    buy_cnt = sum(1 for c in candidates if c.get("action") == "BUY")
    hold_cnt = sum(1 for c in candidates if c.get("action") == "HOLD")
    reduce_cnt = sum(1 for c in candidates if c.get("action") == "REDUCE")

    # no-trade watchdog: strict에서 무매수 연속 시 balanced 단기 전환
    no_buy_streak = _to_int(mode_state.get("no_buy_streak"), 0)
    no_buy_streak = (no_buy_streak + 1) if buy_cnt == 0 else 0
    flow_unknown_ratio = (flow_unknown_count / len(candidates)) if candidates else 0.0
    watchdog_triggered = False
    rollback_triggered = False
    mode_change_reason = ""
    if mode == "strict":
        if (
            no_buy_streak >= _to_int(os.getenv("WATCHDOG_NO_BUY_SESSIONS", "10"), 10)
            and not stage1.hard_riskoff
            and market_stage2.shock_level not in {"ALERT", "EXTREME"}
            and (flow_unknown_ratio >= 0.40 or market_stage2.coverage_ratio < 0.50)
        ):
            override_days = max(1, _to_int(os.getenv("WATCHDOG_OVERRIDE_DAYS", "5"), 5))
            mode_state["override_mode"] = "balanced"
            mode_state["override_until"] = (today + dt.timedelta(days=override_days)).isoformat()
            watchdog_triggered = True
            mode_change_reason = "NO_TRADE_WATCHDOG"
    elif mode == "balanced" and mode_overridden:
        if stage1.hard_riskoff or market_stage2.shock_level in {"ALERT", "EXTREME"}:
            mode_state["override_mode"] = "strict"
            mode_state["override_until"] = today.isoformat()
            rollback_triggered = True
            mode_change_reason = "WATCHDOG_ROLLBACK_RISK"

    mode_state["no_buy_streak"] = no_buy_streak
    mode_state["last_run_date"] = today.isoformat()
    mode_state["last_effective_mode"] = mode
    _save_mode_state(mode_state)

    stage_debug = {
        "mode": {
            "requested": args.mode,
            "effective": mode,
            "overridden": mode_overridden,
            "buy_threshold": mode_cfg["buy_threshold"],
            "stage2_min": mode_cfg["stage2_min"],
            "universe_forced_watchlist": True,
            "watchlist_active_source": os.getenv("WATCHLIST_ACTIVE_SOURCE", "enrich_data"),
            "stage2_extreme_only_block": True,
            "stage3_gate_enabled": False,
            "stage4_gate_enabled": False,
            "rsi_overheat_block_enabled": rsi_overheat_block_enabled,
        },
        "stage1": {
            "score": round(stage1.score, 2),
            "pass_for_buy": bool(stage1.passed_for_buy),
            "hard_riskoff": bool(stage1.hard_riskoff),
            "action_posture": stage1.action_posture,
            "stress_flags": stage1.stress_flags,
        },
        "stage2": {
            "market_score": round(market_stage2.score, 2),
            "valid": market_stage2.valid,
            "flags": market_stage2.flags,
            "source": market_stage2.source,
            "universe_n": market_stage2.universe_n,
            "coverage_ratio": market_stage2.coverage_ratio,
            "flow_conf": market_stage2.flow_conf,
            "shock_level": market_stage2.shock_level,
            "shock_abs_ratio_pct": market_stage2.shock_abs_ratio_pct,
            "raw_shock_abs_ratio_pct": market_stage2.raw_shock_abs_ratio_pct,
            "shock_threshold_pct": {"pass_max": 3.0, "warn_max": 8.0, "alert_max": 12.0},
            "foreign_net_krw_5d": round(market_stage2.foreign_net_krw, 2),
            "foreign_traded_krw_5d": round(market_stage2.foreign_traded_krw, 2),
            "foreign_net_pct_turnover_5d": round(market_stage2.foreign_pct, 4),
            "foreign_adj_pct_turnover_5d": round(market_stage2.foreign_adj_pct, 4),
            "inst_net_krw_5d": round(market_stage2.inst_net_krw, 2),
            "inst_traded_krw_5d": round(market_stage2.inst_traded_krw, 2),
            "inst_net_pct_turnover_5d": round(market_stage2.inst_pct, 4),
            "inst_adj_pct_turnover_5d": round(market_stage2.inst_adj_pct, 4),
        },
        "stage5": {
            "fail_summary": dict(stage5_fail_counter),
            "exec_zero_summary": dict(stage5_exec_zero_counter),
            "prefilter_liquidity_krw": prefilter_liquidity_krw,
            "prefilter_low_liquidity_count": prefilter_low_liquidity_count,
        },
        "watchdog": {
            "no_buy_streak": no_buy_streak,
            "flow_unknown_ratio": round(flow_unknown_ratio, 4),
            "triggered": watchdog_triggered,
            "rollback_triggered": rollback_triggered,
            "reason": mode_change_reason,
            "override_mode": str(mode_state.get("override_mode", "") or ""),
            "override_until": str(mode_state.get("override_until", "") or ""),
        },
    }

    run_row = {
        "decision_id": decision_id,
        "decision_time": now_ts,
        "horizon": args.horizon,
        "universe": "watchlist",
        "stage0_pass": 1 if stage0.passed else 0,
        "stage0_score": round(stage0.score, 2),
        "stage1_pass": 1 if stage1.passed_for_buy else 0,
        "stage1_score": round(stage1.score, 2),
        "stage2_pass": 1 if (market_stage2.shock_level != "EXTREME") else 0,
        "stage2_score": stage2_run,
        "stage3_pass": 1 if stage3_run >= 50 else 0,
        "stage3_score": stage3_run,
        "stage4_pass": 1 if stage4_run >= 55 else 0,
        "stage4_score": stage4_run,
        "stage5_pass": 1 if stage5_run >= 40 else 0,
        "stage5_score": stage5_run,
        "total_score": total_run,
        "penalty_score": 0.0,
        "absolute_block_reason": sorted(set(run_abs_blocks)),
        "data_freshness_json": json.dumps(stage0.freshness_map, ensure_ascii=False),
        "stage_debug_json": json.dumps(stage_debug, ensure_ascii=False),
        "model_version": args.model_version,
        "prompt_hash": "",
        "created_at": now_ts,
    }

    ch_insert_json_each_row("trading.decision_run", [run_row], timeout_sec=60)
    ch_insert_json_each_row("trading.decision_candidate", candidates, timeout_sec=120)
    _log(
        f"decision_id={decision_id} stage0={run_row['stage0_pass']} stage1={run_row['stage1_pass']} "
        f"mode={mode} candidates={len(candidates)} buy={buy_cnt} hold={hold_cnt} reduce={reduce_cnt}"
    )
    print(
        json.dumps(
            {
                "ok": True,
                "decision_id": decision_id,
                "total_score": total_run,
                "buy": buy_cnt,
                "hold": hold_cnt,
                "reduce": reduce_cnt,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        _log(f"실행 실패: {e}")
        print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False), flush=True)
        raise
