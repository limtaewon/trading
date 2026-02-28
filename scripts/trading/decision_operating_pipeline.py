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
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen


def _log(msg: str) -> None:
    print(f"{dt.datetime.now().strftime('%H:%M:%S')} [decision-p0] {msg}", flush=True)


def _ch_url_and_headers() -> tuple[str, dict[str, str]]:
    host = os.getenv("CLICKHOUSE_HOST", "").strip()
    user = os.getenv("CLICKHOUSE_USER", "").strip()
    pw = os.getenv("CLICKHOUSE_PASS", "").strip()
    headers: dict[str, str] = {"Content-Type": "text/plain; charset=utf-8"}

    if host:
        if not user:
            user = "default"
        if not pw:
            pw = "trading"
        sep = "&" if "?" in host else "?"
        return f"{host}{sep}user={user}&password={pw}", headers

    url = os.getenv("CLICKHOUSE_URL", "http://localhost:8123").strip()
    sp = urlsplit(url)
    if sp.username is not None:
        auth = f"{sp.username}:{sp.password or ''}".encode("utf-8")
        headers["Authorization"] = "Basic " + base64.b64encode(auth).decode("ascii")
        netloc = sp.hostname or "localhost"
        if sp.port:
            netloc = f"{netloc}:{sp.port}"
        clean = urlunsplit((sp.scheme or "http", netloc, sp.path or "", sp.query, sp.fragment))
        return clean, headers

    return url, headers


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
    freshness_cfg = {
        "feature_snapshot": (
            "SELECT if(count()=0, 99999, dateDiff('minute', max(ts), now())) AS stale FROM trading.feature_snapshot",
            int(os.getenv("STAGE0_MAX_STALE_FEATURE_MIN", "30")),
            True,
        ),
        "technical_signals": (
            "SELECT if(count()=0, 99999, dateDiff('minute', max(updated_at), now())) AS stale FROM trading.technical_signals",
            int(os.getenv("STAGE0_MAX_STALE_TECH_MIN", "1500")),
            True,
        ),
        "market_regime": (
            "SELECT if(count()=0, 99999, dateDiff('minute', max(updated_at), now())) AS stale FROM trading.market_regime",
            int(os.getenv("STAGE0_MAX_STALE_REGIME_MIN", "1500")),
            True,
        ),
        "market_index": (
            "SELECT if(count()=0, 99999, dateDiff('minute', max(collected_at), now())) AS stale FROM trading.market_index",
            int(os.getenv("STAGE0_MAX_STALE_MARKET_INDEX_MIN", "1500")),
            False,
        ),
        "exchange_rate": (
            "SELECT if(count()=0, 99999, dateDiff('minute', max(collected_at), now())) AS stale FROM trading.exchange_rate",
            int(os.getenv("STAGE0_MAX_STALE_FX_MIN", "1500")),
            False,
        ),
        "interest_rate": (
            "SELECT if(count()=0, 99999, dateDiff('minute', max(collected_at), now())) AS stale FROM trading.interest_rate",
            int(os.getenv("STAGE0_MAX_STALE_RATE_MIN", "2880")),
            False,
        ),
        "news_clusters": (
            "SELECT if(count()=0, 99999, dateDiff('minute', max(asof_ts), now())) AS stale FROM trading.news_clusters",
            int(os.getenv("STAGE0_MAX_STALE_NEWS_CLUSTER_MIN", "1440")),
            False,
        ),
        "dart_disclosure": (
            "SELECT if(count()=0, 99999, dateDiff('minute', max(collected_at), now())) AS stale FROM trading.dart_disclosure",
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
                    """
SELECT
    if(
        count() = 0,
        1.0,
        sum(if(symbol = '' OR toFloat64(price) <= 0, 1, 0)) / count()
    )
FROM trading.feature_snapshot
WHERE ts >= now() - INTERVAL 1 DAY
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
                    """
SELECT countIf(
    abs(toFloat64(news_event_score)) > 100000000
    OR abs(toFloat64(inst_flow)) > 100000000
    OR toFloat64(price) <= 0
    OR toFloat64(price) > 5000000
)
FROM trading.feature_snapshot
WHERE ts >= now() - INTERVAL 1 DAY
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
                ch_scalar("SELECT count() FROM trading.news_event_frames WHERE published_at > now() + INTERVAL 1 MINUTE"),
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

    if table_exists("market_regime"):
        try:
            rows = ch_select(
                """
SELECT trend, volatility, risk_appetite, vix_level
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
    hard_riskoff = bool(
        (vix_level > 35 and vix_level > 0)
        or (usdkrw_3d > 2.0)
        or (kospi_5d < -3.5)
    )
    passed = score >= 55 and not hard_riskoff
    return Stage1Result(score=float(round(score, 2)), passed_for_buy=passed, hard_riskoff=hard_riskoff)


def load_universe(universe: str, limit: int) -> list[dict[str, str]]:
    lim = max(1, int(limit))
    if universe == "watchlist" and table_exists("interest_watchlist"):
        try:
            rows = ch_select(
                f"""
SELECT
    ticker,
    anyLast(ticker_name) AS ticker_name,
    max(ts) AS ts_max
FROM trading.interest_watchlist
WHERE toDate(ts) >= today() - 3
GROUP BY ticker
HAVING match(ticker, '^[0-9]{{6}}$')
ORDER BY ts_max DESC
LIMIT {lim}
"""
            )
            out = [{"ticker": str(r.get("ticker", "")), "ticker_name": str(r.get("ticker_name", ""))} for r in rows]
            out = [r for r in out if _is_ticker(r["ticker"])]
            if out:
                return out
        except Exception:
            pass

    if table_exists("technical_signals"):
        try:
            rows = ch_select(
                f"""
SELECT
    ticker,
    any(ticker_name) AS ticker_name
FROM trading.technical_signals
WHERE date = (SELECT max(date) FROM trading.technical_signals)
GROUP BY ticker
HAVING match(ticker, '^[0-9]{{6}}$')
ORDER BY max(signal_score) DESC, max(vol_ratio) DESC
LIMIT {lim}
"""
            )
            out = [{"ticker": str(r.get("ticker", "")), "ticker_name": str(r.get("ticker_name", ""))} for r in rows]
            out = [r for r in out if _is_ticker(r["ticker"])]
            if out:
                return out
        except Exception:
            pass

    if table_exists("feature_snapshot"):
        rows = ch_select(
            f"""
SELECT
    symbol AS ticker,
    '' AS ticker_name,
    max(ts) AS ts_max
FROM trading.feature_snapshot
WHERE ts >= now() - INTERVAL 2 DAY
  AND match(symbol, '^[0-9]{{6}}$')
GROUP BY symbol
ORDER BY ts_max DESC
LIMIT {lim}
"""
        )
        out = [{"ticker": str(r.get("ticker", "")), "ticker_name": str(r.get("ticker_name", ""))} for r in rows]
        return [r for r in out if _is_ticker(r["ticker"])]
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


def compute_stage2_market_score() -> float:
    if not table_exists("market_flow_daily"):
        return 0.0
    rows = ch_select(
        """
SELECT
    investor_type,
    sum(net_buy_value_krw) AS net_buy_value_krw,
    sum(market_traded_value_krw) AS market_traded_value_krw
FROM trading.market_flow_daily
WHERE trade_date >= today() - 5
  AND market = 'ALL'
GROUP BY investor_type
"""
    )
    if not rows:
        rows = ch_select(
            """
SELECT
    investor_type,
    sum(net_buy_value_krw) AS net_buy_value_krw,
    sum(market_traded_value_krw) AS market_traded_value_krw
FROM trading.market_flow_daily
WHERE trade_date >= today() - 5
  AND market IN ('KOSPI', 'KOSDAQ')
GROUP BY investor_type
"""
        )
    foreign_pct = 0.0
    inst_pct = 0.0
    for r in rows:
        inv = str(r.get("investor_type", "")).upper()
        net = _to_float(r.get("net_buy_value_krw"), 0.0)
        traded = _to_float(r.get("market_traded_value_krw"), 0.0)
        pct = (net / traded * 100.0) if traded > 0 else 0.0
        if inv == "FOREIGN":
            foreign_pct = pct
        if inv == "INST":
            inst_pct = pct
    return float(round(_score_market_flow_ratio(foreign_pct, "foreign") + _score_market_flow_ratio(inst_pct, "inst"), 2))


def load_stage_maps() -> dict[str, dict[str, Any]]:
    maps: dict[str, dict[str, Any]] = {
        "flow": {},
        "event": {},
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

    if table_exists("news_event_frames"):
        rows = ch_select(
            """
SELECT
    ticker,
    argMax(frame_id_s, published_at) AS frame_id,
    max(importance_val) AS importance_max,
    avg(importance_val) AS importance_avg,
    count() AS event_cnt,
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
    argMax(liquidity_krw, ts) AS liquidity_krw,
    argMax(spread_bp, ts) AS spread_bp
FROM trading.feature_snapshot
WHERE ts >= now() - INTERVAL 2 DAY
  AND match(symbol, '^[0-9]{6}$')
GROUP BY symbol
"""
        )
        maps["risk"] = {str(r.get("ticker")): r for r in rows}

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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", default="INTRADAY", choices=["INTRADAY", "D1_3", "W1_2"])
    ap.add_argument("--universe", default="watchlist", choices=["watchlist", "all"])
    ap.add_argument("--limit", type=int, default=30)
    ap.add_argument("--model-version", default="decision-operating-spec-p0")
    args = ap.parse_args()

    ensure_decision_tables()
    stage0 = compute_stage0()
    stage1 = compute_stage1()
    market_stage2_score = compute_stage2_market_score()
    maps = load_stage_maps()
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

    candidates: list[dict[str, Any]] = []
    s2_scores: list[float] = []
    s3_scores: list[float] = []
    s4_scores: list[float] = []
    s5_scores: list[float] = []
    total_scores: list[float] = []

    for item in tickers:
        ticker = item["ticker"]
        ticker_name = item.get("ticker_name", "")
        abs_blocks = list(run_abs_blocks)
        explain_codes: list[str] = []

        flow = maps["flow"].get(ticker, {})
        foreign_net = _to_float(flow.get("foreign_net_value_3d"), 0.0)
        inst_net = _to_float(flow.get("inst_net_value_3d"), 0.0)
        traded = _to_float(flow.get("traded_value_3d"), 0.0)
        foreign_pct = (foreign_net / traded * 100.0) if traded > 0 else 0.0
        inst_pct = (inst_net / traded * 100.0) if traded > 0 else 0.0
        foreign_pos_days = _to_int(flow.get("foreign_pos_days"), 0)
        inst_pos_days = _to_int(flow.get("inst_pos_days"), 0)
        flow_persistence = max(foreign_pos_days, inst_pos_days)
        if flow_persistence >= 3:
            persist_score = 15.0
        elif flow_persistence == 2:
            persist_score = 8.0
        elif flow_persistence == 1:
            persist_score = 3.0
        else:
            persist_score = 0.0

        s2_stock = _score_stock_flow_ratio(foreign_pct, "foreign") + _score_stock_flow_ratio(inst_pct, "inst") + persist_score
        stage2_score = _clamp(market_stage2_score + s2_stock, 0, 100)
        distribution_block = foreign_pct <= -6.0 and inst_pct <= -3.0
        stage2_pass = stage2_score >= 55 and not distribution_block
        if distribution_block:
            abs_blocks.append("FLOW_DISTRIBUTION_BLOCK")
        if foreign_pct >= 2.0:
            explain_codes.append("FOREIGN_ACCUM_3D")
        if inst_pct >= 1.0:
            explain_codes.append("INST_ACCUM_3D")

        event = maps["event"].get(ticker, {})
        cluster = maps["cluster"].get(ticker, {})
        dart = maps["dart"].get(ticker, {})
        event_imp_avg = _to_float(event.get("importance_avg"), 0.0)
        event_imp_max = _to_float(event.get("importance_max"), 0.0)
        event_cnt = _to_int(event.get("event_cnt"), 0)
        event_type = str(event.get("event_type", "") or "")
        cluster_state = str(cluster.get("state_label", "") or "")
        cluster_score = _cluster_state_score(cluster_state)
        event_score = _clamp((event_imp_avg / 5.0) * 30.0, 0, 30)
        relevance_score = 20.0
        if event_cnt <= 2:
            novelty_score = 15.0
        elif event_cnt <= 5:
            novelty_score = 10.0
        else:
            novelty_score = 5.0
        stage3_score = _clamp(cluster_score + event_score + relevance_score + novelty_score, 0, 100)
        redflag = False
        if _is_event_redflag(event_type, event_imp_max):
            redflag = True
            abs_blocks.append("EVENT_REDFLAG")
        dart_imp = _to_float(dart.get("importance_max"), 0.0)
        dart_nm = str(dart.get("report_nm", "") or "")
        if _is_dart_redflag(dart_nm, dart_imp):
            redflag = True
            abs_blocks.append("DART_REDFLAG")
        stage3_pass = stage3_score >= 50 and not redflag
        if stage3_score >= 60:
            explain_codes.append("NEWS_CLUSTER_SUPPORT")

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
        mode_a = close > ma20 > ma60 > 0 and vol_ratio >= 1.0 and rsi <= 70
        mode_b = ma20 > ma60 > 0 and (ma20 * 0.975 <= close <= ma20 * 0.995) and (35 <= rsi <= 65) and vol_ratio >= 0.0
        stage4_pass = stage4_score >= 55 and rsi <= 70 and (mode_a or mode_b)
        if rsi > 70:
            abs_blocks.append("TECH_OVERHEAT_RSI")
        if close > ma20 > ma60 > 0:
            explain_codes.append("TREND_STRUCTURE_UP")

        risk = maps["risk"].get(ticker, {})
        liquidity = _to_float(risk.get("liquidity_krw"), 0.0)
        spread_bp = _to_float(risk.get("spread_bp"), 0.0)
        if liquidity >= 5_000_000_000:
            stage5_score = 100.0
        elif liquidity >= 1_000_000_000:
            stage5_score = 80.0
        elif liquidity >= 500_000_000:
            stage5_score = 60.0
        else:
            stage5_score = 20.0
        if spread_bp > 50:
            stage5_score = max(0.0, stage5_score - 10.0)
        stage5_pass = liquidity >= 1_000_000_000
        if not stage5_pass:
            abs_blocks.append("LOW_LIQUIDITY")
        else:
            explain_codes.append("LIQUIDITY_OK")

        penalty = 0.0
        if redflag:
            penalty += 30.0
        if distribution_block:
            penalty += 15.0
        if rsi > 70:
            penalty += 10.0

        total = (
            weights["s1"] * stage1.score
            + weights["s2"] * stage2_score
            + weights["s3"] * stage3_score
            + weights["s4"] * stage4_score
            + weights["s5"] * stage5_score
        ) / 100.0
        total = _clamp(total - penalty, 0, 100)

        all_pass = stage0.passed and stage1.passed_for_buy and stage2_pass and stage3_pass and stage4_pass and stage5_pass
        has_block = len(abs_blocks) > 0
        action = "HOLD"
        if not has_block and all_pass and total >= 70:
            action = "BUY"
        elif total <= 35:
            action = "REDUCE"

        target_weight = 0.0
        if action == "BUY":
            buy_mult = _clamp((stage1.score - 45.0) / 25.0, 0.0, 1.0)
            target_weight = _clamp((0.03 + max(0.0, total - 70.0) / 200.0) * buy_mult, 0.0, 0.10)

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

    run_row = {
        "decision_id": decision_id,
        "decision_time": now_ts,
        "horizon": args.horizon,
        "universe": args.universe,
        "stage0_pass": 1 if stage0.passed else 0,
        "stage0_score": round(stage0.score, 2),
        "stage1_pass": 1 if stage1.passed_for_buy else 0,
        "stage1_score": round(stage1.score, 2),
        "stage2_pass": 1 if stage2_run >= 55 else 0,
        "stage2_score": stage2_run,
        "stage3_pass": 1 if stage3_run >= 50 else 0,
        "stage3_score": stage3_run,
        "stage4_pass": 1 if stage4_run >= 55 else 0,
        "stage4_score": stage4_run,
        "stage5_pass": 1 if stage5_run >= 60 else 0,
        "stage5_score": stage5_run,
        "total_score": total_run,
        "penalty_score": 0.0,
        "absolute_block_reason": sorted(set(run_abs_blocks)),
        "data_freshness_json": json.dumps(stage0.freshness_map, ensure_ascii=False),
        "model_version": args.model_version,
        "prompt_hash": "",
        "created_at": now_ts,
    }

    ch_insert_json_each_row("trading.decision_run", [run_row], timeout_sec=60)
    ch_insert_json_each_row("trading.decision_candidate", candidates, timeout_sec=120)

    buy_cnt = sum(1 for c in candidates if c.get("action") == "BUY")
    hold_cnt = sum(1 for c in candidates if c.get("action") == "HOLD")
    reduce_cnt = sum(1 for c in candidates if c.get("action") == "REDUCE")
    _log(
        f"decision_id={decision_id} stage0={run_row['stage0_pass']} stage1={run_row['stage1_pass']} "
        f"candidates={len(candidates)} buy={buy_cnt} hold={hold_cnt} reduce={reduce_cnt}"
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
