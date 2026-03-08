#!/usr/bin/env python3
"""stock_rag_report_api.py

종목(코드/이름) 기준으로 트레이딩 RAG 데이터를 종합해 보고서를 반환하는 HTTP API.

지원:
- GET  /healthz
- GET  /api/v1/stock-report?q=005930
- POST /api/v1/stock-report {"q":"삼성전자"}

응답:
- JSON (구조화 데이터 + report_markdown)
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse, quote_plus
from urllib.request import Request, urlopen
from urllib.error import HTTPError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env
from market_realtime import fetch_naver_realtime_indices, fetch_naver_usdkrw
from codex_exec_guard import run_codex_cached
from llm_model_config import resolve_model

bootstrap_openclaw_env()


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://localhost:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASS", os.getenv("CLICKHOUSE_PASSWORD", ""))
OPENCLAW_HOME = Path(os.path.expanduser("~/.openclaw"))
KRX_STOCKS_PATH = OPENCLAW_HOME / "data" / "krx_stocks.json"

CODEX_BIN = os.getenv("CODEX_BIN", os.getenv("OPENCLAW_BIN", "openclaw"))
CODEX_MODEL = resolve_model("CODEX_MODEL")
CODEX_EXEC_CACHE_DIR = os.getenv("CODEX_EXEC_CACHE_DIR", os.path.expanduser("~/.openclaw/cache/codex-exec"))
CODEX_EXEC_CACHE_TTL = int(os.getenv("STOCK_REPORT_CODEX_CACHE_TTL", os.getenv("CODEX_EXEC_CACHE_TTL", "180")))
CODEX_EXEC_CACHE_LOCK_WAIT = int(
    os.getenv("STOCK_REPORT_CODEX_CACHE_LOCK_WAIT", os.getenv("CODEX_EXEC_CACHE_LOCK_WAIT", "20"))
)
LLM_DEFAULT_ENABLED = os.getenv("STOCK_REPORT_LLM_DEFAULT", "true").lower() in {"1", "true", "yes", "on"}
LLM_DEFAULT_TIMEOUT = int(os.getenv("STOCK_REPORT_LLM_TIMEOUT", "90"))
PUBLIC_BASE_URL = os.getenv("STOCK_REPORT_PUBLIC_BASE_URL", "").strip().rstrip("/")


def now_kst_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def bool_param(v: Optional[str], default: bool = False) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on", "y"}


def to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def sql_quote(s: str) -> str:
    return "'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"


def normalize_key(s: str) -> str:
    s = str(s or "").strip()
    s = re.sub(r"[^0-9A-Za-z가-힣]", "", s)
    return s.upper()


def ch_query(sql: str, timeout_sec: int = 30) -> List[Dict[str, Any]]:
    query = sql.strip() + "\nFORMAT JSON"
    url = (
        f"{CLICKHOUSE_HOST}/?user={quote_plus(CLICKHOUSE_USER)}"
        f"&password={quote_plus(CLICKHOUSE_PASS)}"
    )
    req = Request(url, data=query.encode("utf-8"), method="POST")
    try:
        with urlopen(req, timeout=timeout_sec) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            body = ""
        raise RuntimeError(f"clickhouse_http_{e.code}: {body[:600]}")
    return payload.get("data", [])


class StockResolver:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.stocks: Dict[str, str] = {}
        self.aliases: Dict[str, str] = {}
        self.code_to_name: Dict[str, str] = {}
        self.norm_name_to_name: Dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        obj = json.loads(self.path.read_text(encoding="utf-8"))
        stocks = obj.get("stocks", {})
        aliases = obj.get("aliases", {})
        if isinstance(stocks, dict):
            self.stocks = {str(k): str(v) for k, v in stocks.items()}
        if isinstance(aliases, dict):
            self.aliases = {str(k): str(v) for k, v in aliases.items()}
        for name, code in self.stocks.items():
            code_s = str(code).zfill(6)
            self.code_to_name[code_s] = name
            self.norm_name_to_name[normalize_key(name)] = name
        for alias, canonical in self.aliases.items():
            self.norm_name_to_name[normalize_key(alias)] = canonical

    def resolve(self, raw_q: str) -> Tuple[Optional[str], Optional[str], List[Dict[str, str]]]:
        q = str(raw_q or "").strip()
        if not q:
            return None, None, []

        if re.fullmatch(r"\d{6}", q):
            ticker = q
            name = self.code_to_name.get(ticker)
            return ticker, name, []

        norm = normalize_key(q)

        # exact canonical name
        if q in self.stocks:
            return str(self.stocks[q]).zfill(6), q, []

        # exact normalized
        if norm in self.norm_name_to_name:
            name = self.norm_name_to_name[norm]
            code = self.stocks.get(name)
            if code:
                return str(code).zfill(6), name, []

        # partial candidates
        candidates: List[Dict[str, str]] = []
        seen = set()
        for name, code in self.stocks.items():
            n_name = normalize_key(name)
            if norm and (norm in n_name or n_name in norm):
                ticker = str(code).zfill(6)
                key = (ticker, name)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append({"ticker": ticker, "name": name})
                if len(candidates) >= 12:
                    break

        if len(candidates) == 1:
            c = candidates[0]
            return c["ticker"], c["name"], []

        return None, None, candidates


def load_adaptive_policy() -> Dict[str, Any]:
    p = OPENCLAW_HOME / "state" / "adaptive_policy.json"
    base = {
        "mode": "normal",
        "min_confidence": 0.70,
        "min_cash_ratio": 0.15,
        "daily_order_limit": 3,
    }
    if not p.exists():
        return base
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(obj, dict):
            base.update(obj)
    except Exception:
        pass
    return base


def get_market_session_info(now: datetime) -> Dict[str, str]:
    dow = now.weekday()  # 0=Mon
    hhmm = int(now.strftime("%H%M"))
    if dow >= 5:
        return {"market_open": "false", "session": "WEEKEND_CLOSED", "notes": "주말(휴장)"}
    if 800 <= hhmm < 850:
        return {"market_open": "partial", "session": "NXT_PREMARKET", "notes": "NXT 프리마켓"}
    if 850 <= hhmm < 900:
        return {"market_open": "auction", "session": "KRX_OPEN_AUCTION", "notes": "시가 단일가"}
    if 900 <= hhmm < 1520:
        return {"market_open": "true", "session": "REGULAR_CONTINUOUS", "notes": "정규 연속매매"}
    if 1520 <= hhmm < 1530:
        return {"market_open": "auction", "session": "KRX_CLOSE_AUCTION", "notes": "종가 단일가"}
    if 1530 <= hhmm < 2000:
        return {"market_open": "partial", "session": "NXT_AFTERMARKET", "notes": "애프터마켓"}
    return {"market_open": "false", "session": "AFTER_HOURS_CLOSED", "notes": "시간 외"}


def fetch_market_snapshot() -> Dict[str, Any]:
    rows = ch_query(
        "SELECT "
        "(SELECT close_price FROM trading.market_index WHERE index_code='KOSPI' ORDER BY date DESC LIMIT 1) AS kospi_db, "
        "(SELECT close_price FROM trading.market_index WHERE index_code='KOSDAQ' ORDER BY date DESC LIMIT 1) AS kosdaq_db, "
        "(SELECT close_rate FROM trading.exchange_rate WHERE currency_pair='USDKRW' ORDER BY date DESC LIMIT 1) AS usdkrw_db, "
        "(SELECT toString(max(date)) FROM trading.market_index WHERE index_code IN ('KOSPI','KOSDAQ')) AS index_db_date, "
        "(SELECT toString(max(date)) FROM trading.exchange_rate WHERE currency_pair='USDKRW') AS fx_db_date"
    )
    db = rows[0] if rows else {}
    rt_idx = fetch_naver_realtime_indices(timeout_sec=8)
    rt_fx = fetch_naver_usdkrw(timeout_sec=8)

    kospi_db = to_float(db.get("kospi_db"), 0.0)
    kosdaq_db = to_float(db.get("kosdaq_db"), 0.0)
    usdkrw_db = to_float(db.get("usdkrw_db"), 0.0)

    kospi_rt = to_float((rt_idx.get("KOSPI", {}) or {}).get("price"), 0.0)
    kosdaq_rt = to_float((rt_idx.get("KOSDAQ", {}) or {}).get("price"), 0.0)
    usdkrw_rt = to_float(rt_fx.get("price"), 0.0) if isinstance(rt_fx, dict) else 0.0

    return {
        "kospi": kospi_rt if kospi_rt > 0 else kospi_db,
        "kosdaq": kosdaq_rt if kosdaq_rt > 0 else kosdaq_db,
        "usdkrw": usdkrw_rt if usdkrw_rt > 0 else usdkrw_db,
        "kospi_source": "RT" if kospi_rt > 0 else "DB",
        "kosdaq_source": "RT" if kosdaq_rt > 0 else "DB",
        "usdkrw_source": "RT" if usdkrw_rt > 0 else "DB",
        "usdkrw_rt_time": rt_fx.get("observed_at", "") if isinstance(rt_fx, dict) else "",
        "index_db_date": str(db.get("index_db_date", "") or ""),
        "fx_db_date": str(db.get("fx_db_date", "") or ""),
    }


def fetch_context(
    ticker: str,
    ticker_name: Optional[str],
    news_hours: int,
    news_limit: int,
) -> Dict[str, Any]:
    t = str(ticker).zfill(6)
    name = str(ticker_name or "").strip()
    has_name = bool(name)
    name_cond = f" OR positionCaseInsensitiveUTF8(title, {sql_quote(name)}) > 0" if has_name else ""

    technical_rows = ch_query(
        "SELECT ticker, ticker_name, close_price, pct, rsi, macd_h, bb, vol_r, signal, score, "
        "toString(data_date) AS data_date, regime, mkt_trend, news_mood "
        "FROM trading.v_trading_dashboard "
        f"WHERE ticker={sql_quote(t)} "
        "ORDER BY data_date DESC LIMIT 1"
    )
    technical = technical_rows[0] if technical_rows else {}
    if not technical:
        fallback_rows = ch_query(
            "SELECT ticker, ticker_name, close_price, change_pct AS pct, rsi14 AS rsi, "
            "macd_hist AS macd_h, bb_pct AS bb, vol_ratio AS vol_r, signal, signal_score AS score, "
            "toString(date) AS data_date, '' AS regime, '' AS mkt_trend, '' AS news_mood "
            "FROM trading.technical_signals "
            f"WHERE ticker={sql_quote(t)} "
            "ORDER BY date DESC LIMIT 1"
        )
        if fallback_rows:
            technical = fallback_rows[0]

    technical_hist = ch_query(
        "SELECT toString(date) AS date, close_price, change_pct, rsi14, macd_hist, bb_pct, vol_ratio, signal, signal_score "
        "FROM trading.technical_signals "
        f"WHERE ticker={sql_quote(t)} "
        "ORDER BY date DESC LIMIT 10"
    )

    regime_rows = ch_query("SELECT * FROM trading.v_regime ORDER BY date DESC LIMIT 1")
    regime = regime_rows[0] if regime_rows else {}

    news_rows = ch_query(
        "SELECT toString(published_at) AS published_at_str, title, summary, sentiment, importance, source_url, tickers "
        "FROM trading.news "
        f"WHERE published_at >= now() - INTERVAL {max(1, news_hours)} HOUR "
        f"AND (has(tickers, {sql_quote(t)}){name_cond}) "
        "ORDER BY published_at DESC, importance DESC "
        f"LIMIT {max(1, news_limit)}"
    )

    news_stats_rows = ch_query(
        "SELECT "
        "count() AS news_cnt, "
        "countIf(sentiment='positive') AS pos_cnt, "
        "countIf(sentiment='negative') AS neg_cnt, "
        "countIf(sentiment='neutral') AS neu_cnt, "
        "round(avg(importance), 2) AS avg_importance, "
        "max(importance) AS max_importance "
        "FROM trading.news "
        f"WHERE published_at >= now() - INTERVAL {max(1, news_hours)} HOUR "
        f"AND (has(tickers, {sql_quote(t)}){name_cond})"
    )
    news_stats = news_stats_rows[0] if news_stats_rows else {}

    frame_rows = ch_query(
        "SELECT toString(published_at) AS published_at_str, event_type, event_subtype, importance, sentiment, "
        "impact_type, time_horizon, lag_hours, analysis_confidence, thesis_path, invalidation, "
        "source_url, title, summary, evidence_json, channels "
        "FROM trading.news_event_frames "
        f"WHERE published_at >= now() - INTERVAL {max(1, news_hours)} HOUR "
        "AND relevant = 1 "
        f"AND (has(tickers, {sql_quote(t)}){name_cond}) "
        "ORDER BY published_at DESC, importance DESC "
        "LIMIT 20"
    )

    event_memory_rows = ch_query(
        "SELECT toString(published_at) AS published_at_str, event_type, time_horizon, pred_direction, pred_confidence, "
        "status, realized_ret_1d, realized_ret_3d, calibration_error, source_url, thesis_path "
        "FROM trading.event_memory "
        f"WHERE ticker={sql_quote(t)} "
        "ORDER BY published_at DESC LIMIT 20"
    )

    cluster_state_rows = ch_query(
        "SELECT cluster_id, state_label, n_news, importance_max, delta_news, delta_sentiment, storyline, top_tickers, top_categories "
        "FROM trading.news_cluster_state "
        "WHERE asof_ts = (SELECT max(asof_ts) FROM trading.news_cluster_state) "
        f"AND has(top_tickers, {sql_quote(t)}) "
        "ORDER BY importance_max DESC, n_news DESC "
        "LIMIT 10"
    )

    cluster_news_rows = ch_query(
        "SELECT m.cluster_id, toString(n.published_at) AS published_at_str, n.title, n.sentiment, n.importance, n.source_url "
        "FROM trading.news_cluster_map AS m "
        "ANY INNER JOIN trading.news AS n ON n.id = m.news_id "
        "WHERE m.asof_ts = (SELECT max(asof_ts) FROM trading.news_cluster_map) "
        f"AND has(n.tickers, {sql_quote(t)}) "
        "ORDER BY n.published_at DESC "
        "LIMIT 20"
    )

    for row in news_rows:
        row["published_at"] = row.pop("published_at_str", row.get("published_at", ""))
    for row in frame_rows:
        row["published_at"] = row.pop("published_at_str", row.get("published_at", ""))
    for row in event_memory_rows:
        row["published_at"] = row.pop("published_at_str", row.get("published_at", ""))
    for row in cluster_news_rows:
        row["published_at"] = row.pop("published_at_str", row.get("published_at", ""))

    hidden_relation_rows = ch_query(
        "SELECT ticker, ticker_name, toString(asof_ts) AS asof_ts, total_relation_score, relation_bias, "
        "direct_event_score, transfer_event_score, cluster_state_score, memory_calibration_score, "
        "support_events, support_clusters, source_tickers, top_roles, top_channels "
        "FROM trading.v_hidden_relation_signals "
        f"WHERE ticker={sql_quote(t)} LIMIT 1"
    )
    hidden_relation = hidden_relation_rows[0] if hidden_relation_rows else {}

    order_rows = ch_query(
        "SELECT toString(ts) AS ts, side, qty, limit_price, state, reject_reason, request_json "
        "FROM trading.order_log "
        f"WHERE symbol={sql_quote(t)} "
        "ORDER BY ts DESC LIMIT 10"
    )

    decision_rows = ch_query(
        "SELECT toString(ts) AS ts, model, output_json "
        "FROM trading.decision_log "
        f"WHERE position(output_json, concat('\"ticker\": \"', {sql_quote(t)}, '\"')) > 0 "
        "ORDER BY ts DESC LIMIT 5"
    )

    return {
        "ticker": t,
        "ticker_name": name or technical.get("ticker_name", ""),
        "market_session": get_market_session_info(datetime.now()),
        "market_snapshot": fetch_market_snapshot(),
        "regime": regime,
        "technical": technical,
        "technical_history": technical_hist,
        "news_stats": news_stats,
        "news": news_rows,
        "event_frames": frame_rows,
        "event_memory": event_memory_rows,
        "cluster_states": cluster_state_rows,
        "cluster_news": cluster_news_rows,
        "hidden_relation": hidden_relation,
        "recent_orders": order_rows,
        "recent_decisions": decision_rows,
        "adaptive_policy": load_adaptive_policy(),
    }


def parse_recent_order_brief(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        req = {}
        try:
            req = json.loads(r.get("request_json", "") or "{}")
            if not isinstance(req, dict):
                req = {}
        except Exception:
            req = {}
        out.append(
            {
                "ts": r.get("ts", ""),
                "side": r.get("side", ""),
                "qty": to_int(r.get("qty", 0), 0),
                "limit_price": to_float(r.get("limit_price", 0), 0.0),
                "state": r.get("state", ""),
                "reject_reason": r.get("reject_reason", ""),
                "confidence": to_float(req.get("confidence", 0), 0.0),
                "time_horizon": str(req.get("time_horizon", "") or ""),
                "thesis_path": str(req.get("thesis_path", "") or ""),
                "evidence_urls": req.get("evidence_urls", []) if isinstance(req.get("evidence_urls", []), list) else [],
            }
        )
    return out


def parse_recent_decisions(rows: List[Dict[str, Any]], ticker: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        parsed = {}
        try:
            parsed = json.loads(r.get("output_json", "") or "{}")
        except Exception:
            parsed = {}
        if not isinstance(parsed, dict):
            continue
        orders = parsed.get("orders", [])
        if not isinstance(orders, list):
            continue
        selected = None
        for o in orders:
            if isinstance(o, dict) and str(o.get("ticker", "")).strip() == ticker:
                selected = o
                break
        if not selected:
            continue
        out.append(
            {
                "ts": r.get("ts", ""),
                "model": r.get("model", ""),
                "market_assessment": parsed.get("market_assessment", ""),
                "regime_action": parsed.get("regime_action", ""),
                "action": selected.get("action", ""),
                "confidence": to_float(selected.get("confidence", 0), 0.0),
                "price": to_float(selected.get("price", 0), 0.0),
                "quantity": to_int(selected.get("quantity", 0), 0),
                "time_horizon": selected.get("time_horizon", ""),
                "thesis_path": selected.get("thesis_path", ""),
            }
        )
    return out


def codex_exec_json(
    prompt: str,
    schema: Dict[str, Any],
    timeout_sec: int,
    model: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], str]:
    codex = shutil.which(CODEX_BIN) or CODEX_BIN
    if not shutil.which(codex) and not Path(codex).exists():
        return None, f"codex_not_found:{codex}"

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as sf:
        schema_path = sf.name
        json.dump(schema, sf, ensure_ascii=False)
    try:
        output = run_codex_cached(
            prompt=prompt,
            codex_bin=codex,
            model=model or CODEX_MODEL,
            workdir=None,
            timeout_sec=max(30, timeout_sec),
            base_args=[
                "--skip-git-repo-check",
                "--full-auto",
            ],
            output_schema_path=schema_path,
            cache_dir=CODEX_EXEC_CACHE_DIR,
            cache_ttl_sec=CODEX_EXEC_CACHE_TTL,
            cache_lock_wait_sec=CODEX_EXEC_CACHE_LOCK_WAIT,
        )
        raw = output.strip()
        if not raw:
            return None, "codex_empty_output"
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                return obj, ""
        except Exception:
            pass

        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            try:
                obj = json.loads(m.group(0))
                if isinstance(obj, dict):
                    return obj, ""
            except Exception:
                pass
        return None, "codex_json_parse_failed"
    except Exception as e:
        return None, f"codex_error:{type(e).__name__}:{e}"
    finally:
        try:
            Path(schema_path).unlink(missing_ok=True)
        except Exception:
            pass


def build_llm_prompt(context: Dict[str, Any]) -> str:
    tech = context.get("technical", {})
    news = context.get("news", [])[:12]
    frames = context.get("event_frames", [])[:8]
    relation = context.get("hidden_relation", {})
    regime = context.get("regime", {})
    mkt = context.get("market_snapshot", {})
    policy = context.get("adaptive_policy", {})
    orders = parse_recent_order_brief(context.get("recent_orders", [])[:5])

    compact = {
        "ticker": context.get("ticker"),
        "ticker_name": context.get("ticker_name"),
        "market_session": context.get("market_session"),
        "market_regime": regime,
        "adaptive_policy": policy,
        "technical": tech,
        "news_stats": context.get("news_stats", {}),
        "news_top": news,
        "event_frames_top": frames,
        "hidden_relation": relation,
        "recent_order_logs": orders,
    }

    rules = [
        "RSI > 70이면 BUY 금지.",
        "BUY는 thesis_path/time_horizon/evidence_urls(1개 이상) 없으면 금지.",
        "confidence는 adaptive_policy.min_confidence 이상이어야 실행 가능.",
        "응답은 제공된 데이터 기반으로만 판단하고, 과장/추정 금지.",
    ]

    return (
        "너는 한국 주식 트레이딩 리포트 판단 엔진이다.\n"
        "아래 컨텍스트를 기반으로 단일 종목 매매 판단을 내려라.\n"
        "하드룰:\n- " + "\n- ".join(rules) + "\n\n"
        "출력은 JSON만.\n\n"
        "[CONTEXT_JSON]\n"
        + json.dumps(compact, ensure_ascii=False, indent=2)
    )


def run_llm_decision(context: Dict[str, Any], timeout_sec: int) -> Dict[str, Any]:
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "action": {"type": "string", "enum": ["BUY", "SELL", "HOLD", "WATCH", "AVOID"]},
            "confidence": {"type": "number"},
            "summary": {"type": "string"},
            "thesis_path": {"type": "string"},
            "time_horizon": {"type": "string", "enum": ["intraday", "1-3d", "1-2w", "1-3m", "swing"]},
            "risk_flags": {"type": "array", "items": {"type": "string"}},
            "evidence_urls": {"type": "array", "items": {"type": "string"}},
            "entry_guide": {"type": "string"},
            "take_profit_pct": {"type": "number"},
            "stop_loss_pct": {"type": "number"},
            "position_size_hint": {"type": "string"},
        },
        "required": [
            "action",
            "confidence",
            "summary",
            "thesis_path",
            "time_horizon",
            "risk_flags",
            "evidence_urls",
            "entry_guide",
            "take_profit_pct",
            "stop_loss_pct",
            "position_size_hint",
        ],
    }
    prompt = build_llm_prompt(context)
    obj, err = codex_exec_json(prompt, schema, timeout_sec=timeout_sec, model=CODEX_MODEL)
    if obj is None:
        return {"ok": False, "error": err}
    return {"ok": True, "decision": obj}


def build_markdown_report(context: Dict[str, Any], llm_result: Dict[str, Any]) -> str:
    t = context.get("ticker", "")
    n = context.get("ticker_name", "")
    tech = context.get("technical", {})
    regime = context.get("regime", {})
    mkt = context.get("market_snapshot", {})
    session = context.get("market_session", {})
    news_stats = context.get("news_stats", {})
    news_rows = context.get("news", [])
    frames = context.get("event_frames", [])
    clusters = context.get("cluster_states", [])
    relation = context.get("hidden_relation", {})
    recent_orders = parse_recent_order_brief(context.get("recent_orders", []))
    recent_decisions = parse_recent_decisions(context.get("recent_decisions", []), t)

    lines: List[str] = []
    lines.append(f"# 종목 종합 리포트: {n or '-'}({t})")
    lines.append("")
    lines.append(f"- 생성시각: {now_kst_str()}")
    lines.append(f"- 세션: {session.get('session','-')} / market_open={session.get('market_open','-')} ({session.get('notes','-')})")
    lines.append("")

    lines.append("## 1) 시장 온도")
    if mkt:
        lines.append(
            f"- 스냅샷: KOSPI {to_float(mkt.get('kospi'), 0):,.2f}({mkt.get('kospi_source','-')}) | "
            f"KOSDAQ {to_float(mkt.get('kosdaq'), 0):,.2f}({mkt.get('kosdaq_source','-')}) | "
            f"USDKRW {to_float(mkt.get('usdkrw'), 0):,.2f}({mkt.get('usdkrw_source','-')})"
        )
        lines.append(
            f"- 기준시각: usdkrw_rt={mkt.get('usdkrw_rt_time','-')} | db_index_date={mkt.get('index_db_date','-')} | db_fx_date={mkt.get('fx_db_date','-')}"
        )
    lines.append(
        f"- 레짐: {regime.get('regime_label','-')} | trend={regime.get('trend','-')} "
        f"| vol={regime.get('volatility','-')} | risk={regime.get('risk_appetite','-')} | mood={regime.get('news_mood','-')}"
    )
    lines.append(f"- 요약: {regime.get('summary','-')}")
    lines.append("")

    lines.append("## 2) 기술 점수")
    if tech:
        lines.append(
            f"- signal={tech.get('signal','-')} / score={tech.get('score','-')} / rsi={tech.get('rsi','-')} "
            f"/ macd_h={tech.get('macd_h','-')} / bb={tech.get('bb','-')} / vol_r={tech.get('vol_r','-')}"
        )
        lines.append(
            f"- close={tech.get('close_price','-')} / pct={tech.get('pct','-')} / data_date={tech.get('data_date','-')}"
        )
    else:
        lines.append("- 기술 데이터 없음(v_trading_dashboard 미존재)")
    lines.append("")

    lines.append("## 3) 관련 뉴스 (원문 링크 포함)")
    lines.append(
        f"- 집계: count={news_stats.get('news_cnt',0)}, pos={news_stats.get('pos_cnt',0)}, "
        f"neg={news_stats.get('neg_cnt',0)}, neu={news_stats.get('neu_cnt',0)}, "
        f"avg_imp={news_stats.get('avg_importance',0)}"
    )
    if news_rows:
        for i, r in enumerate(news_rows[:15], 1):
            url = str(r.get("source_url", "") or "").strip()
            link = url if url else "-"
            title = str(r.get("title", "") or "").strip()
            lines.append(
                f"{i}. [{r.get('published_at','-')}] ({r.get('sentiment','-')}/imp{r.get('importance','-')}) {title}"
            )
            lines.append(f"   - 원문: {link}")
    else:
        lines.append("- 관련 뉴스 없음")
    lines.append("")

    lines.append("## 4) 이벤트 프레임 / 클러스터")
    if frames:
        for i, f in enumerate(frames[:8], 1):
            lines.append(
                f"{i}. {f.get('published_at','-')} | {f.get('event_type','-')}/{f.get('event_subtype','-')} "
                f"| {f.get('sentiment','-')} imp={f.get('importance','-')} "
                f"| horizon={f.get('time_horizon','-')} conf={f.get('analysis_confidence','-')}"
            )
            lines.append(f"   - thesis: {f.get('thesis_path','-')}")
            lines.append(f"   - 원문: {f.get('source_url','-')}")
    else:
        lines.append("- 이벤트 프레임 없음")

    if clusters:
        lines.append("- 클러스터 상태:")
        for c in clusters[:6]:
            lines.append(
                f"  - {c.get('cluster_id','-')} | {c.get('state_label','-')} "
                f"| n={c.get('n_news','-')} imp_max={c.get('importance_max','-')} "
                f"| delta_news={c.get('delta_news','-')} delta_sent={c.get('delta_sentiment','-')}"
            )
    else:
        lines.append("- 연계 클러스터 상태 없음")
    lines.append("")

    lines.append("## 5) Hidden Relation")
    if relation:
        lines.append(
            f"- total={relation.get('total_relation_score','-')} ({relation.get('relation_bias','-')}) "
            f"| direct={relation.get('direct_event_score','-')} transfer={relation.get('transfer_event_score','-')} "
            f"| cluster={relation.get('cluster_state_score','-')} support_events={relation.get('support_events','-')}"
        )
        src = relation.get("source_tickers", [])
        if isinstance(src, list) and src:
            lines.append(f"- source_tickers: {', '.join(src[:8])}")
    else:
        lines.append("- hidden relation 신호 없음")
    lines.append("")

    lines.append("## 6) LLM 최근 판단/실행 이력")
    if recent_decisions:
        lines.append("- decision_log:")
        for d in recent_decisions[:5]:
            lines.append(
                f"  - {d.get('ts','-')} | {d.get('action','-')} conf={d.get('confidence','-')} "
                f"| horizon={d.get('time_horizon','-')} | model={d.get('model','-')}"
            )
    else:
        lines.append("- decision_log 내 종목 판단 이력 없음")
    if recent_orders:
        lines.append("- order_log:")
        for o in recent_orders[:5]:
            lines.append(
                f"  - {o.get('ts','-')} | {o.get('side','-')} {o.get('qty','-')}주 @{o.get('limit_price','-')} "
                f"| state={o.get('state','-')} reason={o.get('reject_reason','-')} conf={o.get('confidence','-')}"
            )
    else:
        lines.append("- order_log 이력 없음")
    lines.append("")

    lines.append("## 7) LLM 온디맨드 판단")
    if llm_result.get("ok"):
        d = llm_result.get("decision", {})
        lines.append(
            f"- action={d.get('action','-')} / confidence={d.get('confidence','-')} / "
            f"time_horizon={d.get('time_horizon','-')}"
        )
        lines.append(f"- summary: {d.get('summary','-')}")
        lines.append(f"- thesis: {d.get('thesis_path','-')}")
        lines.append(f"- risk_flags: {', '.join(d.get('risk_flags', [])) if isinstance(d.get('risk_flags'), list) else '-'}")
        ev_urls = d.get("evidence_urls", [])
        if isinstance(ev_urls, list) and ev_urls:
            lines.append("- evidence_urls:")
            for u in ev_urls[:8]:
                lines.append(f"  - {u}")
        lines.append(
            f"- take_profit_pct={d.get('take_profit_pct','-')} / stop_loss_pct={d.get('stop_loss_pct','-')} "
            f"/ size_hint={d.get('position_size_hint','-')}"
        )
    else:
        lines.append(f"- LLM 판단 실패: {llm_result.get('error','unknown')}")
    lines.append("")
    lines.append("## 8) 실행 하드룰 체크(요약)")
    if tech and to_float(tech.get("rsi", 0), 0) > 70:
        lines.append("- BUY 차단: RSI > 70")
    else:
        lines.append("- RSI 하드룰 통과 (RSI<=70)")
    lines.append("- BUY 시 thesis_path/time_horizon/evidence_urls 충족 여부 필수")
    lines.append("- BUY 시 adaptive_policy.min_confidence 이상 필요")
    lines.append("")

    return "\n".join(lines)


def generate_stock_report(
    resolver: StockResolver,
    raw_q: str,
    news_hours: int,
    news_limit: int,
    include_llm: bool,
    llm_timeout_sec: int,
) -> Dict[str, Any]:
    ticker, name, candidates = resolver.resolve(raw_q)
    if ticker is None:
        return {
            "ok": False,
            "error": "ticker_not_resolved",
            "message": "종목 코드/이름을 정확히 지정해 주세요.",
            "input": raw_q,
            "candidates": candidates,
        }

    context = fetch_context(ticker=ticker, ticker_name=name, news_hours=news_hours, news_limit=news_limit)
    llm_result = {"ok": False, "error": "llm_skipped"}
    if include_llm:
        llm_result = run_llm_decision(context, timeout_sec=llm_timeout_sec)

    report_md = build_markdown_report(context, llm_result)

    return {
        "ok": True,
        "generated_at": now_kst_str(),
        "input": raw_q,
        "resolved": {"ticker": ticker, "ticker_name": context.get("ticker_name", "")},
        "params": {
            "news_hours": news_hours,
            "news_limit": news_limit,
            "include_llm": include_llm,
            "llm_timeout_sec": llm_timeout_sec,
        },
        "llm": llm_result,
        "data": {
            "market_session": context.get("market_session", {}),
            "market_snapshot": context.get("market_snapshot", {}),
            "regime": context.get("regime", {}),
            "technical": context.get("technical", {}),
            "technical_history": context.get("technical_history", []),
            "news_stats": context.get("news_stats", {}),
            "news": context.get("news", []),
            "event_frames": context.get("event_frames", []),
            "event_memory": context.get("event_memory", []),
            "cluster_states": context.get("cluster_states", []),
            "cluster_news": context.get("cluster_news", []),
            "hidden_relation": context.get("hidden_relation", {}),
            "recent_orders": parse_recent_order_brief(context.get("recent_orders", [])),
            "recent_decisions": parse_recent_decisions(context.get("recent_decisions", []), ticker),
            "adaptive_policy": context.get("adaptive_policy", {}),
        },
        "report_markdown": report_md,
    }


def parse_dooray_text(raw: str) -> Dict[str, Any]:
    """Dooray slash command text 파싱.

    예:
    - "삼성전자"
    - "005930 --llm=0 --news_hours=168 --news_limit=20"
    """
    text = str(raw or "").strip()
    if not text:
        return {"q": "", "include_llm": LLM_DEFAULT_ENABLED, "news_hours": 72, "news_limit": 30, "llm_timeout_sec": LLM_DEFAULT_TIMEOUT}

    parts = text.split()
    q = ""
    flags: Dict[str, str] = {}
    q_tokens: List[str] = []
    for tok in parts:
        if tok.startswith("--") and "=" in tok:
            k, v = tok[2:].split("=", 1)
            flags[k.strip()] = v.strip()
        else:
            q_tokens.append(tok)
    if q_tokens:
        q = " ".join(q_tokens).strip()

    return {
        "q": q,
        "include_llm": bool_param(flags.get("llm"), LLM_DEFAULT_ENABLED),
        "news_hours": max(1, min(to_int(flags.get("news_hours", 72), 72), 24 * 30)),
        "news_limit": max(1, min(to_int(flags.get("news_limit", 20), 20), 100)),
        "llm_timeout_sec": max(30, min(to_int(flags.get("llm_timeout_sec", LLM_DEFAULT_TIMEOUT), LLM_DEFAULT_TIMEOUT), 300)),
    }


def build_dooray_text(out: Dict[str, Any]) -> str:
    if not out.get("ok"):
        cands = out.get("candidates", []) or []
        lines = ["[종목분석] 요청 처리 실패"]
        msg = out.get("message") or out.get("error") or "unknown"
        lines.append(f"- 사유: {msg}")
        if cands:
            lines.append("- 후보:")
            for c in cands[:8]:
                lines.append(f"  - {c.get('name','')}({c.get('ticker','')})")
        lines.append("- 사용법: /종목분석 삼성전자")
        lines.append("- 옵션: /종목분석 삼성전자 --llm=1 --news_hours=168 --news_limit=20")
        return "\n".join(lines)

    resolved = out.get("resolved", {})
    data = out.get("data", {})
    llm = out.get("llm", {})
    tech = data.get("technical", {})
    regime = data.get("regime", {})
    news = data.get("news", [])
    nstats = data.get("news_stats", {})
    rel = data.get("hidden_relation", {})

    lines: List[str] = []
    lines.append(f"[종목분석] {resolved.get('ticker_name','-')}({resolved.get('ticker','-')})")
    lines.append(f"- 생성: {out.get('generated_at','-')}")
    lines.append(
        f"- 레짐: {regime.get('regime_label','-')} ({regime.get('trend','-')}/{regime.get('volatility','-')})"
    )
    if tech:
        lines.append(
            f"- 기술: signal={tech.get('signal','-')} score={tech.get('score','-')} "
            f"rsi={tech.get('rsi','-')} macd_h={tech.get('macd_h','-')} bb={tech.get('bb','-')} vol_r={tech.get('vol_r','-')}"
        )
    lines.append(
        f"- 뉴스: {nstats.get('news_cnt',0)}건 (pos={nstats.get('pos_cnt',0)} / neg={nstats.get('neg_cnt',0)} / avg_imp={nstats.get('avg_importance',0)})"
    )
    if rel:
        lines.append(
            f"- 연관성: total={rel.get('total_relation_score','-')} ({rel.get('relation_bias','-')}) "
            f"direct={rel.get('direct_event_score','-')} transfer={rel.get('transfer_event_score','-')}"
        )
    if llm.get("ok"):
        d = llm.get("decision", {})
        lines.append(
            f"- LLM 판단: {d.get('action','-')} (conf={d.get('confidence','-')}, horizon={d.get('time_horizon','-')})"
        )
        lines.append(f"- 요약: {d.get('summary','-')}")
    else:
        lines.append(f"- LLM 판단: 실패/스킵 ({llm.get('error','-')})")

    lines.append("- 관련 뉴스 링크:")
    if news:
        for i, r in enumerate(news[:6], 1):
            title = str(r.get("title", "") or "").strip()
            url = str(r.get("source_url", "") or "").strip() or "-"
            lines.append(f"  {i}. {title}")
            lines.append(f"     {url}")
    else:
        lines.append("  - 없음")

    lines.append("")
    lines.append("상세 JSON 리포트:")
    q = quote_plus(str(resolved.get("ticker", "")))
    if PUBLIC_BASE_URL:
        lines.append(f"{PUBLIC_BASE_URL}/api/v1/stock-report?q={q}&include_llm=1")
    else:
        lines.append(f"/api/v1/stock-report?q={q}&include_llm=1")
    return "\n".join(lines)


class StockReportHandler(BaseHTTPRequestHandler):
    resolver: StockResolver = StockResolver(KRX_STOCKS_PATH)

    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(raw)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self._send_json(200, {"ok": True, "time": now_kst_str()})
            return
        if parsed.path == "/":
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "stock-rag-report-api",
                    "endpoints": [
                        "GET /healthz",
                        "GET /api/v1/stock-report?q=005930",
                        "POST /api/v1/stock-report",
                    ],
                },
            )
            return
        if parsed.path != "/api/v1/stock-report":
            if parsed.path == "/api/v1/dooray/stock-report":
                q = parse_qs(parsed.query)
                text = (q.get("text") or q.get("q") or [""])[0]
                parsed_req = parse_dooray_text(text)
                out = generate_stock_report(
                    resolver=self.resolver,
                    raw_q=parsed_req["q"],
                    news_hours=parsed_req["news_hours"],
                    news_limit=parsed_req["news_limit"],
                    include_llm=parsed_req["include_llm"],
                    llm_timeout_sec=parsed_req["llm_timeout_sec"],
                )
                self._send_json(200, {"text": build_dooray_text(out)})
                return
            self._send_json(404, {"ok": False, "error": "not_found"})
            return

        q = parse_qs(parsed.query)
        raw_q = (q.get("q") or q.get("ticker") or q.get("name") or [""])[0]
        news_hours = to_int((q.get("news_hours") or ["72"])[0], 72)
        news_limit = to_int((q.get("news_limit") or ["30"])[0], 30)
        include_llm = bool_param((q.get("include_llm") or ["1"])[0], LLM_DEFAULT_ENABLED)
        llm_timeout = to_int((q.get("llm_timeout_sec") or [str(LLM_DEFAULT_TIMEOUT)])[0], LLM_DEFAULT_TIMEOUT)

        try:
            out = generate_stock_report(
                resolver=self.resolver,
                raw_q=raw_q,
                news_hours=max(1, min(news_hours, 24 * 30)),
                news_limit=max(1, min(news_limit, 100)),
                include_llm=include_llm,
                llm_timeout_sec=max(30, min(llm_timeout, 300)),
            )
            self._send_json(200 if out.get("ok") else 400, out)
        except Exception as e:
            self._send_json(500, {"ok": False, "error": f"server_error:{type(e).__name__}:{e}"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/v1/dooray/stock-report":
            try:
                cl = int(self.headers.get("Content-Length", "0"))
            except Exception:
                cl = 0
            raw = self.rfile.read(max(0, cl)) if cl > 0 else b""
            ctype = str(self.headers.get("Content-Type", "") or "").lower()

            params: Dict[str, Any] = {}
            if "application/x-www-form-urlencoded" in ctype:
                qd = parse_qs(raw.decode("utf-8", errors="ignore"))
                for k, v in qd.items():
                    params[k] = v[0] if isinstance(v, list) and v else ""
            else:
                try:
                    obj = json.loads(raw.decode("utf-8")) if raw else {}
                    if isinstance(obj, dict):
                        params = obj
                except Exception:
                    params = {}

            # Dooray slash command에서 일반적으로 text에 사용자 입력이 들어온다고 가정.
            text = str(
                params.get("text")
                or params.get("commandText")
                or params.get("q")
                or params.get("ticker")
                or params.get("name")
                or ""
            ).strip()
            parsed_req = parse_dooray_text(text)
            try:
                out = generate_stock_report(
                    resolver=self.resolver,
                    raw_q=parsed_req["q"],
                    news_hours=parsed_req["news_hours"],
                    news_limit=parsed_req["news_limit"],
                    include_llm=parsed_req["include_llm"],
                    llm_timeout_sec=parsed_req["llm_timeout_sec"],
                )
                self._send_json(200, {"text": build_dooray_text(out)})
            except Exception as e:
                self._send_json(200, {"text": f"[종목분석] 서버 오류: {type(e).__name__}: {e}"})
            return

        if parsed.path != "/api/v1/stock-report":
            self._send_json(404, {"ok": False, "error": "not_found"})
            return

        try:
            cl = int(self.headers.get("Content-Length", "0"))
        except Exception:
            cl = 0
        raw = self.rfile.read(max(0, cl)) if cl > 0 else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
            if not isinstance(body, dict):
                body = {}
        except Exception:
            body = {}

        raw_q = str(body.get("q") or body.get("ticker") or body.get("name") or "").strip()
        news_hours = to_int(body.get("news_hours", 72), 72)
        news_limit = to_int(body.get("news_limit", 30), 30)
        include_llm = bool(body.get("include_llm", LLM_DEFAULT_ENABLED))
        llm_timeout = to_int(body.get("llm_timeout_sec", LLM_DEFAULT_TIMEOUT), LLM_DEFAULT_TIMEOUT)

        try:
            out = generate_stock_report(
                resolver=self.resolver,
                raw_q=raw_q,
                news_hours=max(1, min(news_hours, 24 * 30)),
                news_limit=max(1, min(news_limit, 100)),
                include_llm=include_llm,
                llm_timeout_sec=max(30, min(llm_timeout, 300)),
            )
            self._send_json(200 if out.get("ok") else 400, out)
        except Exception as e:
            self._send_json(500, {"ok": False, "error": f"server_error:{type(e).__name__}:{e}"})

    def log_message(self, fmt: str, *args: Any) -> None:
        # 기본 access log는 과도하므로 간략 출력.
        print(f"{now_kst_str()} [stock-rag-report-api] {self.address_string()} {fmt % args}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Stock RAG report HTTP API")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8787)
    args = ap.parse_args()

    srv = ThreadingHTTPServer((args.host, args.port), StockReportHandler)
    print(
        f"{now_kst_str()} [stock-rag-report-api] serving on http://{args.host}:{args.port}",
        flush=True,
    )
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
