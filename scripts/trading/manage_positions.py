#!/usr/bin/env python3
"""LLM-driven position manager for live holdings.

Flow:
1) Load live holdings from KIS (mcporter)
2) Build context (market regime + technical + flow + recent events + prior thesis state)
3) Ask LLM for per-position actions (HOLD/REDUCE/EXIT/ADD/TIGHTEN_STOP/TAKE_PROFIT_PARTIAL)
4) Convert actions -> trading_response.json orders + risk_targets
5) Execute via execute_gpt_orders.py (same hard guardrails)
6) Persist review/action logs and thesis state
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests

from codex_exec_guard import run_codex_cached
from env_bootstrap import bootstrap_openclaw_env
from llm_model_config import resolve_model

bootstrap_openclaw_env()

KST = datetime.now().astimezone().tzinfo
HOME = Path.home()
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent.parent

STATE_ROOT = HOME / ".openclaw" / "state"
POSITION_STATE_FILE = STATE_ROOT / "position_manager_state.json"
POSITION_REVIEW_DIR = STATE_ROOT / "position_manager" / "reviews"
POSITION_LATEST_FILE = STATE_ROOT / "position_manager" / "latest_review.json"
DYNAMIC_EXIT_STATE_FILE = STATE_ROOT / "stock_dynamic_exits.json"
EXECUTION_MODE_STATE_FILE = STATE_ROOT / "market_execution_mode.json"
PERSISTENT_MEMORY_PATH = HOME / ".openclaw" / "workspace" / "CODEX_PERSISTENT_MEMORY.md"
HEARTBEAT_PATH = HOME / ".openclaw" / "workspace" / "HEARTBEAT.md"
SOUL_PATH = HOME / ".openclaw" / "workspace" / "SOUL.md"
DEFAULT_RESPONSE_PATH = "/tmp/position_manager_response.json"
POSITION_SCHEMA_FILE = SCRIPT_DIR / "position_manager_response_schema.json"
ORDER_EXEC_SCRIPT = SCRIPT_DIR / "execute_gpt_orders.py"
TELEGRAM_NOTIFY_SCRIPT = HOME / ".openclaw" / "scripts" / "telegram_notify.py"
MCPORTER_BIN = os.getenv("MCPORTER_BIN") or shutil.which("mcporter") or "/opt/homebrew/bin/mcporter"
MCP_CONFIG = str(HOME / ".openclaw" / "config" / "mcporter.json")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://localhost:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASS", os.getenv("CLICKHOUSE_PASSWORD", ""))
CH_QUERY_TIMEOUT_SEC = max(5, int(os.getenv("CH_QUERY_TIMEOUT_SEC", "25")))
CH_QUERY_RETRIES = max(0, int(os.getenv("CH_QUERY_RETRIES", "2")))
CH_RETRY_BACKOFF_SEC = max(0.1, float(os.getenv("CH_QUERY_RETRY_BACKOFF_SEC", "0.35")))
CH_MAX_EXECUTION_TIME = max(5, int(os.getenv("CH_MAX_EXECUTION_TIME", "15")))
CH_MAX_THREADS = max(1, int(os.getenv("CH_MAX_THREADS", "2")))

POSITION_MANAGER_ENABLED = os.getenv("POSITION_MANAGER_ENABLED", "1") == "1"
POSITION_MANAGER_ALLOW_ADD = os.getenv("POSITION_MANAGER_ALLOW_ADD", "0") == "1"
POSITION_MANAGER_BLOCK_REENTRY_SAME_DAY = os.getenv("POSITION_MANAGER_BLOCK_REENTRY_SAME_DAY", "1") == "1"
POSITION_MANAGER_MAX_ACTIONS = max(1, int(os.getenv("POSITION_MANAGER_MAX_ACTIONS", "3")))
POSITION_MANAGER_COOLDOWN_MIN = max(5, int(os.getenv("POSITION_MANAGER_COOLDOWN_MIN", "45")))
POSITION_MANAGER_DAILY_ACTION_LIMIT = max(1, int(os.getenv("POSITION_MANAGER_DAILY_ACTION_LIMIT", "2")))
POSITION_MANAGER_NEWS_WINDOW_HOURS = max(12, int(os.getenv("POSITION_MANAGER_NEWS_WINDOW_HOURS", "72")))
POSITION_MANAGER_MIN_CONF = max(0.4, min(0.95, float(os.getenv("POSITION_MANAGER_MIN_CONFIDENCE", "0.62"))))
POSITION_MANAGER_LLM_TIMEOUT_SEC = max(30, int(os.getenv("POSITION_MANAGER_LLM_TIMEOUT_SEC", "140")))
POSITION_MANAGER_CACHE_TTL_SEC = max(0, int(os.getenv("POSITION_MANAGER_LLM_CACHE_TTL_SEC", "120")))
POSITION_MANAGER_NOTIFY_TELEGRAM = os.getenv("POSITION_MANAGER_NOTIFY_TELEGRAM", "1") == "1"
POSITION_MANAGER_DEFAULT_TP = max(0.03, min(0.5, float(os.getenv("POSITION_MANAGER_DEFAULT_TP", "0.12"))))
POSITION_MANAGER_DEFAULT_SL = float(os.getenv("POSITION_MANAGER_DEFAULT_SL", "-0.06"))
if POSITION_MANAGER_DEFAULT_SL >= 0:
    POSITION_MANAGER_DEFAULT_SL = -abs(POSITION_MANAGER_DEFAULT_SL)
POSITION_MANAGER_MIN_ORDER_QTY = max(1, int(os.getenv("POSITION_MANAGER_MIN_ORDER_QTY", "1")))
POSITION_MANAGER_MARKET_VIEW_MAX_CHARS = max(120, int(os.getenv("POSITION_MANAGER_MARKET_VIEW_MAX_CHARS", "320")))

VALID_ACTIONS = {
    "HOLD",
    "REDUCE",
    "EXIT",
    "ADD",
    "TIGHTEN_STOP",
    "TAKE_PROFIT_PARTIAL",
    "NO_ACTION_REVIEW_LATER",
}
ACTION_PRIORITY = {
    "EXIT": 6,
    "REDUCE": 5,
    "TAKE_PROFIT_PARTIAL": 4,
    "TIGHTEN_STOP": 3,
    "ADD": 2,
    "HOLD": 1,
    "NO_ACTION_REVIEW_LATER": 1,
}
THESIS_STATUSES = {"maintain", "strengthen", "weaken", "invalidate"}
EVENT_HORIZON_SET = {"intraday", "1d", "1-3d", "1w", "1-2w", "2w+"}


@dataclass
class Holding:
    ticker: str
    ticker_name: str
    qty: int
    avg_price: float
    current_price: float
    pnl_rate: float
    eval_amt: float


@dataclass
class PositionContext:
    holding: Holding
    tech: dict[str, Any]
    flow: dict[str, Any]
    news: dict[str, Any]
    relation: dict[str, Any]
    prior_state: dict[str, Any]


@dataclass
class ActionPlan:
    ticker: str
    ticker_name: str
    action: str
    confidence: float
    size_change_pct: float
    thesis_status: str
    thesis_update: str
    reasoning: str
    invalidation: str
    time_horizon: str
    evidence_refs: list[str]
    risk_flags: list[str]
    next_review_trigger: str
    take_profit_pct: float
    stop_loss_pct: float
    block_codes: list[str]

def now_kst() -> datetime:
    if KST is None:
        return datetime.now()
    return datetime.now(KST)


def ts_now_str() -> str:
    return now_kst().strftime("%Y-%m-%d %H:%M:%S")


def to_float(v: Any, default: float = 0.0) -> float:
    try:
        if isinstance(v, str):
            v = v.replace(",", "")
        return float(v)
    except Exception:
        return default


def to_int(v: Any, default: int = 0) -> int:
    try:
        if isinstance(v, str):
            v = v.replace(",", "")
        return int(float(v))
    except Exception:
        return default


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(v)))


def is_ticker(v: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", v or ""))


def normalize_text(s: Any, max_len: int = 280) -> str:
    txt = str(s or "").replace("\n", " ").strip()
    if len(txt) > max_len:
        txt = txt[:max_len]
    return txt


def read_text_file(path: Path, max_chars: int = 12000) -> str:
    try:
        if not path.exists():
            return ""
        txt = path.read_text(encoding="utf-8")
        if len(txt) > max_chars:
            txt = txt[:max_chars]
        return txt.strip()
    except Exception:
        return ""


def normalize_text_list(v: Any, max_items: int = 6, max_len: int = 160) -> list[str]:
    raw = v if isinstance(v, list) else ([v] if isinstance(v, str) else [])
    out: list[str] = []
    seen: set[str] = set()
    for it in raw:
        s = normalize_text(it, max_len=max_len)
        if not s or s in seen:
            continue
        out.append(s)
        seen.add(s)
        if len(out) >= max_items:
            break
    return out


def load_execution_mode_state() -> dict[str, Any]:
    try:
        if EXECUTION_MODE_STATE_FILE.exists():
            raw = json.loads(EXECUTION_MODE_STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return raw
    except Exception:
        pass
    return {
        "execution_mode": "normal",
        "allowed_universe": "watchlist",
        "allowed_tickers": [],
        "avg_down_block": True,
        "sell_urgency": "normal",
        "llm_style": "stock_selection",
    }


def sql_quote(v: str) -> str:
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _build_ch_url() -> str:
    host = (CLICKHOUSE_HOST or "http://localhost:8123").strip()
    sp = urlsplit(host)
    scheme = sp.scheme or "http"
    netloc = sp.netloc or sp.path
    path = sp.path if sp.netloc else ""
    if "@" in netloc:
        netloc = netloc.split("@", 1)[1]
    pairs = parse_qsl(sp.query, keep_blank_values=True)
    if CLICKHOUSE_USER:
        pairs.append(("user", CLICKHOUSE_USER))
    if CLICKHOUSE_PASS:
        pairs.append(("password", CLICKHOUSE_PASS))
    query = urlencode(pairs, doseq=True)
    return urlunsplit((scheme, netloc, path or "", query, sp.fragment))


def _build_ch_query(sql: str) -> str:
    q = sql.strip().rstrip(";")
    upper = q.upper()
    if "FORMAT JSON" not in upper:
        if " SETTINGS " not in upper:
            q += (
                f"\nSETTINGS max_execution_time={CH_MAX_EXECUTION_TIME},"
                f" max_threads={CH_MAX_THREADS}"
            )
        q += "\nFORMAT JSON"
    return q


def ch_query(sql: str) -> list[dict[str, Any]]:
    url = _build_ch_url()
    q = _build_ch_query(sql)
    last_err: Exception | None = None
    for attempt in range(1, CH_QUERY_RETRIES + 2):
        try:
            resp = requests.post(url, data=q.encode("utf-8"), timeout=CH_QUERY_TIMEOUT_SEC)
            status = int(resp.status_code or 0)
            if status >= 500 and attempt <= CH_QUERY_RETRIES:
                time.sleep(CH_RETRY_BACKOFF_SEC * attempt)
                continue
            resp.raise_for_status()
            payload = resp.json()
            rows = payload.get("data", [])
            return rows if isinstance(rows, list) else []
        except Exception as e:
            last_err = e
            if attempt <= CH_QUERY_RETRIES:
                time.sleep(CH_RETRY_BACKOFF_SEC * attempt)
                continue
            break
    if last_err:
        print(f"[position_manager] CH query failed: {last_err}", file=sys.stderr)
    return []


def ch_execute(sql: str) -> bool:
    url = _build_ch_url()
    try:
        resp = requests.post(url, data=sql.encode("utf-8"), timeout=CH_QUERY_TIMEOUT_SEC)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[position_manager] CH execute failed: {e}", file=sys.stderr)
        return False


def ensure_feature_snapshot_view() -> None:
    sql = """
CREATE VIEW IF NOT EXISTS trading.v_feature_snapshot AS
SELECT
    ts,
    symbol,
    session,
    toFloat64(price) AS price,
    toFloat64(vwap) AS vwap,
    toFloat64(atr14) AS atr14,
    toFloat64(rsi14) AS rsi14,
    toFloat64(spread_bp) AS spread_bp,
    toFloat64(liquidity_krw) AS liquidity_krw,
    toFloat64(foreign_flow) AS foreign_flow,
    toFloat64(inst_flow) AS inst_flow,
    toFloat64(news_event_score) AS news_event_score,
    toFloat64(dart_event_score) AS dart_event_score,
    regime_label,
    toFloat64(foreign_flow) AS foreign_ownership_pct,
    toFloat64(news_event_score) AS foreign_net_flow,
    toFloat64(inst_flow) AS inst_net_flow
FROM trading.feature_snapshot
"""
    _ = ch_execute(sql)


def ch_insert_json_rows(table: str, rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return True
    url = _build_ch_url()
    lines = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
    sql = f"INSERT INTO {table} FORMAT JSONEachRow\n{lines}"
    try:
        resp = requests.post(url, data=sql.encode("utf-8"), timeout=max(CH_QUERY_TIMEOUT_SEC, 30))
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[position_manager] CH insert failed({table}): {e}", file=sys.stderr)
        return False


def ensure_review_tables() -> None:
    ddl_run = """
    CREATE TABLE IF NOT EXISTS trading.position_review_run
    (
        review_id String,
        review_time DateTime,
        mode LowCardinality(String),
        holdings_count UInt16,
        llm_status LowCardinality(String),
        market_regime String,
        market_summary String,
        proposed_actions UInt16,
        executable_orders UInt16,
        dry_run UInt8,
        response_path String,
        created_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY (review_time, review_id)
    """
    ddl_action = """
    CREATE TABLE IF NOT EXISTS trading.position_review_action
    (
        review_id String,
        review_time DateTime,
        ticker String,
        ticker_name String,
        action LowCardinality(String),
        size_change_pct Float32,
        confidence Float32,
        thesis_status LowCardinality(String),
        reasoning String,
        invalidation String,
        time_horizon LowCardinality(String),
        evidence_refs Array(String),
        risk_flags Array(String),
        block_codes Array(String),
        order_action String,
        order_qty UInt32,
        created_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY (review_time, review_id, ticker)
    """
    _ = ch_execute(ddl_run)
    _ = ch_execute(ddl_action)


def mcporter_call(tool: str, args: str = "") -> dict[str, Any] | None:
    cmd = f"kis-trading.{tool}"
    if args:
        cmd += f"({args})"
    try:
        res = subprocess.run(
            [MCPORTER_BIN, "--config", MCP_CONFIG, "call", cmd, "--output", "json"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if res.returncode != 0:
            return None
        data = json.loads(res.stdout)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def load_balance() -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    bal = mcporter_call("inquery-balance")
    if not isinstance(bal, dict):
        return {}, {}, []
    summary = bal.get("output2", {})
    if isinstance(summary, list):
        summary = summary[0] if summary else {}
    if not isinstance(summary, dict):
        summary = {}
    holdings = bal.get("output1", [])
    if not isinstance(holdings, list):
        holdings = []
    return bal, summary, holdings


def parse_holdings(rows: list[dict[str, Any]]) -> dict[str, Holding]:
    out: dict[str, Holding] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("pdno", row.get("ticker", "")) or "").strip()
        if not is_ticker(ticker):
            continue
        qty = to_int(row.get("hldg_qty", row.get("quantity", 0)), 0)
        if qty <= 0:
            continue
        out[ticker] = Holding(
            ticker=ticker,
            ticker_name=normalize_text(row.get("prdt_name", row.get("name", "")), 64),
            qty=qty,
            avg_price=to_float(row.get("pchs_avg_pric", row.get("avg_price", 0)), 0.0),
            current_price=to_float(row.get("prpr", row.get("current_price", 0)), 0.0),
            pnl_rate=to_float(row.get("evlu_pfls_rt", row.get("pnl_rate", 0)), 0.0),
            eval_amt=to_float(row.get("evlu_amt", row.get("eval_amount", 0)), 0.0),
        )
    return out


def ticker_in_clause(tickers: list[str]) -> str:
    uniq = [t for t in sorted(set(tickers)) if is_ticker(t)]
    if not uniq:
        return "('000000')"
    return "(" + ", ".join(sql_quote(t) for t in uniq) + ")"


def ticker_array_literal(tickers: list[str]) -> str:
    uniq = [t for t in sorted(set(tickers)) if is_ticker(t)]
    if not uniq:
        return "['000000']"
    return "[" + ", ".join(sql_quote(t) for t in uniq) + "]"


def load_market_regime() -> dict[str, Any]:
    q = """
    SELECT
      toString(date) AS date,
      regime_label,
      trend,
      volatility,
      risk_appetite,
      ifNull(action_posture, 'normal') AS action_posture,
      ifNull(arrayStringConcat(stress_flags, ', '), '') AS stress_flags,
      ifNull(guide_text, '') AS guide_text,
      summary,
      kospi_close,
      kosdaq_close,
      usdkrw,
      vix_level,
      dxy_level
    FROM trading.market_regime
    ORDER BY date DESC, updated_at DESC
    LIMIT 1
    """
    rows = ch_query(q)
    return rows[0] if rows else {}


def load_technical(tickers: list[str]) -> dict[str, dict[str, Any]]:
    q = f"""
    SELECT
      ticker,
      argMax(ticker_name, date) AS ticker_name,
      argMax(close_price, date) AS close_price,
      argMax(change_pct, date) AS change_pct,
      argMax(rsi14, date) AS rsi14,
      argMax(ma20, date) AS ma20,
      argMax(ma60, date) AS ma60,
      argMax(signal, date) AS signal,
      argMax(signal_score, date) AS signal_score,
      argMax(vol_ratio, date) AS vol_ratio,
      argMax(bb_pct, date) AS bb_pct,
      max(date) AS asof_date
    FROM trading.technical_signals
    WHERE ticker IN {ticker_in_clause(tickers)}
    GROUP BY ticker
    """
    rows = ch_query(q)
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        t = str(r.get("ticker", "") or "")
        if is_ticker(t):
            out[t] = r
    return out


def load_flow_snapshot(tickers: list[str]) -> dict[str, dict[str, Any]]:
    q = f"""
    SELECT
      symbol AS ticker,
      argMax(price, ts) AS price,
      argMax(liquidity_krw, ts) AS liquidity_krw,
      argMax(foreign_ownership_pct, ts) AS foreign_ownership,
      argMax(foreign_net_flow, ts) AS foreign_net_flow,
      argMax(inst_net_flow, ts) AS inst_net_flow,
      max(ts) AS asof_ts
    FROM trading.v_feature_snapshot
    WHERE ts >= now() - INTERVAL 2 DAY
      AND symbol IN {ticker_in_clause(tickers)}
    GROUP BY symbol
    """
    rows = ch_query(q)
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        t = str(r.get("ticker", "") or "")
        if is_ticker(t):
            out[t] = r
    return out


def load_relation_signals(tickers: list[str]) -> dict[str, dict[str, Any]]:
    q = f"""
    WITH latest_rel AS (SELECT max(asof_ts) AS ts FROM trading.hidden_relation_signals)
    SELECT
      ticker,
      total_relation_score,
      relation_bias,
      support_events,
      support_clusters,
      arrayStringConcat(source_tickers, ', ') AS source_tickers,
      arrayStringConcat(top_channels, ', ') AS top_channels
    FROM trading.hidden_relation_signals
    WHERE asof_ts = (SELECT ts FROM latest_rel)
      AND ticker IN {ticker_in_clause(tickers)}
    """
    rows = ch_query(q)
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        t = str(r.get("ticker", "") or "")
        if is_ticker(t):
            out[t] = r
    return out


def load_news_context(tickers: list[str], hours: int) -> dict[str, dict[str, Any]]:
    ticker_arr = ticker_array_literal(tickers)
    q_news = f"""
    SELECT
      arrayJoin(tickers) AS ticker,
      title,
      source_url,
      sentiment,
      importance,
      published_at
    FROM trading.news
    WHERE published_at >= now() - INTERVAL {int(hours)} HOUR
      AND hasAny(tickers, {ticker_arr})
    ORDER BY published_at DESC
    LIMIT 8000
    """
    q_frames = f"""
    SELECT
      arrayJoin(tickers) AS ticker,
      countIf(relevant=1 AND thesis_path!='' AND evidence_json!='[]') AS explain_ready
    FROM trading.news_event_frames
    WHERE published_at >= now() - INTERVAL {int(hours)} HOUR
      AND hasAny(tickers, {ticker_arr})
    GROUP BY ticker
    """
    news_rows = ch_query(q_news)
    frame_rows = ch_query(q_frames)

    out: dict[str, dict[str, Any]] = {
        t: {
            "ticker": t,
            "news_cnt": 0,
            "pos_cnt": 0,
            "neg_cnt": 0,
            "max_importance": 0,
            "explain_ready": 0,
            "top_titles": [],
            "top_urls": [],
        }
        for t in tickers
        if is_ticker(t)
    }

    top_candidates: dict[str, list[tuple[int, str, str]]] = {t: [] for t in out.keys()}
    for r in news_rows:
        t = str(r.get("ticker", "") or "")
        if t not in out:
            continue
        out[t]["news_cnt"] = int(out[t]["news_cnt"]) + 1
        sent = str(r.get("sentiment", "") or "").lower().strip()
        if sent == "positive":
            out[t]["pos_cnt"] = int(out[t]["pos_cnt"]) + 1
        elif sent == "negative":
            out[t]["neg_cnt"] = int(out[t]["neg_cnt"]) + 1
        imp = to_int(r.get("importance", 0), 0)
        if imp > to_int(out[t]["max_importance"], 0):
            out[t]["max_importance"] = imp
        title = normalize_text(r.get("title", ""), 160)
        url = normalize_text(r.get("source_url", ""), 280)
        if title or url:
            top_candidates[t].append((imp, title, url))

    for t, vals in top_candidates.items():
        vals.sort(key=lambda x: x[0], reverse=True)
        out[t]["top_titles"] = [x[1] for x in vals[:3]]
        out[t]["top_urls"] = [x[2] for x in vals[:3]]

    for r in frame_rows:
        t = str(r.get("ticker", "") or "")
        if t in out:
            out[t]["explain_ready"] = to_int(r.get("explain_ready", 0), 0)
    return out


def load_dynamic_exit_state() -> dict[str, dict[str, Any]]:
    if not DYNAMIC_EXIT_STATE_FILE.exists():
        return {}
    try:
        raw = json.loads(DYNAMIC_EXIT_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    positions = raw.get("positions", {})
    return positions if isinstance(positions, dict) else {}


def load_position_state() -> dict[str, Any]:
    base = {
        "updated_at": "",
        "positions": {},
        "daily_action_count": {},
    }
    try:
        if POSITION_STATE_FILE.exists():
            raw = json.loads(POSITION_STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                if isinstance(raw.get("positions"), dict):
                    base["positions"] = raw["positions"]
                if isinstance(raw.get("daily_action_count"), dict):
                    base["daily_action_count"] = raw["daily_action_count"]
                base["updated_at"] = str(raw.get("updated_at", "") or "")
    except Exception:
        pass
    return base


def save_position_state(state: dict[str, Any]) -> None:
    POSITION_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = ts_now_str()
    POSITION_STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _extract_json_obj(raw: str) -> dict[str, Any] | None:
    txt = str(raw or "").strip()
    if not txt:
        return None
    try:
        obj = json.loads(txt)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    m = re.search(r"```json\s*(\{.*?\})\s*```", txt, re.S)
    if m:
        try:
            obj = json.loads(m.group(1))
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    m2 = re.search(r"\{.*\}", txt, re.S)
    if m2:
        try:
            obj = json.loads(m2.group(0))
            if isinstance(obj, dict):
                return obj
        except Exception:
            return None
    return None


def _safe_pair(tp_raw: Any, sl_raw: Any) -> tuple[float, float] | None:
    tp = to_float(tp_raw, 0.0)
    sl = to_float(sl_raw, 0.0)
    if tp <= 0.0 or tp > 0.6:
        return None
    if sl >= 0.0 or abs(sl) > 0.6:
        return None
    return round(tp, 6), round(sl, 6)


def format_regime_line(regime: dict[str, Any]) -> str:
    if not regime:
        return "regime unknown"
    s = normalize_text(regime.get("summary", ""), POSITION_MANAGER_MARKET_VIEW_MAX_CHARS)
    if s:
        posture = normalize_text(regime.get("action_posture", ""), 24)
        flags = normalize_text(regime.get("stress_flags", ""), 120)
        if posture:
            if flags:
                return f"{s} | posture={posture} | flags={flags}"
            return f"{s} | posture={posture}"
        return s
    return (
        f"{regime.get('regime_label', 'UNKNOWN')} | trend={regime.get('trend', '?')} | "
        f"vol={regime.get('volatility', '?')} | risk={regime.get('risk_appetite', '?')} | "
        f"posture={regime.get('action_posture', 'normal')} | "
        f"flags={regime.get('stress_flags', '-') or '-'}"
    )


def build_position_contexts(
    holdings: dict[str, Holding],
    tech: dict[str, dict[str, Any]],
    flow: dict[str, dict[str, Any]],
    news: dict[str, dict[str, Any]],
    relation: dict[str, dict[str, Any]],
    state: dict[str, Any],
) -> list[PositionContext]:
    pos_state = state.get("positions", {}) if isinstance(state.get("positions"), dict) else {}
    out: list[PositionContext] = []
    for ticker, h in holdings.items():
        t = dict(tech.get(ticker, {}) or {})
        f = dict(flow.get(ticker, {}) or {})
        n = dict(news.get(ticker, {}) or {})
        r = dict(relation.get(ticker, {}) or {})
        p = dict(pos_state.get(ticker, {}) or {})
        if h.current_price <= 0:
            h.current_price = to_float(t.get("close_price", 0), 0.0) or to_float(f.get("price", 0), 0.0)
        if not h.ticker_name:
            h.ticker_name = normalize_text(t.get("ticker_name", ""), 64)
        out.append(PositionContext(holding=h, tech=t, flow=f, news=n, relation=r, prior_state=p))
    return out


def build_llm_prompt(
    review_id: str,
    contexts: list[PositionContext],
    regime: dict[str, Any],
    dynamic_exits: dict[str, dict[str, Any]],
    max_actions: int,
    allow_add: bool,
    event_block: str,
    engine_context: str,
    persistent_memory: str,
    heartbeat_text: str,
    soul_text: str,
) -> str:
    slim_positions: list[dict[str, Any]] = []
    for c in contexts:
        n_titles = c.news.get("top_titles", []) if isinstance(c.news.get("top_titles"), list) else []
        n_urls = c.news.get("top_urls", []) if isinstance(c.news.get("top_urls"), list) else []
        news_items = []
        for i in range(min(3, max(len(n_titles), len(n_urls)))):
            title = normalize_text(n_titles[i] if i < len(n_titles) else "", 140)
            url = normalize_text(n_urls[i] if i < len(n_urls) else "", 260)
            if title or url:
                news_items.append({"title": title, "url": url})
        exit_state = dynamic_exits.get(c.holding.ticker, {}) if isinstance(dynamic_exits.get(c.holding.ticker), dict) else {}
        slim_positions.append(
            {
                "ticker": c.holding.ticker,
                "ticker_name": c.holding.ticker_name,
                "qty": c.holding.qty,
                "avg_price": round(c.holding.avg_price, 2),
                "current_price": round(c.holding.current_price, 2),
                "pnl_rate_pct": round(c.holding.pnl_rate, 3),
                "eval_amt_krw": int(c.holding.eval_amt),
                "technical": {
                    "signal": str(c.tech.get("signal", "")),
                    "signal_score": round(to_float(c.tech.get("signal_score", 0), 0.0), 3),
                    "rsi14": round(to_float(c.tech.get("rsi14", 0), 0.0), 3),
                    "change_pct": round(to_float(c.tech.get("change_pct", 0), 0.0), 3),
                    "ma20": round(to_float(c.tech.get("ma20", 0), 0.0), 3),
                    "ma60": round(to_float(c.tech.get("ma60", 0), 0.0), 3),
                    "vol_ratio": round(to_float(c.tech.get("vol_ratio", 0), 0.0), 3),
                    "bb_pct": round(to_float(c.tech.get("bb_pct", 0), 0.0), 3),
                },
                "flow": {
                    "foreign_ownership_pct": round(to_float(c.flow.get("foreign_ownership", 0), 0.0), 3),
                    "foreign_net_flow": round(to_float(c.flow.get("foreign_net_flow", 0), 0.0), 3),
                    "inst_net_flow": round(to_float(c.flow.get("inst_net_flow", 0), 0.0), 3),
                    # Legacy aliases for older prompt parsers
                    "foreign_flow": round(to_float(c.flow.get("foreign_ownership", 0), 0.0), 3),
                    "inst_flow": round(to_float(c.flow.get("inst_net_flow", 0), 0.0), 3),
                    "liquidity_krw": int(to_float(c.flow.get("liquidity_krw", 0), 0.0)),
                },
                "news": {
                    "news_cnt": int(to_int(c.news.get("news_cnt", 0), 0)),
                    "pos_cnt": int(to_int(c.news.get("pos_cnt", 0), 0)),
                    "neg_cnt": int(to_int(c.news.get("neg_cnt", 0), 0)),
                    "explain_ready": int(to_int(c.news.get("explain_ready", 0), 0)),
                    "max_importance": int(to_int(c.news.get("max_importance", 0), 0)),
                    "top_news": news_items,
                },
                "relation": {
                    "score": round(to_float(c.relation.get("total_relation_score", 0), 0.0), 4),
                    "bias": str(c.relation.get("relation_bias", "neutral")),
                    "support_events": int(to_int(c.relation.get("support_events", 0), 0)),
                    "support_clusters": int(to_int(c.relation.get("support_clusters", 0), 0)),
                    "source_tickers": normalize_text(c.relation.get("source_tickers", ""), 120),
                    "top_channels": normalize_text(c.relation.get("top_channels", ""), 120),
                },
                "prior_state": {
                    "entry_thesis": normalize_text(c.prior_state.get("entry_thesis", ""), 180),
                    "last_action": str(c.prior_state.get("last_action", "")),
                    "last_action_at": str(c.prior_state.get("last_action_at", "")),
                    "cooldown_until": str(c.prior_state.get("cooldown_until", "")),
                    "last_confidence": round(to_float(c.prior_state.get("last_confidence", 0), 0.0), 3),
                },
                "dynamic_exit": {
                    "take_profit_pct": round(to_float(exit_state.get("take_profit_pct", 0), 0.0), 4),
                    "stop_loss_pct": round(to_float(exit_state.get("stop_loss_pct", 0), 0.0), 4),
                    "updated_at": str(exit_state.get("updated_at", "")),
                },
            }
        )

    policy = {
        "max_non_hold_actions": int(max_actions),
        "allowed_actions": sorted(VALID_ACTIONS),
        "allow_add": bool(allow_add),
        "min_confidence": round(POSITION_MANAGER_MIN_CONF, 4),
        "cooldown_min": int(POSITION_MANAGER_COOLDOWN_MIN),
        "daily_action_limit_per_ticker": int(POSITION_MANAGER_DAILY_ACTION_LIMIT),
        "block_reentry_same_day_after_exit": bool(POSITION_MANAGER_BLOCK_REENTRY_SAME_DAY),
        "do_not_overtrade": True,
        "flip_flop_forbidden": True,
        "prefer_risk_reduction_when_uncertain": True,
        "units": {
            "size_change_pct": "-1.0~+1.0",
            "take_profit_pct": "0~0.6",
            "stop_loss_pct": "-0.6~0",
        },
    }

    return (
        "너는 한국 주식 보유포지션 매니저다. 신규 테마 발굴이 아니라 현재 보유종목의 동적 관리가 목적이다.\n"
        "행동 원칙:\n"
        "1) 불확실하면 HOLD 또는 리스크 축소(REDUCE/TIGHTEN_STOP)\n"
        "2) 근거 없이 잦은 방향 전환 금지\n"
        "3) 액션 이유는 반드시 데이터 근거(뉴스/기술/수급/레짐)로 설명\n"
        "4) size_change_pct는 포지션 대비 변화 비율\n"
        "5) evidence_refs는 반드시 입력 데이터에서 관측 가능한 근거만 사용(가짜 근거 생성 금지)\n"
        "6) allow_add=false면 ADD 제안 금지, EXIT 후 당일 재진입(ADD) 금지\n"
        "7) cooldown/daily_action_limit을 우선 고려하고 필요 시 NO_ACTION_REVIEW_LATER 사용\n"
        "8) take_profit_pct는 양수, stop_loss_pct는 음수로 유지\n"
        "9) JSON 스키마를 정확히 준수\n\n"
        f"[ENGINE_CONTEXT]\n{engine_context}\n\n"
        f"[SYSTEM_EVENT]\n{event_block}\n\n"
        f"[PERSISTENT_MEMORY]\n{persistent_memory}\n\n"
        f"[HEARTBEAT]\n{heartbeat_text}\n\n"
        f"[SOUL]\n{soul_text}\n\n"
        f"[REVIEW_ID]\n{review_id}\n\n"
        f"[MARKET_REGIME]\n{json.dumps(regime, ensure_ascii=False)}\n\n"
        f"[POLICY]\n{json.dumps(policy, ensure_ascii=False)}\n\n"
        f"[POSITIONS]\n{json.dumps(slim_positions, ensure_ascii=False, indent=2)}\n"
    )


def run_llm_review(prompt: str) -> tuple[dict[str, Any] | None, str]:
    if not POSITION_SCHEMA_FILE.exists():
        return None, "schema_missing"
    try:
        raw = run_codex_cached(
            prompt=prompt,
            codex_bin=os.getenv("CODEX_BIN", "openclaw"),
            model=resolve_model("POSITION_MANAGER_MODEL", "CODEX_MODEL"),
            workdir=str(SCRIPT_DIR),
            timeout_sec=POSITION_MANAGER_LLM_TIMEOUT_SEC,
            base_args=[],
            output_schema_path=str(POSITION_SCHEMA_FILE),
            cache_dir=os.getenv("CODEX_EXEC_CACHE_DIR", str(HOME / ".openclaw" / "cache" / "codex-exec")),
            cache_ttl_sec=POSITION_MANAGER_CACHE_TTL_SEC,
        )
    except Exception as e:
        return None, f"llm_call_failed:{type(e).__name__}:{e}"
    obj = _extract_json_obj(raw)
    if not isinstance(obj, dict):
        return None, "llm_parse_failed"
    return obj, "ok"


def fallback_actions(contexts: list[PositionContext], execution_mode: str = "normal") -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    mode_name = str(execution_mode or "normal").strip().lower()
    for c in contexts:
        pnl = c.holding.pnl_rate
        rsi = to_float(c.tech.get("rsi14", 50), 50.0)
        action = "HOLD"
        conf = 0.6
        size = 0.0
        reason = "fallback_hold"
        thesis_status = "maintain"
        exit_cut = -7.0
        reduce_cut = -4.0
        reduce_rsi = 38.0
        take_profit = 12.0
        take_profit_rsi = 66.0
        reduce_size = -0.35
        take_profit_size = -0.40
        if mode_name == "shock":
            exit_cut = -5.0
            reduce_cut = -3.0
            reduce_rsi = 45.0
            take_profit = 8.0
            take_profit_rsi = 62.0
            reduce_size = -0.50
            take_profit_size = -0.50
        elif mode_name == "recovery":
            exit_cut = -6.0
            reduce_cut = -3.5
            reduce_rsi = 40.0
            take_profit = 10.0
            take_profit_rsi = 64.0
            reduce_size = -0.40
            take_profit_size = -0.45
        if pnl <= exit_cut:
            action = "EXIT"
            conf = 0.96
            size = -1.0
            reason = f"fallback_risk_cut:{mode_name}: pnl<={exit_cut:.1f}%"
            thesis_status = "invalidate"
        elif pnl <= reduce_cut and rsi < reduce_rsi:
            action = "REDUCE"
            conf = 0.82
            size = reduce_size
            reason = f"fallback_reduce:{mode_name}: drawdown+rsi_weak"
            thesis_status = "weaken"
        elif pnl >= take_profit and rsi >= take_profit_rsi:
            action = "TAKE_PROFIT_PARTIAL"
            conf = 0.84
            size = take_profit_size
            reason = f"fallback_take_profit:{mode_name}: pnl_high+rsi_hot"
            thesis_status = "weaken"
        out.append(
            {
                "ticker": c.holding.ticker,
                "action": action,
                "confidence": conf,
                "size_change_pct": size,
                "thesis_status": thesis_status,
                "thesis_update": reason,
                "reasoning": reason,
                "invalidation": "fallback_risk_guard",
                "time_horizon": "intraday",
                "evidence_refs": [f"pnl={pnl:.2f}%", f"rsi14={rsi:.1f}"],
                "risk_flags": ["llm_unavailable"],
                "next_review_trigger": "market_update",
                "take_profit_pct": 0.0,
                "stop_loss_pct": 0.0,
            }
        )
    return out


def parse_action_items(llm_obj: dict[str, Any], contexts: list[PositionContext]) -> list[dict[str, Any]]:
    items = llm_obj.get("positions", [])
    if not isinstance(items, list):
        return []
    holding_set = {c.holding.ticker for c in contexts}
    out: list[dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        ticker = str(it.get("ticker", "") or "").strip()
        if ticker not in holding_set:
            continue
        action = str(it.get("action", "HOLD") or "HOLD").upper().strip()
        if action not in VALID_ACTIONS:
            action = "HOLD"
        out.append(
            {
                "ticker": ticker,
                "action": action,
                "confidence": clamp(to_float(it.get("confidence", 0.6), 0.6), 0.0, 1.0),
                "size_change_pct": clamp(to_float(it.get("size_change_pct", 0.0), 0.0), -1.0, 1.0),
                "thesis_status": str(it.get("thesis_status", "maintain") or "maintain").lower().strip(),
                "thesis_update": normalize_text(it.get("thesis_update", ""), 240),
                "reasoning": normalize_text(it.get("reasoning", ""), 280),
                "invalidation": normalize_text(it.get("invalidation", ""), 220),
                "time_horizon": str(it.get("time_horizon", "1-3d") or "1-3d").lower().strip(),
                "evidence_refs": normalize_text_list(it.get("evidence_refs", []), max_items=6, max_len=140),
                "risk_flags": normalize_text_list(it.get("risk_flags", []), max_items=6, max_len=80),
                "next_review_trigger": normalize_text(it.get("next_review_trigger", ""), 160),
                "take_profit_pct": to_float(it.get("take_profit_pct", 0.0), 0.0),
                "stop_loss_pct": to_float(it.get("stop_loss_pct", 0.0), 0.0),
            }
        )
    return out


def normalize_action_plans(
    raw_items: list[dict[str, Any]],
    contexts: list[PositionContext],
    state: dict[str, Any],
    dynamic_exits: dict[str, dict[str, Any]],
    max_actions: int,
    allow_add: bool,
    execution_mode: str,
) -> list[ActionPlan]:
    by_ticker = {c.holding.ticker: c for c in contexts}
    today = now_kst().strftime("%Y-%m-%d")
    daily_count = state.get("daily_action_count", {}) if isinstance(state.get("daily_action_count"), dict) else {}

    plans: list[ActionPlan] = []
    mode_name = str(execution_mode or "normal").strip().lower()
    for ticker, c in by_ticker.items():
        src = next((x for x in raw_items if x.get("ticker") == ticker), None)
        if not src:
            src = {
                "ticker": ticker,
                "action": "HOLD",
                "confidence": 0.55,
                "size_change_pct": 0.0,
                "thesis_status": "maintain",
                "thesis_update": "",
                "reasoning": "no_llm_item_hold",
                "invalidation": "",
                "time_horizon": "1-3d",
                "evidence_refs": ["auto_hold"],
                "risk_flags": [],
                "next_review_trigger": "next_cycle",
                "take_profit_pct": 0.0,
                "stop_loss_pct": 0.0,
            }

        action = str(src.get("action", "HOLD") or "HOLD").upper().strip()
        if action not in VALID_ACTIONS:
            action = "HOLD"
        conf = clamp(to_float(src.get("confidence", 0.55), 0.55), 0.0, 1.0)
        size = clamp(to_float(src.get("size_change_pct", 0.0), 0.0), -1.0, 1.0)
        thesis_status = str(src.get("thesis_status", "maintain") or "maintain").lower().strip()
        if thesis_status not in THESIS_STATUSES:
            thesis_status = "maintain"
        reason = normalize_text(src.get("reasoning", ""), 280)
        if not reason:
            reason = "position_review"
        invalidation = normalize_text(src.get("invalidation", ""), 220)
        horizon = str(src.get("time_horizon", "1-3d") or "1-3d").strip().lower()
        if horizon and horizon not in EVENT_HORIZON_SET:
            horizon = "1-3d"

        refs = normalize_text_list(src.get("evidence_refs", []), max_items=6, max_len=140)
        if not refs:
            refs = [f"pnl={c.holding.pnl_rate:.2f}%", f"rsi14={to_float(c.tech.get('rsi14',0),0.0):.1f}"]
        risk_flags = normalize_text_list(src.get("risk_flags", []), max_items=6, max_len=80)
        next_trigger = normalize_text(src.get("next_review_trigger", "next_cycle"), 160) or "next_cycle"
        thesis_update = normalize_text(src.get("thesis_update", ""), 240)

        block_codes: list[str] = []

        if conf < POSITION_MANAGER_MIN_CONF and action in {"ADD", "REDUCE", "EXIT", "TAKE_PROFIT_PARTIAL"}:
            block_codes.append("LOW_CONFIDENCE")
            action = "HOLD"

        if action == "ADD" and not allow_add:
            block_codes.append("ADD_DISABLED")
            action = "HOLD"
        if mode_name == "close_only" and action not in {"EXIT", "REDUCE", "TIGHTEN_STOP", "HOLD", "TAKE_PROFIT_PARTIAL"}:
            block_codes.append("CLOSE_ONLY_ACTION_BLOCK")
            action = "HOLD"
        if mode_name == "shock":
            if action == "ADD":
                block_codes.append("SHOCK_MODE_ADD_BLOCK")
                action = "HOLD"
            elif action == "TAKE_PROFIT_PARTIAL":
                block_codes.append("SHOCK_MODE_PARTIAL_REMAP")
                action = "REDUCE"
                if size >= 0:
                    size = -0.5
        if mode_name == "recovery" and action == "ADD":
            if c.holding.pnl_rate < 0 or thesis_status != "strengthen":
                block_codes.append("RECOVERY_ADD_GATED")
                action = "HOLD"

        ps = c.prior_state if isinstance(c.prior_state, dict) else {}
        cooldown_until = str(ps.get("cooldown_until", "") or "").strip()
        cooldown_dt = None
        if cooldown_until:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    cooldown_dt = datetime.strptime(cooldown_until[:19], fmt)
                    break
                except Exception:
                    continue
        if cooldown_dt and now_kst().replace(tzinfo=None) < cooldown_dt and action in {"ADD", "REDUCE", "EXIT", "TAKE_PROFIT_PARTIAL"}:
            block_codes.append("COOLDOWN_ACTIVE")
            action = "HOLD"

        key = f"{today}:{ticker}"
        day_cnt = to_int(daily_count.get(key, 0), 0)
        if day_cnt >= POSITION_MANAGER_DAILY_ACTION_LIMIT and action in {"ADD", "REDUCE", "EXIT", "TAKE_PROFIT_PARTIAL"}:
            block_codes.append("DAILY_ACTION_LIMIT")
            action = "HOLD"

        last_action = str(ps.get("last_action", "") or "").strip().upper()
        last_action_at = str(ps.get("last_action_at", "") or "").strip()
        if (
            POSITION_MANAGER_BLOCK_REENTRY_SAME_DAY
            and action == "ADD"
            and last_action == "EXIT"
            and last_action_at
        ):
            last_action_date = ""
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    last_dt = datetime.strptime(last_action_at[:19], fmt)
                    last_action_date = last_dt.strftime("%Y-%m-%d")
                    break
                except Exception:
                    continue
            if last_action_date == today:
                block_codes.append("NO_REENTRY_SAME_DAY")
                action = "HOLD"

        if action == "EXIT":
            size = -1.0
        elif action in {"REDUCE", "TAKE_PROFIT_PARTIAL"}:
            if size >= 0:
                size = -0.35 if action == "REDUCE" else -0.40
            size = -abs(clamp(size, -1.0, -0.1))
        elif action == "ADD":
            if size <= 0:
                size = 0.2
            size = clamp(size, 0.05, 0.5)
        else:
            size = 0.0

        tp = to_float(src.get("take_profit_pct", 0.0), 0.0)
        sl = to_float(src.get("stop_loss_pct", 0.0), 0.0)
        pair = _safe_pair(tp, sl)
        if pair is None:
            ex = dynamic_exits.get(ticker, {}) if isinstance(dynamic_exits.get(ticker), dict) else {}
            ex_tp = to_float(ex.get("take_profit_pct", 0.0), 0.0)
            ex_sl = to_float(ex.get("stop_loss_pct", 0.0), 0.0)
            if action == "TIGHTEN_STOP" and ex_sl < 0:
                sl = max(ex_sl, -0.03)
                tp = ex_tp if ex_tp > 0 else POSITION_MANAGER_DEFAULT_TP
                pair = _safe_pair(tp, sl)
            elif ex_tp > 0 and ex_sl < 0:
                pair = _safe_pair(ex_tp, ex_sl)
            else:
                pair = _safe_pair(POSITION_MANAGER_DEFAULT_TP, POSITION_MANAGER_DEFAULT_SL)
        if pair is None:
            pair = (POSITION_MANAGER_DEFAULT_TP, POSITION_MANAGER_DEFAULT_SL)

        plans.append(
            ActionPlan(
                ticker=ticker,
                ticker_name=c.holding.ticker_name,
                action=action,
                confidence=round(conf, 4),
                size_change_pct=round(size, 4),
                thesis_status=thesis_status,
                thesis_update=thesis_update,
                reasoning=reason,
                invalidation=invalidation,
                time_horizon=horizon,
                evidence_refs=refs,
                risk_flags=risk_flags,
                next_review_trigger=next_trigger,
                take_profit_pct=pair[0],
                stop_loss_pct=pair[1],
                block_codes=block_codes,
            )
        )

    ranked = sorted(
        plans,
        key=lambda p: (ACTION_PRIORITY.get(p.action, 0), p.confidence),
        reverse=True,
    )
    action_budget = max_actions
    kept: list[ActionPlan] = []
    for p in ranked:
        actionable = p.action in {"ADD", "REDUCE", "EXIT", "TAKE_PROFIT_PARTIAL"}
        if actionable and action_budget <= 0:
            p.block_codes.append("MAX_ACTIONS_EXCEEDED")
            p.action = "HOLD"
            p.size_change_pct = 0.0
            kept.append(p)
            continue
        kept.append(p)
        if actionable:
            action_budget -= 1

    # restore ticker order for stable output
    torder = {t: i for i, t in enumerate(by_ticker.keys())}
    kept.sort(key=lambda p: torder.get(p.ticker, 99999))
    return kept


def build_orders_and_targets(
    plans: list[ActionPlan],
    contexts: list[PositionContext],
    cash_krw: float,
    allow_add: bool,
    execution_mode: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    ctx_map = {c.holding.ticker: c for c in contexts}
    orders: list[dict[str, Any]] = []
    risk_targets: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []

    for p in plans:
        c = ctx_map.get(p.ticker)
        if not c:
            continue
        hold = c.holding

        order_action = ""
        qty = 0
        price_ref = hold.current_price if hold.current_price > 0 else to_float(c.tech.get("close_price", 0), 0.0)

        if p.action == "EXIT":
            order_action = "SELL"
            qty = hold.qty
        elif p.action in {"REDUCE", "TAKE_PROFIT_PARTIAL"}:
            order_action = "SELL"
            ratio = abs(p.size_change_pct) if abs(p.size_change_pct) > 0 else (0.35 if p.action == "REDUCE" else 0.40)
            qty = max(POSITION_MANAGER_MIN_ORDER_QTY, int(round(hold.qty * ratio)))
            qty = min(qty, hold.qty)
        elif p.action == "ADD" and allow_add:
            order_action = "BUY"
            ratio = max(0.05, p.size_change_pct if p.size_change_pct > 0 else 0.2)
            if price_ref > 0:
                base_notional = hold.eval_amt if hold.eval_amt > 0 else price_ref * hold.qty
                add_notional = min(base_notional * ratio, max(cash_krw * 0.25, 0.0))
                qty = max(POSITION_MANAGER_MIN_ORDER_QTY, int(add_notional // price_ref))
            else:
                qty = max(POSITION_MANAGER_MIN_ORDER_QTY, int(round(hold.qty * ratio)))

        strategy_family = "core_defensive"
        if order_action == "BUY":
            mode_name = str(execution_mode.get("execution_mode", "normal") or "normal")
            if mode_name == "shock":
                strategy_family = "shock_hedge"
            elif mode_name == "recovery":
                strategy_family = "shock_rebound"
            else:
                strategy_family = "stock_selection"
        elif order_action == "SELL":
            strategy_family = "forced_exit" if p.action == "EXIT" else "core_defensive"

        if order_action and qty > 0:
            orders.append(
                {
                    "action": order_action,
                    "ticker": p.ticker,
                    "ticker_name": p.ticker_name,
                    "quantity": int(qty),
                    "order_type": "LIMIT",
                    "price_type": "askp1" if order_action == "BUY" else "bidp1",
                    "price": 0,
                    "confidence": round(clamp(p.confidence, 0.0, 1.0), 4),
                    "strategy_family": strategy_family,
                    "playbook_id": f"pm_{str(execution_mode.get('execution_mode', 'normal') or 'normal')}",
                    "priority": 9 if p.action == "EXIT" else (7 if p.action == "REDUCE" else 5),
                    "order_role": "forced_exit" if p.action == "EXIT" else ("reduce" if p.action in {"REDUCE", "TAKE_PROFIT_PARTIAL"} else "new_entry"),
                    "close_only": str(execution_mode.get("execution_mode", "normal") or "normal") == "close_only",
                    "expected_holding_window": p.time_horizon,
                    "venue_preference": "SOR",
                    "reasoning": normalize_text(f"position_manager:{p.action} | {p.reasoning}", 280),
                    "event_type": "position_manager",
                    "event_signature": f"pm:{p.action}:{p.ticker}:{now_kst().strftime('%Y%m%d%H%M')}",
                    "time_horizon": p.time_horizon,
                    "lag_hours": 0,
                    "channels": ["portfolio", "risk", "news"],
                    "thesis_path": normalize_text(p.thesis_update or p.reasoning, 220),
                    "invalidation": p.invalidation,
                    "evidence_refs": p.evidence_refs,
                    "evidence_urls": [],
                    "take_profit_pct": p.take_profit_pct,
                    "stop_loss_pct": p.stop_loss_pct,
                    "rule_reason": "PositionManager",
                }
            )

        risk_targets.append(
            {
                "ticker": p.ticker,
                "ticker_name": p.ticker_name,
                "take_profit_pct": p.take_profit_pct,
                "stop_loss_pct": p.stop_loss_pct,
                "confidence": round(clamp(max(p.confidence, 0.5), 0.0, 1.0), 4),
                "time_horizon": p.time_horizon,
                "reasoning": normalize_text(f"position_manager_target:{p.action} | {p.reasoning}", 240),
                "invalidation": p.invalidation,
            }
        )

        action_rows.append(
            {
                "ticker": p.ticker,
                "ticker_name": p.ticker_name,
                "action": p.action,
                "size_change_pct": round(p.size_change_pct, 4),
                "confidence": round(p.confidence, 4),
                "thesis_status": p.thesis_status,
                "reasoning": p.reasoning,
                "invalidation": p.invalidation,
                "time_horizon": p.time_horizon,
                "evidence_refs": p.evidence_refs,
                "risk_flags": p.risk_flags,
                "block_codes": p.block_codes,
                "order_action": order_action,
                "order_qty": int(qty),
            }
        )

    return orders, risk_targets, action_rows


def regime_to_action(regime_label: str) -> str:
    label = str(regime_label or "").upper().strip()
    if label in {"BEAR_VOL", "BEAR_CALM"}:
        return "defensive"
    if label in {"BULL_CALM"}:
        return "normal"
    if label in {"BULL_VOL"}:
        return "cautious"
    return "cautious"


def build_response_payload(
    market_assessment: str,
    regime_label: str,
    orders: list[dict[str, Any]],
    risk_targets: list[dict[str, Any]],
    llm_status: str,
    execution_mode: str,
    allowed_universe: str,
) -> dict[str, Any]:
    return {
        "timestamp": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "execution_mode": execution_mode,
        "allowed_universe": allowed_universe,
        "playbook_summary": f"position_manager:{execution_mode}:{allowed_universe}:{llm_status}",
        "market_assessment": normalize_text(market_assessment, 320),
        "regime_action": regime_to_action(regime_label),
        "orders": orders,
        "risk_targets": risk_targets,
        "watch_list": [],
        "portfolio_advice": f"position_manager_review ({llm_status})",
        "self_evaluation": "position_manager_loop",
        "next_focus": "holdings_dynamic_management",
    }


def run_order_executor(response_path: str, dry_run: bool) -> tuple[dict[str, Any] | None, str]:
    cmd = [sys.executable, str(ORDER_EXEC_SCRIPT), "--response", response_path]
    if dry_run:
        cmd.append("--dry-run")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=240, check=False)
    except Exception as e:
        return None, f"exec_failed:{type(e).__name__}:{e}"
    if proc.returncode != 0:
        stderr = normalize_text(proc.stderr, 200)
        stdout = normalize_text(proc.stdout, 200)
        return None, f"exec_failed:returncode={proc.returncode}:stdout={stdout}:stderr={stderr}"

    out = (proc.stdout or "").strip().splitlines()
    last = out[-1] if out else "{}"
    try:
        payload = json.loads(last)
        if isinstance(payload, dict):
            return payload, "ok"
    except Exception:
        pass
    return None, "exec_parse_failed"


def send_telegram(text: str) -> None:
    if not POSITION_MANAGER_NOTIFY_TELEGRAM:
        return
    if not TELEGRAM_NOTIFY_SCRIPT.exists():
        return
    try:
        _ = subprocess.run(
            [sys.executable, str(TELEGRAM_NOTIFY_SCRIPT), text],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except Exception:
        return


def build_brief(
    review_id: str,
    regime_line: str,
    llm_status: str,
    action_rows: list[dict[str, Any]],
    exec_result: dict[str, Any] | None,
    dry_run: bool,
) -> str:
    lines: list[str] = []
    lines.append("[포지션매니저 브리핑]")
    lines.append(f"- review_id: {review_id}")
    lines.append(f"- llm_status: {llm_status}")
    lines.append(f"- dry_run: {str(dry_run).lower()}")
    lines.append(f"- market: {normalize_text(regime_line, 180)}")

    actionable = [r for r in action_rows if str(r.get("action", "")) in {"ADD", "REDUCE", "EXIT", "TAKE_PROFIT_PARTIAL"}]
    lines.append(f"- actionables: {len(actionable)}")
    for i, r in enumerate(sorted(actionable, key=lambda x: to_float(x.get("confidence", 0), 0.0), reverse=True)[:5], 1):
        lines.append(
            f"  {i}) {r.get('ticker_name','')}({r.get('ticker','')}) {r.get('action','')} "
            f"conf={to_float(r.get('confidence',0),0.0):.2f} qty={to_int(r.get('order_qty',0),0)}"
        )

    if isinstance(exec_result, dict):
        lines.append(
            f"- execution: attempted={len(exec_result.get('attempted', []))} "
            f"executed={len(exec_result.get('executed', []))} skipped={len(exec_result.get('skipped', []))}"
        )
    return "\n".join(lines)


def persist_review_files(review_id: str, payload: dict[str, Any], review_meta: dict[str, Any]) -> None:
    POSITION_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    fp = POSITION_REVIEW_DIR / f"{review_id}.json"
    obj = {
        "review_id": review_id,
        "saved_at": ts_now_str(),
        "payload": payload,
        "meta": review_meta,
    }
    fp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    POSITION_LATEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    POSITION_LATEST_FILE.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def update_position_state(
    state: dict[str, Any],
    plans: list[ActionPlan],
    exec_result: dict[str, Any] | None,
) -> None:
    positions = state.setdefault("positions", {})
    if not isinstance(positions, dict):
        positions = {}
        state["positions"] = positions
    daily = state.setdefault("daily_action_count", {})
    if not isinstance(daily, dict):
        daily = {}
        state["daily_action_count"] = daily

    now_dt = now_kst()
    now_s = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    today = now_dt.strftime("%Y-%m-%d")

    executed_map: set[tuple[str, str]] = set()
    if isinstance(exec_result, dict):
        for e in exec_result.get("executed", []):
            if not isinstance(e, dict):
                continue
            t = str(e.get("ticker", "") or "").strip()
            a = str(e.get("action", "") or "").upper().strip()
            if is_ticker(t) and a in {"BUY", "SELL"}:
                executed_map.add((t, a))

    for p in plans:
        row = positions.get(p.ticker, {})
        if not isinstance(row, dict):
            row = {}
        row["ticker_name"] = p.ticker_name
        row["entry_thesis"] = p.thesis_update or row.get("entry_thesis", "")
        row["invalidation"] = p.invalidation
        row["last_review_at"] = now_s
        row["last_confidence"] = round(p.confidence, 4)
        row["next_review_trigger"] = p.next_review_trigger
        row["take_profit_pct"] = round(p.take_profit_pct, 6)
        row["stop_loss_pct"] = round(p.stop_loss_pct, 6)

        if p.action in {"ADD", "REDUCE", "EXIT", "TAKE_PROFIT_PARTIAL"}:
            expected_side = "BUY" if p.action == "ADD" else "SELL"
            if (p.ticker, expected_side) in executed_map:
                row["last_action"] = p.action
                row["last_action_at"] = now_s
                cd = now_dt + timedelta(minutes=POSITION_MANAGER_COOLDOWN_MIN)
                row["cooldown_until"] = cd.strftime("%Y-%m-%d %H:%M:%S")
                dkey = f"{today}:{p.ticker}"
                daily[dkey] = to_int(daily.get(dkey, 0), 0) + 1
        elif p.action == "TIGHTEN_STOP":
            row["last_action"] = "TIGHTEN_STOP"
            row["last_action_at"] = now_s

        positions[p.ticker] = row

    # prune old daily counters
    stale_keys = [k for k in daily.keys() if not str(k).startswith(today)]
    for k in stale_keys:
        daily.pop(k, None)


def insert_review_logs(
    review_id: str,
    llm_status: str,
    regime: dict[str, Any],
    plans: list[ActionPlan],
    action_rows: list[dict[str, Any]],
    response_path: str,
    dry_run: bool,
    orders_count: int,
) -> None:
    ensure_review_tables()
    review_time = ts_now_str()
    run_row = {
        "review_id": review_id,
        "review_time": review_time,
        "mode": "position_manager",
        "holdings_count": len(plans),
        "llm_status": llm_status,
        "market_regime": str(regime.get("regime_label", "")),
        "market_summary": normalize_text(regime.get("summary", ""), 240),
        "proposed_actions": len([p for p in plans if p.action in {"ADD", "REDUCE", "EXIT", "TAKE_PROFIT_PARTIAL"}]),
        "executable_orders": int(orders_count),
        "dry_run": 1 if dry_run else 0,
        "response_path": response_path,
    }
    _ = ch_insert_json_rows("trading.position_review_run", [run_row])

    rows: list[dict[str, Any]] = []
    by_ticker = {p.ticker: p for p in plans}
    for r in action_rows:
        p = by_ticker.get(str(r.get("ticker", "")), None)
        rows.append(
            {
                "review_id": review_id,
                "review_time": review_time,
                "ticker": str(r.get("ticker", "")),
                "ticker_name": str(r.get("ticker_name", "")),
                "action": str(r.get("action", "HOLD")),
                "size_change_pct": float(r.get("size_change_pct", 0.0) or 0.0),
                "confidence": float(r.get("confidence", 0.0) or 0.0),
                "thesis_status": p.thesis_status if p else "maintain",
                "reasoning": normalize_text(r.get("reasoning", ""), 280),
                "invalidation": normalize_text(r.get("invalidation", ""), 220),
                "time_horizon": str(r.get("time_horizon", "")),
                "evidence_refs": r.get("evidence_refs", []) if isinstance(r.get("evidence_refs"), list) else [],
                "risk_flags": r.get("risk_flags", []) if isinstance(r.get("risk_flags"), list) else [],
                "block_codes": r.get("block_codes", []) if isinstance(r.get("block_codes"), list) else [],
                "order_action": str(r.get("order_action", "")),
                "order_qty": int(r.get("order_qty", 0) or 0),
            }
        )
    _ = ch_insert_json_rows("trading.position_review_action", rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="LLM position manager")
    ap.add_argument("--response", default=DEFAULT_RESPONSE_PATH)
    ap.add_argument("--execute", action="store_true", help="execute orders via execute_gpt_orders.py")
    ap.add_argument("--dry-run", action="store_true", help="forward --dry-run to executor")
    ap.add_argument("--skip-llm", action="store_true", help="use fallback policy only")
    ap.add_argument("--dump-prompt", default="", help="write built position-manager prompt to this path")
    ap.add_argument("--max-actions", type=int, default=POSITION_MANAGER_MAX_ACTIONS)
    ap.add_argument("--news-hours", type=int, default=POSITION_MANAGER_NEWS_WINDOW_HOURS)
    ap.add_argument("--allow-add", action="store_true", help="allow ADD action for this run")
    ap.add_argument("--only-modes", default="", help="comma separated execution modes to allow")
    args = ap.parse_args()

    if not POSITION_MANAGER_ENABLED:
        print(json.dumps({"status": "skipped", "reason": "position_manager_disabled"}, ensure_ascii=False))
        return 0

    ensure_feature_snapshot_view()

    do_execute = bool(args.execute)
    dry_run = bool(args.dry_run)
    if dry_run:
        do_execute = True

    review_id = str(uuid.uuid4())
    trigger_source = "none"
    system_event = os.getenv("OPENCLAW_SYSTEM_EVENT", "").strip()
    if system_event:
        trigger_source = "openclaw_system_event"
    if not system_event:
        system_event = os.getenv("SYSTEM_EVENT_TEXT", "").strip()
        if system_event:
            trigger_source = "system_event_text"
    if not system_event:
        system_event = os.getenv("MACOS_SYSTEM_EVENT", "").strip()
        if system_event:
            trigger_source = "macos_system_event"
    event_name = (
        os.getenv("OPENCLAW_EVENT_NAME", "").strip()
        or os.getenv("CRON_JOB_NAME", "").strip()
        or os.getenv("JOB_NAME", "").strip()
    )
    event_block = f"[{event_name or 'external-trigger'}]\\n{system_event}" if system_event else "(없음: 정기 실행)"
    engine_context = (
        f"engine=macOS cron router + Python pipeline | trigger_source={trigger_source} "
        f"| event_name={event_name or '-'} | system_event_present={'yes' if system_event else 'no'}"
    )

    persistent_memory = read_text_file(PERSISTENT_MEMORY_PATH, max_chars=10000)
    heartbeat_text = read_text_file(HEARTBEAT_PATH, max_chars=10000)
    soul_text = read_text_file(SOUL_PATH, max_chars=10000)
    if not persistent_memory:
        persistent_memory = "영구 메모리 없음. 하드룰과 데이터 기반 판단을 유지할 것."
    if not heartbeat_text:
        heartbeat_text = "HEARTBEAT.md 로드 실패"
    if not soul_text:
        soul_text = "SOUL.md 로드 실패"

    bal_raw, bal_summary, holdings_rows = load_balance()
    _ = bal_raw
    holdings = parse_holdings(holdings_rows)
    dump_prompt_path = str(args.dump_prompt or "").strip()
    if not holdings and not dump_prompt_path:
        print(json.dumps({"status": "ok", "review_id": review_id, "reason": "no_holdings"}, ensure_ascii=False))
        return 0

    tickers = list(holdings.keys())
    state = load_position_state()
    dynamic_exits = load_dynamic_exit_state()
    execution_mode_state = load_execution_mode_state()
    mode_name = str(execution_mode_state.get("execution_mode", "normal") or "normal")
    only_modes = [s.strip().lower() for s in str(args.only_modes or "").split(",") if s.strip()]
    if only_modes and mode_name.lower() not in set(only_modes):
        print(json.dumps({"status": "skipped", "reason": "execution_mode_filtered", "execution_mode": mode_name}, ensure_ascii=False))
        return 0
    regime = load_market_regime()
    tech = load_technical(tickers)
    flow = load_flow_snapshot(tickers)
    news = load_news_context(tickers, max(12, int(args.news_hours)))
    relation = load_relation_signals(tickers)

    contexts = build_position_contexts(
        holdings=holdings,
        tech=tech,
        flow=flow,
        news=news,
        relation=relation,
        state=state,
    )

    max_actions = max(1, int(args.max_actions))
    allow_add = (POSITION_MANAGER_ALLOW_ADD or bool(args.allow_add)) and mode_name not in {"shock", "close_only"}

    llm_obj: dict[str, Any] | None = None
    llm_status = "fallback"
    prompt_for_llm = ""
    if (not args.skip_llm) or dump_prompt_path:
        prompt = build_llm_prompt(
            review_id=review_id,
            contexts=contexts,
            regime=regime,
            dynamic_exits=dynamic_exits,
            max_actions=max_actions,
            allow_add=allow_add,
            event_block=event_block,
            engine_context=engine_context,
            persistent_memory=persistent_memory,
            heartbeat_text=heartbeat_text,
            soul_text=soul_text,
        )
        prompt_for_llm = prompt
        if dump_prompt_path:
            dump_path = Path(dump_prompt_path)
            dump_path.parent.mkdir(parents=True, exist_ok=True)
            dump_path.write_text(prompt, encoding="utf-8")
    if not args.skip_llm:
        if not prompt_for_llm:
            llm_status = "prompt_missing_fallback"
        else:
            llm_obj, llm_status = run_llm_review(prompt_for_llm)

    raw_items = parse_action_items(llm_obj or {}, contexts) if llm_obj else []
    if not raw_items:
        raw_items = fallback_actions(contexts, execution_mode=mode_name)
        if llm_status == "ok":
            llm_status = "ok_but_empty_fallback"

    plans = normalize_action_plans(
        raw_items=raw_items,
        contexts=contexts,
        state=state,
        dynamic_exits=dynamic_exits,
        max_actions=max_actions,
        allow_add=allow_add,
        execution_mode=mode_name,
    )

    cash_krw = to_float(bal_summary.get("dnca_tot_amt", 0), 0.0)
    orders, risk_targets, action_rows = build_orders_and_targets(
        plans=plans,
        contexts=contexts,
        cash_krw=cash_krw,
        allow_add=allow_add,
        execution_mode=execution_mode_state,
    )

    market_line = format_regime_line(regime)
    payload = build_response_payload(
        market_assessment=market_line,
        regime_label=str(regime.get("regime_label", "")),
        orders=orders,
        risk_targets=risk_targets,
        llm_status=llm_status,
        execution_mode=mode_name,
        allowed_universe=str(execution_mode_state.get("allowed_universe", "watchlist") or "watchlist"),
    )

    response_path = str(args.response)
    Path(response_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    exec_result: dict[str, Any] | None = None
    exec_status = "skipped"
    if do_execute:
        exec_result, exec_status = run_order_executor(response_path=response_path, dry_run=dry_run)

    update_position_state(state=state, plans=plans, exec_result=exec_result)
    save_position_state(state)

    insert_review_logs(
        review_id=review_id,
        llm_status=llm_status,
        regime=regime,
        plans=plans,
        action_rows=action_rows,
        response_path=response_path,
        dry_run=dry_run,
        orders_count=len(orders),
    )

    review_meta = {
        "review_id": review_id,
        "llm_status": llm_status,
        "exec_status": exec_status,
        "orders_count": len(orders),
        "risk_targets_count": len(risk_targets),
        "dry_run": dry_run,
        "execute": do_execute,
        "market_line": market_line,
        "action_rows": action_rows,
    }
    persist_review_files(review_id=review_id, payload=payload, review_meta=review_meta)

    brief = build_brief(
        review_id=review_id,
        regime_line=market_line,
        llm_status=llm_status,
        action_rows=action_rows,
        exec_result=exec_result,
        dry_run=dry_run,
    )
    send_telegram(brief)

    result = {
        "status": "ok",
        "review_id": review_id,
        "llm_status": llm_status,
        "exec_status": exec_status,
        "execute": do_execute,
        "dry_run": dry_run,
        "holdings": len(holdings),
        "orders": len(orders),
        "actionables": len([a for a in action_rows if str(a.get("action", "")) in {"ADD", "REDUCE", "EXIT", "TAKE_PROFIT_PARTIAL"}]),
        "response_path": response_path,
    }
    if isinstance(exec_result, dict):
        result["executed"] = len(exec_result.get("executed", []))
        result["skipped"] = len(exec_result.get("skipped", []))
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
