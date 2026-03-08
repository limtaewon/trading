#!/usr/bin/env python3
"""execute_gpt_orders.py

Codex가 생성한 `/tmp/gpt_response.json`의 orders를 파싱해 KIS MCP로 직접 주문 실행.
OpenClaw 런타임 파서를 거치지 않고 독립적으로 동작한다.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib import parse, request
from zoneinfo import ZoneInfo

from env_bootstrap import bootstrap_openclaw_env
from report_payload_builder import build_owner_payload
from report_renderer_telegram_owner import render_owner_execution_message
from llm_model_config import resolve_model
from response_enricher import enrich_trading_response
from response_validator import validate_trading_response

bootstrap_openclaw_env()

KST = ZoneInfo("Asia/Seoul")
HOME = Path.home()

# sandbox-safe cache 경로 (mcporter/uv 권한 이슈 회피)
XDG_CACHE_HOME = os.getenv("XDG_CACHE_HOME", str(HOME / ".openclaw" / "workspace" / ".cache"))
UV_CACHE_DIR = os.getenv("UV_CACHE_DIR", str(Path(XDG_CACHE_HOME) / "uv"))
os.environ.setdefault("XDG_CACHE_HOME", XDG_CACHE_HOME)
os.environ.setdefault("UV_CACHE_DIR", UV_CACHE_DIR)
Path(UV_CACHE_DIR).mkdir(parents=True, exist_ok=True)
MCP_CONFIG = str(HOME / ".openclaw" / "config" / "mcporter.json")
MCPORTER_BIN = os.getenv("MCPORTER_BIN") or shutil.which("mcporter") or "/opt/homebrew/bin/mcporter"
STATE_DIR = HOME / ".openclaw" / "state" / "codex_brain"
KILL_STATE_FILE = HOME / ".openclaw" / "state" / "kill_switch_state.json"
ADAPTIVE_POLICY_FILE = HOME / ".openclaw" / "state" / "adaptive_policy.json"
EXIT_STATE_FILE = HOME / ".openclaw" / "state" / "stock_dynamic_exits.json"
EXECUTION_MODE_FILE = HOME / ".openclaw" / "state" / "market_execution_mode.json"
PENDING_EXIT_FILE = HOME / ".openclaw" / "state" / "pending_exit_orders.json"
EXEC_RETRY_STATE_FILE = HOME / ".openclaw" / "state" / "execution_retry_state.json"
EXEC_DIR = STATE_DIR / "executions"
JOURNAL_FILE = STATE_DIR / "events.jsonl"
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://localhost:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASS", os.getenv("CLICKHOUSE_PASSWORD", ""))
INPUT_VERSION = os.getenv("TRADING_INPUT_VERSION", "v2")
MODEL_NAME = resolve_model("TRADING_MODEL_NAME", "CODEX_MODEL")
MAX_DATA_STALENESS_MIN = int(os.getenv("MAX_DATA_STALENESS_MIN", "20"))
BLOCK_ALL_ON_STALE = os.getenv("BLOCK_ALL_ON_STALE", "1") == "1"
REQUIRE_REAL_ACCOUNT = os.getenv("REQUIRE_REAL_ACCOUNT", "1") == "1"
NEWS_MAX_AGE_MIN = int(os.getenv("NEWS_MAX_AGE_MIN", "90"))
TECH_MAX_AGE_DAYS = int(os.getenv("TECH_MAX_AGE_DAYS", "2"))
REGIME_MAX_AGE_DAYS = int(os.getenv("REGIME_MAX_AGE_DAYS", "2"))
ENFORCE_STALE_WHEN_CLOSED = os.getenv("ENFORCE_STALE_WHEN_CLOSED", "0") == "1"
DEFAULT_MIN_CONFIDENCE = float(os.getenv("DEFAULT_MIN_CONFIDENCE", "0.70"))
DEFAULT_MIN_CASH_RATIO = float(os.getenv("DEFAULT_MIN_CASH_RATIO", "0.15"))
DEFAULT_ORDER_CAP_MULT = float(os.getenv("DEFAULT_ORDER_CAP_MULT", "1.0"))
DEFAULT_DAILY_ORDER_LIMIT = int(os.getenv("DEFAULT_DAILY_ORDER_LIMIT", "3"))
DEFAULT_POSITION_WEIGHT_LIMIT = float(os.getenv("DEFAULT_POSITION_WEIGHT_LIMIT", "0.25"))
ENABLE_RSI_OVERHEAT_BLOCK = os.getenv("ENABLE_RSI_OVERHEAT_BLOCK", "0") == "1"
STAGE2_EXTREME_BLOCK_ENABLED = os.getenv("STAGE2_EXTREME_BLOCK_ENABLED", "1") == "1"
STAGE2_SHOCK_LOOKBACK_DAYS = max(3, int(os.getenv("STAGE2_SHOCK_LOOKBACK_DAYS", "5")))
REQUIRE_EVENT_EXPLAIN_FOR_BUY = os.getenv("REQUIRE_EVENT_EXPLAIN_FOR_BUY", "1") == "1"
MIN_EVENT_EVIDENCE_REFS = max(1, int(os.getenv("MIN_EVENT_EVIDENCE_REFS", "1")))
# Per-order notional cap in KRW. Set 0 to disable (we still enforce position_weight_limit <= 25%).
# User requested removing the 5% of AUM cap; default is disabled.
BASE_PER_ORDER_CAP_KRW = int(os.getenv("BASE_PER_ORDER_CAP_KRW", "0"))
ENABLE_RELATION_SCORE_BUY_FILTER = os.getenv("ENABLE_RELATION_SCORE_BUY_FILTER", "1") == "1"
MIN_RELATION_SCORE_BUY = float(os.getenv("MIN_RELATION_SCORE_BUY", "-0.20"))
ENABLE_DYNAMIC_EXIT_ENFORCEMENT = os.getenv("ENABLE_DYNAMIC_EXIT_ENFORCEMENT", "1") == "1"
DYNAMIC_EXIT_COOLDOWN_MIN = max(1, int(os.getenv("DYNAMIC_EXIT_COOLDOWN_MIN", "120")))
DYNAMIC_EXIT_MAX_TP = float(os.getenv("DYNAMIC_EXIT_MAX_TP", "0.60"))
DYNAMIC_EXIT_MAX_SL_ABS = float(os.getenv("DYNAMIC_EXIT_MAX_SL_ABS", "0.60"))
EXECUTION_MODE_STALE_MIN = max(3, int(os.getenv("EXECUTION_MODE_STALE_MIN", "20")))
PENDING_EXIT_TTL_HOURS = max(1, int(os.getenv("PENDING_EXIT_TTL_HOURS", "72")))
PENDING_EXIT_MAX_REPLAY_PER_RUN = max(1, int(os.getenv("PENDING_EXIT_MAX_REPLAY_PER_RUN", "8")))
PENDING_EXIT_MAX_ATTEMPTS = max(1, int(os.getenv("PENDING_EXIT_MAX_ATTEMPTS", "12")))
TP_PARTIAL_SELL_RATIO = float(os.getenv("TP_PARTIAL_SELL_RATIO", "0.50"))
_HARD_STOP_LOSS_ENV = os.getenv("HARD_EMERGENCY_STOP_LOSS_PCT", "-0.08").strip()
try:
    _HARD_STOP_LOSS_PCT = float(_HARD_STOP_LOSS_ENV)
except Exception:
    _HARD_STOP_LOSS_PCT = -0.10
HARD_EMERGENCY_STOP_LOSS_PCT = _HARD_STOP_LOSS_PCT if _HARD_STOP_LOSS_PCT <= 0.0 else -abs(_HARD_STOP_LOSS_PCT)
HARD_STOP_LOSS_ENABLED = os.getenv("HARD_STOP_LOSS_ENABLED", "1") == "1"
HARD_STOP_LOSS_ORDER_TYPE = os.getenv("HARD_STOP_LOSS_ORDER_TYPE", "MARKET").upper().strip() or "MARKET"
HARD_STOP_LOSS_ORDER_TYPE = "MARKET" if HARD_STOP_LOSS_ORDER_TYPE in {"MARKET", "LIMIT"} else "MARKET"
HARD_STOP_LOSS_PRICE_TYPE = os.getenv("HARD_STOP_LOSS_PRICE_TYPE", "bidp1").lower().strip() or "bidp1"
HARD_STOP_LOSS_PRICE_TYPE = HARD_STOP_LOSS_PRICE_TYPE if HARD_STOP_LOSS_PRICE_TYPE in {"bidp1", "askp1", "mid", "limit"} else "bidp1"
EVENT_HORIZON_SET = {"intraday", "1d", "1-3d", "1w", "1-2w", "2w+"}
TELEGRAM_ORDER_BRIEF_ENABLED = os.getenv("TELEGRAM_ORDER_BRIEF_ENABLED", "1") == "1"
TELEGRAM_ORDER_BRIEF_MAX_EXEC = max(1, int(os.getenv("TELEGRAM_ORDER_BRIEF_MAX_EXEC", "6")))
TELEGRAM_ORDER_BRIEF_MAX_SKIP = max(1, int(os.getenv("TELEGRAM_ORDER_BRIEF_MAX_SKIP", "6")))

try:
    HARD_TAKE_PROFIT_PCT = float(os.getenv("HARD_TAKE_PROFIT_PCT", "0.15"))
except Exception:
    HARD_TAKE_PROFIT_PCT = 0.15
try:
    HARD_TAKE_PROFIT_RATIO = float(os.getenv("HARD_TAKE_PROFIT_RATIO", "0.50"))
except Exception:
    HARD_TAKE_PROFIT_RATIO = 0.50
HARD_TAKE_PROFIT_RATIO = max(0.05, min(1.0, HARD_TAKE_PROFIT_RATIO))
try:
    HARD_TAKE_PROFIT_MIN_QTY = int(os.getenv("HARD_TAKE_PROFIT_MIN_QTY", "1") or "1")
except Exception:
    HARD_TAKE_PROFIT_MIN_QTY = 1
HARD_TAKE_PROFIT_MIN_QTY = max(1, HARD_TAKE_PROFIT_MIN_QTY)

try:
    FLOW_EXPLAIN_DAILY_THRESHOLD = float(os.getenv("FLOW_EXPLAIN_DAILY_THRESHOLD", "0"))
except Exception:
    FLOW_EXPLAIN_DAILY_THRESHOLD = 0.0
try:
    FLOW_EXPLAIN_BYPASS_DAYS = max(2, int(os.getenv("FLOW_EXPLAIN_BYPASS_DAYS", "3")))
except Exception:
    FLOW_EXPLAIN_BYPASS_DAYS = 3
try:
    FLOW_EXPLAIN_TRADE_SIZE_THRESHOLD = float(os.getenv("FLOW_EXPLAIN_TRADE_SIZE_THRESHOLD", "0"))
except Exception:
    FLOW_EXPLAIN_TRADE_SIZE_THRESHOLD = 0.0

ACTIVE_SESSIONS = {
    "NXT_PREMARKET",
    "KRX_OPEN_AUCTION",
    "REGULAR_CONTINUOUS",
    "KRX_CLOSE_AUCTION",
    "NXT_AFTERMARKET",
}

RELATION_CACHE: dict[str, dict[str, Any] | None] = {}
FLOW_CONTEXT_CACHE: dict[str, list[dict[str, Any]] | None] = {}
REASONING_CACHE: dict[str, dict[str, Any] | None] = {}


def now_kst() -> datetime:
    return datetime.now(KST)


def to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def kst_time_hhmm() -> int:
    return int(now_kst().strftime("%H%M"))


def is_six_digit_ticker(v: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", v or ""))


def normalize_text_list(v: Any, max_items: int = 8, max_len: int = 240) -> list[str]:
    items: list[str] = []
    if isinstance(v, list):
        raw = v
    elif isinstance(v, str):
        raw = [v]
    else:
        raw = []
    for it in raw:
        s = str(it or "").strip()
        if not s:
            continue
        s = s.replace("\n", " ").strip()
        if len(s) > max_len:
            s = s[:max_len]
        items.append(s)
        if len(items) >= max_items:
            break
    # dedupe while preserving order
    seen: set[str] = set()
    out: list[str] = []
    for s in items:
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def parse_evidence_fields(order: dict[str, Any]) -> tuple[list[str], list[str]]:
    refs = normalize_text_list(order.get("evidence_refs", []), max_items=8, max_len=220)
    urls = normalize_text_list(order.get("evidence_urls", []), max_items=8, max_len=300)
    evidence = order.get("evidence", [])
    if isinstance(evidence, list):
        for it in evidence:
            if not isinstance(it, dict):
                continue
            quote = str(it.get("quote", "") or "").strip()
            url = str(it.get("url", "") or "").strip()
            if quote and len(refs) < 8:
                refs.append(quote[:220])
            if url and len(urls) < 8:
                urls.append(url[:300])
    refs = normalize_text_list(refs, max_items=8, max_len=220)
    urls = normalize_text_list(urls, max_items=8, max_len=300)
    return refs, urls


def _now_kst_str() -> str:
    return now_kst().strftime("%Y-%m-%d %H:%M:%S")


def _normalize_exit_pair(tp_raw: Any, sl_raw: Any) -> tuple[float, float] | None:
    tp = to_float(tp_raw, 0.0)
    sl = to_float(sl_raw, 0.0)
    if tp <= 0.0 or tp > DYNAMIC_EXIT_MAX_TP:
        return None
    if sl >= 0.0 or abs(sl) > DYNAMIC_EXIT_MAX_SL_ABS:
        return None
    return round(tp, 6), round(sl, 6)


def load_json_file(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8"))
            return raw
    except Exception:
        pass
    return default


def save_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_exit_state() -> dict[str, Any]:
    base = {"updated_at": "", "positions": {}}
    try:
        if EXIT_STATE_FILE.exists():
            raw = json.loads(EXIT_STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                positions = raw.get("positions", {})
                if isinstance(positions, dict):
                    base["positions"] = positions
                base["updated_at"] = str(raw.get("updated_at", "") or "")
    except Exception:
        pass
    return base


def load_execution_mode() -> dict[str, Any]:
    default = {
        "execution_mode": "normal",
        "allowed_universe": "watchlist",
        "allowed_tickers": [],
        "max_new_positions": 3,
        "max_buys_per_run": 3,
        "avg_down_block": True,
        "sell_urgency": "normal",
        "llm_style": "stock_selection",
        "updated_at": "",
        "shock_level": "UNKNOWN",
        "hard_riskoff": False,
    }
    raw = load_json_file(EXECUTION_MODE_FILE, default)
    if not isinstance(raw, dict):
        raw = dict(default)
    mode = {**default, **raw}
    updated = parse_kst_dt(str(mode.get("updated_at", "") or ""))
    if updated is not None:
        age_min = max(0.0, (now_kst() - updated).total_seconds() / 60.0)
        mode["stale"] = age_min > EXECUTION_MODE_STALE_MIN
        mode["age_min"] = round(age_min, 2)
    else:
        mode["stale"] = True
        mode["age_min"] = None
    allowed = mode.get("allowed_tickers", [])
    if not isinstance(allowed, list):
        allowed = []
    mode["allowed_tickers"] = [str(x).strip() for x in allowed if is_six_digit_ticker(str(x).strip())]
    return mode


def load_pending_exit_orders() -> list[dict[str, Any]]:
    rows = load_json_file(PENDING_EXIT_FILE, [])
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    now_dt = now_kst()
    for row in rows:
        if not isinstance(row, dict):
            continue
        queued_at = parse_kst_dt(str(row.get("queued_at", "") or ""))
        attempts = to_int(row.get("replay_attempts", 0), 0)
        if queued_at is not None:
            age_h = max(0.0, (now_dt - queued_at).total_seconds() / 3600.0)
            if age_h > PENDING_EXIT_TTL_HOURS:
                continue
        if attempts >= PENDING_EXIT_MAX_ATTEMPTS:
            continue
        out.append(row)
    return out


def save_pending_exit_orders(rows: list[dict[str, Any]]) -> None:
    save_json_file(PENDING_EXIT_FILE, rows)


def pending_exit_signature(item: dict[str, Any]) -> str:
    return "|".join(
        [
            str(item.get("action", "") or ""),
            str(item.get("ticker", "") or ""),
            str(item.get("quantity", "") or ""),
            str(item.get("strategy_family", "") or ""),
            str(item.get("event_type", "") or ""),
            str(item.get("reasoning", "") or "")[:80],
        ]
    )


def enqueue_pending_exit(item: dict[str, Any], reason: str) -> int:
    queued = load_pending_exit_orders()
    sig = pending_exit_signature(item)
    existing = {str(r.get("queue_signature", "") or "") for r in queued if isinstance(r, dict)}
    if sig not in existing:
        queued.append(
            {
                **item,
                "queued_at": _now_kst_str(),
                "queue_reason": reason,
                "queue_signature": sig,
                "replay_attempts": 0,
            }
        )
        save_pending_exit_orders(queued)
    return len(queued)


def merge_pending_exit_orders(orders: list[dict[str, Any]], session_id: str) -> tuple[list[dict[str, Any]], int]:
    queued = load_pending_exit_orders()
    if not queued:
        return orders, 0
    if session_id not in ACTIVE_SESSIONS:
        return orders, len(queued)
    existing_sig = {pending_exit_signature(o) for o in orders if isinstance(o, dict)}
    replay: list[dict[str, Any]] = []
    kept: list[dict[str, Any]] = []
    replay_count = 0
    for row in queued:
        if not isinstance(row, dict):
            continue
        sig = str(row.get("queue_signature", "") or pending_exit_signature(row))
        if sig in existing_sig:
            kept.append(row)
            continue
        if replay_count >= PENDING_EXIT_MAX_REPLAY_PER_RUN:
            kept.append(row)
            continue
        replay.append({k: v for k, v in row.items() if k not in {"queued_at", "queue_reason", "queue_signature", "replay_attempts", "last_replayed_at"}})
        row["replay_attempts"] = to_int(row.get("replay_attempts", 0), 0) + 1
        row["last_replayed_at"] = _now_kst_str()
        kept.append(row)
        replay_count += 1
    if replay:
        orders = replay + orders
    save_pending_exit_orders(kept)
    return orders, len(replay)


def load_retry_state() -> dict[str, Any]:
    raw = load_json_file(EXEC_RETRY_STATE_FILE, {"orders": {}})
    if isinstance(raw, dict):
        orders = raw.get("orders", {})
        if isinstance(orders, dict):
            return {"orders": orders}
    return {"orders": {}}


def save_retry_state(state: dict[str, Any]) -> None:
    save_json_file(EXEC_RETRY_STATE_FILE, state)


def record_retry_state(state: dict[str, Any], item: dict[str, Any], reason: str) -> None:
    orders = state.setdefault("orders", {})
    if not isinstance(orders, dict):
        orders = {}
        state["orders"] = orders
    sig = pending_exit_signature(item)
    row = orders.get(sig, {})
    if not isinstance(row, dict):
        row = {}
    row["ticker"] = str(item.get("ticker", "") or "")
    row["action"] = str(item.get("action", "") or "")
    row["reason"] = reason
    row["attempts"] = to_int(row.get("attempts", 0), 0) + 1
    row["updated_at"] = _now_kst_str()
    orders[sig] = row


def refresh_live_holding(holdings: dict[str, "Holding"], ticker: str) -> "Holding":
    try:
        _, _, rows = load_balance(max_retries=1)
        latest = build_holdings_map(rows)
        holdings.clear()
        holdings.update(latest)
    except Exception:
        pass
    return holdings.get(ticker, Holding(qty=0, eval_amt=0.0))


def allowed_buy_in_mode(mode_state: dict[str, Any], ticker: str) -> bool:
    mode = str(mode_state.get("execution_mode", "normal") or "normal")
    allowed = set(mode_state.get("allowed_tickers", []) or [])
    if mode == "close_only":
        return False
    if mode in {"shock", "recovery"}:
        return ticker in allowed
    return True


def compute_order_priority(item: dict[str, Any]) -> int:
    action = str(item.get("action", "") or "").upper()
    event_type = str(item.get("event_type", "") or "").lower()
    strategy_family = str(item.get("strategy_family", "") or "").lower()
    explicit = to_int(item.get("priority", 0), 0)
    if explicit > 0:
        return explicit
    if action == "SELL" and event_type == "hard_exit":
        return 100
    if action == "SELL" and event_type == "dynamic_exit":
        return 95
    if action == "SELL" and strategy_family == "forced_exit":
        return 90
    if action == "SELL":
        return 70
    if strategy_family in {"shock_hedge", "index_etf"}:
        return 50
    return 30


def order_strategy_family(item: dict[str, Any], mode_state: dict[str, Any], action: str) -> str:
    family = str(item.get("strategy_family", "") or "").strip()
    if family:
        return family
    mode = str(mode_state.get("execution_mode", "normal") or "normal")
    if action == "SELL":
        return "forced_exit" if str(item.get("event_type", "") or "") in {"hard_exit", "dynamic_exit"} else "core_defensive"
    if mode == "shock":
        return "shock_hedge"
    if mode == "recovery":
        return "shock_rebound"
    return "stock_selection"


def collect_existing_sell_tickers(existing_orders: list[dict[str, Any]]) -> set[str]:
    sell_tickers: set[str] = set()
    for o in existing_orders:
        if not isinstance(o, dict):
            continue
        if str(o.get("action", "") or "").upper().strip() == "SELL":
            t = str(o.get("ticker", "") or "").strip()
            if is_six_digit_ticker(t):
                sell_tickers.add(t)
    return sell_tickers


def build_hard_stop_loss_orders(
    holdings: dict[str, "Holding"],
    existing_orders: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not HARD_STOP_LOSS_ENABLED:
        return []
    if HARD_EMERGENCY_STOP_LOSS_PCT >= 0:
        return []
    now_dt = now_kst()
    existing_sell_tickers = collect_existing_sell_tickers(existing_orders)
    out: list[dict[str, Any]] = []
    hard_stop_threshold_pct = HARD_EMERGENCY_STOP_LOSS_PCT * 100.0
    for ticker, h in holdings.items():
        if h.qty <= 0:
            continue
        if ticker in existing_sell_tickers:
            continue
        if h.pnl_rate > hard_stop_threshold_pct:
            continue
        out.append(
            {
                "action": "SELL",
                "ticker": ticker,
                "ticker_name": "",
                "quantity": int(h.qty),
                "order_type": HARD_STOP_LOSS_ORDER_TYPE,
                "price_type": HARD_STOP_LOSS_PRICE_TYPE,
                "price": 0,
                "confidence": 1.0,
                "venue_preference": "SOR",
                "reasoning": (
                    f"hard_stop_loss: pnl={h.pnl_rate:.2f}% threshold={hard_stop_threshold_pct:.2f}% "
                    "(system hard rule, no LLM override)"
                )[:280],
                "take_profit_pct": 0.0,
                "stop_loss_pct": HARD_EMERGENCY_STOP_LOSS_PCT,
                "event_type": "hard_exit",
                "event_signature": f"hardstop:{ticker}:{now_dt.strftime('%Y%m%d%H%M')}",
                "time_horizon": "intraday",
                "lag_hours": 0,
                "channels": ["risk", "kill_switch"],
                "thesis_path": "hard_exit_protection",
                "invalidation": "system_guardrail_activated",
                "evidence_refs": [f"holding_pnl={h.pnl_rate:.2f}%"],
                "evidence_urls": [],
                "rule_reason": "Hard_StopLoss_8pct",
            }
        )
    return out


def build_hard_take_profit_orders(
    holdings: dict[str, "Holding"],
    existing_orders: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if HARD_TAKE_PROFIT_PCT <= 0:
        return []
    if HARD_TAKE_PROFIT_MIN_QTY <= 0:
        return []
    existing_sell_tickers = collect_existing_sell_tickers(existing_orders)
    now_dt = now_kst()
    out: list[dict[str, Any]] = []
    threshold_pct = HARD_TAKE_PROFIT_PCT * 100.0

    for ticker, h in holdings.items():
        if h.qty <= 0:
            continue
        if ticker in existing_sell_tickers:
            continue
        if h.pnl_rate < threshold_pct:
            continue
        qty = max(1, int(math.ceil(h.qty * HARD_TAKE_PROFIT_RATIO)))
        qty = max(qty, HARD_TAKE_PROFIT_MIN_QTY)
        qty = min(qty, h.qty)
        if qty <= 0:
            continue

        out.append(
            {
                "action": "SELL",
                "ticker": ticker,
                "ticker_name": "",
                "quantity": int(qty),
                "order_type": "MARKET",
                "price_type": "bidp1",
                "price": 0,
                "confidence": 1.0,
                "venue_preference": "SOR",
                "reasoning": (
                    f"hard_take_profit: pnl={h.pnl_rate:.2f}% threshold={threshold_pct:.2f}% "
                    "(system hard rule, no LLM override)"
                )[:280],
                "take_profit_pct": HARD_TAKE_PROFIT_PCT,
                "stop_loss_pct": 0.0,
                "event_type": "hard_exit",
                "event_signature": f"hardtp:{ticker}:{now_dt.strftime('%Y%m%d%H%M')}",
                "time_horizon": "intraday",
                "lag_hours": 0,
                "channels": ["risk", "kill_switch"],
                "thesis_path": "hard_take_profit_protection",
                "invalidation": "system_guardrail_activated",
                "evidence_refs": [f"holding_pnl={h.pnl_rate:.2f}%"],
                "evidence_urls": [],
                "rule_reason": "Hard_TakeProfit_15pct",
            }
        )
    return out


def save_exit_state(state: dict[str, Any]) -> None:
    try:
        EXIT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        state["updated_at"] = _now_kst_str()
        EXIT_STATE_FILE.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


def extract_risk_targets(response_json: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    rows = response_json.get("risk_targets", [])
    if not isinstance(rows, list):
        return out
    for r in rows:
        if not isinstance(r, dict):
            continue
        ticker = str(r.get("ticker", "") or "").strip()
        if not is_six_digit_ticker(ticker):
            continue
        pair = _normalize_exit_pair(r.get("take_profit_pct"), r.get("stop_loss_pct"))
        if pair is None:
            continue
        tp, sl = pair
        conf = clamp(to_float(r.get("confidence", 0.0), 0.0), 0.0, 1.0)
        horizon = str(r.get("time_horizon", "") or "").strip().lower()
        if horizon and horizon not in EVENT_HORIZON_SET:
            horizon = ""
        out.append(
            {
                "ticker": ticker,
                "ticker_name": str(r.get("ticker_name", "") or "").strip(),
                "take_profit_pct": tp,
                "stop_loss_pct": sl,
                "confidence": round(conf, 4),
                "time_horizon": horizon,
                "reasoning": str(r.get("reasoning", "") or "").strip().replace("\n", " ")[:280],
                "invalidation": str(r.get("invalidation", "") or "").strip().replace("\n", " ")[:280],
            }
        )
    return out


def extract_buy_order_exit_targets(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for o in orders:
        if not isinstance(o, dict):
            continue
        action = str(o.get("action", "") or "").upper().strip()
        if action != "BUY":
            continue
        ticker = str(o.get("ticker", "") or "").strip()
        if not is_six_digit_ticker(ticker):
            continue
        pair = _normalize_exit_pair(o.get("take_profit_pct"), o.get("stop_loss_pct"))
        if pair is None:
            continue
        tp, sl = pair
        out.append(
            {
                "ticker": ticker,
                "ticker_name": str(o.get("ticker_name", "") or "").strip(),
                "take_profit_pct": tp,
                "stop_loss_pct": sl,
                "confidence": round(clamp(to_float(o.get("confidence", 0.0), 0.0), 0.0, 1.0), 4),
                "time_horizon": str(o.get("time_horizon", "") or "").strip().lower(),
                "reasoning": str(o.get("reasoning", "") or "").strip().replace("\n", " ")[:280],
                "invalidation": str(o.get("invalidation", "") or "").strip().replace("\n", " ")[:280],
            }
        )
    return out


def apply_exit_targets(
    exit_state: dict[str, Any],
    targets: list[dict[str, Any]],
    source: str,
    holdings: dict[str, "Holding"],
) -> int:
    positions = exit_state.setdefault("positions", {})
    if not isinstance(positions, dict):
        positions = {}
        exit_state["positions"] = positions
    updated = 0
    for t in targets:
        ticker = str(t.get("ticker", "") or "").strip()
        if ticker not in holdings:
            # 현재 미보유 종목은 상태에 누적하지 않음.
            continue
        row = positions.get(ticker, {})
        if not isinstance(row, dict):
            row = {}
        row.update(
            {
                "ticker_name": str(t.get("ticker_name", row.get("ticker_name", "")) or "").strip(),
                "take_profit_pct": round(to_float(t.get("take_profit_pct", row.get("take_profit_pct", 0.0)), 0.0), 6),
                "stop_loss_pct": round(to_float(t.get("stop_loss_pct", row.get("stop_loss_pct", 0.0)), 0.0), 6),
                "confidence": round(to_float(t.get("confidence", row.get("confidence", 0.0)), 0.0), 4),
                "time_horizon": str(t.get("time_horizon", row.get("time_horizon", "")) or "").strip().lower(),
                "reasoning": str(t.get("reasoning", row.get("reasoning", "")) or "").strip(),
                "invalidation": str(t.get("invalidation", row.get("invalidation", "")) or "").strip(),
                "updated_at": _now_kst_str(),
                "source": source,
            }
        )
        positions[ticker] = row
        updated += 1

    # 정리: 더 이상 보유하지 않는 종목의 동적 exit는 제거
    for ticker in list(positions.keys()):
        if ticker not in holdings:
            positions.pop(ticker, None)
    return updated


def build_dynamic_exit_orders(
    holdings: dict[str, "Holding"],
    exit_state: dict[str, Any],
    existing_orders: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not ENABLE_DYNAMIC_EXIT_ENFORCEMENT:
        return []

    positions = exit_state.get("positions", {})
    if not isinstance(positions, dict):
        return []

    existing_sell_tickers = collect_existing_sell_tickers(existing_orders)

    now_dt = now_kst()
    out: list[dict[str, Any]] = []
    for ticker, h in holdings.items():
        if h.qty <= 0:
            continue
        if ticker in existing_sell_tickers:
            continue
        st = positions.get(ticker, {})
        if not isinstance(st, dict):
            continue
        pair = _normalize_exit_pair(st.get("take_profit_pct"), st.get("stop_loss_pct"))
        if pair is None:
            continue
        tp, sl = pair
        pnl = float(h.pnl_rate)
        kind = ""
        qty = 0
        if pnl <= (sl * 100.0):
            kind = "stop_loss"
            qty = h.qty
        elif pnl >= (tp * 100.0):
            kind = "take_profit"
            partial = int(math.ceil(h.qty * clamp(TP_PARTIAL_SELL_RATIO, 0.05, 1.0)))
            qty = max(1, min(h.qty, partial))
        if not kind or qty <= 0:
            continue

        last_trigger = st.get("last_trigger", {})
        if isinstance(last_trigger, dict):
            last_type = str(last_trigger.get("type", "") or "").strip()
            last_at = str(last_trigger.get("at", "") or "").strip()
            last_dt = parse_kst_dt(last_at)
            if last_type == kind and last_dt is not None:
                gap_min = (now_dt - last_dt).total_seconds() / 60.0
                if gap_min < DYNAMIC_EXIT_COOLDOWN_MIN:
                    continue

        st["last_trigger"] = {
            "type": kind,
            "at": _now_kst_str(),
            "pnl_rate": round(pnl, 4),
        }
        positions[ticker] = st

        threshold = tp if kind == "take_profit" else sl
        out.append(
            {
                "action": "SELL",
                "ticker": ticker,
                "ticker_name": str(st.get("ticker_name", "") or ""),
                "quantity": int(qty),
                "order_type": "LIMIT",
                "price_type": "bidp1",
                "price": 0,
                "confidence": 0.99,
                "venue_preference": "SOR",
                "reasoning": (
                    f"dynamic_exit_{kind}: pnl={pnl:.2f}% threshold={threshold*100:.2f}% "
                    f"(LLM-updated target)"
                )[:280],
                "take_profit_pct": tp,
                "stop_loss_pct": sl,
                "event_type": "dynamic_exit",
                "event_signature": f"dynexit:{kind}:{ticker}:{now_dt.strftime('%Y%m%d%H%M')}",
                "time_horizon": str(st.get("time_horizon", "intraday") or "intraday"),
                "lag_hours": 0,
                "channels": ["risk"],
                "thesis_path": "dynamic_exit_protection",
                "invalidation": str(st.get("invalidation", "") or "")[:280],
                "evidence_refs": [f"holding_pnl={pnl:.2f}%"],
                "evidence_urls": [],
            }
        )
    return out


def ch_url() -> str:
    sep = "&" if "?" in CLICKHOUSE_HOST else "?"
    return f"{CLICKHOUSE_HOST}{sep}user={CLICKHOUSE_USER}&password={CLICKHOUSE_PASS}"


def ch_scalar(query: str) -> str | None:
    payload = (query.strip() + "\nFORMAT TSV").encode("utf-8")
    req = request.Request(ch_url(), data=payload, method="POST")
    try:
        with request.urlopen(req, timeout=10) as resp:
            return resp.read().decode("utf-8").strip()
    except Exception:
        return None


def ch_select(query: str) -> list[dict[str, Any]]:
    payload = (query.strip() + "\nFORMAT JSON").encode("utf-8")
    req = request.Request(ch_url(), data=payload, method="POST")
    try:
        with request.urlopen(req, timeout=12) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="replace"))
            data = body.get("data", [])
            return data if isinstance(data, list) else []
    except Exception:
        return []


def ch_execute(query: str) -> bool:
    payload = query.strip().encode("utf-8")
    req = request.Request(ch_url(), data=payload, method="POST")
    try:
        with request.urlopen(req, timeout=10):
            return True
    except Exception:
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


def get_stage2_market_flow_shock(lookback_days: int = 5) -> dict[str, Any]:
    n = max(3, int(lookback_days))
    rows = ch_select(
        f"""
SELECT
  investor_type,
  sum(toFloat64(net_buy_value_krw)) AS net_buy_krw,
  sum(toFloat64(market_traded_value_krw)) AS traded_krw,
  anyHeavy(market_traded_value_krw_source) AS denom_source,
  max(market_traded_value_krw_universe_n) AS universe_n
FROM trading.market_flow_daily
WHERE market = 'ALL'
  AND investor_type IN ('FOREIGN', 'INST')
  AND trade_date >= addDays(today(), -{n + 2})
  AND trade_date <= today()
GROUP BY investor_type
"""
    )
    by_inv: dict[str, dict[str, Any]] = {}
    for r in rows:
        inv = str(r.get("investor_type", "") or "").upper().strip()
        if inv:
            by_inv[inv] = r
    fr = by_inv.get("FOREIGN", {})
    ins = by_inv.get("INST", {})
    fr_traded = to_float(fr.get("traded_krw"), 0.0)
    in_traded = to_float(ins.get("traded_krw"), 0.0)
    if fr_traded <= 0.0 or in_traded <= 0.0:
        return {
            "valid": False,
            "shock_level": "UNKNOWN",
            "foreign_pct": 0.0,
            "inst_pct": 0.0,
            "reason": "DENOM_ZERO_OR_MISSING",
            "lookback_days": n,
        }
    fr_pct = (to_float(fr.get("net_buy_krw"), 0.0) / fr_traded) * 100.0
    in_pct = (to_float(ins.get("net_buy_krw"), 0.0) / in_traded) * 100.0
    shock_abs = max(abs(fr_pct), abs(in_pct))
    if shock_abs > 12.0:
        level = "EXTREME"
    elif shock_abs > 8.0:
        level = "ALERT"
    elif shock_abs > 3.0:
        level = "WARN"
    else:
        level = "PASS"
    return {
        "valid": True,
        "shock_level": level,
        "foreign_pct": round(fr_pct, 4),
        "inst_pct": round(in_pct, 4),
        "lookback_days": n,
        "denom_source_foreign": str(fr.get("denom_source", "") or ""),
        "denom_source_inst": str(ins.get("denom_source", "") or ""),
        "universe_n_foreign": to_int(fr.get("universe_n"), 0),
        "universe_n_inst": to_int(ins.get("universe_n"), 0),
    }


def sql_quote(v: Any) -> str:
    s = str(v if v is not None else "")
    s = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def parse_kst_dt(ts: str) -> datetime | None:
    if not ts:
        return None
    s = ts.strip().replace("T", " ")
    if "." in s:
        s = s.split(".", 1)[0]
    try:
        d = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return d.replace(tzinfo=KST)
    except Exception:
        return None


def get_flow_context_recent(ticker: str) -> list[dict[str, Any]]:
    if ticker in FLOW_CONTEXT_CACHE:
        return FLOW_CONTEXT_CACHE.get(ticker, []) or []

    rows = ch_select(
        "SELECT "
        "toDate(ts) AS flow_date, "
        "sum(toFloat64OrNull(foreign_net_flow)) AS frgn_ntby_qty, "
        "sum(toFloat64OrNull(inst_net_flow)) AS pgtr_ntby_qty "
        f"FROM trading.v_feature_snapshot "
        f"WHERE symbol='{ticker}' AND toDate(ts) >= today()-{FLOW_EXPLAIN_BYPASS_DAYS} "
        "GROUP BY flow_date "
        "ORDER BY flow_date DESC "
        "LIMIT 10"
    )
    if not isinstance(rows, list):
        rows = []
    FLOW_CONTEXT_CACHE[ticker] = rows
    return rows


def is_flow_explain_bypass(ticker: str, rsi_value: float | None) -> bool:
    if not is_six_digit_ticker(ticker):
        return False
    if rsi_value is not None and rsi_value > 70:
        return False

    rows = get_flow_context_recent(ticker)
    if not rows:
        return False

    today = rows[0]
    today_frgn = abs(to_float(today.get("frgn_ntby_qty", 0.0), 0.0))
    today_inst = abs(to_float(today.get("pgtr_ntby_qty", 0.0), 0.0))
    if FLOW_EXPLAIN_DAILY_THRESHOLD > 0:
        if today_frgn >= FLOW_EXPLAIN_DAILY_THRESHOLD or today_inst >= FLOW_EXPLAIN_DAILY_THRESHOLD:
            return True

    if FLOW_EXPLAIN_TRADE_SIZE_THRESHOLD > 0:
        if today_frgn >= FLOW_EXPLAIN_TRADE_SIZE_THRESHOLD:
            return True

    check_rows = rows[:FLOW_EXPLAIN_BYPASS_DAYS]
    if len(check_rows) >= FLOW_EXPLAIN_BYPASS_DAYS:
        if all(to_float(row.get("frgn_ntby_qty", 0.0), 0.0) > 0 for row in check_rows):
            return True
        if all(to_float(row.get("pgtr_ntby_qty", 0.0), 0.0) > 0 for row in check_rows):
            return True

    return False


def parse_date_str(s: str) -> date | None:
    raw = (s or "").strip()
    if not raw:
        return None
    try:
        if " " in raw:
            raw = raw.split(" ", 1)[0]
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except Exception:
        return None


def get_data_freshness(session_id: str) -> dict[str, Any]:
    now = now_kst()
    today = now.date()
    is_active = session_id in ACTIVE_SESSIONS
    out: dict[str, Any] = {
        "stale": False,
        "block": False,
        "session_active": is_active,
        "max_age_min": None,
        "detail": [],
    }

    max_age_min = 0.0

    # 1) 뉴스 신선도: 수집시각 기준
    news_ts = ch_scalar(
        "SELECT toString(greatest("
        "ifNull((SELECT max(toTimeZone(collected_at,'Asia/Seoul')) FROM trading.news_raw), toDateTime('1970-01-01 00:00:00')),"
        "ifNull((SELECT max(toTimeZone(collected_at,'Asia/Seoul')) FROM trading.news), toDateTime('1970-01-01 00:00:00'))"
        "))"
    ) or ""
    news_dt = parse_kst_dt(news_ts)
    if news_dt is None:
        out["stale"] = True
        if is_active or ENFORCE_STALE_WHEN_CLOSED:
            out["block"] = True
        out["detail"].append(
            {
                "source": "news_raw",
                "type": "timestamp",
                "max_ts": news_ts,
                "age_min": None,
                "threshold_min": NEWS_MAX_AGE_MIN,
                "ok": False,
            }
        )
    else:
        age_min = max(0.0, (now - news_dt).total_seconds() / 60.0)
        max_age_min = max(max_age_min, age_min)
        ok = age_min <= NEWS_MAX_AGE_MIN
        if not ok:
            out["stale"] = True
            if is_active or ENFORCE_STALE_WHEN_CLOSED:
                out["block"] = True
        out["detail"].append(
            {
                "source": "news_raw",
                "type": "timestamp",
                "max_ts": news_ts,
                "age_min": round(age_min, 2),
                "threshold_min": NEWS_MAX_AGE_MIN,
                "ok": ok,
            }
        )

    # 2) 기술지표 신선도: 최신 거래일 date 기준
    tech_date_s = ch_scalar("SELECT toString(max(date)) FROM trading.technical_signals") or ""
    tech_date = parse_date_str(tech_date_s)
    if tech_date is None:
        out["stale"] = True
        if is_active or ENFORCE_STALE_WHEN_CLOSED:
            out["block"] = True
        out["detail"].append(
            {
                "source": "technical_signals",
                "type": "date",
                "max_date": tech_date_s,
                "age_days": None,
                "threshold_days": TECH_MAX_AGE_DAYS,
                "ok": False,
            }
        )
    else:
        age_days = max(0, (today - tech_date).days)
        ok = age_days <= TECH_MAX_AGE_DAYS
        if not ok:
            out["stale"] = True
            if is_active or ENFORCE_STALE_WHEN_CLOSED:
                out["block"] = True
        out["detail"].append(
            {
                "source": "technical_signals",
                "type": "date",
                "max_date": tech_date_s,
                "age_days": age_days,
                "threshold_days": TECH_MAX_AGE_DAYS,
                "ok": ok,
            }
        )

    # 3) 레짐 신선도: 최신 거래일 date 기준
    regime_date_s = ch_scalar("SELECT toString(max(date)) FROM trading.market_regime") or ""
    regime_date = parse_date_str(regime_date_s)
    if regime_date is None:
        out["stale"] = True
        if is_active or ENFORCE_STALE_WHEN_CLOSED:
            out["block"] = True
        out["detail"].append(
            {
                "source": "market_regime",
                "type": "date",
                "max_date": regime_date_s,
                "age_days": None,
                "threshold_days": REGIME_MAX_AGE_DAYS,
                "ok": False,
            }
        )
    else:
        age_days = max(0, (today - regime_date).days)
        ok = age_days <= REGIME_MAX_AGE_DAYS
        if not ok:
            out["stale"] = True
            if is_active or ENFORCE_STALE_WHEN_CLOSED:
                out["block"] = True
        out["detail"].append(
            {
                "source": "market_regime",
                "type": "date",
                "max_date": regime_date_s,
                "age_days": age_days,
                "threshold_days": REGIME_MAX_AGE_DAYS,
                "ok": ok,
            }
        )

    out["max_age_min"] = round(max_age_min, 2)
    return out


def load_kill_state() -> dict[str, Any]:
    default = {
        "state": "NORMAL",
        "updated_at": "",
        "reason_code": "",
    }
    try:
        if KILL_STATE_FILE.exists():
            raw = json.loads(KILL_STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return {**default, **raw}
    except Exception:
        pass
    return default


def log_rule_kill_switch(
    rule_reason: str,
    kill_name: str,
    decision_id: str,
    session_id: str,
    action: str,
    ticker: str,
    qty: int,
    pnl_rate: float,
    confidence: float,
) -> None:
    if not rule_reason:
        return
    log_kill_switch_event(
        state_from=kill_name,
        state_to=kill_name,
        reason_code=rule_reason,
        metrics={
            "decision_id": decision_id,
            "session_id": session_id,
            "action": action,
            "ticker": ticker,
            "qty": int(qty),
            "pnl_rate": round(pnl_rate, 4),
            "confidence": round(confidence, 4),
        },
    )


def load_adaptive_policy() -> dict[str, Any]:
    def _norm_daily_order_limit(v: Any, fallback: int) -> int:
        n = to_int(v, fallback)
        if n <= 0:
            return 0  # unlimited
        return max(1, min(50, n))

    policy = {
        "mode": "normal",
        "min_confidence": clamp(DEFAULT_MIN_CONFIDENCE, 0.55, 0.9),
        "min_cash_ratio": clamp(DEFAULT_MIN_CASH_RATIO, 0.10, 0.40),
        "order_cap_mult": clamp(DEFAULT_ORDER_CAP_MULT, 0.50, 1.20),
        "daily_order_limit": _norm_daily_order_limit(DEFAULT_DAILY_ORDER_LIMIT, 3),
        "position_weight_limit": clamp(DEFAULT_POSITION_WEIGHT_LIMIT, 0.10, 0.60),
        "updated_at": "",
        "source": "default",
    }
    try:
        if ADAPTIVE_POLICY_FILE.exists():
            raw = json.loads(ADAPTIVE_POLICY_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                policy["mode"] = str(raw.get("mode", policy["mode"]))
                policy["min_confidence"] = clamp(
                    to_float(raw.get("min_confidence", policy["min_confidence"]), policy["min_confidence"]),
                    0.55,
                    0.9,
                )
                policy["min_cash_ratio"] = clamp(
                    to_float(raw.get("min_cash_ratio", policy["min_cash_ratio"]), policy["min_cash_ratio"]),
                    0.10,
                    0.40,
                )
                policy["order_cap_mult"] = clamp(
                    to_float(raw.get("order_cap_mult", policy["order_cap_mult"]), policy["order_cap_mult"]),
                    0.50,
                    1.20,
                )
                policy["daily_order_limit"] = _norm_daily_order_limit(
                    raw.get("daily_order_limit", policy["daily_order_limit"]),
                    policy["daily_order_limit"],
                )
                policy["position_weight_limit"] = clamp(
                    to_float(raw.get("position_weight_limit", policy["position_weight_limit"]), policy["position_weight_limit"]),
                    0.10,
                    0.60,
                )
                policy["updated_at"] = str(raw.get("updated_at", ""))
                policy["source"] = "adaptive_policy_file"
    except Exception:
        pass
    return policy


def detect_market_session_fallback() -> str:
    hhmm = kst_time_hhmm()
    if hhmm < 800:
        return "CLOSED"
    if 800 <= hhmm < 850:
        return "NXT_PREMARKET"
    if 850 <= hhmm < 900:
        return "KRX_OPEN_AUCTION"
    if 900 <= hhmm < 1520:
        return "REGULAR_CONTINUOUS"
    if 1520 <= hhmm < 1530:
        return "KRX_CLOSE_AUCTION"
    if 1530 <= hhmm < 1540:
        return "TRANSITION"
    if 1540 <= hhmm < 2000:
        return "NXT_AFTERMARKET"
    return "CLOSED"


def detect_market_session() -> str:
    if now_kst().weekday() >= 5:
        return "CLOSED"
    q = """
    SELECT session_id
    FROM trading.session_calendar
    WHERE effective_from <= toDate(now('Asia/Seoul'))
      AND effective_to >= toDate(now('Asia/Seoul'))
      AND is_tradable = 1
      AND formatDateTime(trade_start, '%H:%M:%S') <= formatDateTime(now('Asia/Seoul'), '%H:%M:%S')
      AND formatDateTime(now('Asia/Seoul'), '%H:%M:%S') < formatDateTime(trade_end, '%H:%M:%S')
    ORDER BY
      multiIf(
        session_id='NXT_PREMARKET', 1,
        session_id='KRX_OPEN_AUCTION', 2,
        session_id='REGULAR_CONTINUOUS', 3,
        session_id='KRX_CLOSE_AUCTION', 4,
        session_id='NXT_AFTERMARKET', 5,
        99
      ),
      venue
    LIMIT 1
    """
    session_id = (ch_scalar(q) or "").strip()
    if session_id:
        return session_id
    return detect_market_session_fallback()


def is_venue_allowed(session_id: str, venue_pref: str) -> bool:
    v = (venue_pref or "SOR").upper()
    if v == "SOR":
        return True
    if v == "KRX":
        return session_id in {"KRX_OPEN_AUCTION", "REGULAR_CONTINUOUS", "KRX_CLOSE_AUCTION"}
    if v in {"NXT", "NEXTRADE"}:
        return session_id in {"NXT_PREMARKET", "REGULAR_CONTINUOUS", "NXT_AFTERMARKET"}
    return False


def log_kill_switch_event(state_from: str, state_to: str, reason_code: str, metrics: dict[str, Any]) -> None:
    q = (
        "INSERT INTO trading.kill_switch_event "
        "(ts, state_from, state_to, reason_code, metrics, auto_action) VALUES ("
        f"now(), {sql_quote(state_from)}, {sql_quote(state_to)}, {sql_quote(reason_code)}, "
        f"{sql_quote(json.dumps(metrics, ensure_ascii=False)[:12000])}, {sql_quote('order_guard')})"
    )
    ch_execute(q)


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def estimate_execution_metrics(
    action: str,
    session_id: str,
    order_type: str,
    price_type: str,
    confidence: float,
) -> tuple[float, float, float]:
    active_session = session_id in ACTIVE_SESSIONS
    if not active_session:
        base_fill = 0.30
        base_slip = 18.0
    elif session_id == "REGULAR_CONTINUOUS":
        base_fill = 0.72
        base_slip = 6.0
    elif session_id in {"KRX_OPEN_AUCTION", "KRX_CLOSE_AUCTION"}:
        base_fill = 0.62
        base_slip = 8.0
    else:
        base_fill = 0.48
        base_slip = 12.0

    ot = (order_type or "LIMIT").upper()
    pt = (price_type or "limit").lower()
    if ot == "MARKET":
        base_fill += 0.20
        base_slip += 5.0
    if pt == "mid":
        base_fill -= 0.05
        base_slip += 1.0
    if pt == "bidp1" and action == "BUY":
        base_fill -= 0.12
        base_slip -= 1.0
    if pt == "askp1" and action == "BUY":
        base_fill += 0.08
        base_slip += 2.0
    if pt == "bidp1" and action == "SELL":
        base_fill += 0.08
        base_slip += 2.0
    if pt == "askp1" and action == "SELL":
        base_fill -= 0.12
        base_slip -= 1.0

    conf_adj = (clamp(confidence, 0.0, 1.0) - 0.7) * 0.30
    p_fill = clamp(base_fill + conf_adj, 0.05, 0.99)
    slip_mean = max(1.0, base_slip)
    slip_p95 = max(slip_mean * 2.0, slip_mean + 6.0)
    return round(p_fill, 4), round(slip_mean, 2), round(slip_p95, 2)


def log_execution_pred_clickhouse(
    decision_id: str,
    session_id: str,
    attempted: list[dict[str, Any]],
) -> None:
    if not attempted:
        return
    values: list[str] = []
    for a in attempted:
        values.append(
            "("
            f"{sql_quote(decision_id)}, now(), {sql_quote(a.get('ticker',''))}, "
            f"{sql_quote(a.get('venue_preference','SOR'))}, {sql_quote(session_id)}, "
            f"{sql_quote(a.get('order_type','LIMIT'))}, {to_int(a.get('quantity',0),0)}, "
            f"{to_float(a.get('p_fill',0.0),0.0)}, {to_float(a.get('slip_mean_bps',0.0),0.0)}, "
            f"{to_float(a.get('slip_p95_bps',0.0),0.0)}, {sql_quote(a.get('exec_model_version','heuristic-v1'))}"
            ")"
        )
    q = (
        "INSERT INTO trading.execution_pred "
        "(decision_id, ts, symbol, venue, session_id, order_type, qty, p_fill, slip_mean_bps, slip_p95_bps, model_version) VALUES "
        + ",".join(values)
    )
    ch_execute(q)


def get_rsi14(ticker: str) -> float | None:
    q = (
        "SELECT rsi14 FROM trading.technical_signals "
        f"WHERE date >= today()-1 AND ticker='{ticker}' "
        "ORDER BY date DESC LIMIT 1"
    )
    out = ch_scalar(q)
    if out is None or out == "":
        return None
    return to_float(out, default=0.0)


def get_eps_if_available(ticker: str) -> float | None:
    # EPS 테이블이 환경마다 다를 수 있어 존재 시에만 확인.
    table = ch_scalar(
        "SELECT name FROM system.tables "
        "WHERE database='trading' AND name IN ('stock_fundamentals','fundamentals','financial_metrics') "
        "LIMIT 1"
    )
    if not table:
        return None

    candidates = [
        f"SELECT eps FROM trading.{table} WHERE ticker='{ticker}' ORDER BY date DESC LIMIT 1",
        f"SELECT eps_ttm FROM trading.{table} WHERE ticker='{ticker}' ORDER BY date DESC LIMIT 1",
        f"SELECT EPS FROM trading.{table} WHERE ticker='{ticker}' ORDER BY date DESC LIMIT 1",
    ]
    for q in candidates:
        out = ch_scalar(q)
        if out is not None and out != "":
            return to_float(out, default=0.0)
    return None


def get_hidden_relation_signal(ticker: str) -> dict[str, Any] | None:
    t = str(ticker or "").strip()
    if not is_six_digit_ticker(t):
        return None
    if t in RELATION_CACHE:
        return RELATION_CACHE[t]

    rows = ch_select(
        "SELECT "
        "total_relation_score, relation_bias, direct_event_score, transfer_event_score, "
        "cluster_state_score, support_events, support_clusters "
        "FROM trading.v_hidden_relation_signals "
        f"WHERE ticker='{t}' "
        "LIMIT 1"
    )
    if not rows:
        RELATION_CACHE[t] = None
        return None

    r = rows[0]
    out = {
        "total_relation_score": to_float(r.get("total_relation_score", 0.0), 0.0),
        "relation_bias": str(r.get("relation_bias", "neutral") or "neutral"),
        "direct_event_score": to_float(r.get("direct_event_score", 0.0), 0.0),
        "transfer_event_score": to_float(r.get("transfer_event_score", 0.0), 0.0),
        "cluster_state_score": to_float(r.get("cluster_state_score", 0.0), 0.0),
        "support_events": to_int(r.get("support_events", 0), 0),
        "support_clusters": to_int(r.get("support_clusters", 0), 0),
    }
    RELATION_CACHE[t] = out
    return out


def get_hidden_relation_reasoning(ticker: str) -> dict[str, Any] | None:
    t = str(ticker or "").strip()
    if not is_six_digit_ticker(t):
        return None
    if t in REASONING_CACHE:
        return REASONING_CACHE[t]

    rows = ch_select(
        "SELECT "
        "toString(asof_ts) AS asof_ts, "
        "ticker_name, "
        "confidence, "
        "causal_chain, "
        "summary, "
        "time_horizon, "
        "source_cluster, "
        "arrayStringConcat(source_tickers, ',') AS source_tickers_str "
        "FROM trading.v_hidden_relation_reasoning "
        f"WHERE ticker='{t}' "
        "LIMIT 1"
    )
    if not rows:
        REASONING_CACHE[t] = None
        return None

    r = rows[0]
    out = {
        "asof_ts": str(r.get("asof_ts", "")),
        "ticker_name": str(r.get("ticker_name", "") or ""),
        "confidence": to_float(r.get("confidence", 0.0), 0.0),
        "causal_chain": str(r.get("causal_chain", "") or "").strip(),
        "summary": str(r.get("summary", "") or "").strip(),
        "time_horizon": str(r.get("time_horizon", "") or "").strip(),
        "source_cluster": str(r.get("source_cluster", "") or "").strip(),
        "source_tickers_str": str(r.get("source_tickers_str", "") or "").strip(),
    }
    REASONING_CACHE[t] = out
    return out


def mcporter_call(expression: str) -> dict[str, Any]:
    if not Path(MCPORTER_BIN).exists():
        raise RuntimeError(f"mcporter not found: {MCPORTER_BIN}")
    cmd = [
        MCPORTER_BIN,
        "--config",
        MCP_CONFIG,
        "call",
        expression,
        "--output",
        "json",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(f"mcporter failed: {proc.stderr.strip()[:240]}")
    try:
        return json.loads(proc.stdout)
    except Exception as e:
        raise RuntimeError(f"mcporter output parse failed: {e}") from e


def build_mcp_order_expression(
    ticker: str,
    action: str,
    qty: int,
    price: int,
    order_type: str,
    price_type: str,
    venue: str,
) -> str:
    side = action.lower()
    ot = (order_type or "LIMIT").upper()
    pt = (price_type or "limit").lower()
    base_expr = f'kis-trading.order-stock(symbol: "{ticker}", quantity: {qty}, price: {price}, order_type: "{side}")'

    if ot == "MARKET":
        # Some MCP/KIS bridges support explicit execution mode override.
        # Keep side argument above as the stable field; append optional explicit price_type only when known.
        if pt != "limit" and pt:
            return base_expr[:-1] + f', order_price_type: "{pt}")'
        return base_expr

    if pt not in {"limit", "bidp1", "askp1", "mid"}:
        pt = "limit"
    extra_kwargs = []
    if pt != "limit":
        extra_kwargs.append(f'order_price_type: "{pt}"')
    if venue != "SOR":
        extra_kwargs.append(f'venue: "{venue}"')
    if not extra_kwargs:
        return base_expr
    return base_expr[:-1] + ", " + ", ".join(extra_kwargs) + ")"


def load_kis_profile_from_mcporter_config() -> dict[str, str]:
    profile = {
        "account_type": "",
        "cano": "",
        "acnt_prdt_cd": "",
    }
    try:
        p = Path(MCP_CONFIG)
        if not p.exists():
            return profile
        raw = json.loads(p.read_text(encoding="utf-8"))
        env = (
            raw.get("mcpServers", {})
            .get("kis-trading", {})
            .get("env", {})
        )
        if not isinstance(env, dict):
            return profile
        profile["account_type"] = str(env.get("KIS_ACCOUNT_TYPE", "")).upper().strip()
        profile["cano"] = str(env.get("KIS_CANO", "")).strip()
        profile["acnt_prdt_cd"] = str(env.get("KIS_ACNT_PRDT_CD", "")).strip()
    except Exception:
        pass
    return profile


def load_balance(max_retries: int = 3) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    last_err = "unknown"
    for attempt in range(1, max_retries + 1):
        bal = mcporter_call("kis-trading.inquery-balance")
        rt_cd = str(bal.get("rt_cd", ""))
        if rt_cd == "0":
            summary = {}
            output2 = bal.get("output2", [{}])
            if isinstance(output2, list) and output2:
                summary = output2[0]
            elif isinstance(output2, dict):
                summary = output2
            holdings = bal.get("output1", [])
            if not isinstance(holdings, list):
                holdings = []
            return bal, summary, holdings
        last_err = str(bal.get("msg1", ""))[:240] or f"rt_cd={rt_cd}"
        if attempt < max_retries:
            time.sleep(0.7)
    raise RuntimeError(f"balance_query_failed:{last_err}")


def load_today_orders() -> list[dict[str, Any]]:
    today = now_kst().strftime("%Y%m%d")
    data = mcporter_call(
        f'kis-trading.inquery-order-list(start_date: "{today}", end_date: "{today}")'
    )
    orders = data.get("output1", [])
    if not isinstance(orders, list):
        return []
    return orders


def resolve_order_price(ticker: str, action: str, price_type: str, raw_price: int) -> int:
    if price_type in {"limit", ""}:
        return int(raw_price)

    if price_type in {"bidp1", "askp1", "mid"}:
        ask = mcporter_call(f'kis-trading.inquery-stock-ask(symbol: "{ticker}")')
        out1 = ask.get("output1", {})
        if not isinstance(out1, dict):
            raise RuntimeError("stock ask response malformed")

        bidp1 = to_int(out1.get("bidp1", 0), 0)
        askp1 = to_int(out1.get("askp1", 0), 0)
        if price_type == "mid":
            if bidp1 > 0 and askp1 > 0:
                return int((bidp1 + askp1) / 2)
            if bidp1 > 0:
                return bidp1
            if askp1 > 0:
                return askp1
            return 0
        if price_type == "askp1":
            return askp1 if action == "BUY" else bidp1
        # bidp1 legacy mode
        return bidp1 if action == "SELL" else askp1

    return int(raw_price)


def build_order_attempt_specs(
    *,
    ticker: str,
    action: str,
    qty: int,
    order_type: str,
    price_type: str,
    raw_price: int,
    venue: str,
    is_hard_exit: bool,
) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = [
        {
            "label": "primary",
            "order_type": order_type,
            "price_type": price_type,
            "raw_price": raw_price,
        }
    ]
    if action == "SELL":
        if order_type == "LIMIT" and price_type != "bidp1":
            specs.append(
                {
                    "label": "sell_limit_bidp1_retry",
                    "order_type": "LIMIT",
                    "price_type": "bidp1",
                    "raw_price": 0,
                }
            )
        if order_type != "MARKET":
            specs.append(
                {
                    "label": "sell_market_retry",
                    "order_type": "MARKET",
                    "price_type": "bidp1",
                    "raw_price": 0,
                }
            )
    elif is_hard_exit and order_type != "MARKET":
        specs.append(
            {
                "label": "hard_exit_market_fallback",
                "order_type": "MARKET",
                "price_type": "bidp1" if action == "SELL" else "askp1",
                "raw_price": 0,
            }
        )
    return specs


def resolve_quantity(order: dict[str, Any]) -> int:
    q = to_int(order.get("quantity", 0), 0)
    if q > 0:
        return q
    qmax = to_int(order.get("qty_max", 0), 0)
    qmin = to_int(order.get("qty_min", 0), 0)
    if qmax > 0 and qmin > 0 and qmax < qmin:
        return 0
    if qmax > 0:
        return qmax
    if qmin > 0:
        return qmin
    return 0


def response_prompt_hash() -> str:
    p = Path("/tmp/gpt_prompt.txt")
    if not p.exists():
        return ""
    raw = p.read_bytes()
    return hashlib.sha256(raw).hexdigest()


def log_decision_clickhouse(response_json: dict[str, Any], result_json: dict[str, Any]) -> None:
    out_raw = json.dumps(response_json, ensure_ascii=False)[:120000]
    validator_raw = json.dumps(
        {
            "orders_total": result_json.get("orders_total", 0),
            "executed_count": len(result_json.get("executed", [])),
            "skipped_count": len(result_json.get("skipped", [])),
            "dry_run": bool(result_json.get("dry_run", False)),
        },
        ensure_ascii=False,
    )[:32000]
    q = (
        "INSERT INTO trading.decision_log "
        "(ts, model, prompt_hash, input_version, output_json, validator_result) VALUES ("
        f"now(), {sql_quote(MODEL_NAME)}, {sql_quote(response_prompt_hash())}, "
        f"{sql_quote(INPUT_VERSION)}, {sql_quote(out_raw)}, {sql_quote(validator_raw)})"
    )
    ch_execute(q)


def log_orders_clickhouse(
    attempted: list[dict[str, Any]],
    executed: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    ts: str,
) -> None:
    ok_index = {(r.get("idx"), r.get("ticker"), r.get("action")): r for r in executed}
    skip_index = {(r.get("idx"), r.get("ticker"), r.get("action")): r for r in skipped}
    for a in attempted:
        key = (a.get("idx"), a.get("ticker"), a.get("action"))
        ex = ok_index.get(key)
        sk = skip_index.get(key)
        state = "rejected"
        reject_reason = sk.get("reason", "") if sk else ""
        response_obj: dict[str, Any] = {}
        limit_price = to_int(a.get("price", 0), 0)
        if ex:
            state = str(ex.get("status", "ok"))
            limit_price = to_int(ex.get("order_price", limit_price), limit_price)
            response_obj = {
                "rt_cd": ex.get("rt_cd", ""),
                "msg1": ex.get("msg1", ""),
                "order_value": ex.get("order_value", 0),
            }
        rid = f"{ts}_{a.get('idx',0)}_{a.get('ticker','')}_{a.get('action','')}"
        q = (
            "INSERT INTO trading.order_log "
            "(order_id, ts, symbol, side, qty, limit_price, venue, state, reject_reason, request_json, response_json) VALUES ("
            f"{sql_quote(rid)}, now(), {sql_quote(a.get('ticker',''))}, {sql_quote(a.get('action',''))}, "
            f"{to_int(a.get('quantity',0),0)}, {limit_price}, {sql_quote(a.get('venue_preference','SOR'))}, "
            f"{sql_quote(state)}, {sql_quote(reject_reason[:400])}, "
            f"{sql_quote(json.dumps(a, ensure_ascii=False)[:12000])}, "
            f"{sql_quote(json.dumps(response_obj, ensure_ascii=False)[:12000])})"
        )
        ch_execute(q)


@dataclass
class Holding:
    qty: int
    eval_amt: float
    pnl_rate: float = 0.0


def build_holdings_map(rows: list[dict[str, Any]]) -> dict[str, Holding]:
    out: dict[str, Holding] = {}
    for r in rows:
        ticker = str(r.get("pdno", "")).strip()
        if not ticker:
            continue
        out[ticker] = Holding(
            qty=to_int(r.get("hldg_qty", 0), 0),
            eval_amt=to_float(r.get("evlu_amt", 0), 0.0),
            pnl_rate=to_float(r.get("evlu_pfls_rt", 0), 0.0),
        )
    return out


def append_journal(event: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with JOURNAL_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def count_reason_values(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("reason", row.get("status", "")) or "").strip()
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def _load_telegram_notify():
    here = Path(__file__).resolve().parent
    scripts_parent = str(here.parent)
    if scripts_parent not in sys.path:
        sys.path.insert(0, scripts_parent)
    from telegram_notify import notify  # type: ignore

    return notify


def _short(v: Any, max_len: int = 140) -> str:
    s = str(v or "").replace("\n", " ").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def _ticker_label(item: dict[str, Any]) -> str:
    ticker = str(item.get("ticker", "") or "").strip()
    name = str(item.get("ticker_name", "") or "").strip()
    if name and ticker:
        return f"{name}({ticker})"
    return ticker or name or "-"


def build_telegram_order_brief(result: dict[str, Any]) -> str:
    executed = result.get("executed", [])
    skipped = result.get("skipped", [])
    attempted = result.get("attempted", [])

    if not isinstance(executed, list):
        executed = []
    if not isinstance(skipped, list):
        skipped = []
    if not isinstance(attempted, list):
        attempted = []

    decision_id = str(result.get("decision_id", "") or "")
    payload = build_owner_payload(
        report_type="telegram_owner_execution",
        top_candidates=0,
        previous_payload_path=None,
        decision_id_override=decision_id,
    )

    reason_counts: dict[str, int] = {}
    for s in skipped:
        reason = str(s.get("reason", "unknown") or "unknown")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    top_reason_items = sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[:TELEGRAM_ORDER_BRIEF_MAX_SKIP]
    top_skip_rows = [{"skip_reason": k, "count": v} for k, v in top_reason_items]

    execution_context = payload.get("execution_context")
    if not isinstance(execution_context, dict):
        execution_context = {}
        payload["execution_context"] = execution_context
    execution_context["today_orders"] = {
        "attempted": len(attempted),
        "executed": len(executed),
        "skipped": len(skipped),
    }
    execution_context["executed_orders"] = executed[:TELEGRAM_ORDER_BRIEF_MAX_EXEC]
    execution_context["skipped_orders"] = top_skip_rows
    execution_context["skip_reason_counts"] = reason_counts

    cash_after_estimate = result.get("cash_after_estimate")
    total_asset = result.get("total_asset")
    portfolio_delta = {}
    try:
        cash_after = float(cash_after_estimate) / float(total_asset) if float(total_asset) > 0 else None
    except Exception:
        cash_after = None
    if cash_after is not None:
        portfolio_delta["cash_ratio_after"] = cash_after
    execution_context["portfolio_delta"] = portfolio_delta

    mode_context = payload.get("mode_context")
    if isinstance(mode_context, dict):
        session_id = str(result.get("session_id", "") or "").strip()
        if session_id:
            mode_context["session"] = session_id

    return render_owner_execution_message(payload)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--response", default="/tmp/gpt_response.json")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--replay-pending-exit-only", action="store_true")
    args = parser.parse_args()

    ensure_feature_snapshot_view()

    response_path = Path(args.response)

    kis_profile = load_kis_profile_from_mcporter_config()
    if REQUIRE_REAL_ACCOUNT and kis_profile.get("account_type") != "REAL":
        print(
            json.dumps(
                {
                    "status": "error",
                    "reason": "real_account_required",
                    "account_type": kis_profile.get("account_type", ""),
                },
                ensure_ascii=False,
            )
        )
        return 1

    execution_mode_state = load_execution_mode()
    if args.replay_pending_exit_only:
        data = {
            "timestamp": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
            "market_assessment": "pending exit replay",
            "regime_action": "defensive",
            "execution_mode": "close_only",
            "orders": [],
            "risk_targets": [],
            "watch_list": [],
            "portfolio_advice": "replay queued exits only",
            "self_evaluation": "system replay path",
            "next_focus": "pending exit queue",
        }
    else:
        if not response_path.exists():
            print(json.dumps({"status": "error", "reason": "response_file_missing"}))
            return 1
        data = json.loads(response_path.read_text(encoding="utf-8"))
    data = enrich_trading_response(data, execution_mode_state=execution_mode_state, source_label="executor")
    if response_path.exists() and not args.replay_pending_exit_only:
        response_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    validation_errors = validate_trading_response(data, execution_mode_state=execution_mode_state)
    if validation_errors:
        print(
            json.dumps(
                {
                    "status": "error",
                    "reason": "invalid_trading_response",
                    "validation_errors": validation_errors[:20],
                },
                ensure_ascii=False,
            )
        )
        return 1
    missing_fields = data.get("missing_fields", [])
    if isinstance(missing_fields, list) and missing_fields:
        # 데이터 부족이 명시되면 주문 실행 대신 안전 중단.
        orders = []
    else:
        orders = data.get("orders", [])
    if not isinstance(orders, list):
        orders = []

    try:
        bal_raw, summary, holdings_rows = load_balance()
    except Exception as e:
        print(json.dumps({"status": "error", "reason": f"balance_unavailable:{e}"}))
        return 1
    _ = bal_raw
    total_asset = to_float(summary.get("tot_evlu_amt", 0), 0.0)
    cash = to_float(summary.get("dnca_tot_amt", 0), 0.0)
    if total_asset <= 0:
        print(json.dumps({"status": "error", "reason": "invalid_total_asset"}))
        return 1
    adaptive_policy = load_adaptive_policy()
    execution_mode = execution_mode_state
    min_cash = total_asset * to_float(adaptive_policy.get("min_cash_ratio", 0.15), 0.15)
    base_per_order_cap = float(BASE_PER_ORDER_CAP_KRW)
    per_order_cap = 0.0
    if base_per_order_cap > 0:
        per_order_cap = base_per_order_cap * to_float(adaptive_policy.get("order_cap_mult", 1.0), 1.0)
    min_confidence = to_float(adaptive_policy.get("min_confidence", 0.7), 0.7)
    daily_order_limit = to_int(adaptive_policy.get("daily_order_limit", 3), 3)
    position_weight_limit = clamp(
        to_float(adaptive_policy.get("position_weight_limit", 0.25), 0.25),
        0.10,
        0.60,
    )

    today_orders = load_today_orders()
    holdings = build_holdings_map(holdings_rows)
    exit_state = load_exit_state()
    risk_targets = extract_risk_targets(data)
    buy_order_targets = extract_buy_order_exit_targets(orders)
    updated_from_risk_targets = apply_exit_targets(
        exit_state=exit_state,
        targets=risk_targets,
        source="risk_targets",
        holdings=holdings,
    )
    updated_from_buy_fields = apply_exit_targets(
        exit_state=exit_state,
        targets=buy_order_targets,
        source="buy_order_fields",
        holdings=holdings,
    )
    hard_stop_orders = build_hard_stop_loss_orders(
        holdings=holdings,
        existing_orders=orders,
    )
    hard_tp_orders = build_hard_take_profit_orders(
        holdings=holdings,
        existing_orders=orders + hard_stop_orders,
    )
    protection_orders = build_dynamic_exit_orders(
        holdings=holdings,
        exit_state=exit_state,
        existing_orders=orders + hard_stop_orders + hard_tp_orders,
    )
    protection_orders = hard_stop_orders + hard_tp_orders + protection_orders
    if protection_orders:
        # 보호매도는 일반 주문보다 우선순위를 높인다.
        orders = protection_orders + orders
    orders, replayed_pending_exit_count = merge_pending_exit_orders(orders, session_id=detect_market_session())
    orders = sorted(
        orders,
        key=lambda row: compute_order_priority(row if isinstance(row, dict) else {}),
        reverse=True,
    )
    decision_id = (
        f"pending_exit_replay_{now_kst().strftime('%Y%m%d_%H%M%S')}"
        if args.replay_pending_exit_only
        else now_kst().strftime("%Y%m%d_%H%M%S")
    )
    kill_state = load_kill_state()
    kill_name = str(kill_state.get("state", "NORMAL")).upper()
    session_id = detect_market_session()
    freshness = get_data_freshness(session_id)
    stage2_market_shock = get_stage2_market_flow_shock(STAGE2_SHOCK_LOOKBACK_DAYS)

    if freshness.get("block"):
        log_kill_switch_event(
            state_from=kill_name,
            state_to=kill_name,
            reason_code="data_stale",
            metrics={
                "max_age_min": freshness.get("max_age_min"),
                "detail": freshness.get("detail", []),
                "session_id": session_id,
                "session_active": freshness.get("session_active"),
            },
        )

    # 일중 주문 수/미체결 상태 집계
    day_count: dict[str, int] = {}
    unfilled_side: set[tuple[str, str]] = set()
    for o in today_orders:
        ticker = str(o.get("pdno", "")).strip()
        side = "BUY" if str(o.get("sll_buy_dvsn_cd_name", "")).strip() == "매수" else "SELL"
        if ticker:
            day_count[ticker] = day_count.get(ticker, 0) + 1
        if to_int(o.get("rmn_qty", 0), 0) > 0 and ticker:
            unfilled_side.add((ticker, side))

    attempted: list[dict[str, Any]] = []
    executed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    retry_state = load_retry_state()
    mode_name = str(execution_mode.get("execution_mode", "normal") or "normal")
    max_buys_per_run = max(0, to_int(execution_mode.get("max_buys_per_run", 3), 3))
    max_new_positions = max(0, to_int(execution_mode.get("max_new_positions", 3), 3))
    buy_count_this_run = 0
    new_position_buy_count = 0

    for idx, o in enumerate(orders, start=1):
        action = str(o.get("action", "")).upper().strip()
        ticker = str(o.get("ticker", "")).strip()
        qty = resolve_quantity(o)
        raw_price = to_int(o.get("price", 0), 0)
        price_type = str(o.get("price_type", "")).lower().strip()
        if not price_type:
            pref = str(o.get("price_reference", "")).upper().strip()
            if pref == "BID":
                price_type = "bidp1"
            elif pref == "ASK":
                price_type = "askp1"
            elif pref == "MID":
                price_type = "mid"
            else:
                price_type = "limit"
        conf = to_float(o.get("confidence", 0.0), 0.0)
        venue_pref = str(o.get("venue_preference", "SOR")).upper().strip() or "SOR"
        order_type = str(o.get("order_type", "LIMIT")).upper().strip() or "LIMIT"
        event_signature = str(o.get("event_signature", "") or "").strip()[:64]
        event_type = str(o.get("event_type", "") or "").strip()[:40]
        strategy_family = order_strategy_family(o, execution_mode, action)
        close_only = bool(o.get("close_only", False))
        time_horizon = str(o.get("time_horizon", "") or "").strip()[:16].lower()
        lag_hours = max(0, min(336, to_int(o.get("lag_hours", 0), 0)))
        channels = normalize_text_list(o.get("channels", []), max_items=8, max_len=24)
        ticker_name = str(o.get("ticker_name", "") or "").strip()
        reasoning = str(o.get("reasoning", "") or "").strip().replace("\n", " ")[:280]
        thesis_path = str(o.get("thesis_path", "") or "").strip().replace("\n", " ")[:280]
        invalidation = str(o.get("invalidation", "") or "").strip().replace("\n", " ")[:280]
        evidence_refs, evidence_urls = parse_evidence_fields(o)
        pair = _normalize_exit_pair(o.get("take_profit_pct"), o.get("stop_loss_pct"))
        take_profit_pct = pair[0] if pair else 0.0
        stop_loss_pct = pair[1] if pair else 0.0
        rule_reason = str(o.get("rule_reason", "") or "").strip()
        rsi = get_rsi14(ticker) if action == "BUY" else None
        relation_sig = get_hidden_relation_signal(ticker)
        relation_score = to_float(relation_sig.get("total_relation_score", 0.0), 0.0) if relation_sig else 0.0
        relation_bias = str(relation_sig.get("relation_bias", "neutral")) if relation_sig else "unknown"
        relation_events = to_int(relation_sig.get("support_events", 0), 0) if relation_sig else 0
        relation_reasoning = get_hidden_relation_reasoning(ticker)
        relation_reasoning_conf = to_float(
            relation_reasoning.get("confidence", 0.0), 0.0
        ) if relation_reasoning else 0.0

        p_fill = to_float(
            o.get("expected_fill_probability", o.get("p_fill", 0.0)),
            0.0,
        )
        slip_mean = to_float(
            o.get("expected_slippage_bps", o.get("slip_mean_bps", 0.0)),
            0.0,
        )
        slip_p95 = to_float(
            o.get("slippage_p95_bps", o.get("slip_p95_bps", 0.0)),
            0.0,
        )
        exec_model_version = str(
            o.get("execution_model_version", "heuristic-v1")
        ).strip() or "heuristic-v1"
        if p_fill <= 0.0 or slip_mean <= 0.0 or slip_p95 <= 0.0:
            p_fill, slip_mean, slip_p95 = estimate_execution_metrics(
                action=action,
                session_id=session_id,
                order_type=order_type,
                price_type=price_type,
                confidence=conf,
            )

        is_hard_exit = event_type == "hard_exit"
        item = {
            "idx": idx,
            "action": action,
            "ticker": ticker,
            "ticker_name": ticker_name,
            "quantity": qty,
            "order_type": order_type,
            "price_type": price_type,
            "price": raw_price,
            "reasoning": reasoning,
            "confidence": conf,
            "venue_preference": venue_pref,
            "p_fill": p_fill,
            "slip_mean_bps": slip_mean,
            "slip_p95_bps": slip_p95,
            "exec_model_version": exec_model_version,
            "event_signature": event_signature,
            "event_type": event_type,
            "time_horizon": time_horizon,
            "lag_hours": lag_hours,
            "channels": channels,
            "thesis_path": thesis_path,
            "invalidation": invalidation,
            "evidence_refs": evidence_refs,
            "evidence_urls": evidence_urls,
            "take_profit_pct": take_profit_pct,
            "stop_loss_pct": stop_loss_pct,
            "relation_score": round(relation_score, 6),
            "relation_bias": relation_bias,
            "relation_support_events": relation_events,
            "relation_reasoning_conf": round(relation_reasoning_conf, 4),
            "relation_reasoning_chain": (relation_reasoning.get("causal_chain", "") if relation_reasoning else "")[:120],
            "relation_reasoning_summary": (relation_reasoning.get("summary", "") if relation_reasoning else "")[:160],
            "rule_reason": rule_reason,
            "execution_mode": mode_name,
            "strategy_family": strategy_family,
            "close_only": close_only,
        }
        attempted.append(item)

        # 기본 검증
        if action not in {"BUY", "SELL"}:
            skipped.append({**item, "reason": "invalid_action"})
            continue
        if action == "SELL" and session_id not in ACTIVE_SESSIONS and not args.dry_run:
            queued_count = enqueue_pending_exit(item, f"market_closed:{session_id}")
            skipped.append({**item, "reason": f"queued_exit_market_closed:{session_id}", "queue_depth": queued_count})
            record_retry_state(retry_state, item, f"queued_exit_market_closed:{session_id}")
            continue
        if session_id not in ACTIVE_SESSIONS:
            skipped.append({**item, "reason": f"market_closed:{session_id}"})
            continue
        if not is_venue_allowed(session_id, venue_pref):
            skipped.append({**item, "reason": f"venue_session_restricted:{venue_pref}:{session_id}"})
            continue
        if (not is_hard_exit) and freshness.get("block") and BLOCK_ALL_ON_STALE:
            skipped.append({**item, "reason": f"data_stale:max_age_min={freshness.get('max_age_min')}"})
            continue
        if kill_name in {"DISABLED"}:
            skipped.append({**item, "reason": f"kill_switch_state:{kill_name}"})
            continue
        if action == "BUY" and kill_name in {"HALT_NEW", "CLOSE_ONLY", "PANIC_FLATTEN"}:
            skipped.append({**item, "reason": f"kill_switch_state:{kill_name}"})
            continue
        if action == "BUY" and (close_only or mode_name == "close_only"):
            skipped.append({**item, "reason": "execution_mode_close_only"})
            continue
        if not is_six_digit_ticker(ticker):
            skipped.append({**item, "reason": "invalid_ticker"})
            continue
        if qty <= 0:
            skipped.append({**item, "reason": "invalid_quantity"})
            continue
        if conf < min_confidence:
            skipped.append({**item, "reason": "low_confidence"})
            continue
        if action == "BUY" and not allowed_buy_in_mode(execution_mode, ticker):
            skipped.append({**item, "reason": f"execution_mode_universe_block:{mode_name}"})
            continue
        if action == "BUY" and STAGE2_EXTREME_BLOCK_ENABLED:
            if str(stage2_market_shock.get("shock_level", "UNKNOWN")) == "EXTREME":
                skipped.append({**item, "reason": "stage2_extreme_shock_block"})
                continue
        if action == "BUY" and ENABLE_RELATION_SCORE_BUY_FILTER and relation_sig is not None:
            if relation_score < MIN_RELATION_SCORE_BUY:
                skipped.append({**item, "reason": f"relation_score_too_low:{relation_score:.3f}"})
                continue
        if action == "BUY" and REQUIRE_EVENT_EXPLAIN_FOR_BUY:
            missing_meta: list[str] = []
            if not thesis_path:
                missing_meta.append("thesis_path")
            if not time_horizon:
                missing_meta.append("time_horizon")
            elif time_horizon not in EVENT_HORIZON_SET:
                skipped.append({**item, "reason": f"invalid_time_horizon:{time_horizon}"})
                continue
            if len(evidence_refs) < MIN_EVENT_EVIDENCE_REFS and len(evidence_urls) == 0:
                missing_meta.append("evidence")
            if missing_meta:
                if action == "BUY" and is_flow_explain_bypass(ticker=ticker, rsi_value=rsi):
                    item["rule_reason"] = "Foreign_NetBuy_Bypass"
                else:
                    skipped.append({**item, "reason": "missing_event_explain:" + ",".join(missing_meta)})
                    continue
        if (ticker, action) in unfilled_side:
            skipped.append({**item, "reason": "same_side_unfilled_exists"})
            continue
        if daily_order_limit > 0 and day_count.get(ticker, 0) >= daily_order_limit:
            skipped.append({**item, "reason": "daily_order_limit_reached"})
            continue

        hold = holdings.get(ticker, Holding(qty=0, eval_amt=0.0))
        if action == "SELL":
            hold = refresh_live_holding(holdings, ticker)
            if hold.qty <= 0:
                skipped.append({**item, "reason": "reconciliation_holding_zero"})
                record_retry_state(retry_state, item, "reconciliation_holding_zero")
                continue
            if hold.qty < qty:
                qty = hold.qty
                item["quantity"] = int(qty)
        if action == "BUY" and hold.qty > 0 and hold.pnl_rate < 0 and bool(execution_mode.get("avg_down_block", True)):
            skipped.append({**item, "reason": "avg_down_block"})
            continue
        order_value_est = max(raw_price, 0) * qty

        if action == "BUY":
            if buy_count_this_run >= max_buys_per_run:
                skipped.append({**item, "reason": "execution_mode_buy_budget_exceeded"})
                continue
            if hold.qty <= 0 and new_position_buy_count >= max_new_positions:
                skipped.append({**item, "reason": "execution_mode_new_position_limit"})
                continue
            if kst_time_hhmm() >= 1510:
                skipped.append({**item, "reason": "buy_cutoff_after_1510"})
                continue
            if ENABLE_RSI_OVERHEAT_BLOCK and rsi is not None and rsi > 70:
                skipped.append({**item, "reason": f"rsi_too_high:{rsi:.2f}"})
                continue
            eps = get_eps_if_available(ticker)
            if eps is not None and eps < 0:
                skipped.append({**item, "reason": f"negative_eps:{eps}"})
                continue
            if per_order_cap > 0 and order_value_est > per_order_cap:
                skipped.append({**item, "reason": "per_order_cap_exceeded"})
                continue
            if (cash - order_value_est) < min_cash:
                skipped.append({**item, "reason": "min_cash_ratio_violation"})
                continue
            if total_asset > 0 and ((hold.eval_amt + order_value_est) / total_asset) > position_weight_limit:
                skipped.append({**item, "reason": "position_weight_limit_violation"})
                continue

        if args.dry_run:
            executed.append(
                {**item, "status": "dry_run", "order_price": raw_price, "order_value": order_value_est}
            )
            continue

        try:
            order_attempts: list[tuple[str, str]] = []
            order_specs = build_order_attempt_specs(
                ticker=ticker,
                action=action,
                qty=qty,
                order_type=order_type,
                price_type=price_type,
                raw_price=raw_price,
                venue=venue_pref,
                is_hard_exit=is_hard_exit,
            )

            ok = False
            last_msg = ""
            for spec in order_specs:
                attempt_label = str(spec.get("label", "primary") or "primary")
                attempt_order_type = str(spec.get("order_type", order_type) or order_type)
                attempt_price_type = str(spec.get("price_type", price_type) or price_type)
                attempt_raw_price = to_int(spec.get("raw_price", raw_price), raw_price)
                try:
                    attempt_price = 0 if attempt_order_type == "MARKET" else resolve_order_price(
                        ticker, action, attempt_price_type, attempt_raw_price
                    )
                except Exception as e:
                    last_msg = f"price_resolution_failed:{e}"
                    if attempt_order_type == "MARKET":
                        attempt_price = 0
                    else:
                        continue
                if attempt_price <= 0 and attempt_order_type != "MARKET":
                    last_msg = "invalid_order_price"
                    continue
                order_value = attempt_price * qty
                expr = build_mcp_order_expression(
                    ticker=ticker,
                    action=action,
                    qty=qty,
                    price=attempt_price,
                    order_type=attempt_order_type,
                    price_type=attempt_price_type,
                    venue=venue_pref,
                )
                order_attempts.append((attempt_label, expr))
                try:
                    res = mcporter_call(expr)
                except Exception as e:
                    last_msg = str(e)
                    continue
                rt_cd = str(res.get("rt_cd", ""))
                msg1 = str(res.get("msg1", ""))
                if rt_cd == "0":
                    if item.get("rule_reason"):
                        log_rule_kill_switch(
                            rule_reason=str(item.get("rule_reason", "")),
                            kill_name=kill_name,
                            decision_id=decision_id,
                            session_id=session_id,
                            action=action,
                            ticker=ticker,
                            qty=int(qty),
                            pnl_rate=float(holdings.get(ticker, Holding(qty=0, eval_amt=0.0)).pnl_rate),
                            confidence=conf,
                        )
                    executed.append(
                        {
                            **item,
                            "status": "ok",
                            "order_price": attempt_price,
                            "order_value": order_value,
                            "rt_cd": rt_cd,
                            "msg1": msg1,
                            "order_call_attempts": order_attempts,
                            "order_call_success_attempt": attempt_label,
                        }
                    )
                    day_count[ticker] = day_count.get(ticker, 0) + 1
                    if action == "BUY":
                        buy_count_this_run += 1
                        if hold.qty <= 0:
                            new_position_buy_count += 1
                        cash -= order_value
                        holdings[ticker] = Holding(
                            qty=hold.qty + qty,
                            eval_amt=hold.eval_amt + order_value,
                            pnl_rate=hold.pnl_rate,
                        )
                        if take_profit_pct > 0.0 and stop_loss_pct < 0.0:
                            apply_exit_targets(
                                exit_state=exit_state,
                                targets=[
                                    {
                                        "ticker": ticker,
                                        "ticker_name": str(o.get("ticker_name", "") or ""),
                                        "take_profit_pct": take_profit_pct,
                                        "stop_loss_pct": stop_loss_pct,
                                        "confidence": conf,
                                        "time_horizon": time_horizon,
                                        "reasoning": str(o.get("reasoning", "") or ""),
                                        "invalidation": invalidation,
                                    }
                                ],
                                source="executed_buy_order",
                                holdings=holdings,
                            )
                    else:
                        cash += order_value
                        next_qty = max(hold.qty - qty, 0)
                        holdings[ticker] = Holding(
                            qty=next_qty,
                            eval_amt=max(hold.eval_amt - order_value, 0.0),
                            pnl_rate=hold.pnl_rate,
                        )
                        if next_qty <= 0:
                            pos = exit_state.get("positions", {})
                            if isinstance(pos, dict):
                                pos.pop(ticker, None)
                        queued = load_pending_exit_orders()
                        if queued:
                            sig = pending_exit_signature(item)
                            queued = [r for r in queued if str(r.get("queue_signature", "") or "") != sig]
                            save_pending_exit_orders(queued)
                    ok = True
                    break
                last_msg = msg1
            if not ok:
                record_retry_state(retry_state, item, f"broker_reject:{last_msg}" if last_msg else "broker_reject:unknown")
                skipped.append(
                    {
                        **item,
                        "reason": f"broker_reject:{last_msg}" if last_msg else "broker_reject:unknown",
                        "order_call_attempts": order_attempts,
                    }
                )
        except Exception as e:
            record_retry_state(retry_state, item, f"execution_error:{e}")
            skipped.append({**item, "reason": f"execution_error:{e}"})

    log_execution_pred_clickhouse(decision_id, session_id, attempted)
    save_exit_state(exit_state)
    save_retry_state(retry_state)

    result = {
        "timestamp": now_kst().isoformat(),
        "decision_id": decision_id,
        "dry_run": args.dry_run,
        "execution_mode": execution_mode,
        "session_id": session_id,
        "kill_switch_state": kill_name,
        "account_profile": {
            "account_type": kis_profile.get("account_type", ""),
            "cano_tail": kis_profile.get("cano", "")[-4:] if kis_profile.get("cano") else "",
            "acnt_prdt_cd": kis_profile.get("acnt_prdt_cd", ""),
        },
        "adaptive_policy": adaptive_policy,
        "stage2_market_flow_shock": stage2_market_shock,
        "data_freshness": freshness,
        "missing_fields": missing_fields if isinstance(missing_fields, list) else [],
        "validation_errors": [],
        "orders_total": len(orders),
        "attempted": attempted,
        "executed": executed,
        "skipped": skipped,
        "skip_reason_counts": count_reason_values(skipped),
        "executed_status_counts": count_reason_values(executed),
        "dynamic_exit": {
            "state_file": str(EXIT_STATE_FILE),
            "risk_targets_input": len(risk_targets),
            "updated_from_risk_targets": int(updated_from_risk_targets),
            "updated_from_buy_fields": int(updated_from_buy_fields),
            "protection_orders_added": len(protection_orders),
            "hard_stop_orders_added": len(hard_stop_orders),
            "hard_take_profit_orders_added": len(hard_tp_orders),
            "hard_stop_enabled": HARD_STOP_LOSS_ENABLED,
            "hard_stop_loss_pct": round(HARD_EMERGENCY_STOP_LOSS_PCT, 4),
            "hard_take_profit_pct": round(HARD_TAKE_PROFIT_PCT, 4),
            "hard_take_profit_ratio": round(HARD_TAKE_PROFIT_RATIO, 4),
            "hard_take_profit_min_qty": HARD_TAKE_PROFIT_MIN_QTY,
            "enforcement_enabled": ENABLE_DYNAMIC_EXIT_ENFORCEMENT,
            "cooldown_min": int(DYNAMIC_EXIT_COOLDOWN_MIN),
        },
        "pending_exit": {
            "file": str(PENDING_EXIT_FILE),
            "replayed_count": replayed_pending_exit_count,
            "queue_size": len(load_pending_exit_orders()),
        },
        "cash_after_estimate": int(cash),
        "total_asset": int(total_asset),
        "base_per_order_cap": int(base_per_order_cap),
        "per_order_cap": int(per_order_cap),
        "min_confidence": round(min_confidence, 4),
        "relation_filter_enabled": ENABLE_RELATION_SCORE_BUY_FILTER,
        "min_relation_score_buy": round(MIN_RELATION_SCORE_BUY, 4),
        "daily_order_limit": int(daily_order_limit),
        "position_weight_limit": round(position_weight_limit, 4),
        "stage2_extreme_block_enabled": bool(STAGE2_EXTREME_BLOCK_ENABLED),
        "rsi_overheat_block_enabled": bool(ENABLE_RSI_OVERHEAT_BLOCK),
    }

    EXEC_DIR.mkdir(parents=True, exist_ok=True)
    stamp = decision_id
    out_path = EXEC_DIR / f"{stamp}_orders.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    # Best-effort: decision/order 감사 로그를 ClickHouse에 남긴다.
    log_decision_clickhouse(data, result)
    log_orders_clickhouse(attempted, executed, skipped, stamp)

    append_journal(
        {
            "timestamp": now_kst().strftime("%Y-%m-%dT%H:%M:%S%z"),
            "event": "orders_execution",
            "response_file": str(response_path),
            "result_file": str(out_path),
            "orders_total": len(orders),
            "executed_count": len(executed),
            "skipped_count": len(skipped),
            "skip_reason_counts": count_reason_values(skipped),
            "dry_run": args.dry_run,
        }
    )

    if TELEGRAM_ORDER_BRIEF_ENABLED and not args.dry_run and (attempted or executed or skipped):
        try:
            notify = _load_telegram_notify()
            msg = build_telegram_order_brief(result)
            _ = bool(notify(msg))
        except Exception:
            pass

    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
