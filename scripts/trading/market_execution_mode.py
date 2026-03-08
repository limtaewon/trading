#!/usr/bin/env python3
"""Build shared market execution mode for the trading stack."""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import parse, request
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
HOME = Path.home()
STATE_FILE = HOME / ".openclaw" / "state" / "market_execution_mode.json"
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://localhost:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASS", os.getenv("CLICKHOUSE_PASSWORD", ""))
SHOCK_CORE_DEFAULT = "005930,000660,017670,069500,122630,252670"
RECOVERY_CORE_DEFAULT = "005930,000660,017670,069500,122630"
SHOCK_CORE_LIMIT = max(4, int(os.getenv("SHOCK_CORE_LIMIT", "8")))
RECOVERY_CORE_LIMIT = max(4, int(os.getenv("RECOVERY_CORE_LIMIT", "8")))
WATCHLIST_SOURCE = os.getenv("WATCHLIST_ACTIVE_SOURCE", "gpt54_shadow").strip() or "gpt54_shadow"


def now_kst() -> datetime:
    return datetime.now(KST)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _build_ch_url() -> str:
    host = (CLICKHOUSE_HOST or "http://localhost:8123").strip()
    sp = parse.urlsplit(host)
    scheme = sp.scheme or "http"
    netloc = sp.netloc or sp.path
    path = sp.path if sp.netloc else ""
    pairs = parse.parse_qsl(sp.query, keep_blank_values=True)
    if CLICKHOUSE_USER:
        pairs.append(("user", CLICKHOUSE_USER))
    if CLICKHOUSE_PASS:
        pairs.append(("password", CLICKHOUSE_PASS))
    query = parse.urlencode(pairs, doseq=True)
    return parse.urlunsplit((scheme, netloc, path or "", query, sp.fragment))


def ch_select(sql: str) -> list[dict[str, Any]]:
    payload = (sql.strip() + "\nFORMAT JSON").encode("utf-8")
    req = request.Request(_build_ch_url(), data=payload, method="POST")
    try:
        with request.urlopen(req, timeout=12) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="replace"))
            data = body.get("data", [])
            return data if isinstance(data, list) else []
    except Exception:
        return []


def detect_market_session() -> str:
    hm = int(now_kst().strftime("%H%M"))
    wd = now_kst().weekday()
    if wd >= 5:
        return "WEEKEND_CLOSED"
    if hm < 830:
        return "PREMARKET_CLOSED"
    if 830 <= hm < 900:
        return "NXT_PREMARKET"
    if 900 <= hm < 1520:
        return "REGULAR_CONTINUOUS"
    if 1520 <= hm < 1530:
        return "KRX_CLOSE_AUCTION"
    if 1530 <= hm < 1800:
        return "NXT_AFTERMARKET"
    return "CLOSED"


def load_previous_state() -> dict[str, Any]:
    try:
        if STATE_FILE.exists():
            raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return raw
    except Exception:
        pass
    return {}


def _parse_csv_tickers(raw: str) -> list[str]:
    out: list[str] = []
    for token in str(raw or "").split(","):
        s = token.strip()
        if len(s) == 6 and s.isdigit() and s not in out:
            out.append(s)
    return out


def load_balance_tickers() -> list[str]:
    rows = ch_select(
        """
SELECT symbol
FROM trading.position_snapshot
ORDER BY snapshot_time DESC
LIMIT 20
"""
    )
    out: list[str] = []
    for row in rows:
        s = str(row.get("symbol", "") or "").strip()
        if len(s) == 6 and s.isdigit() and s not in out:
            out.append(s)
    return out


def load_watchlist_tickers(limit: int = 12) -> list[str]:
    rows = ch_select(
        f"""
SELECT ticker
FROM trading.interest_watchlist
WHERE source = '{WATCHLIST_SOURCE}'
ORDER BY rank ASC, updated_at DESC
LIMIT {int(limit)}
"""
    )
    out: list[str] = []
    for row in rows:
        s = str(row.get("ticker", "") or "").strip()
        if len(s) == 6 and s.isdigit() and s not in out:
            out.append(s)
    return out


def build_core(base: list[str], holdings: list[str], watchlist: list[str], limit: int) -> list[str]:
    out: list[str] = []
    for group in (base, holdings, watchlist):
        for ticker in group:
            if ticker not in out and len(ticker) == 6 and ticker.isdigit():
                out.append(ticker)
            if len(out) >= limit:
                return out
    return out


def build_mode_snapshot() -> dict[str, Any]:
    session_id = detect_market_session()
    prev = load_previous_state()
    latest = ch_select(
        """
SELECT
  decision_id,
  decision_time,
  stage1_score,
  stage2_score,
  stage_debug_json
FROM trading.decision_run
ORDER BY decision_time DESC
LIMIT 1
"""
    )
    stage_debug = {}
    hard_riskoff = False
    action_posture = "normal"
    stress_flags = ""
    shock_level = "UNKNOWN"
    latest_decision_id = ""
    latest_decision_time = ""
    if latest:
        row = latest[0]
        latest_decision_id = str(row.get("decision_id", "") or "")
        latest_decision_time = str(row.get("decision_time", "") or "")
        try:
            stage_debug = json.loads(str(row.get("stage_debug_json", "") or "{}"))
        except Exception:
            stage_debug = {}
        s1 = stage_debug.get("stage1", {}) if isinstance(stage_debug, dict) else {}
        s2 = stage_debug.get("stage2", {}) if isinstance(stage_debug, dict) else {}
        hard_riskoff = bool(s1.get("hard_riskoff", False))
        action_posture = str(s1.get("action_posture", "normal") or "normal").lower()
        stress_flags = str(s1.get("stress_flags", "") or "")
        shock_level = str(s2.get("shock_level", "UNKNOWN") or "UNKNOWN").upper()

    reason_codes: list[str] = []
    if hard_riskoff:
        reason_codes.append("HARD_RISKOFF")
    if shock_level in {"WARN", "ALERT", "EXTREME"}:
        reason_codes.append(f"STAGE2_{shock_level}")
    if action_posture == "defensive":
        reason_codes.append("ACTION_POSTURE_DEFENSIVE")
    if "SIDECAR" in stress_flags.upper():
        reason_codes.append("SIDECAR_RISK")

    prev_mode = str(prev.get("execution_mode", "normal") or "normal")
    mode = "normal"
    if shock_level == "EXTREME":
        mode = "close_only"
    elif hard_riskoff or shock_level == "ALERT" or action_posture == "defensive":
        mode = "shock"
    elif prev_mode in {"shock", "close_only", "recovery"} and shock_level in {"PASS", "WARN"} and not hard_riskoff:
        mode = "recovery"

    holdings = load_balance_tickers()
    watchlist = load_watchlist_tickers()
    shock_core_base = _parse_csv_tickers(os.getenv("SHOCK_CORE_TICKERS", SHOCK_CORE_DEFAULT))
    recovery_core_base = _parse_csv_tickers(os.getenv("RECOVERY_CORE_TICKERS", RECOVERY_CORE_DEFAULT))
    shock_core = build_core(shock_core_base, holdings, watchlist, SHOCK_CORE_LIMIT)
    recovery_core = build_core(recovery_core_base, holdings, watchlist, RECOVERY_CORE_LIMIT)
    if mode == "close_only":
        allowed_universe = "shock_core"
        allowed_tickers: list[str] = []
        max_new_positions = 0
        max_buys_per_run = 0
        sell_urgency = "panic"
        llm_style = "portfolio_cleanup"
    elif mode == "shock":
        allowed_universe = "shock_core"
        allowed_tickers = shock_core
        max_new_positions = 1
        max_buys_per_run = 1
        sell_urgency = "high"
        llm_style = "tactical_beta"
    elif mode == "recovery":
        allowed_universe = "recovery_core"
        allowed_tickers = recovery_core
        max_new_positions = 2
        max_buys_per_run = 2
        sell_urgency = "normal"
        llm_style = "core_defensive"
    else:
        allowed_universe = "watchlist"
        allowed_tickers = []
        max_new_positions = 3
        max_buys_per_run = 3
        sell_urgency = "normal"
        llm_style = "stock_selection"

    snapshot = {
        "updated_at": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "execution_mode": mode,
        "mode_reason_codes": reason_codes,
        "allowed_universe": allowed_universe,
        "allowed_tickers": allowed_tickers,
        "max_new_positions": max_new_positions,
        "max_buys_per_run": max_buys_per_run,
        "avg_down_block": True,
        "sell_urgency": sell_urgency,
        "llm_style": llm_style,
        "session_id": session_id,
        "latest_decision_id": latest_decision_id,
        "latest_decision_time": latest_decision_time,
        "hard_riskoff": hard_riskoff,
        "action_posture": action_posture,
        "stress_flags": stress_flags,
        "shock_level": shock_level,
        "shock_core": shock_core,
        "recovery_core": recovery_core,
        "watchlist_source": WATCHLIST_SOURCE,
        "holding_tickers": holdings,
    }
    return snapshot


def save_snapshot(snapshot: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    snapshot = build_mode_snapshot()
    save_snapshot(snapshot)
    print(json.dumps(snapshot, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
