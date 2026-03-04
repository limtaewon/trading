#!/usr/bin/env python3
from __future__ import annotations

"""position_manager + KIS 잔고를 ClickHouse position_snapshot으로 동기화."""

import argparse
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import requests

from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()


def _resolve_clickhouse() -> tuple[str, tuple[str, str] | None]:
    raw_url = (
        os.environ.get("CLICKHOUSE_URL", "").strip()
        or os.environ.get("CLICKHOUSE_HOST", "").strip()
        or "http://localhost:8123"
    )
    user = os.environ.get("CLICKHOUSE_USER", "").strip()
    pw = os.environ.get("CLICKHOUSE_PASS", os.environ.get("CLICKHOUSE_PASSWORD", "")).strip()
    sp = urlsplit(raw_url)
    q = dict(parse_qsl(sp.query, keep_blank_values=True))
    if not user:
        user = (sp.username or q.get("user") or "").strip()
    if not pw:
        pw = (sp.password or q.get("password") or "").strip()
    netloc = sp.hostname or "localhost"
    if sp.port:
        netloc = f"{netloc}:{sp.port}"
    raw_url = urlunsplit((sp.scheme or "http", netloc, "", "", ""))
    auth = (user, pw) if user else None
    return raw_url, auth


CH_URL, CH_AUTH = _resolve_clickhouse()
CH_DB = os.environ.get("CLICKHOUSE_DB", "trading").strip() or "trading"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema_reporting_extensions.sql"
PM_STATE = Path(os.path.expanduser("~/.openclaw/state/position_manager_state.json"))
MCPORTER_BIN = os.getenv("MCPORTER_BIN") or "mcporter"
MCPORTER_CONFIG = os.getenv(
    "MCPORTER_CONFIG",
    os.path.expanduser("~/.openclaw/config/mcporter.json"),
)


def _sql_quote(v: Any) -> str:
    return "'" + str(v or "").replace("\\", "\\\\").replace("'", "\\'") + "'"


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def ch_exec(sql: str) -> None:
    requests.post(
        CH_URL,
        params={"database": CH_DB},
        data=(sql + "\n").encode("utf-8"),
        timeout=30,
        auth=CH_AUTH,
    ).raise_for_status()


def exec_sql_script(sql_text: str) -> None:
    for raw in (sql_text or "").split(";"):
        stmt = raw.strip()
        if not stmt:
            continue
        ch_exec(stmt)


def ensure_schema() -> None:
    if SCHEMA_PATH.exists():
        exec_sql_script(SCHEMA_PATH.read_text(encoding="utf-8"))


def load_pm_state() -> dict[str, dict[str, Any]]:
    if not PM_STATE.exists():
        return {}
    try:
        obj = json.loads(PM_STATE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}
    pos = obj.get("positions", {})
    return pos if isinstance(pos, dict) else {}


def get_kis_balance() -> list[dict[str, Any]]:
    cmd = [MCPORTER_BIN]
    if MCPORTER_CONFIG:
        cmd.extend(["--config", MCPORTER_CONFIG])
    cmd.extend(["call", "kis-trading.inquery-balance", "--output", "json"])
    try:
        r = subprocess.run(cmd, text=True, capture_output=True, timeout=35)
        if r.returncode != 0:
            return []
        obj = json.loads(r.stdout or "{}")
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            data = obj.get("output1", obj.get("output", []))
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def build_rows(balance: list[dict[str, Any]], pm_state: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for item in balance:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("pdno", item.get("ticker", "")) or "").strip()
        if len(ticker) != 6:
            continue
        qty = int(_f(item.get("hldg_qty", item.get("quantity", 0)), 0))
        if qty <= 0:
            continue
        st = pm_state.get(ticker, {})
        if not isinstance(st, dict):
            st = {}
        rows.append(
            {
                "snapshot_time": now_s,
                "ticker": ticker,
                "ticker_name": str(item.get("prdt_name", item.get("name", "")) or "").strip(),
                "qty": qty,
                "avg_price": _f(item.get("pchs_avg_pric", item.get("avg_price", 0))),
                "current_price": _f(item.get("prpr", item.get("current_price", 0))),
                "pnl_rate": _f(item.get("evlu_pfls_rt", item.get("pnl_rate", 0))),
                "eval_amount": _f(item.get("evlu_amt", item.get("eval_amount", 0))),
                "take_profit_pct": _f(st.get("take_profit_pct", 0)),
                "stop_loss_pct": _f(st.get("stop_loss_pct", 0)),
                "pm_confidence": _f(st.get("confidence", 0)),
                "thesis_status": str(st.get("thesis_status", "unknown") or "unknown"),
                "source": "kis+pm_state",
            }
        )
    return rows


def insert_rows(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    values: list[str] = []
    for r in rows:
        values.append(
            "("
            f"toDateTime({_sql_quote(r['snapshot_time'])}),"
            f"{_sql_quote(r['ticker'])},"
            f"{_sql_quote(r['ticker_name'])},"
            f"{int(r['qty'])},"
            f"{_f(r['avg_price'])},"
            f"{_f(r['current_price'])},"
            f"{_f(r['pnl_rate'])},"
            f"{_f(r['eval_amount'])},"
            f"{_f(r['take_profit_pct'])},"
            f"{_f(r['stop_loss_pct'])},"
            f"{_f(r['pm_confidence'])},"
            f"{_sql_quote(r['thesis_status'])},"
            f"{_sql_quote(r['source'])},"
            "now()"
            ")"
        )
    sql = (
        "INSERT INTO trading.position_snapshot "
        "(snapshot_time, ticker, ticker_name, qty, avg_price, current_price, pnl_rate, eval_amount, "
        "take_profit_pct, stop_loss_pct, pm_confidence, thesis_status, source, created_at) VALUES "
        + ",".join(values)
    )
    ch_exec(sql)
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="position_snapshot 동기화")
    _ = ap.parse_args()
    ensure_schema()
    pm_state = load_pm_state()
    balance = get_kis_balance()
    rows = build_rows(balance, pm_state)
    n = insert_rows(rows)
    print(f"sync_position_snapshot: holdings={len(rows)} inserted={n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
