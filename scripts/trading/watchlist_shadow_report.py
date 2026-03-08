#!/usr/bin/env python3
"""Run a shadow watchlist pass and compare it with the latest active watchlist."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env
from llm_model_config import resolve_model

bootstrap_openclaw_env()

SCRIPT_DIR = Path(__file__).resolve().parent
HOME = Path.home()
REPORT_DIR = HOME / ".openclaw" / "reports" / "watchlist_shadow"
TELEGRAM_NOTIFY = HOME / ".openclaw" / "scripts" / "telegram_notify.py"


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


def ch_select(sql: str, timeout_sec: int = 90) -> list[dict[str, Any]]:
    url, headers = _ch_url_and_headers()
    q = sql.strip() + "\nFORMAT JSON"
    req = Request(url, data=q.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        body = r.read().decode("utf-8", errors="replace")
    return json.loads(body).get("data", []) or []


def _sql_quote(s: str) -> str:
    return "'" + (s or "").replace("\\", "\\\\").replace("'", "\\'") + "'"


def _latest_run(source: str, run_id: str = "") -> dict[str, Any] | None:
    clauses = [f"source = {_sql_quote(source)}"]
    if run_id:
        clauses.append(f"run_id = {_sql_quote(run_id)}")
    rows = ch_select(
        f"""
SELECT
    run_id,
    ts,
    source,
    status,
    limit_n,
    inserted_rows,
    llm_rows,
    llm_error
FROM trading.interest_watchlist_runs
WHERE {' AND '.join(clauses)}
ORDER BY ts DESC
LIMIT 1
"""
    )
    return rows[0] if rows else None


def _load_watchlist(run_id: str, source: str) -> list[dict[str, Any]]:
    return ch_select(
        f"""
SELECT
    ticker,
    ticker_name,
    rank,
    action,
    confidence,
    context_score,
    reason
FROM trading.interest_watchlist
WHERE decision_id = {_sql_quote(run_id)}
  AND source = {_sql_quote(source)}
ORDER BY rank ASC
"""
    )


def _count_actions(rows: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        action = str(row.get("action", "") or "UNKNOWN").strip() or "UNKNOWN"
        out[action] = out.get(action, 0) + 1
    return dict(sorted(out.items(), key=lambda item: (-item[1], item[0])))


def _run_shadow_refresh(args: argparse.Namespace, run_id: str) -> tuple[int, str, str]:
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "refresh_interest_watchlist.py"),
        "--limit",
        str(args.limit),
        "--source",
        args.shadow_source,
        "--candidate-pool",
        str(args.candidate_pool),
        "--llm",
        "on",
        "--llm-timeout",
        str(args.llm_timeout),
        "--llm-cache-ttl",
        str(args.llm_cache_ttl),
        "--llm-model",
        args.model,
        "--run-id",
        run_id,
        "--replace-existing-run",
    ]
    env = os.environ.copy()
    env["CODEX_MODEL"] = args.model
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _format_ticker_list(rows: list[dict[str, Any]], limit: int = 10) -> list[str]:
    out: list[str] = []
    for row in rows[:limit]:
        ticker = str(row.get("ticker", "") or "")
        name = str(row.get("ticker_name", "") or "")
        rank = int(float(row.get("rank", 0) or 0))
        action = str(row.get("action", "") or "")
        out.append(f"{rank}. {ticker} {name} {action}".strip())
    return out


def _build_report(
    now_ts: str,
    args: argparse.Namespace,
    active_run: dict[str, Any] | None,
    shadow_run: dict[str, Any] | None,
    active_rows: list[dict[str, Any]],
    shadow_rows: list[dict[str, Any]],
    shadow_exec: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    active_map = {str(r.get("ticker", "")): r for r in active_rows}
    shadow_map = {str(r.get("ticker", "")): r for r in shadow_rows}

    active_tickers = set(active_map)
    shadow_tickers = set(shadow_map)
    overlap = sorted(active_tickers & shadow_tickers)
    active_only = [row for row in active_rows if str(row.get("ticker", "")) not in shadow_tickers]
    shadow_only = [row for row in shadow_rows if str(row.get("ticker", "")) not in active_tickers]

    rank_changes: list[dict[str, Any]] = []
    for ticker in overlap:
        a = active_map[ticker]
        s = shadow_map[ticker]
        a_rank = int(float(a.get("rank", 0) or 0))
        s_rank = int(float(s.get("rank", 0) or 0))
        rank_changes.append(
            {
                "ticker": ticker,
                "ticker_name": str(s.get("ticker_name", "") or a.get("ticker_name", "")),
                "active_rank": a_rank,
                "shadow_rank": s_rank,
                "delta": s_rank - a_rank,
                "active_action": str(a.get("action", "") or ""),
                "shadow_action": str(s.get("action", "") or ""),
            }
        )
    rank_changes.sort(key=lambda row: (abs(int(row["delta"])), row["shadow_rank"]), reverse=True)

    summary = {
        "generated_at": now_ts,
        "active_source": args.active_source,
        "shadow_source": args.shadow_source,
        "shadow_model": args.model,
        "active_run": active_run or {},
        "shadow_run": shadow_run or {},
        "shadow_exec": shadow_exec,
        "active_count": len(active_rows),
        "shadow_count": len(shadow_rows),
        "overlap_count": len(overlap),
        "active_only_count": len(active_only),
        "shadow_only_count": len(shadow_only),
        "active_actions": _count_actions(active_rows),
        "shadow_actions": _count_actions(shadow_rows),
        "top_active_only": _format_ticker_list(active_only),
        "top_shadow_only": _format_ticker_list(shadow_only),
        "top_rank_changes": rank_changes[:10],
    }

    md = [
        f"# GPT-5.4 Watchlist Shadow Report",
        "",
        f"- Generated at: {now_ts}",
        f"- Active source: `{args.active_source}`",
        f"- Shadow source: `{args.shadow_source}`",
        f"- Shadow model: `{args.model}`",
        f"- Active run id: `{(active_run or {}).get('run_id', '-')}`",
        f"- Shadow run id: `{(shadow_run or {}).get('run_id', '-')}`",
        f"- Overlap: `{len(overlap)}` / active `{len(active_rows)}` / shadow `{len(shadow_rows)}`",
        "",
        "## Action Counts",
        "",
        f"- Active: `{json.dumps(summary['active_actions'], ensure_ascii=False)}`",
        f"- Shadow: `{json.dumps(summary['shadow_actions'], ensure_ascii=False)}`",
        "",
        "## Active Only",
        "",
    ]
    md.extend(f"- {line}" for line in summary["top_active_only"] or ["-"])
    md.extend(
        [
            "",
            "## Shadow Only",
            "",
        ]
    )
    md.extend(f"- {line}" for line in summary["top_shadow_only"] or ["-"])
    md.extend(
        [
            "",
            "## Largest Rank Changes",
            "",
        ]
    )
    if summary["top_rank_changes"]:
        for row in summary["top_rank_changes"]:
            md.append(
                "- "
                f"{row['ticker']} {row['ticker_name']} "
                f"active#{row['active_rank']} -> shadow#{row['shadow_rank']} "
                f"(delta {row['delta']:+d}) "
                f"{row['active_action']} -> {row['shadow_action']}"
            )
    else:
        md.append("- none")
    md.extend(
        [
            "",
            "## Shadow Execution",
            "",
            f"- exit_code: `{shadow_exec['exit_code']}`",
            f"- stdout: `{shadow_exec['stdout'] or '-'}`",
            f"- stderr: `{shadow_exec['stderr'] or '-'}`",
            "",
        ]
    )
    return "\n".join(md), summary


def _format_action_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "-"
    return " / ".join(f"{k} {v}" for k, v in counts.items())


def _telegram_lines_for_rows(rows: list[str], limit: int = 3) -> str:
    if not rows:
        return "-"
    return "\n".join(f"• {row}" for row in rows[:limit])


def _summarize_shadow_posture(summary: dict[str, Any]) -> str:
    active_actions = summary.get("active_actions", {}) or {}
    shadow_actions = summary.get("shadow_actions", {}) or {}
    active_buy = int(active_actions.get("BUY_REVIEW", 0) or 0)
    shadow_buy = int(shadow_actions.get("BUY_REVIEW", 0) or 0)
    active_hold = int(active_actions.get("HOLD", 0) or 0)
    shadow_hold = int(shadow_actions.get("HOLD", 0) or 0)

    if shadow_buy < active_buy and shadow_hold > active_hold:
        return "섀도우 결과는 운영안보다 더 보수적입니다."
    if shadow_buy > active_buy and shadow_hold < active_hold:
        return "섀도우 결과는 운영안보다 더 공격적입니다."
    return "섀도우 결과는 운영안과 전반적 성향이 비슷합니다."


def _build_telegram_summary(summary: dict[str, Any], report_name: str) -> str:
    rank_changes = summary.get("top_rank_changes", []) or []
    top_moves: list[str] = []
    for row in rank_changes[:3]:
        top_moves.append(
            f"{row['ticker']} {row['active_action']} → {row['shadow_action']} "
            f"({int(row['active_rank'])}위 → {int(row['shadow_rank'])}위)"
        )

    lines = [
        f"<b>워치리스트 섀도우 비교</b>",
        f"모델: <code>{summary.get('shadow_model', '-')}</code>",
        _summarize_shadow_posture(summary),
        "",
        f"운영 {summary.get('active_count', 0)}개 / 섀도우 {summary.get('shadow_count', 0)}개 / 겹침 {summary.get('overlap_count', 0)}개",
        f"운영 전용 {summary.get('active_only_count', 0)}개 / 섀도우 전용 {summary.get('shadow_only_count', 0)}개",
        "",
        f"<b>액션 분포</b>",
        f"운영: {_format_action_counts(summary.get('active_actions', {}))}",
        f"섀도우: {_format_action_counts(summary.get('shadow_actions', {}))}",
        "",
        f"<b>순위 크게 바뀐 종목</b>",
        _telegram_lines_for_rows(top_moves),
        "",
        f"<b>운영안에만 있는 상위 종목</b>",
        _telegram_lines_for_rows(summary.get("top_active_only", [])),
        "",
        f"<b>섀도우에만 있는 상위 종목</b>",
        _telegram_lines_for_rows(summary.get("top_shadow_only", [])),
        "",
        f"리포트: <code>{report_name}</code>",
    ]
    return "\n".join(lines)


def _send_telegram(message: str) -> None:
    if not TELEGRAM_NOTIFY.exists():
        return
    subprocess.run([sys.executable, str(TELEGRAM_NOTIFY), message], check=False, capture_output=True, text=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--active-source", default=os.getenv("WATCHLIST_ACTIVE_SOURCE", "enrich_data"))
    ap.add_argument("--shadow-source", default=os.getenv("WATCHLIST_SHADOW_SOURCE", "gpt54_shadow"))
    ap.add_argument("--model", default=resolve_model("WATCHLIST_SHADOW_MODEL", "CODEX_MODEL"))
    ap.add_argument("--limit", type=int, default=max(5, int(os.getenv("WATCHLIST_SHADOW_LIMIT", "30"))))
    ap.add_argument("--candidate-pool", type=int, default=max(30, int(os.getenv("WATCHLIST_SHADOW_CANDIDATE_POOL", os.getenv("WATCHLIST_CANDIDATE_POOL", "200")))))
    ap.add_argument("--llm-timeout", type=int, default=max(30, int(os.getenv("WATCHLIST_SHADOW_TIMEOUT_SEC", "120"))))
    ap.add_argument("--llm-cache-ttl", type=int, default=max(0, int(os.getenv("WATCHLIST_SHADOW_CACHE_TTL_SEC", "0"))))
    ap.add_argument("--notify", action="store_true")
    args = ap.parse_args()

    now = dt.datetime.now()
    now_ts = now.strftime("%Y-%m-%d %H:%M:%S")
    run_id = f"{args.shadow_source}_{now.strftime('%Y%m%d_%H%M%S')}"

    exit_code, stdout, stderr = _run_shadow_refresh(args, run_id)
    shadow_run = _latest_run(args.shadow_source, run_id=run_id)
    active_run = _latest_run(args.active_source)

    active_rows = _load_watchlist(str(active_run.get("run_id", "") or ""), args.active_source) if active_run else []
    shadow_rows = _load_watchlist(str(shadow_run.get("run_id", "") or ""), args.shadow_source) if shadow_run else []
    markdown, summary = _build_report(
        now_ts,
        args,
        active_run,
        shadow_run,
        active_rows,
        shadow_rows,
        {"exit_code": exit_code, "stdout": stdout, "stderr": stderr},
    )

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = now.strftime("%Y%m%d_%H%M%S")
    md_path = REPORT_DIR / f"watchlist_shadow_{stamp}.md"
    json_path = REPORT_DIR / f"watchlist_shadow_{stamp}.json"
    md_path.write_text(markdown, encoding="utf-8")
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({"ok": exit_code == 0, "markdown": str(md_path), "json": str(json_path), **summary}, ensure_ascii=False))

    if args.notify or os.getenv("WATCHLIST_SHADOW_NOTIFY", "0") == "1":
        msg = _build_telegram_summary(summary, md_path.name)
        _send_telegram(msg)

    return 0 if exit_code == 0 else exit_code


if __name__ == "__main__":
    raise SystemExit(main())
