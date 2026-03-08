#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()

HOME = Path.home()
BASE = HOME / ".openclaw"
REPORT_DIR = BASE / "reports" / "weekly_market"
STOCKS_CSV = BASE / "workspace" / "STOCKS.csv"
TG_SCRIPT = BASE / "scripts" / "telegram_notify.py"

CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123").strip()
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "trading").strip() or "trading"

WAR_WINNERS = {"012450", "079550", "329180", "064350"}
DEFENSIVE_NAMES = {"017670", "032640", "105560", "055550"}
TECH_REBOUND = {"000660", "042700", "009150", "373220", "096770"}


def ch_query(sql: str) -> list[dict[str, Any]]:
    resp = requests.post(
        f"{CLICKHOUSE_URL}?database={CLICKHOUSE_DB}&default_format=JSON",
        data=sql.encode("utf-8"),
        timeout=30,
    )
    resp.raise_for_status()
    obj = json.loads(resp.text)
    data = obj.get("data", [])
    return data if isinstance(data, list) else []


def monday_of_week(d: date) -> date:
    return d - timedelta(days=d.weekday())


def friday_of_week(d: date) -> date:
    return monday_of_week(d) + timedelta(days=4)


def fmt_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.2f}%"


def pct_change(start: float | None, end: float | None) -> float | None:
    if start in (None, 0) or end is None:
        return None
    return (float(end) / float(start) - 1.0) * 100.0


def load_stock_names() -> dict[str, str]:
    mapping: dict[str, str] = {}
    if not STOCKS_CSV.exists():
        return mapping
    with STOCKS_CSV.open(encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = str(row.get("Code") or "").strip()
            name = str(row.get("Name") or "").strip()
            if code and name:
                mapping[code] = name
    return mapping


def get_market_rows(start_d: date, end_d: date) -> list[dict[str, Any]]:
    sql = f"""
SELECT date, index_code, close_price, change_pct
FROM trading.market_index
WHERE date >= '{start_d.isoformat()}' AND date <= '{end_d.isoformat()}'
  AND index_code IN ('KOSPI', 'KOSDAQ', 'VIX')
ORDER BY date, index_code
FORMAT JSON
"""
    return ch_query(sql)


def get_fx_rows(start_d: date, end_d: date) -> list[dict[str, Any]]:
    sql = f"""
SELECT date, currency_pair, close_rate, change_pct
FROM trading.exchange_rate
WHERE date >= '{start_d.isoformat()}' AND date <= '{end_d.isoformat()}'
  AND currency_pair = 'USDKRW'
ORDER BY date
FORMAT JSON
"""
    return ch_query(sql)


def get_regime_rows(start_d: date, end_d: date) -> list[dict[str, Any]]:
    sql = f"""
SELECT date, regime_label, action_posture, trend, stress_flags, summary
FROM trading.market_regime
WHERE date >= '{start_d.isoformat()}' AND date <= '{end_d.isoformat()}'
ORDER BY date
FORMAT JSON
"""
    return ch_query(sql)


def get_news_rows(start_d: date, end_d: date) -> list[dict[str, Any]]:
    sql = f"""
SELECT asof_ts, n_news, importance_max, summary
FROM trading.news_clusters
WHERE toDate(asof_ts) >= '{start_d.isoformat()}' AND toDate(asof_ts) <= '{end_d.isoformat()}'
ORDER BY n_news DESC, importance_max DESC, asof_ts DESC
LIMIT 6
FORMAT JSON
"""
    return ch_query(sql)


def get_latest_watchlist(as_of: date) -> list[dict[str, Any]]:
    sql = f"""
SELECT ticker, action, rank, confidence
FROM trading.interest_watchlist
WHERE toDate(ts) = '{as_of.isoformat()}'
ORDER BY ts DESC, rank ASC
LIMIT 10
FORMAT JSON
"""
    return ch_query(sql)


def get_latest_decision_run(as_of: date) -> dict[str, Any]:
    sql = f"""
SELECT decision_id, decision_time, stage1_score, stage_debug_json
FROM trading.decision_run
WHERE toDate(decision_time) = '{as_of.isoformat()}'
ORDER BY decision_time DESC
LIMIT 1
FORMAT JSON
"""
    rows = ch_query(sql)
    return rows[0] if rows else {}


def get_top_decisions(decision_id: str) -> list[dict[str, Any]]:
    if not decision_id:
        return []
    sql = f"""
SELECT ticker, action, total_score, target_weight
FROM trading.decision_candidate
WHERE decision_id = '{decision_id}'
  AND action = 'BUY'
ORDER BY total_score DESC
LIMIT 10
FORMAT JSON
"""
    return ch_query(sql)


def by_code(rows: list[dict[str, Any]], code_key: str) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get(code_key) or "").strip()
        out.setdefault(key, []).append(row)
    return out


def summarize_actions(
    stress_flags: list[str],
    watchlist: list[dict[str, Any]],
    top_decisions: list[dict[str, Any]],
    stock_names: dict[str, str],
) -> list[str]:
    flags = {str(x) for x in stress_flags}
    watch_codes = [str(r.get("ticker") or "").strip() for r in watchlist]
    decision_codes = [str(r.get("ticker") or "").strip() for r in top_decisions]
    merged = [c for c in watch_codes + decision_codes if c]

    war_names = [stock_names.get(c, c) for c in merged if c in WAR_WINNERS]
    defensive_names = [stock_names.get(c, c) for c in merged if c in DEFENSIVE_NAMES]
    tech_names = [stock_names.get(c, c) for c in merged if c in TECH_REBOUND]

    lines: list[str] = []
    lines.append("- 기본은 방어적 대응 유지, 초반 갭 변동성에는 분할 대응")
    if "GEOPOLITICAL_RISK" in flags or "OIL_SHOCK_RISK" in flags:
        picked = ", ".join(dict.fromkeys(war_names[:4])) or "한화에어로스페이스, LIG넥스원, HD현대중공업, 현대로템"
        lines.append(f"- 전쟁/유가 리스크 지속 시 방산·조선 우선 관찰: {picked}")
    if defensive_names:
        lines.append(f"- 변동성 완충용 방어주 축: {', '.join(dict.fromkeys(defensive_names[:4]))}")
    if tech_names:
        lines.append(f"- 환율 진정 시 기술주 반등 확인 후보: {', '.join(dict.fromkeys(tech_names[:5]))}")
    lines.append("- 월요일 시가 급락에는 즉시 풀베팅 금지, 30~60분 확인 후 진입")
    lines.append("- 반등 하루만으로 레버리지 확대하지 말고, 비중은 평소보다 작게 유지")
    return lines


def build_report(as_of: date) -> tuple[str, Path]:
    week_start = monday_of_week(as_of)
    week_end = friday_of_week(as_of)
    prev_friday = week_start - timedelta(days=3)
    next_week_start = week_start + timedelta(days=7)
    next_week_end = next_week_start + timedelta(days=4)

    stock_names = load_stock_names()
    market_rows = get_market_rows(prev_friday, week_end)
    fx_rows = get_fx_rows(prev_friday, week_end)
    regime_rows = get_regime_rows(week_start, week_end)
    news_rows = get_news_rows(week_start, week_end)
    latest_watchlist = get_latest_watchlist(week_end)
    latest_run = get_latest_decision_run(week_end)
    top_decisions = get_top_decisions(str(latest_run.get("decision_id") or ""))

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

    latest_regime = regime_rows[-1] if regime_rows else {}
    stress_flags = latest_regime.get("stress_flags") or []
    decision_debug = {}
    try:
        decision_debug = json.loads(str(latest_run.get("stage_debug_json") or "{}"))
    except Exception:
        decision_debug = {}
    stage1 = decision_debug.get("stage1") if isinstance(decision_debug, dict) else {}
    hard_riskoff = bool((stage1 or {}).get("hard_riskoff"))
    action_posture = str((stage1 or {}).get("action_posture") or latest_regime.get("action_posture") or "neutral")

    lines: list[str] = []
    lines.append(f"📘 주간 시장 보고 + 다음 주 대응 ({week_start.isoformat()}~{week_end.isoformat()})")
    lines.append("")
    lines.append("1) 이번 주 요약")
    lines.append(
        f"- 코스피 {kospi_start:,.2f} -> {kospi_end:,.2f} ({fmt_pct(pct_change(kospi_start, kospi_end))})"
        if kospi_start and kospi_end
        else "- 코스피 주간 데이터 부족"
    )
    lines.append(
        f"- 코스닥 {kosdaq_start:,.2f} -> {kosdaq_end:,.2f} ({fmt_pct(pct_change(kosdaq_start, kosdaq_end))})"
        if kosdaq_start and kosdaq_end
        else "- 코스닥 주간 데이터 부족"
    )
    if usdkrw_start and usdkrw_end:
        lines.append(f"- 원/달러 {usdkrw_start:,.2f} -> {usdkrw_end:,.2f} ({fmt_pct(pct_change(usdkrw_start, usdkrw_end))})")
    if vix_start and vix_end:
        lines.append(f"- VIX {vix_start:,.2f} -> {vix_end:,.2f} ({fmt_pct(pct_change(vix_start, vix_end))})")

    if regime_rows:
        latest_summary = str(latest_regime.get("summary") or "").strip()
        if latest_summary:
            lines.append(f"- 내부 레짐 요약: {latest_summary}")

    if news_rows:
        lines.append("")
        lines.append("2) 이번 주 핵심 이슈")
        seen: set[str] = set()
        for row in news_rows:
            summary = str(row.get("summary") or "").strip()
            if not summary or summary in seen:
                continue
            seen.add(summary)
            lines.append(f"- {summary}")
            if len(seen) >= 3:
                break

    lines.append("")
    lines.append(f"3) 다음 주 예상 ({next_week_start.isoformat()}~{next_week_end.isoformat()})")
    if hard_riskoff or action_posture == "defensive":
        lines.append("- 기본 시나리오는 높은 변동성 속 방어적 대응 유지")
    else:
        lines.append("- 기본 시나리오는 반등 연장 시도지만, 재확인 변동성은 열어둬야 함")
    if "GEOPOLITICAL_RISK" in stress_flags:
        lines.append("- 주말 중동 뉴스 악화 시 월요일 시가 갭다운 가능성 높음")
    if "OIL_SHOCK_RISK" in stress_flags:
        lines.append("- 유가 재급등 시 항공/소비 민감주보다 방산·에너지·조선이 상대 강세 가능")
    if "USDKRW>=1430" in stress_flags:
        lines.append("- 환율이 아직 높은 구간이라 기술주 반등은 확인 후 접근이 안전")
    lines.append("- 정책 안정화와 지수 안착이 동시에 나오면 주중 중반부터 낙폭과대 반등 확률 상승")

    lines.append("")
    lines.append("4) 내가 어떻게 해야 하는지")
    lines.extend(summarize_actions(list(stress_flags), latest_watchlist, top_decisions, stock_names))

    if latest_watchlist:
        lines.append("")
        lines.append("5) 우선 감시 리스트")
        for row in latest_watchlist[:5]:
            code = str(row.get("ticker") or "").strip()
            name = stock_names.get(code, code)
            action = str(row.get("action") or "").strip()
            lines.append(f"- {name}({code}) | {action}")

    if top_decisions:
        lines.append("")
        lines.append("6) 금요일 기준 내부 BUY 상위")
        for row in top_decisions[:5]:
            code = str(row.get("ticker") or "").strip()
            name = stock_names.get(code, code)
            score = float(row.get("total_score") or 0.0)
            weight = float(row.get("target_weight") or 0.0) * 100.0
            lines.append(f"- {name}({code}) | score {score:.1f} | target {weight:.1f}%")

    lines.append("")
    lines.append("한줄 결론: 다음 주는 공격보다 선별이 먼저다. 월요일 시가 급변동은 확인 후 대응하고, 전쟁 지속 시 방산·방어주, 환율 안정 시 반도체·기술주 순서로 보는 게 맞다.")

    report_text = "\n".join(lines).strip()
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORT_DIR / f"weekly_market_report_{week_end.isoformat()}.md"
    out_path.write_text(report_text + "\n", encoding="utf-8")
    return report_text, out_path


def send_telegram(text: str) -> bool:
    if not TG_SCRIPT.exists():
        raise RuntimeError(f"telegram script missing: {TG_SCRIPT}")
    ns: dict[str, Any] = {}
    exec(TG_SCRIPT.read_text(encoding="utf-8"), ns)
    notify_plain = ns.get("notify_plain")
    if not callable(notify_plain):
        raise RuntimeError("notify_plain not available")
    return bool(notify_plain(text))


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Generate weekly market summary + next-week action report")
    ap.add_argument("--as-of", default="", help="기준일 YYYY-MM-DD (기본: 오늘)")
    ap.add_argument("--send", action="store_true", help="텔레그램 전송")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if args.as_of:
        as_of = date.fromisoformat(args.as_of)
    else:
        as_of = datetime.now().date()
    text, out_path = build_report(as_of)
    print(text)
    print(f"\n[report_path] {out_path}")
    if args.send:
        ok = send_telegram(text)
        print(f"[telegram_sent] {ok}")
        return 0 if ok else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
