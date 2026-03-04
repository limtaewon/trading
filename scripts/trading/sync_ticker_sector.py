#!/usr/bin/env python3
from __future__ import annotations

"""ticker_sector 보강 동기화.

역할:
- watchlist/decision 중심 티커를 대상으로 KIS 업종(bstp_kor_isnm) 보강
- 최근 뉴스/이벤트/리서치 키워드로 theme_tags를 생성
- trading.ticker_sector에 append upsert
"""

import argparse
import json
import os
import re
import subprocess
from datetime import datetime, timedelta
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
    # ClickHouse HTTP endpoint는 루트로 고정해 404(path mismatch) 리스크를 줄인다.
    raw_url = urlunsplit((sp.scheme or "http", netloc, "", "", ""))
    auth = (user, pw) if user else None
    return raw_url, auth


CH_URL, CH_AUTH = _resolve_clickhouse()
CH_DB = os.environ.get("CLICKHOUSE_DB", "trading").strip() or "trading"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema_reporting_extensions.sql"
MCPORTER_BIN = os.getenv("MCPORTER_BIN") or "mcporter"
MCPORTER_CONFIG = os.getenv(
    "MCPORTER_CONFIG",
    os.path.expanduser("~/.openclaw/config/mcporter.json"),
)

THEME_KEYWORDS: dict[str, list[str]] = {
    "방산": ["방산", "수주", "미사일", "전투기", "탄약", "KDDX", "해군", "잠수함"],
    "조선": ["조선", "선박", "해운", "LNG선", "호르무즈", "운임"],
    "반도체": ["반도체", "D램", "HBM", "파운드리", "낸드", "메모리"],
    "2차전지": ["배터리", "2차전지", "양극재", "음극재", "LMR", "리튬"],
    "AI": ["AI", "인공지능", "데이터센터", "GPU", "클라우드"],
    "로봇": ["로봇", "자율주행", "드론"],
    "바이오": ["바이오", "신약", "임상", "의약품", "치료제"],
    "금융": ["은행", "보험", "증권", "배당", "밸류업"],
    "에너지": ["유가", "원유", "정유", "가스", "에너지", "원전", "SMR"],
    "자동차": ["자동차", "완성차", "EV", "전기차", "모빌리티"],
}


def _sql_quote(v: Any) -> str:
    return "'" + str(v or "").replace("\\", "\\\\").replace("'", "\\'") + "'"


def ch_query(sql: str) -> list[dict[str, Any]]:
    resp = requests.post(
        CH_URL,
        params={"database": CH_DB, "default_format": "JSON"},
        data=(sql + "\n").encode("utf-8"),
        timeout=30,
        auth=CH_AUTH,
    )
    resp.raise_for_status()
    if not resp.text:
        return []
    return (resp.json() or {}).get("data", [])


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


def is_ticker(v: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", v or ""))


def load_target_tickers(limit: int, include_master: bool = False) -> list[dict[str, str]]:
    rows = ch_query(
        f"""
WITH latest_w AS (SELECT max(ts) AS ts FROM trading.interest_watchlist),
     latest_d AS (SELECT max(decision_time) AS dt FROM trading.decision_run),
     latest_t AS (SELECT max(date) AS d FROM trading.technical_signals)
SELECT ticker, any(ticker_name) AS ticker_name
FROM (
  SELECT ticker, ticker_name
  FROM trading.interest_watchlist
  WHERE ts = (SELECT ts FROM latest_w)
    AND match(ticker, '^[0-9]{{6}}$')
  UNION ALL
  SELECT ticker, '' AS ticker_name
  FROM trading.decision_candidate
  WHERE decision_id = (SELECT decision_id FROM trading.decision_run WHERE decision_time=(SELECT dt FROM latest_d) LIMIT 1)
    AND match(ticker, '^[0-9]{{6}}$')
  UNION ALL
  SELECT ticker, ticker_name
  FROM trading.technical_signals
  WHERE date = (SELECT d FROM latest_t)
    AND match(ticker, '^[0-9]{{6}}$')
  LIMIT {max(100, int(limit) * 10)}
)
GROUP BY ticker
LIMIT {max(50, int(limit))}
"""
    )
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for r in rows:
        tk = str(r.get("ticker") or "").strip()
        if not is_ticker(tk) or tk in seen:
            continue
        seen.add(tk)
        out.append({"ticker": tk, "ticker_name": str(r.get("ticker_name") or "").strip()})
    if include_master:
        m_rows = ch_query(
            f"""
SELECT ticker, ticker_name
FROM trading.ticker_master_kr
WHERE match(ticker, '^[0-9]{{6}}$')
LIMIT {max(500, int(limit) * 20)}
"""
        )
        for r in m_rows:
            tk = str(r.get("ticker") or "").strip()
            if not is_ticker(tk) or tk in seen:
                continue
            seen.add(tk)
            out.append({"ticker": tk, "ticker_name": str(r.get("ticker_name") or "").strip()})
    return out


def load_recent_titles(ticker: str, days: int = 14) -> list[str]:
    rows = ch_query(
        f"""
SELECT title
FROM (
  SELECT title
  FROM trading.news
  WHERE published_at >= now() - INTERVAL {max(1, int(days))} DAY
    AND has(tickers, {_sql_quote(ticker)})
  UNION ALL
  SELECT title
  FROM trading.news_event_frames
  WHERE published_at >= now() - INTERVAL {max(1, int(days))} DAY
    AND has(tickers, {_sql_quote(ticker)})
  UNION ALL
  SELECT title
  FROM trading.news_research
  WHERE published_at >= now() - INTERVAL {max(1, int(days))} DAY
    AND (has(direct_tickers, {_sql_quote(ticker)}) OR has(secondary_tickers, {_sql_quote(ticker)}) OR has(tertiary_tickers, {_sql_quote(ticker)}))
)
LIMIT 120
"""
    )
    out: list[str] = []
    for r in rows:
        t = str(r.get("title") or "").strip()
        if t:
            out.append(t)
    return out


def infer_theme_tags(titles: list[str]) -> list[str]:
    txt = " ".join(titles)
    tags: list[str] = []
    lower = txt.lower()
    for tag, kws in THEME_KEYWORDS.items():
        for kw in kws:
            if kw.lower() in lower:
                tags.append(tag)
                break
    # stable order + dedupe
    out: list[str] = []
    seen: set[str] = set()
    for t in tags:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out[:6]


def get_cached_sector(ticker: str, max_age_days: int = 30) -> tuple[str, float]:
    rows = ch_query(
        f"""
SELECT sector, confidence
FROM trading.ticker_sector
WHERE ticker = {_sql_quote(ticker)}
  AND updated_at >= now() - INTERVAL {max(1, int(max_age_days))} DAY
ORDER BY updated_at DESC
LIMIT 1
"""
    )
    if not rows:
        return "", 0.0
    return str(rows[0].get("sector") or "").strip(), float(rows[0].get("confidence") or 0.0)


def fetch_sector_kis(ticker: str) -> tuple[str, float, str]:
    cmd = [MCPORTER_BIN]
    if MCPORTER_CONFIG:
        cmd.extend(["--config", MCPORTER_CONFIG])
    cmd.extend(["call", f'kis-trading.inquery-stock-price(symbol: "{ticker}")', "--output", "json"])
    try:
        r = subprocess.run(cmd, text=True, capture_output=True, timeout=25)
        if r.returncode != 0:
            return "", 0.0, f"kis_err:{r.returncode}"
        obj = json.loads(r.stdout or "{}")
        sector = str(obj.get("bstp_kor_isnm") or "").strip()
        if sector:
            return sector, 0.85, "kis"
        return "", 0.0, "kis_empty"
    except Exception as e:
        return "", 0.0, f"kis_exc:{type(e).__name__}"


def insert_rows(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    values: list[str] = []
    for r in rows:
        tags = ",".join(_sql_quote(x) for x in (r.get("theme_tags") or []))
        values.append(
            "("
            f"{_sql_quote(r.get('ticker'))},"
            f"{_sql_quote(r.get('ticker_name'))},"
            f"{_sql_quote(r.get('sector'))},"
            f"{_sql_quote(r.get('sub_sector'))},"
            f"[{tags}],"
            f"{_sql_quote(r.get('source'))},"
            f"{float(r.get('confidence') or 0.0)},"
            "now()"
            ")"
        )
    sql = (
        "INSERT INTO trading.ticker_sector "
        "(ticker, ticker_name, sector, sub_sector, theme_tags, source, confidence, updated_at) VALUES "
        + ",".join(values)
    )
    ch_exec(sql)
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="ticker_sector 동기화")
    ap.add_argument("--limit", type=int, default=240, help="동기화 대상 티커 수")
    ap.add_argument("--include-master", action="store_true", help="ticker_master_kr도 포함")
    ap.add_argument("--max-age-days", type=int, default=30, help="캐시 유효일")
    args = ap.parse_args()

    ensure_schema()
    targets = load_target_tickers(args.limit, include_master=args.include_master)
    if not targets:
        print("sync_ticker_sector: no targets")
        return 0

    upserts: list[dict[str, Any]] = []
    for item in targets:
        tk = item["ticker"]
        name = item["ticker_name"]

        sector, conf = get_cached_sector(tk, max_age_days=args.max_age_days)
        source = "cached"
        if not sector:
            sector, conf, source = fetch_sector_kis(tk)
        if not sector:
            sector = "미분류"
            conf = 0.25
            source = "unknown"

        titles = load_recent_titles(tk, days=14)
        tags = infer_theme_tags(titles)
        sub_sector = tags[0] if tags else "-"

        upserts.append(
            {
                "ticker": tk,
                "ticker_name": name,
                "sector": sector,
                "sub_sector": sub_sector,
                "theme_tags": tags,
                "source": source,
                "confidence": conf,
            }
        )

    n = insert_rows(upserts)
    print(
        f"sync_ticker_sector: targets={len(targets)} inserted={n} "
        f"include_master={1 if args.include_master else 0}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
