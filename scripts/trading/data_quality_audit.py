#!/usr/bin/env python3
"""data_quality_audit.py

트레이딩 DB(trading)의 핵심 데이터 품질을 점검한다.
- 신선도(freshness)
- 핵심 테이블 null/invalid
- 시점 정합성(time alignment)
- 뉴스 임베딩 누락/차원 이상치
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import sys
from collections import OrderedDict
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()


def _log(msg: str) -> None:
    print(f"{dt.datetime.now().strftime('%H:%M:%S')} [dq-audit] {msg}", flush=True)


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


def ch_select(sql: str, timeout_sec: int = 40) -> list[dict]:
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


def _to_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _to_int(v, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _table_exists(name: str) -> bool:
    safe = name.replace("'", "\\'")
    q = f"""
SELECT count()
FROM system.tables
WHERE database='trading' AND name='{safe}'
"""
    return _to_int(ch_scalar(q), 0) > 0


def _section_table_freshness(core_tables: list[str]) -> list[str]:
    lines: list[str] = ["## 테이블 신선도"]
    cols = ch_select(
        """
SELECT table, name, type
FROM system.columns
WHERE database='trading'
  AND table IN (
    'feature_snapshot','technical_signals','market_regime','market_index',
    'exchange_rate','interest_rate','news','news_event_frames','news_clusters',
    'news_cluster_state','dart_disclosure','stock_flow_daily','market_flow_daily'
  )
"""
    )
    by_table: dict[str, list[tuple[str, str]]] = {}
    for r in cols:
        by_table.setdefault(str(r.get("table")), []).append((str(r.get("name")), str(r.get("type"))))

    preferred_cols = [
        "ts",
        "updated_at",
        "collected_at",
        "published_at",
        "asof_ts",
        "created_at",
        "date",
        "trade_date",
        "rcept_dt",
    ]

    for t in core_tables:
        if not _table_exists(t):
            lines.append(f"- {t}: 테이블 없음")
            continue
        candidates = {c for c, _ in by_table.get(t, [])}
        col = ""
        for c in preferred_cols:
            if c in candidates:
                col = c
                break
        if not col:
            cnt = _to_int(ch_scalar(f"SELECT count() FROM trading.{t}"), 0)
            lines.append(f"- {t}: row={cnt:,}, 시간컬럼 미확인")
            continue
        q = f"""
SELECT
    count() AS n,
    max({col}) AS mx
FROM trading.{t}
"""
        row = (ch_select(q) or [{}])[0]
        cnt = _to_int(row.get("n"), 0)
        mx = str(row.get("mx", ""))
        stale_min = _to_int(ch_scalar(f"SELECT if(count()=0,99999,greatest(dateDiff('minute', max({col}), now()),0)) FROM trading.{t}"), 99999)
        lines.append(f"- {t}: row={cnt:,}, max({col})={mx}, stale={stale_min}m")
    return lines


def _section_stage0_diagnostics(feature_hours: int) -> list[str]:
    lines = ["## Stage0 진단"]
    row = (ch_select(
        f"""
SELECT
    count() AS n,
    sum(if(symbol='' OR toFloat64(price)<=0,1,0)) AS bad_n,
    round(100 * sum(if(symbol='' OR toFloat64(price)<=0,1,0)) / greatest(count(),1), 2) AS bad_pct
FROM trading.feature_snapshot
WHERE ts >= now() - INTERVAL {int(feature_hours)} HOUR
  AND session='REGULAR'
"""
    ) or [{}])[0]
    lines.append(
        f"- feature_snapshot(REGULAR,{feature_hours}h): bad {_to_int(row.get('bad_n')):,}/{_to_int(row.get('n')):,} ({_to_float(row.get('bad_pct')):.2f}%)"
    )

    row2 = (ch_select(
        """
SELECT
    count() AS n,
    sum(if(toFloat64(price)<=0,1,0)) AS zero_n
FROM trading.feature_snapshot
WHERE ts >= now() - INTERVAL 3 DAY
  AND session IN ('AFTER','OFF','PRE')
"""
    ) or [{}])[0]
    lines.append(
        f"- feature_snapshot(비정규세션,3d): price<=0 {_to_int(row2.get('zero_n')):,}/{_to_int(row2.get('n')):,}"
    )

    row3 = (ch_select(
        """
SELECT
    count() AS n,
    sum(if(ticker='' OR toFloat64(close_price)<=0 OR rsi14<0 OR rsi14>100,1,0)) AS bad_n,
    round(100 * sum(if(ticker='' OR toFloat64(close_price)<=0 OR rsi14<0 OR rsi14>100,1,0)) / greatest(count(),1), 2) AS bad_pct
FROM trading.technical_signals
WHERE date = (SELECT max(date) FROM trading.technical_signals)
"""
    ) or [{}])[0]
    lines.append(
        f"- technical_signals(latest): bad {_to_int(row3.get('bad_n')):,}/{_to_int(row3.get('n')):,} ({_to_float(row3.get('bad_pct')):.2f}%)"
    )

    now_future = _to_int(
        ch_scalar("SELECT count() FROM trading.news_event_frames WHERE published_at > now() + INTERVAL 1 MINUTE"),
        0,
    )
    collected_future = _to_int(
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
    lines.append(f"- news_event_frames 미래시각(기존 now 기준): {now_future:,}")
    lines.append(f"- news_event_frames 미래시각(정합 기준 collected_at+10m): {collected_future:,}")
    return lines


def _section_embedding() -> list[str]:
    lines = ["## 임베딩 품질"]
    kpis = OrderedDict(
        [
            (
                "24h",
                """
SELECT count() AS n, countIf(length(embedding)>0) AS with_emb
FROM trading.news
WHERE collected_at >= now() - INTERVAL 24 HOUR
""",
            ),
            (
                "7d",
                """
SELECT count() AS n, countIf(length(embedding)>0) AS with_emb
FROM trading.news
WHERE collected_at >= now() - INTERVAL 7 DAY
""",
            ),
            (
                "7d_imp>=4",
                """
SELECT count() AS n, countIf(length(embedding)>0) AS with_emb
FROM trading.news
WHERE collected_at >= now() - INTERVAL 7 DAY
  AND importance >= 4
""",
            ),
        ]
    )

    for k, q in kpis.items():
        row = (ch_select(q) or [{}])[0]
        n = _to_int(row.get("n"))
        with_emb = _to_int(row.get("with_emb"))
        pct = 100.0 * with_emb / max(1, n)
        lines.append(f"- news {k}: embedding {with_emb:,}/{n:,} ({pct:.2f}%)")

    dist = ch_select(
        """
SELECT length(embedding) AS dim, count() AS n
FROM trading.news
WHERE collected_at >= now() - INTERVAL 7 DAY
GROUP BY dim
ORDER BY n DESC
LIMIT 10
"""
    )
    if dist:
        mode_dim = _to_int(dist[0].get("dim"), 0)
        mode_n = _to_int(dist[0].get("n"), 0)
        total_7d = _to_int(ch_scalar("SELECT count() FROM trading.news WHERE collected_at >= now() - INTERVAL 7 DAY"), 0)
        anomaly = max(0, total_7d - mode_n)
        lines.append(f"- embedding 차원 mode: {mode_dim} (rows={mode_n:,}), 비표준 차원 rows={anomaly:,}")
        top_dims = ", ".join(f"{_to_int(r.get('dim'))}:{_to_int(r.get('n'))}" for r in dist[:5])
        lines.append(f"- 차원 분포 상위: {top_dims}")
    return lines


def _section_event_and_cluster() -> list[str]:
    lines = ["## 이벤트/클러스터 품질"]
    row = (ch_select(
        """
SELECT
    count() AS n,
    countIf(length(tickers)>0) AS with_ticker,
    round(100*countIf(length(tickers)>0)/greatest(count(),1), 2) AS ticker_cov_pct
FROM trading.news_event_frames
WHERE collected_at >= now() - INTERVAL 7 DAY
"""
    ) or [{}])[0]
    lines.append(
        f"- news_event_frames 7d: ticker 매핑 {_to_int(row.get('with_ticker')):,}/{_to_int(row.get('n')):,} ({_to_float(row.get('ticker_cov_pct')):.2f}%)"
    )

    row2 = (ch_select(
        """
SELECT
    count() AS n_news,
    countIf(length(embedding)>0) AS emb_news
FROM trading.news
WHERE collected_at >= now() - INTERVAL 72 HOUR
"""
    ) or [{}])[0]
    lines.append(
        f"- cluster 입력 가능 뉴스(72h): embedding 보유 {_to_int(row2.get('emb_news')):,}/{_to_int(row2.get('n_news')):,}"
    )

    recent_clusters = _to_int(ch_scalar("SELECT count() FROM trading.news_cluster_state WHERE asof_ts >= now() - INTERVAL 24 HOUR"), 0)
    lines.append(f"- news_cluster_state 최근 24h rows: {recent_clusters:,}")
    return lines


def _section_flow_quality() -> list[str]:
    lines = ["## 수급 데이터 품질"]
    latest_date = ch_scalar("SELECT toString(max(date)) FROM trading.investor_flow")
    lines.append(f"- investor_flow latest date: {latest_date}")

    if latest_date:
        rows = ch_select(
            f"""
SELECT market, investor_type, net_amount
FROM trading.investor_flow
WHERE date = toDate('{latest_date}')
ORDER BY market, investor_type
"""
        )
        for r in rows:
            lines.append(
                f"  - {r.get('market')} {r.get('investor_type')}: { _to_int(r.get('net_amount')):,} (백만원)"
            )

    n_stock = _to_int(ch_scalar("SELECT count() FROM trading.stock_flow_daily WHERE trade_date >= today()-7"), 0)
    n_market = _to_int(ch_scalar("SELECT count() FROM trading.market_flow_daily WHERE trade_date >= today()-7"), 0)
    lines.append(f"- stock_flow_daily(7d): {n_stock:,} rows")
    lines.append(f"- market_flow_daily(7d): {n_market:,} rows")
    return lines


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--feature-hours", type=int, default=72, help="feature_snapshot 진단 lookback 시간")
    ap.add_argument("--output", default="", help="리포트 저장 경로 (옵션)")
    args = ap.parse_args()

    core_tables = [
        "feature_snapshot",
        "technical_signals",
        "market_regime",
        "market_index",
        "exchange_rate",
        "interest_rate",
        "news",
        "news_event_frames",
        "news_clusters",
        "news_cluster_state",
        "dart_disclosure",
        "stock_flow_daily",
        "market_flow_daily",
        "investor_flow",
        "decision_run",
    ]

    lines: list[str] = []
    lines.append(f"# 데이터 품질 리포트 ({dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    lines.extend(_section_table_freshness(core_tables))
    lines.extend(_section_stage0_diagnostics(max(24, int(args.feature_hours))))
    lines.extend(_section_embedding())
    lines.extend(_section_event_and_cluster())
    lines.extend(_section_flow_quality())

    report = "\n".join(lines)
    print(report, flush=True)

    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report)
        _log(f"리포트 저장: {args.output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
