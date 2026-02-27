#!/usr/bin/env python3
"""동적 관심목록 재산출 및 저장

요약:
- technical_signals(최근일자)
- 최근 3일 뉴스 점수
- 최근 3일 explainability 충족 기사수
- hidden_relation_signals 최신 점수
를 합성해 후보를 산출 후 trading.interest_watchlist에 저장

실행:
  python3 scripts/refresh_interest_watchlist.py --limit 20
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from typing import Any
from urllib.parse import quote_plus

import requests

CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")


def ch_query(q: str):
    resp = requests.get(CLICKHOUSE_URL, params={"query": q}, timeout=60)
    resp.raise_for_status()
    return resp.json().get("data", [])


def ch_execute(sql: str):
    resp = requests.post(CLICKHOUSE_URL, data=(sql + "\n").encode("utf-8"), timeout=120)
    resp.raise_for_status()
    return True


def ch_insert_sql(table: str, rows: list[dict[str, Any]]):
    if not rows:
        return 0
    cols = [
        "ts",
        "decision_id",
        "source",
        "action",
        "ticker",
        "ticker_name",
        "rank",
        "reason",
        "technical_score",
        "relation_score",
        "news_score",
        "foreign_flow",
        "inst_flow",
        "context_score",
        "confidence",
        "request_json",
    ]

    def q(v: Any) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, (int, float)):
            return str(v)
        s = str(v)
        s = s.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{s}'"

    vals = []
    for r in rows:
        vals.append(
            "(" + ", ".join(
                [
                    q(r["ts"]),
                    q(r["decision_id"]),
                    q(r["source"]),
                    q(r["action"]),
                    q(r["ticker"]),
                    q(r["ticker_name"]),
                    q(r["rank"]),
                    q(r["reason"]),
                    q(r["technical_score"]),
                    q(r["relation_score"]),
                    q(r["news_score"]),
                    q(r["foreign_flow"]),
                    q(r["inst_flow"]),
                    q(r["context_score"]),
                    q(r["confidence"]),
                    q(r["request_json"]),
                ]
            ) + ")"
        )

    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES " + ", ".join(vals)
    resp = requests.post(CLICKHOUSE_URL, data=sql.encode("utf-8"), timeout=120)
    resp.raise_for_status()
    return len(rows)


def classify_action(p: dict[str, float]) -> tuple[str, str, float, float]:
    score = float(p.get("score", 0) or 0)
    rsi = float(p.get("rsi", 0) or 0)
    rel_score = float(p.get("rel_score", 0) or 0)
    explain_ready = int(p.get("explain_ready", 0) or 0)
    if rsi > 70:
        return "AVOID(RSI>70)", "단기 과열 구간(RSI>70)으로 추격 성향이 높아 보류", 0.0, 0.0
    if score >= 2 and explain_ready > 0 and rel_score >= 0:
        why = "기술점수 +2 이상, 근거 충족, 연관성 우호 신호로 BUY 우선 검토"
        conf = min(0.95, 0.55 + 0.18 * min(score, 5))
    elif score >= 2 and explain_ready == 0:
        why = "기술점수 +2 이상이나 근거 완성도 부족. WATCH로 근거 보강 필요"
        conf = 0.45
    elif score >= 1:
        why = "기술점수 +1 이상이나 추가 확인 필요. 관찰/부분 검토 후보"
        conf = 0.34
    else:
        why = "기술 신호가 약함. 데이터 보강 후 재평가"
        conf = 0.12
    return classify_action_action(score, explain_ready, rel_score, why)


def classify_action_action(score: float, explain_ready: int, rel_score: float, why: str):
    if score >= 2 and explain_ready > 0 and rel_score >= 0:
        return "BUY_REVIEW", why, 0.75, min(1.0, 0.6 + 0.05 * score + 0.06 * min(abs(rel_score), 8))
    if score >= 2 and explain_ready == 0:
        return "WATCH(근거보강)", why, 0.45, 0.40
    if score >= 1:
        return "WATCH", why, 0.35, 0.32
    return "HOLD", why, 0.15, 0.20


def load_candidates(limit: int):
    q = f"""
    WITH
      latest_date AS (SELECT max(date) AS d FROM trading.technical_signals),
      latest_rel AS (SELECT max(asof_ts) AS ts FROM trading.hidden_relation_signals),
      news_agg AS (
        SELECT
          arrayJoin(tickers) AS ticker,
          countIf(sentiment='positive') AS pos,
          countIf(sentiment='negative') AS neg,
          count() AS news_cnt
        FROM trading.news
        WHERE published_at >= now() - INTERVAL 3 DAY
        GROUP BY ticker
      ),
      frame_agg AS (
        SELECT
          arrayJoin(tickers) AS ticker,
          countIf(relevant=1 AND thesis_path!='' AND evidence_json!='[]') AS explain_ready_3d
        FROM trading.news_event_frames
        WHERE published_at >= now() - INTERVAL 3 DAY
        GROUP BY ticker
      ),
      latest_flow AS (
        SELECT
          symbol AS ticker,
          argMax(foreign_flow, ts) AS foreign_flow,
          argMax(inst_flow, ts) AS inst_flow
        FROM trading.feature_snapshot
        WHERE ts >= now() - INTERVAL 1 DAY
        GROUP BY symbol
      )
    SELECT
      ts.ticker AS ticker,
      ts.ticker_name AS ticker_name,
      ts.signal AS signal,
      ts.signal_score AS score,
      round(ts.rsi14, 2) AS rsi,
      round(ts.bb_pct, 4) AS bb,
      round(ts.vol_ratio, 2) AS vol_r,
      round(ts.change_pct, 2) AS pct,
      ifNull(na.pos, 0) AS pos,
      ifNull(na.neg, 0) AS neg,
      ifNull(na.news_cnt, 0) AS news_cnt,
      ifNull(fa.explain_ready_3d, 0) AS explain_ready,
      round(ifNull(hrs.total_relation_score, 0), 6) AS rel_score,
      ifNull(hrs.relation_bias, 'neutral') AS rel_bias,
      ifNull(lf.foreign_flow, 0) AS foreign_flow,
      ifNull(lf.inst_flow, 0) AS inst_flow,
      round(
        (ts.signal_score * 1.6)
        + (ifNull(na.pos,0)-ifNull(na.neg,0)) * 0.25
        + least(ifNull(na.news_cnt,0),10) * 0.10
        + ifNull(hrs.total_relation_score,0) * 2
        + if(ifNull(fa.explain_ready_3d,0) > 0, 1.0, -0.4)
        + if(ts.rsi14 BETWEEN 45 AND 65, 0.4, 0.0), 4) AS composite_score
    FROM trading.technical_signals ts
    LEFT JOIN news_agg na ON na.ticker = ts.ticker
    LEFT JOIN frame_agg fa ON fa.ticker = ts.ticker
    LEFT JOIN trading.hidden_relation_signals hrs
      ON hrs.ticker = ts.ticker AND hrs.asof_ts = (SELECT ts FROM latest_rel)
    LEFT JOIN latest_flow lf
      ON lf.ticker = ts.ticker
    WHERE ts.date = (SELECT d FROM latest_date)
      AND ts.ticker_name != ''
      AND ts.signal_score >= 1
      AND ts.rsi14 <= 70
    ORDER BY composite_score DESC, score DESC, vol_r DESC
    LIMIT {int(limit)}
    """
    return ch_query(q)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=30)
    ap.add_argument("--source", default="rule_snapshot")
    args = ap.parse_args()

    limit = max(1, int(args.limit))
    source = args.source.strip() or "rule_snapshot"

    rows = load_candidates(limit)
    if not rows:
        print("candidate 0, skip")
        return 0

    decision_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ch_execute(
        f"DELETE FROM trading.interest_watchlist WHERE toDate(ts) = toDate('{ts[:10]}') AND source = {_sql_quote(source)}"
    )

    out = []
    for i, r in enumerate(rows, 1):
        rr = dict(r)
        action, reason, context_score_hint, conf = classify_action(rr)
        technical_score = float(rr.get("score", 0) or 0)
        rel_score = float(rr.get("rel_score", 0) or 0)
        news_score = float(int(rr.get("pos", 0) or 0) - int(rr.get("neg", 0) or 0))
        context_score = float(rr.get("composite_score", 0) or 0)
        confidence = float(conf)

        payload = {
            "watch": {
                "rank": i,
                "ticker": rr.get("ticker"),
                "ticker_name": rr.get("ticker_name"),
                "action": action,
                "reason": reason,
                "source": source,
                "confidence": confidence,
            },
            "context": {
                "technical_score": technical_score,
                "rsi": float(rr.get("rsi", 0) or 0),
                "bb": float(rr.get("bb", 0) or 0),
                "vol_r": float(rr.get("vol_r", 0) or 0),
                "pct": float(rr.get("pct", 0) or 0),
                "news_score": news_score,
                "news_count": int(rr.get("news_cnt", 0) or 0),
                "news_pos": int(rr.get("pos", 0) or 0),
                "news_neg": int(rr.get("neg", 0) or 0),
                "explain_ready": int(rr.get("explain_ready", 0) or 0),
                "relation_bias": rr.get("rel_bias", "neutral"),
                "foreign_flow": float(rr.get("foreign_flow", 0) or 0),
                "inst_flow": float(rr.get("inst_flow", 0) or 0),
                "technical": technical_score,
                "composite_score": context_score,
            },
            "context_breakdown": {
                "technical_score": technical_score,
                "news_score": int(news_score),
                "relation_score": rel_score,
                "flow_signal": float(rr.get("foreign_flow", 0) or 0),
                "tech_norm": min(1.0, max(0.0, technical_score / 6.0)),
                "news_norm": min(1.0, (int(rr.get("news_cnt", 0) or 0) / 10.0)),
                "rel_norm": min(1.0, abs(rel_score) / 5.0),
            },
        }

        out.append(
            {
                "ts": ts,
                "decision_id": decision_id,
                "source": source,
                "action": action,
                "ticker": rr.get("ticker", ""),
                "ticker_name": rr.get("ticker_name", ""),
                "rank": i,
                "reason": reason,
                "technical_score": technical_score,
                "relation_score": rel_score,
                "news_score": news_score,
                "foreign_flow": float(rr.get("foreign_flow", 0) or 0),
                "inst_flow": float(rr.get("inst_flow", 0) or 0),
                "context_score": context_score,
                "confidence": confidence,
                "request_json": json.dumps(payload, ensure_ascii=False),
            }
        )

    n = ch_insert_sql("trading.interest_watchlist", out)
    print(f"inserted_interest_watchlist={n}")
    return 0


def _sql_quote(v: Any) -> str:
    s = str(v or "")
    s = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


if __name__ == "__main__":
    raise SystemExit(main())
