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
import re
import tempfile
from datetime import datetime
from typing import Any

import requests

from codex_exec_guard import run_codex_cached

CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "").strip()
if not CLICKHOUSE_URL:
    CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_HOST", "http://localhost:8123").strip()
if not CLICKHOUSE_URL:
    CLICKHOUSE_URL = "http://localhost:8123"
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "").strip()
CLICKHOUSE_PASS = os.environ.get("CLICKHOUSE_PASS", os.environ.get("CLICKHOUSE_PASSWORD", "")).strip()
CLICKHOUSE_AUTH = (CLICKHOUSE_USER, CLICKHOUSE_PASS) if CLICKHOUSE_USER else None

LLM_ENABLED_DEFAULT = os.environ.get("WATCHLIST_LLM_ENABLED", "1").strip() == "1"
LLM_TIMEOUT_SEC_DEFAULT = max(30, int(os.environ.get("WATCHLIST_LLM_TIMEOUT_SEC", "120")))
LLM_CACHE_TTL_SEC_DEFAULT = max(0, int(os.environ.get("WATCHLIST_LLM_CACHE_TTL_SEC", "300")))
LLM_RULE_WEIGHT_DEFAULT = float(os.environ.get("WATCHLIST_RULE_WEIGHT", "0.7"))
LLM_WEIGHT_DEFAULT = float(os.environ.get("WATCHLIST_LLM_WEIGHT", "0.3"))
LLM_MODEL_DEFAULT = os.environ.get("WATCHLIST_LLM_MODEL", os.environ.get("CODEX_MODEL", "openai-codex/gpt-5.3-codex-spark")).strip()
LLM_MAX_ITEMS_DEFAULT = max(5, int(os.environ.get("WATCHLIST_LLM_MAX_ITEMS", "30")))


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(v)))


def _is_ticker(v: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", v or ""))


def _normalize_text_list(v: Any, max_items: int = 5, max_len: int = 120) -> list[str]:
    if isinstance(v, list):
        raw = v
    elif isinstance(v, str):
        raw = [v]
    else:
        raw = []
    out: list[str] = []
    seen: set[str] = set()
    for it in raw:
        s = str(it or "").strip().replace("\n", " ")
        if not s:
            continue
        if len(s) > max_len:
            s = s[:max_len]
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
        if len(out) >= max_items:
            break
    return out


def _normalize_news_pairs(titles: Any, urls: Any, max_items: int = 3) -> list[dict[str, str]]:
    title_list = titles if isinstance(titles, list) else []
    url_list = urls if isinstance(urls, list) else []
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    n = min(max_items, max(len(title_list), len(url_list)))
    for i in range(n):
        t = str(title_list[i] if i < len(title_list) else "").strip().replace("\n", " ")
        u = str(url_list[i] if i < len(url_list) else "").strip()
        if not t and not u:
            continue
        if len(t) > 140:
            t = t[:140]
        if len(u) > 260:
            u = u[:260]
        key = (t, u)
        if key in seen:
            continue
        seen.add(key)
        out.append({"title": t, "url": u})
    return out


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


def _build_llm_schema_path() -> str:
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["items"],
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["ticker", "llm_score", "verdict", "reason", "risk_flags", "catalysts"],
                    "properties": {
                        "ticker": {"type": "string"},
                        "llm_score": {"type": "number", "minimum": 0, "maximum": 100},
                        "verdict": {"type": "string", "enum": ["BUY_REVIEW", "WATCH", "HOLD", "AVOID"]},
                        "reason": {"type": "string"},
                        "risk_flags": {"type": "array", "items": {"type": "string"}},
                        "catalysts": {"type": "array", "items": {"type": "string"}},
                    },
                },
            }
        },
    }
    p = tempfile.NamedTemporaryFile(prefix="watchlist_llm_", suffix=".schema.json", delete=False)
    p.write(json.dumps(schema, ensure_ascii=False).encode("utf-8"))
    p.flush()
    p.close()
    return p.name


def _build_llm_prompt(candidates: list[dict[str, Any]]) -> str:
    slim: list[dict[str, Any]] = []
    for c in candidates:
        slim.append(
            {
                "ticker": c.get("ticker", ""),
                "ticker_name": c.get("ticker_name", ""),
                "rule_score_100": round(float(c.get("rule_score_100", 0.0) or 0.0), 2),
                "signal_score": float(c.get("technical_score", 0.0) or 0.0),
                "rsi": float(c.get("rsi", 0.0) or 0.0),
                "news_count": int(c.get("news_cnt", 0) or 0),
                "news_pos": int(c.get("news_pos", 0) or 0),
                "news_neg": int(c.get("news_neg", 0) or 0),
                "explain_ready": int(c.get("explain_ready", 0) or 0),
                "top_news": _normalize_news_pairs(
                    c.get("top_news_titles", []),
                    c.get("top_news_urls", []),
                    max_items=3,
                ),
                "relation_score": round(float(c.get("relation_score", 0.0) or 0.0), 4),
                "relation_context": {
                    "bias": str(c.get("relation_bias", "neutral") or "neutral"),
                    "support_events": int(c.get("rel_support_events", 0) or 0),
                    "support_clusters": int(c.get("rel_support_clusters", 0) or 0),
                    "source_tickers": str(c.get("rel_source_tickers", "") or "")[:120],
                    "roles": str(c.get("rel_roles", "") or "")[:120],
                    "channels": str(c.get("rel_channels", "") or "")[:120],
                },
                "foreign_flow": float(c.get("foreign_flow", 0.0) or 0.0),
                "inst_flow": float(c.get("inst_flow", 0.0) or 0.0),
            }
        )

    return (
        "너는 한국 주식 watchlist 선별 보조모델이다.\n"
        "입력된 후보를 보고 종목별 llm_score(0~100), verdict(BUY_REVIEW/WATCH/HOLD/AVOID), "
        "reason, risk_flags, catalysts를 반환하라.\n"
        "규칙:\n"
        "- 과열(RSI>70), 근거부족(explain_ready=0), 뉴스 악재 우세면 보수적으로.\n"
        "- 유동성/수급/뉴스맥락을 함께 반영.\n"
        "- 반드시 JSON으로만 답하라.\n\n"
        f"[CANDIDATES]\n{json.dumps(slim, ensure_ascii=False, indent=2)}\n"
    )


def _run_llm_rerank(
    candidates: list[dict[str, Any]],
    timeout_sec: int,
    cache_ttl_sec: int,
    model: str,
) -> tuple[dict[str, dict[str, Any]], str]:
    if not candidates:
        return {}, "no_candidates"
    prompt = _build_llm_prompt(candidates)
    schema_path = _build_llm_schema_path()
    try:
        raw = run_codex_cached(
            prompt=prompt,
            codex_bin=os.getenv("CODEX_BIN", "openclaw"),
            model=model,
            workdir=os.path.dirname(os.path.abspath(__file__)),
            timeout_sec=max(30, int(timeout_sec)),
            base_args=[],
            output_schema_path=schema_path,
            cache_dir=os.getenv("CODEX_EXEC_CACHE_DIR", os.path.expanduser("~/.openclaw/cache/codex-exec")),
            cache_ttl_sec=max(0, int(cache_ttl_sec)),
        )
    except Exception as e:
        return {}, f"llm_call_failed:{type(e).__name__}:{e}"
    finally:
        try:
            os.unlink(schema_path)
        except Exception:
            pass

    obj = _extract_json_obj(raw)
    if not isinstance(obj, dict):
        return {}, "llm_parse_failed"
    items = obj.get("items", [])
    if not isinstance(items, list):
        return {}, "llm_items_missing"

    out: dict[str, dict[str, Any]] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        ticker = str(it.get("ticker", "")).strip()
        if not _is_ticker(ticker):
            continue
        verdict = str(it.get("verdict", "WATCH")).strip().upper()
        if verdict not in {"BUY_REVIEW", "WATCH", "HOLD", "AVOID"}:
            verdict = "WATCH"
        llm_score = _clamp(float(it.get("llm_score", 50.0) or 50.0), 0.0, 100.0)
        reason = str(it.get("reason", "") or "").strip()
        out[ticker] = {
            "llm_score": llm_score,
            "verdict": verdict,
            "reason": reason[:240],
            "risk_flags": _normalize_text_list(it.get("risk_flags", []), max_items=5, max_len=80),
            "catalysts": _normalize_text_list(it.get("catalysts", []), max_items=5, max_len=80),
        }
    if not out:
        return {}, "llm_empty_items"
    return out, ""


def ch_query(q: str):
    resp = requests.get(
        CLICKHOUSE_URL,
        params={"query": q, "default_format": "JSON"},
        timeout=60,
        auth=CLICKHOUSE_AUTH,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def ch_execute(sql: str):
    resp = requests.post(
        CLICKHOUSE_URL,
        data=(sql + "\n").encode("utf-8"),
        timeout=120,
        auth=CLICKHOUSE_AUTH,
    )
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
    resp = requests.post(CLICKHOUSE_URL, data=sql.encode("utf-8"), timeout=120, auth=CLICKHOUSE_AUTH)
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


def apply_llm_overlay(
    base_action: str,
    base_reason: str,
    base_conf: float,
    llm_item: dict[str, Any] | None,
    explain_ready: int,
) -> tuple[str, str, float]:
    if not llm_item:
        return base_action, base_reason, base_conf

    verdict = str(llm_item.get("verdict", "WATCH")).strip().upper()
    llm_score = _clamp(float(llm_item.get("llm_score", 50.0) or 50.0), 0.0, 100.0)
    llm_reason = str(llm_item.get("reason", "") or "").strip()

    action = base_action
    if verdict == "AVOID" or llm_score < 30:
        action = "HOLD"
    elif verdict == "HOLD" or llm_score < 45:
        if base_action == "BUY_REVIEW":
            action = "WATCH"
    elif verdict == "BUY_REVIEW" and llm_score >= 75 and explain_ready > 0:
        if base_action.startswith("WATCH"):
            action = "BUY_REVIEW"

    conf = _clamp(base_conf * 0.7 + (llm_score / 100.0) * 0.3, 0.0, 1.0)
    if llm_reason:
        reason = f"{base_reason} | LLM: {llm_reason}"
    else:
        reason = base_reason
    return action, reason[:320], conf


def load_candidates(limit: int):
    q = f"""
    WITH
      latest_date AS (SELECT max(date) AS d FROM trading.technical_signals),
      latest_rel AS (SELECT max(asof_ts) AS ts FROM trading.hidden_relation_signals),
      news_ranked AS (
        SELECT
          ticker,
          title,
          source_url,
          row_number() OVER (PARTITION BY ticker ORDER BY importance DESC, published_at DESC) AS rn
        FROM (
          SELECT
            arrayJoin(tickers) AS ticker,
            title,
            source_url,
            importance,
            published_at
          FROM trading.news
          WHERE published_at >= now() - INTERVAL 3 DAY
        )
      ),
      news_top AS (
        SELECT
          ticker,
          groupArray(title) AS top_news_titles,
          groupArray(source_url) AS top_news_urls
        FROM (
          SELECT ticker, title, source_url
          FROM news_ranked
          WHERE rn <= 3
          ORDER BY ticker, rn
        )
        GROUP BY ticker
      ),
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
      ifNull(nt.top_news_titles, []) AS top_news_titles,
      ifNull(nt.top_news_urls, []) AS top_news_urls,
      ifNull(fa.explain_ready_3d, 0) AS explain_ready,
      round(ifNull(hrs.total_relation_score, 0), 6) AS rel_score,
      ifNull(hrs.relation_bias, 'neutral') AS rel_bias,
      ifNull(hrs.support_events, 0) AS rel_support_events,
      ifNull(hrs.support_clusters, 0) AS rel_support_clusters,
      ifNull(arrayStringConcat(hrs.source_tickers, ', '), '') AS rel_source_tickers,
      ifNull(arrayStringConcat(hrs.top_roles, ', '), '') AS rel_roles,
      ifNull(arrayStringConcat(hrs.top_channels, ', '), '') AS rel_channels,
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
    LEFT JOIN news_top nt ON nt.ticker = ts.ticker
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
    ap.add_argument("--llm", choices=["on", "off"], default="on" if LLM_ENABLED_DEFAULT else "off")
    ap.add_argument("--llm-timeout", type=int, default=LLM_TIMEOUT_SEC_DEFAULT)
    ap.add_argument("--llm-cache-ttl", type=int, default=LLM_CACHE_TTL_SEC_DEFAULT)
    ap.add_argument("--llm-model", default=LLM_MODEL_DEFAULT)
    ap.add_argument("--rule-weight", type=float, default=LLM_RULE_WEIGHT_DEFAULT)
    ap.add_argument("--llm-weight", type=float, default=LLM_WEIGHT_DEFAULT)
    ap.add_argument("--llm-max-items", type=int, default=LLM_MAX_ITEMS_DEFAULT)
    args = ap.parse_args()

    limit = max(1, int(args.limit))
    source = args.source.strip() or "rule_snapshot"
    llm_enabled = str(args.llm).lower() == "on"
    llm_timeout = max(30, int(args.llm_timeout))
    llm_cache_ttl = max(0, int(args.llm_cache_ttl))
    llm_model = str(args.llm_model or LLM_MODEL_DEFAULT).strip() or LLM_MODEL_DEFAULT
    llm_max_items = max(5, int(args.llm_max_items))
    rule_w = max(0.0, float(args.rule_weight))
    llm_w = max(0.0, float(args.llm_weight))
    w_sum = rule_w + llm_w
    if w_sum <= 0:
        rule_w, llm_w = 1.0, 0.0
    else:
        rule_w, llm_w = rule_w / w_sum, llm_w / w_sum

    rows = load_candidates(limit)
    if not rows:
        print("candidate 0, skip")
        return 0

    prepared: list[dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        technical_score = float(rr.get("score", 0) or 0)
        relation_score = float(rr.get("rel_score", 0) or 0)
        news_score = float(int(rr.get("pos", 0) or 0) - int(rr.get("neg", 0) or 0))
        rule_raw = float(rr.get("composite_score", 0) or 0)
        # composite_score(~0-20)를 0-100 영역으로 정규화
        rule_score_100 = _clamp(rule_raw * 5.0, 0.0, 100.0)
        prepared.append(
            {
                "ticker": str(rr.get("ticker", "")).strip(),
                "ticker_name": str(rr.get("ticker_name", "")).strip(),
                "technical_score": technical_score,
                "relation_score": relation_score,
                "news_score": news_score,
                "foreign_flow": float(rr.get("foreign_flow", 0) or 0),
                "inst_flow": float(rr.get("inst_flow", 0) or 0),
                "rule_score_raw": rule_raw,
                "rule_score_100": rule_score_100,
                "signal": str(rr.get("signal", "")).strip(),
                "rsi": float(rr.get("rsi", 0) or 0),
                "bb": float(rr.get("bb", 0) or 0),
                "vol_r": float(rr.get("vol_r", 0) or 0),
                "pct": float(rr.get("pct", 0) or 0),
                "news_pos": int(rr.get("pos", 0) or 0),
                "news_neg": int(rr.get("neg", 0) or 0),
                "news_cnt": int(rr.get("news_cnt", 0) or 0),
                "top_news_titles": rr.get("top_news_titles", []) if isinstance(rr.get("top_news_titles", []), list) else [],
                "top_news_urls": rr.get("top_news_urls", []) if isinstance(rr.get("top_news_urls", []), list) else [],
                "explain_ready": int(rr.get("explain_ready", 0) or 0),
                "relation_bias": str(rr.get("rel_bias", "neutral") or "neutral"),
                "rel_support_events": int(rr.get("rel_support_events", 0) or 0),
                "rel_support_clusters": int(rr.get("rel_support_clusters", 0) or 0),
                "rel_source_tickers": str(rr.get("rel_source_tickers", "") or ""),
                "rel_roles": str(rr.get("rel_roles", "") or ""),
                "rel_channels": str(rr.get("rel_channels", "") or ""),
            }
        )

    llm_scores: dict[str, dict[str, Any]] = {}
    llm_error = ""
    if llm_enabled:
        llm_input = [c for c in prepared if _is_ticker(c["ticker"])][: min(llm_max_items, len(prepared))]
        llm_scores, llm_error = _run_llm_rerank(
            llm_input,
            timeout_sec=llm_timeout,
            cache_ttl_sec=llm_cache_ttl,
            model=llm_model,
        )
        if not llm_scores:
            # LLM 실패 시 룰 기반 폴백
            llm_w = 0.0
            rule_w = 1.0

    decision_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ch_execute(
        f"DELETE FROM trading.interest_watchlist WHERE toDate(ts) = toDate('{ts[:10]}') AND source = {_sql_quote(source)}"
    )

    for c in prepared:
        llm_item = llm_scores.get(c["ticker"])
        llm_score = _clamp(float((llm_item or {}).get("llm_score", 50.0)), 0.0, 100.0)
        final_score = round(rule_w * c["rule_score_100"] + llm_w * llm_score, 4)
        c["llm_item"] = llm_item
        c["llm_score"] = llm_score
        c["final_score"] = final_score

    prepared.sort(key=lambda x: (float(x.get("final_score", 0.0)), float(x.get("rule_score_100", 0.0))), reverse=True)

    out = []
    for i, c in enumerate(prepared, 1):
        rr = {
            "score": c["technical_score"],
            "rsi": c["rsi"],
            "rel_score": c["relation_score"],
            "explain_ready": c["explain_ready"],
        }
        base_action, base_reason, _context_score_hint, base_conf = classify_action(rr)
        action, reason, confidence = apply_llm_overlay(
            base_action=base_action,
            base_reason=base_reason,
            base_conf=float(base_conf),
            llm_item=c.get("llm_item"),
            explain_ready=int(c.get("explain_ready", 0) or 0),
        )
        technical_score = float(c["technical_score"])
        rel_score = float(c["relation_score"])
        news_score = float(c["news_score"])
        context_score = float(c["final_score"])
        llm_item = c.get("llm_item") or {}

        payload = {
            "watch": {
                "rank": i,
                "ticker": c.get("ticker"),
                "ticker_name": c.get("ticker_name"),
                "action": action,
                "reason": reason,
                "source": source,
                "confidence": confidence,
            },
            "context": {
                "technical_score": technical_score,
                "rsi": float(c.get("rsi", 0) or 0),
                "bb": float(c.get("bb", 0) or 0),
                "vol_r": float(c.get("vol_r", 0) or 0),
                "pct": float(c.get("pct", 0) or 0),
                "news_score": news_score,
                "news_count": int(c.get("news_cnt", 0) or 0),
                "news_pos": int(c.get("news_pos", 0) or 0),
                "news_neg": int(c.get("news_neg", 0) or 0),
                "top_news": _normalize_news_pairs(
                    c.get("top_news_titles", []),
                    c.get("top_news_urls", []),
                    max_items=3,
                ),
                "explain_ready": int(c.get("explain_ready", 0) or 0),
                "relation_bias": c.get("relation_bias", "neutral"),
                "relation_support_events": int(c.get("rel_support_events", 0) or 0),
                "relation_support_clusters": int(c.get("rel_support_clusters", 0) or 0),
                "relation_source_tickers": str(c.get("rel_source_tickers", "") or ""),
                "relation_roles": str(c.get("rel_roles", "") or ""),
                "relation_channels": str(c.get("rel_channels", "") or ""),
                "foreign_flow": float(c.get("foreign_flow", 0) or 0),
                "inst_flow": float(c.get("inst_flow", 0) or 0),
                "technical": technical_score,
                "rule_score_raw": float(c.get("rule_score_raw", 0.0) or 0.0),
                "rule_score_100": float(c.get("rule_score_100", 0.0) or 0.0),
                "llm_score": float(c.get("llm_score", 50.0) or 50.0),
                "final_score": context_score,
                "score_weights": {"rule_weight": round(rule_w, 4), "llm_weight": round(llm_w, 4)},
                "llm_verdict": str(llm_item.get("verdict", "WATCH")),
                "llm_reason": str(llm_item.get("reason", "")),
                "llm_risk_flags": _normalize_text_list(llm_item.get("risk_flags", []), max_items=5, max_len=80),
                "llm_catalysts": _normalize_text_list(llm_item.get("catalysts", []), max_items=5, max_len=80),
            },
            "context_breakdown": {
                "technical_score": technical_score,
                "news_score": int(news_score),
                "relation_score": rel_score,
                "flow_signal": float(c.get("foreign_flow", 0) or 0),
                "tech_norm": min(1.0, max(0.0, technical_score / 6.0)),
                "news_norm": min(1.0, (int(c.get("news_cnt", 0) or 0) / 10.0)),
                "rel_norm": min(1.0, abs(rel_score) / 5.0),
                "rule_norm": round(float(c.get("rule_score_100", 0.0) or 0.0) / 100.0, 4),
                "llm_norm": round(float(c.get("llm_score", 50.0) or 50.0) / 100.0, 4),
            },
            "llm_meta": {
                "enabled": llm_enabled,
                "error": llm_error,
                "model": llm_model,
                "timeout_sec": llm_timeout,
                "cache_ttl_sec": llm_cache_ttl,
            },
        }

        out.append(
            {
                "ts": ts,
                "decision_id": decision_id,
                "source": source,
                "action": action,
                "ticker": c.get("ticker", ""),
                "ticker_name": c.get("ticker_name", ""),
                "rank": i,
                "reason": reason,
                "technical_score": technical_score,
                "relation_score": rel_score,
                "news_score": news_score,
                "foreign_flow": float(c.get("foreign_flow", 0) or 0),
                "inst_flow": float(c.get("inst_flow", 0) or 0),
                "context_score": context_score,
                "confidence": confidence,
                "request_json": json.dumps(payload, ensure_ascii=False),
            }
        )

    n = ch_insert_sql("trading.interest_watchlist", out)
    print(f"inserted_interest_watchlist={n} llm_enabled={llm_enabled} llm_rows={len(llm_scores)} llm_error={llm_error or '-'}")
    return 0


def _sql_quote(v: Any) -> str:
    s = str(v or "")
    s = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


if __name__ == "__main__":
    raise SystemExit(main())
