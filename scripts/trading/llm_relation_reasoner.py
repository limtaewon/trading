#!/usr/bin/env python3
"""LLM 기반 숨은 연관성 인과 추론 모듈.

hidden_relation_signals의 점수만으로는 설명력이 부족한 경우를 보완하기 위해
클러스터/이벤트 프레임 문맥을 요약해 LLM으로 인과 사슬을 생성하고 저장한다.
생성 결과는 `trading.hidden_relation_reasoning` 테이블에 저장되어
prepare_gpt_prompt / execute_gpt_orders에서 보조지표로 활용된다.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env
from codex_exec_guard import run_codex_cached
from llm_model_config import default_primary_model

bootstrap_openclaw_env()

LOGGER = logging.getLogger("llm_relation_reasoner")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://localhost:8123").strip()
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default").strip()
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASS", "").strip()
DEFAULT_DRY_RUN = os.getenv("LLM_RELATION_DRY_RUN", "0").strip() == "1"
CODEX_EXEC_TIMEOUT = int(os.getenv("CODEX_EXEC_TIMEOUT", "180"))
CODEX_CACHE_TTL_SEC = int(os.getenv("CODEX_EXEC_CACHE_TTL", "0"))
MAX_EVENT_CONTEXT = int(os.getenv("LLM_RELATION_MAX_EVENT_CONTEXT", "18000"))
MODEL_DEFAULT = default_primary_model()


def _log(msg: str) -> None:
    LOGGER.info(msg)


def _ch_url_and_headers() -> tuple[str, dict[str, str]]:
    headers = {"Content-Type": "text/plain; charset=utf-8"}
    host = (CLICKHOUSE_HOST or "http://localhost:8123").strip()
    if not host:
        host = "http://localhost:8123"

    # Prefer existing env query auth.
    if "user=" in host and "password=" in host:
        return host, headers

    # If credentials embedded in URL, preserve scheme/host/path only and send Basic auth.
    sp = urlsplit(host)
    if sp.scheme and sp.hostname:
        if sp.username is not None:
            import base64

            auth = f"{sp.username}:{sp.password or ''}".encode("utf-8")
            headers["Authorization"] = "Basic " + base64.b64encode(auth).decode("ascii")
            netloc = sp.hostname or "localhost"
            if sp.port:
                netloc = f"{netloc}:{sp.port}"
            return urlunsplit((sp.scheme, netloc, sp.path or "", sp.query or "", ""), headers)
        sep = "&" if "?" in host else "?"
        # add query auth to query-string style URL.
        return f"{host}{sep}user={CLICKHOUSE_USER}&password={CLICKHOUSE_PASS}", headers

    sep = "&" if "?" in host else "?"
    return f"{host}{sep}user={CLICKHOUSE_USER}&password={CLICKHOUSE_PASS}", headers


def _ch_select(sql: str, timeout_sec: int = 60) -> list[dict[str, Any]]:
    url, headers = _ch_url_and_headers()
    payload = (sql.strip() + "\nFORMAT JSON").encode("utf-8")
    req = Request(url, data=payload, headers=headers, method="POST")
    try:
        with urlopen(req, timeout=timeout_sec) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        LOGGER.warning("ClickHouse select failed: %s", e)
        return []
    try:
        obj = json.loads(body)
    except Exception as e:
        LOGGER.warning("ClickHouse response parse failed: %s", e)
        return []
    data = obj.get("data", [])
    if isinstance(data, list):
        return data
    return []


def _ch_execute(sql: str, timeout_sec: int = 60) -> bool:
    url, headers = _ch_url_and_headers()
    req = Request(url, data=f"{sql.strip()}\n".encode("utf-8"), headers=headers, method="POST")
    try:
        with urlopen(req, timeout=timeout_sec):
            return True
    except Exception as e:
        LOGGER.warning("ClickHouse execute failed: %s", e)
        return False


def _sql_quote(v: Any) -> str:
    s = str(v if v is not None else "")
    s = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _ensure_table() -> None:
    _ch_execute(
        """
        CREATE TABLE IF NOT EXISTS trading.hidden_relation_reasoning
        (
            asof_ts                DateTime DEFAULT now(),
            ticker                 String,
            ticker_name            String DEFAULT '',
            causal_chain           String DEFAULT '',
            summary                String DEFAULT '',
            confidence             Float32 DEFAULT 0.5,
            time_horizon           LowCardinality(String) DEFAULT '1-3d',
            source_cluster         String DEFAULT '',
            source_tickers         Array(String) DEFAULT [],
            source_urls            Array(String) DEFAULT [],
            evidence_titles        Array(String) DEFAULT [],
            updated_at             DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(asof_ts)
        ORDER BY (ticker)
        COMMENT 'LLM이 생성한 인과 추론 보조지표'
        """
    )


def _build_schema_file() -> str:
    schema = {
        "type": "object",
        "required": ["items"],
        "properties": {
            "items": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                "required": [
                    "ticker",
                    "causal_chain",
                    "summary",
                    "confidence",
                    "ticker_name",
                    "time_horizon",
                    "source_cluster",
                    "source_tickers",
                    "source_urls",
                    "evidence_titles",
                    "notes",
                ],
                    "properties": {
                        "ticker": {"type": "string"},
                        "ticker_name": {"type": "string"},
                        "causal_chain": {"type": "string"},
                        "summary": {"type": "string"},
                        "time_horizon": {"type": "string"},
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                        "source_cluster": {"type": "string"},
                        "source_tickers": {"type": "array", "items": {"type": "string"}},
                        "source_urls": {"type": "array", "items": {"type": "string"}},
                        "evidence_titles": {"type": "array", "items": {"type": "string"}},
                        "notes": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            },
        },
        "additionalProperties": False,
    }
    p = Path("/tmp/llm_relation_reasoning.schema.json")
    p.write_text(json.dumps(schema, ensure_ascii=False), encoding="utf-8")
    return str(p)


def _collect_candidate_signals(min_score: float, top_tickers: int) -> list[dict[str, Any]]:
    min_score = max(0.0, float(min_score))
    top_tickers = max(1, int(top_tickers))
    return _ch_select(
        f"""
        SELECT
            ticker,
            ticker_name,
            abs(total_relation_score) AS abs_score,
            total_relation_score,
            support_events,
            support_clusters,
            relation_bias,
            arrayStringConcat(source_tickers, ',') AS source_tickers_str
        FROM trading.v_hidden_relation_signals
        WHERE abs(total_relation_score) >= {min_score}
          AND ticker != ''
        ORDER BY abs_score DESC, support_events DESC
        LIMIT {top_tickers}
        """
    )


@dataclass
class CandidateContext:
    ticker: str
    ticker_name: str
    relation: dict[str, Any]
    events: list[dict[str, Any]]
    states: list[dict[str, Any]]


def _collect_event_context(ticker: str, hours: int, limit: int) -> list[dict[str, Any]]:
    limit = max(1, int(limit))
    return _ch_select(
        f"""
        SELECT
            toString(published_at) AS published_at_s,
            title,
            event_type,
            event_subtype,
            importance,
            sentiment,
            time_horizon,
            lag_hours,
            arrayStringConcat(channels, ', ') AS channels_str,
            source_url,
            thesis_path,
            invalidation,
            analysis_confidence
        FROM trading.news_event_frames
        WHERE published_at >= now() - INTERVAL {max(1, int(hours))} HOUR
            AND has(tickers, {_sql_quote(ticker)})
            AND relevant = 1
        ORDER BY published_at DESC
        LIMIT {limit}
        """
    )


def _collect_state_context(ticker: str, hours: int, limit: int) -> list[dict[str, Any]]:
    limit = max(1, int(limit))
    return _ch_select(
        f"""
        SELECT
            cluster_id,
            state_label,
            toString(asof_ts) AS asof_ts_s,
            storyline,
            n_news,
            delta_news,
            round(delta_sentiment, 3) AS delta_sentiment,
            changed,
            importance_max
        FROM trading.news_cluster_state
        WHERE asof_ts >= now() - INTERVAL {max(1, int(hours))} HOUR
          AND has(top_tickers, {_sql_quote(ticker)})
        ORDER BY asof_ts DESC
        LIMIT {limit}
        """
    )


def _build_prompt(candidates: list[CandidateContext]) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S KST")
    lines: list[str] = []
    lines.append(
        "너는 '뉴스 연관성 인과 추론기'다. 아래 종목별 뉴스 클러스터/이벤트 문맥을 바탕으로 "
        "보이는 인과 관계 사슬(Cause -> Effect) 2~5줄 한국어 요약을 생성하라."
    )
    lines.append("출력은 JSON 객체만 허용한다. 키는 반드시 \"items\" 하나여야 하며 빈 배열을 허용하지 않는다.")
    lines.append("후보 개수는 입력된 종목 수만큼 채우고, 최소 1개는 항상 출력한다.")
    lines.append(f"[현재시각] {now}")

    for c in candidates:
        relation = c.relation
        source_tickers = str(relation.get("source_tickers_str", "")).replace(",", ", ")
        lines.append("\n### 종목")
        lines.append(f"- ticker: {c.ticker}")
        lines.append(f"- ticker_name: {c.ticker_name}")
        lines.append(
            "- relation_score: "
            f"{_safe_float(relation.get('total_relation_score', 0), 0):+.3f}, "
            f"bias={relation.get('relation_bias', 'neutral')}, "
            f"events={_safe_int(relation.get('support_events', 0))}, "
            f"clusters={_safe_int(relation.get('support_clusters', 0))}, "
            f"sources=[{source_tickers}]"
        )

        lines.append("- 이벤트 맥락:")
        if not c.events:
            lines.append("  - (없음)")
        else:
            for e in c.events[:6]:
                lines.append(
                    f"  - {e.get('published_at_s','')}: {e.get('event_type','')} "
                    f"importance={e.get('importance','')} sent={e.get('sentiment','')} "
                    f"horizon={e.get('time_horizon','')} lag={e.get('lag_hours','')} "
                    f"channels={e.get('channels_str','')}"
                )
                if e.get("title"):
                    lines.append(f"    title={str(e.get('title'))[:110]}")
                if e.get("source_url"):
                    lines.append(f"    source={e.get('source_url')}")

        lines.append("- 클러스터 상태:")
        if not c.states:
            lines.append("  - (없음)")
        else:
            for s in c.states[:3]:
                lines.append(
                    f"  - {s.get('cluster_id','')} | {s.get('state_label','')} "
                    f"| n={s.get('n_news','')} "
                    f"| delta_news={s.get('delta_news','')} delta_sent={s.get('delta_sentiment','')}"
                )
                if s.get("asof_ts_s"):
                    lines[-1] = f"  - {s.get('cluster_id','')} | {s.get('asof_ts_s','')} | {s.get('state_label','')} "
                    lines[-1] += f"| n={s.get('n_news','')} | delta_news={s.get('delta_news','')} delta_sent={s.get('delta_sentiment','')}"
                if s.get("storyline"):
                    lines.append(f"    {str(s.get('storyline'))[:100]}")

        lines.append(
            "요청 출력(JSON object) fields: "
            "ticker, ticker_name, causal_chain(원인-영향 사슬), summary(매매 관점 해석), "
            "time_horizon, confidence(0~1), source_cluster, source_tickers(array), source_urls(array), "
            "evidence_titles(array), notes(실행 시 주의점)"
        )

    lines.append("\n출력 예시 하나:")
    lines.append('{"items":[{"ticker":"000000","ticker_name":"예시기업","causal_chain":"A사 공급망 지연 → B사 부품 병목 완화 기대","summary":"...","time_horizon":"1-3d","confidence":0.71,"source_cluster":"cluster-id","source_tickers":["000000"],"source_urls":["https://example.com"],"evidence_titles":["제목1"],"notes":"주의사항"}]}')
    return "\n".join(lines)


def _call_llm(prompt: str, timeout_sec: int, cache_ttl_sec: int, workdir: str, codex_bin: str, model: str) -> str:
    schema_path = _build_schema_file()
    return run_codex_cached(
        prompt=prompt,
        codex_bin=codex_bin,
        model=model,
        workdir=workdir,
        timeout_sec=timeout_sec,
        base_args=["--json", "--skip-git-repo-check"],
        output_schema_path=schema_path,
        cache_ttl_sec=cache_ttl_sec,
    )


def _parse_llm_output(raw: str) -> list[dict[str, Any]]:
    if not isinstance(raw, str):
        return []
    text = raw.strip()
    if text.startswith("```"):
        lines = []
        for ln in text.splitlines():
            l = ln.strip()
            if l.startswith("```"):
                continue
            lines.append(ln)
        text = "\n".join(lines).strip()
    if text.lower().startswith("json"):
        text = text[4:].strip()
    try:
        data = json.loads(text)
    except Exception:
        return []

    if isinstance(data, dict):
        rows = data.get("items")
        if not isinstance(rows, list):
            return []
    elif isinstance(data, list):
        rows = data
    else:
        return []

    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        ticker = str(r.get("ticker", "")).strip()
        if not ticker:
            continue
        out.append(
            {
                "ticker": ticker,
                "ticker_name": str(r.get("ticker_name", "")).strip(),
                "causal_chain": str(r.get("causal_chain", "")).strip()[:700],
                "summary": str(r.get("summary", "")).strip()[:1000],
                "time_horizon": str(r.get("time_horizon", "1-3d")).strip(),
                "confidence": max(0.0, min(1.0, _safe_float(r.get("confidence", 0.5), 0.5))),
                "source_cluster": str(r.get("source_cluster", "")).strip(),
                "source_tickers": [str(x).strip() for x in (r.get("source_tickers", []) or []) if str(x).strip()],
                "source_urls": [str(x).strip() for x in (r.get("source_urls", []) or []) if str(x).strip()],
                "evidence_titles": [str(x).strip() for x in (r.get("evidence_titles", []) or []) if str(x).strip()],
                "notes": str(r.get("notes", "")).strip()[:500],
                "asof_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return out


def _write_reasonings(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    _ch_execute(
        "INSERT INTO trading.hidden_relation_reasoning FORMAT JSONEachRow\n" + payload
    )


def _collect_context(candidates: list[dict[str, Any]], lookback_hours: int, events_per_ticker: int, clusters_per_ticker: int) -> list[CandidateContext]:
    out: list[CandidateContext] = []
    for r in candidates:
        ticker = str(r.get("ticker", "")).strip()
        if not ticker:
            continue
        ev = _collect_event_context(ticker, lookback_hours, events_per_ticker)
        st = _collect_state_context(ticker, lookback_hours, clusters_per_ticker)
        out.append(
            CandidateContext(
                ticker=ticker,
                ticker_name=str(r.get("ticker_name", "")).strip(),
                relation={
                    "total_relation_score": _safe_float(r.get("total_relation_score", 0.0)),
                    "relation_bias": str(r.get("relation_bias", "neutral")),
                    "support_events": _safe_int(r.get("support_events", 0)),
                    "support_clusters": _safe_int(r.get("support_clusters", 0)),
                    "source_tickers_str": r.get("source_tickers_str", ""),
                },
                events=ev,
                states=st,
            )
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate hidden-relation causal reasoning rows.")
    parser.add_argument("--lookback-hours", type=int, default=72)
    parser.add_argument("--min-score", type=float, default=0.12)
    parser.add_argument("--top-tickers", type=int, default=30)
    parser.add_argument("--events-per-ticker", type=int, default=5)
    parser.add_argument("--states-per-ticker", type=int, default=3)
    parser.add_argument("--codex-bin", default=os.getenv("CODEX_BIN", "openclaw"))
    parser.add_argument("--model", default=os.getenv("CODEX_MODEL", MODEL_DEFAULT))
    parser.add_argument("--cache-ttl-sec", type=int, default=CODEX_CACHE_TTL_SEC)
    parser.add_argument("--timeout-sec", type=int, default=CODEX_EXEC_TIMEOUT)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-events-context", type=int, default=MAX_EVENT_CONTEXT)
    parser.add_argument("--workdir", default=str(Path.home()))
    args = parser.parse_args()

    max_events_context = args.max_events_context or MAX_EVENT_CONTEXT

    if args.dry_run or DEFAULT_DRY_RUN:
        LOGGER.info("dry-run mode enabled")

    _ensure_table()
    candidates = _collect_candidate_signals(
        min_score=args.min_score,
        top_tickers=args.top_tickers,
    )
    if not candidates:
        LOGGER.info("candidate relation signals: 0")
        return

    _log(f"candidate relation signals: {len(candidates)}")

    contexts = _collect_context(
        candidates=candidates,
        lookback_hours=args.lookback_hours,
        events_per_ticker=args.events_per_ticker,
        clusters_per_ticker=args.states_per_ticker,
    )
    if not contexts:
        LOGGER.warning("No candidate contexts available.")
        return

    prompt = _build_prompt(contexts)
    if len(prompt) > max_events_context:
        prompt = prompt[:max_events_context]

    raw = _call_llm(
        prompt=prompt,
        timeout_sec=args.timeout_sec,
        cache_ttl_sec=args.cache_ttl_sec,
        workdir=args.workdir,
        codex_bin=args.codex_bin,
        model=args.model or MODEL_DEFAULT,
    )
    if not raw.strip():
        LOGGER.warning("empty LLM output")
        return

    parsed = _parse_llm_output(raw)
    if not parsed:
        LOGGER.warning("empty parsed reasonings")
        return
    if args.dry_run:
        LOGGER.info("Dry-run parsed rows=%d", len(parsed))
        print(json.dumps(parsed, ensure_ascii=False, indent=2))
        return

    _write_reasonings(parsed)
    LOGGER.info("stored rows=%d", len(parsed))


if __name__ == "__main__":
    main()
