#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import time
import hashlib
import tempfile
import sys
from urllib.parse import urlparse
from urllib.request import Request, urlopen
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env
from codex_exec_guard import run_codex_cached

bootstrap_openclaw_env()

CH_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
CH_DB = os.environ.get("CLICKHOUSE_DB", "trading")
WINDOW_HOURS = int(os.environ.get("NEWS_RESEARCH_WINDOW_HOURS", "6"))
LIMIT = int(os.environ.get("NEWS_RESEARCH_LIMIT", "8"))
BATCH = int(os.environ.get("NEWS_RESEARCH_BATCH", "4"))
OPENCLAW_BRAIN_MODEL = "openai-codex/gpt-5.3-codex-spark"
_env_model = os.environ.get("NEWS_RESEARCH_MODEL", "").strip() or os.environ.get("CODEX_MODEL", "").strip()
MODEL = _env_model or OPENCLAW_BRAIN_MODEL
WORKDIR = os.environ.get("NEWS_CODEX_WORKDIR", os.path.expanduser("~/.openclaw/logs"))
CODEX_EXEC_CACHE_DIR = os.path.expanduser(os.environ.get("CODEX_EXEC_CACHE_DIR", "~/.openclaw/cache/codex-exec"))
CODEX_EXEC_CACHE_TTL = int(
    os.environ.get("NEWS_RESEARCH_CODEX_CACHE_TTL", os.environ.get("CODEX_EXEC_CACHE_TTL", "300"))
)
CODEX_EXEC_CACHE_LOCK_WAIT = int(
    os.environ.get("NEWS_RESEARCH_CODEX_CACHE_LOCK_WAIT", os.environ.get("CODEX_EXEC_CACHE_LOCK_WAIT", "20"))
)


def normalize_codex_model(model: str) -> str:
    m = (model or "").strip()
    aliases = {
        "openai/gpt-5.2": "gpt-5.3-codex-spark",
        "openai/gpt-5.3": "gpt-5.3-codex-spark",
        "openai-codex/gpt-5.2": "gpt-5.3-codex-spark",
        "openai-codex/gpt-5.3": "gpt-5.3-codex-spark",
        "openai-codex/gpt-5.3-codex-spark": "gpt-5.3-codex-spark",
        "gpt": "gpt-5.3-codex-spark",
    }
    return aliases.get(m, m)

CODEX_CANDIDATES = [
    os.environ.get("CODEX_BIN", ""),
    os.environ.get("OPENCLAW_BIN", ""),
    os.path.expanduser("~/.npm-global/bin/openclaw"),
    "/opt/homebrew/bin/openclaw",
    "/usr/local/bin/openclaw",
    "openclaw",
]


def split_auth(url: str):
    p = urlparse(url)
    if p.username is None:
        return url, None
    import base64
    token = base64.b64encode(f"{p.username}:{p.password or ''}".encode()).decode()
    hostport = p.hostname or ""
    if p.port:
        hostport = f"{hostport}:{p.port}"
    return p._replace(netloc=hostport).geturl(), f"Basic {token}"


def ch_query(sql: str, fmt_json=True):
    url, auth = split_auth(CH_URL)
    params = "?database=%s&default_format=JSON" % CH_DB if fmt_json else f"?database={CH_DB}"
    req = Request(url + params, data=sql.encode("utf-8"), method="POST")
    if auth:
        req.add_header("Authorization", auth)
    with urlopen(req, timeout=30) as r:
        body = r.read().decode("utf-8", errors="replace")
    if fmt_json:
        return json.loads(body).get("data", [])
    return body


def find_codex():
    for c in CODEX_CANDIDATES:
        if not c:
            continue
        if os.path.isfile(c) and os.access(c, os.X_OK):
            return c
        from shutil import which
        w = which(c)
        if w:
            return w
    return None


def codex_exec(prompt: str, schema: dict) -> str:
    schema_file = tempfile.NamedTemporaryFile(prefix="news-research-schema-", suffix=".json", delete=False)
    try:
        schema_file.write(json.dumps(schema, ensure_ascii=False).encode("utf-8"))
        schema_file.close()

        codex = find_codex()
        if not codex:
            raise RuntimeError("llm binary not found")
        raw = run_codex_cached(
            prompt=prompt,
            codex_bin=codex,
            model=MODEL,
            workdir=WORKDIR,
            timeout_sec=300,
            base_args=[
                "--skip-git-repo-check",
                "--full-auto",
                "--cd",
                WORKDIR,
            ],
            output_schema_path=schema_file.name,
            cache_dir=CODEX_EXEC_CACHE_DIR,
            cache_ttl_sec=CODEX_EXEC_CACHE_TTL,
            cache_lock_wait_sec=CODEX_EXEC_CACHE_LOCK_WAIT,
        )
        return raw
    finally:
        try:
            os.remove(schema_file.name)
        except Exception:
            pass


def esc(s: str) -> str:
    return (s or "").replace("\\", "\\\\").replace("'", "\\'")


def hash_news(row: dict) -> str:
    key = f"{row.get('published_at','')}|{row.get('source_url','')}|{row.get('title','')}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def to_arr(items):
    if not isinstance(items, list):
        return []
    out = []
    for x in items:
        s = str(x).strip()
        if s:
            out.append(s)
    return out[:12]


def insert_rows(rows: list[dict]):
    if not rows:
        return 0
    vals = []
    for r in rows:
        d1 = ",".join([f"'{esc(x)}'" for x in to_arr(r.get("direct_tickers", []))])
        d2 = ",".join([f"'{esc(x)}'" for x in to_arr(r.get("secondary_tickers", []))])
        d3 = ",".join([f"'{esc(x)}'" for x in to_arr(r.get("tertiary_tickers", []))])
        tr = ",".join([f"'{esc(x)}'" for x in to_arr(r.get("tickers_raw", []))])
        vals.append(
            "(" 
            f"now(), '{esc(r['news_id'])}', toDateTime('{esc(r['published_at'])}'), '{esc(r['title'])}', '{esc(r['source_url'])}', '{esc(r['source_domain'])}', "
            f"{int(r.get('importance',1))}, '{esc(r.get('sentiment','neutral'))}', '{esc(r.get('impact_type','stock'))}', [{tr}], "
            f"[{d1}], [{d2}], [{d3}], "
            f"'{esc(r.get('source_verdict','uncertain'))}', '{esc(r.get('source_notes',''))}', "
            f"'{esc(r.get('hidden_point',''))}', '{esc(r.get('followup_question',''))}', '{esc(r.get('followup_plan',''))}', "
            f"'{esc(r.get('thesis','mixed'))}', {float(r.get('confidence',0.5))}, {int(r.get('expected_horizon_days',5))}, '{esc(r.get('pnl_hypothesis',''))}', "
            f"'{esc(normalize_codex_model(MODEL))}', '{esc(json.dumps(r.get('model_output', {}), ensure_ascii=False))}', now()"
            ")"
        )

    sql = """
    INSERT INTO trading.news_research
    (analyzed_at, news_id, published_at, title, source_url, source_domain, importance, sentiment, impact_type, tickers_raw,
     direct_tickers, secondary_tickers, tertiary_tickers, source_verdict, source_notes,
     hidden_point, followup_question, followup_plan, thesis, confidence, expected_horizon_days, pnl_hypothesis,
     model, model_output_json, created_at)
    VALUES
    """ + ",".join(vals)
    ch_query(sql, fmt_json=False)
    return len(rows)


def main():
    # ensure table exists
    schema_path = os.path.expanduser("~/.openclaw/scripts/trading/schema_news_research.sql")
    if os.path.exists(schema_path):
        with open(schema_path, "r", encoding="utf-8") as f:
            ch_query(f.read(), fmt_json=False)

    rows = ch_query(f"""
        SELECT published_at, title, summary, source_url, importance, sentiment, impact_type, tickers
        FROM trading.news
        WHERE published_at > now() - INTERVAL {WINDOW_HOURS} HOUR
          AND importance >= 3
        ORDER BY importance DESC, published_at DESC
        LIMIT {LIMIT}
    """)

    if not rows:
        print("[news-research] no recent news")
        return

    existing = ch_query(f"""
        SELECT news_id FROM trading.news_research
        WHERE published_at > now() - INTERVAL {WINDOW_HOURS + 24} HOUR
    """)
    existing_ids = {x.get("news_id") for x in existing}

    candidates = []
    for r in rows:
        nid = hash_news(r)
        if nid in existing_ids:
            continue
        r["news_id"] = nid
        candidates.append(r)

    if not candidates:
        print("[news-research] all already analyzed")
        return

    schema = {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "idx": {"type": "integer"},
                        "direct_tickers": {"type": "array", "items": {"type": "string"}},
                        "secondary_tickers": {"type": "array", "items": {"type": "string"}},
                        "tertiary_tickers": {"type": "array", "items": {"type": "string"}},
                        "source_verdict": {"type": "string"},
                        "source_notes": {"type": "string"},
                        "hidden_point": {"type": "string"},
                        "followup_question": {"type": "string"},
                        "followup_plan": {"type": "string"},
                        "thesis": {"type": "string"},
                        "confidence": {"type": "number"},
                        "expected_horizon_days": {"type": "integer"},
                        "pnl_hypothesis": {"type": "string"}
                    },
                    "required": ["idx","direct_tickers","secondary_tickers","tertiary_tickers","source_verdict","hidden_point","followup_question","thesis","confidence","expected_horizon_days","pnl_hypothesis"]
                }
            }
        },
        "required": ["items"]
    }

    inserted = 0
    for i in range(0, len(candidates), BATCH):
        batch = candidates[i:i+BATCH]
        lines = []
        for j, n in enumerate(batch, start=1):
            lines.append(
                f"[{j}] title={n.get('title')}\\nsummary={n.get('summary')}\\nurl={n.get('source_url')}\\nimportance={n.get('importance')}\\nsentiment={n.get('sentiment')}\\ntickers={n.get('tickers', [])}"
            )
        prompt = (
            "한국 주식 뉴스 심층 연구를 수행하라.\\n"
            "각 기사마다 1차(직접),2차(공급망/경쟁),3차(우회/대체) 연관 종목코드(6자리) 제시.\\n"
            "출처 검증: URL 도메인/기사내용 정합성 관점에서 valid|uncertain|conflict 판정.\\n"
            "사람들이 놓칠 포인트 1개(hidden_point), 후속검증 질문 1개(followup_question), 추적계획 1개(followup_plan) 작성.\\n"
            "수익가설: thesis(bullish/bearish/mixed), confidence(0~1), expected_horizon_days, pnl_hypothesis 제시.\\n"
            "반드시 기사별 idx를 포함해 구조화 응답.\\n\\n"
            + "\\n\\n".join(lines)
        )

        try:
            raw = codex_exec(prompt, schema)
            obj = json.loads(raw)
            mapped = {}
            for it in obj.get("items", []):
                idx = int(it.get("idx", 0))
                if 1 <= idx <= len(batch):
                    mapped[idx] = it

            out_rows = []
            for idx, n in enumerate(batch, start=1):
                m = mapped.get(idx, {})
                domain = urlparse(n.get("source_url", "")).netloc
                out_rows.append({
                    "news_id": n["news_id"],
                    "published_at": n.get("published_at"),
                    "title": n.get("title"),
                    "source_url": n.get("source_url"),
                    "source_domain": domain,
                    "importance": n.get("importance", 1),
                    "sentiment": n.get("sentiment", "neutral"),
                    "impact_type": n.get("impact_type", "stock"),
                    "tickers_raw": n.get("tickers", []),
                    "direct_tickers": m.get("direct_tickers", []),
                    "secondary_tickers": m.get("secondary_tickers", []),
                    "tertiary_tickers": m.get("tertiary_tickers", []),
                    "source_verdict": m.get("source_verdict", "uncertain"),
                    "source_notes": m.get("source_notes", ""),
                    "hidden_point": m.get("hidden_point", ""),
                    "followup_question": m.get("followup_question", ""),
                    "followup_plan": m.get("followup_plan", ""),
                    "thesis": m.get("thesis", "mixed"),
                    "confidence": m.get("confidence", 0.5),
                    "expected_horizon_days": m.get("expected_horizon_days", 5),
                    "pnl_hypothesis": m.get("pnl_hypothesis", ""),
                    "model_output": m,
                })

            inserted += insert_rows(out_rows)
            time.sleep(0.4)
        except Exception as e:
            print(f"[news-research] batch failed: {e}")
            # Codex 실패 시에도 연구 큐를 데이터화(후속 재분석 가능)
            fallback_rows = []
            for n in batch:
                domain = urlparse(n.get("source_url", "")).netloc
                fallback_rows.append({
                    "news_id": n["news_id"],
                    "published_at": n.get("published_at"),
                    "title": n.get("title"),
                    "source_url": n.get("source_url"),
                    "source_domain": domain,
                    "importance": n.get("importance", 1),
                    "sentiment": n.get("sentiment", "neutral"),
                    "impact_type": n.get("impact_type", "stock"),
                    "tickers_raw": n.get("tickers", []),
                    "direct_tickers": n.get("tickers", []),
                    "secondary_tickers": [],
                    "tertiary_tickers": [],
                    "source_verdict": "uncertain",
                    "source_notes": "codex_failed_auto_fallback",
                    "hidden_point": "코덱스 실패로 심층해석 보류",
                    "followup_question": "후속 재분석 시 공급망 2차 수혜/피해를 확인할 것",
                    "followup_plan": "다음 주기에서 재분석",
                    "thesis": "mixed",
                    "confidence": 0.3,
                    "expected_horizon_days": 5,
                    "pnl_hypothesis": "데이터 축적용 임시 레코드",
                    "model_output": {"error": str(e)[:500]},
                })
            inserted += insert_rows(fallback_rows)

    print(f"[news-research] inserted={inserted} model={MODEL}")


if __name__ == "__main__":
    main()
