#!/usr/bin/env python3
"""reembed_news_anomalies.py

news.embedding 차원 이상치(length != target_dim) 행을 재임베딩해 업데이트한다.
기본은 target_dim=1024, Ollama(bge-m3) 사용.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import sys
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

import requests
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()


def _log(msg: str) -> None:
    print(f"{dt.datetime.now().strftime('%H:%M:%S')} [reembed] {msg}", flush=True)


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


def ch_select(sql: str, timeout_sec: int = 30) -> list[dict]:
    url, headers = _ch_url_and_headers()
    q = sql.strip() + "\nFORMAT JSON"
    req = Request(url, data=q.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        body = r.read().decode("utf-8", errors="replace")
    obj = json.loads(body)
    data = obj.get("data", [])
    return data if isinstance(data, list) else []


def ch_execute(sql: str, timeout_sec: int = 120) -> None:
    url, headers = _ch_url_and_headers()
    req = Request(url, data=sql.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        _ = r.read()


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def fetch_anomalies(limit: int, target_dim: int) -> list[dict]:
    return ch_select(
        f"""
SELECT
    id,
    title,
    summary,
    length(embedding) AS dim,
    published_at,
    collected_at
FROM trading.news
WHERE length(embedding) > 0
  AND length(embedding) != {int(target_dim)}
ORDER BY collected_at DESC
LIMIT {int(limit)}
"""
    )


def embed_ollama(text: str, model: str, base_url: str) -> list[float]:
    url = base_url.rstrip("/") + "/api/embeddings"
    payload = {"model": model, "prompt": text[:500]}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    emb = r.json().get("embedding", [])
    if not isinstance(emb, list):
        return []
    return [_safe_float(x) for x in emb]


def fmt_array_f32(arr: list[float]) -> str:
    # ClickHouse Array(Float32) literal
    return "[" + ",".join(f"{x:.8f}" for x in arr) + "]"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-dim", type=int, default=1024)
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--model", default=os.getenv("OLLAMA_EMBED_MODEL", "bge-m3"))
    ap.add_argument("--ollama-url", default=os.getenv("OLLAMA_URL", "http://127.0.0.1:11434"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    rows = fetch_anomalies(limit=max(1, args.limit), target_dim=max(1, args.target_dim))
    if not rows:
        _log("차원 이상치 없음")
        print(json.dumps({"ok": True, "updated": 0, "skipped": 0}, ensure_ascii=False))
        return 0

    _log(f"대상 {len(rows)}건")
    updated = 0
    skipped = 0

    for r in rows:
        nid = str(r.get("id") or "")
        title = str(r.get("title") or "").strip()
        summary = str(r.get("summary") or "").strip()
        dim = int(r.get("dim") or 0)
        text = title if title else summary
        if not text:
            _log(f"skip(no text): {nid}")
            skipped += 1
            continue

        try:
            emb = embed_ollama(text=text, model=args.model, base_url=args.ollama_url)
        except Exception as e:
            _log(f"embed failed {nid}: {e}")
            skipped += 1
            continue

        if len(emb) != args.target_dim:
            _log(f"skip(dim mismatch) {nid}: old={dim}, new={len(emb)}")
            skipped += 1
            continue

        _log(f"reembed {nid}: {dim} -> {len(emb)}")
        if args.dry_run:
            continue

        arr = fmt_array_f32(emb)
        nid_esc = nid.replace("'", "\\'")
        sql = (
            "ALTER TABLE trading.news "
            f"UPDATE embedding = CAST({arr}, 'Array(Float32)') "
            f"WHERE id = '{nid_esc}'"
        )
        ch_execute(sql, timeout_sec=180)
        updated += 1

    summary = {"ok": True, "target_dim": args.target_dim, "total": len(rows), "updated": updated, "skipped": skipped}
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
