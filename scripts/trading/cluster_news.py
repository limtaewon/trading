#!/usr/bin/env python3
"""cluster_news.py

뉴스 임베딩 기반 "이슈 클러스터" 스냅샷 생성기.

목적:
- 기사 단위(개별 news rows)에서 이슈 단위(클러스터)로 묶어 Codex 프롬프트에 제공.
- cluster_id / cluster 요약 / top tickers / sentiment / importance 등을 ClickHouse에 저장.

테이블:
- trading.news_clusters
- trading.news_cluster_map

기본 정책(보수형):
- 최근 window_hours(기본 72h) 내 embedding이 있는 뉴스만 사용
- 단순 online clustering(centroid 기반)으로 클러스터링
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import math
import os
import sys
from collections import Counter
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen


def _log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"{ts} [cluster_news] {msg}", flush=True)


def _notify(text: str) -> None:
    try:
        from telegram_notify import notify
    except Exception:
        return
    try:
        notify(f"🧩 <b>뉴스 클러스터</b>\n{text}")
    except Exception as e:
        _log(f"telegram notify failed: {e}")


def _ch_url_and_headers() -> tuple[str, dict[str, str]]:
    """Build ClickHouse HTTP endpoint and auth headers.

    Prefer CLICKHOUSE_HOST/USER/PASS. If missing, fall back to CLICKHOUSE_URL parsing.
    """
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
        # Convert userinfo into Basic auth header and strip it from URL.
        auth = f"{sp.username}:{sp.password or ''}".encode("utf-8")
        headers["Authorization"] = "Basic " + base64.b64encode(auth).decode("ascii")
        netloc = sp.hostname or "localhost"
        if sp.port:
            netloc = f"{netloc}:{sp.port}"
        clean = urlunsplit((sp.scheme or "http", netloc, sp.path or "", sp.query, sp.fragment))
        return clean, headers

    return url, headers


def ch_select(sql: str, timeout_sec: int = 60) -> list[dict[str, Any]]:
    url, headers = _ch_url_and_headers()
    q = sql.strip() + "\nFORMAT JSON"
    req = Request(url, data=q.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        body = r.read().decode("utf-8", errors="replace")
    obj = json.loads(body)
    return obj.get("data", []) or []


def ch_insert_json_each_row(table: str, rows: list[dict[str, Any]], timeout_sec: int = 120) -> None:
    if not rows:
        return
    url, headers = _ch_url_and_headers()
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    q = f"INSERT INTO {table} FORMAT JSONEachRow\n".encode("utf-8") + payload.encode("utf-8")
    req = Request(url, data=q, headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        _ = r.read()


def _dot(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def _norm(a: list[float]) -> float:
    return math.sqrt(sum(x * x for x in a))


def _normalize(a: list[float]) -> list[float]:
    n = _norm(a)
    if n <= 0:
        return []
    return [x / n for x in a]


@dataclass
class _Item:
    news_id: str
    title: str
    summary: str
    category: str
    importance: int
    sentiment: str
    tickers: list[str]
    source_url: str
    published_at: str
    embedding: list[float]  # normalized


@dataclass
class _Cluster:
    members: list[_Item]
    sum_vec: list[float]
    centroid: list[float]  # normalized

    def add(self, item: _Item) -> float:
        """Add item, return distance to previous centroid."""
        # cos distance since vectors are normalized
        d = 1.0 - _dot(item.embedding, self.centroid)
        if not self.sum_vec:
            self.sum_vec = item.embedding[:]
        else:
            for i, v in enumerate(item.embedding):
                self.sum_vec[i] += v
        self.centroid = _normalize(self.sum_vec)
        self.members.append(item)
        return float(d)


def _cluster_id_for(members: list[_Item]) -> str:
    seed = min((m.source_url or "") for m in members) or (members[0].source_url or members[0].title)
    h = hashlib.sha1(seed.encode("utf-8", errors="replace")).hexdigest()[:12]
    return f"c_{h}"


def _topn(counter: Counter[str], n: int) -> list[str]:
    return [k for k, _ in counter.most_common(n)]


def _summarize_cluster(members: list[_Item]) -> tuple[str, list[str]]:
    # Representative = max importance, then most recent published_at string (best-effort)
    rep = sorted(members, key=lambda m: (m.importance, m.published_at), reverse=True)[0]
    tickers = []
    for m in members:
        tickers.extend(m.tickers or [])
    top_tickers = _topn(Counter(tickers), 5)
    base = rep.summary.strip() if rep.summary.strip() else rep.title.strip()
    base = base.replace("\n", " ").strip()
    if top_tickers:
        return f"{base} (관련: {', '.join(top_tickers)})", top_tickers
    return base, top_tickers


def _load_prev_cluster_state(lookback_hours: int = 168) -> dict[str, dict[str, Any]]:
    sql = f"""
        SELECT
            cluster_id,
            argMax(n_news, asof_ts) AS n_news,
            argMax(sentiment_bias, asof_ts) AS sentiment_bias,
            argMax(storyline, asof_ts) AS storyline
        FROM trading.news_cluster_state
        WHERE asof_ts >= now() - INTERVAL {max(1, int(lookback_hours))} HOUR
        GROUP BY cluster_id
    """
    try:
        rows = ch_select(sql, timeout_sec=45)
    except Exception as e:
        _log(f"news_cluster_state 조회 실패(초기 실행 가능): {e}")
        return {}

    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        cid = str(r.get("cluster_id", "") or "")
        if not cid:
            continue
        try:
            n_news = int(r.get("n_news", 0) or 0)
        except Exception:
            n_news = 0
        try:
            bias = float(r.get("sentiment_bias", 0.0) or 0.0)
        except Exception:
            bias = 0.0
        out[cid] = {
            "n_news": n_news,
            "sentiment_bias": bias,
            "storyline": str(r.get("storyline", "") or ""),
        }
    return out


def _resolve_state(prev_n: int | None, prev_bias: float | None, n: int, bias: float) -> str:
    if prev_n is None or prev_bias is None:
        return "emerging"

    delta_n = n - prev_n
    delta_bias = bias - prev_bias
    crossed = (prev_bias > 0 and bias < 0) or (prev_bias < 0 and bias > 0)

    if crossed and abs(delta_bias) >= 0.20:
        return "reversing"
    if delta_n >= 2 or (delta_n >= 1 and abs(delta_bias) >= 0.08):
        return "reinforcing"
    if abs(delta_n) <= 1 and abs(delta_bias) <= 0.06:
        return "stable"
    if delta_n < 0 and abs(delta_bias) >= 0.10:
        return "reversing"
    return "stable"


def build_clusters(
    items: list[_Item],
    threshold: float,
) -> tuple[list[_Cluster], dict[str, float]]:
    clusters: list[_Cluster] = []
    assigned_dist: dict[str, float] = {}

    for it in items:
        best_idx = -1
        best_dist = 999.0
        for ci, c in enumerate(clusters):
            d = 1.0 - _dot(it.embedding, c.centroid)
            if d < best_dist:
                best_dist = d
                best_idx = ci

        if best_idx >= 0 and best_dist <= threshold:
            d = clusters[best_idx].add(it)
            assigned_dist[it.news_id] = float(d)
        else:
            c = _Cluster(members=[it], sum_vec=it.embedding[:], centroid=it.embedding[:])
            clusters.append(c)
            assigned_dist[it.news_id] = 0.0

    return clusters, assigned_dist


def main() -> int:
    ap = argparse.ArgumentParser(description="뉴스 임베딩 기반 이슈 클러스터 스냅샷 생성")
    ap.add_argument("--window-hours", type=int, default=int(os.getenv("NEWS_CLUSTER_WINDOW_HOURS", "144")))
    ap.add_argument("--threshold", type=float, default=float(os.getenv("NEWS_CLUSTER_THRESHOLD", "0.48")))
    ap.add_argument("--min-size", type=int, default=int(os.getenv("NEWS_CLUSTER_MIN_SIZE", "2")))
    ap.add_argument("--limit", type=int, default=int(os.getenv("NEWS_CLUSTER_LIMIT", "10000")))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    window = max(1, int(args.window_hours))
    threshold = float(args.threshold)
    min_size = max(1, int(args.min_size))
    limit = max(100, int(args.limit))

    _log(f"window_hours={window} threshold={threshold:.3f} min_size={min_size} limit={limit} dry_run={args.dry_run}")

    sql = f"""
        SELECT
            toString(id) AS news_id,
            title,
            summary,
            category,
            toInt32(importance) AS importance,
            sentiment,
            tickers,
            source_url,
            toString(published_at) AS published_at,
            embedding
        FROM trading.news
        WHERE collected_at >= now() - INTERVAL {window} HOUR
          AND length(embedding) > 0
        ORDER BY published_at DESC
        LIMIT {limit}
    """
    rows = ch_select(sql, timeout_sec=90)
    _log(f"fetched news rows={len(rows)}")
    if not rows:
        _notify("⏭️ 최근 윈도우 내 뉴스 없음")
        return 0

    items: list[_Item] = []
    for r in rows:
        emb = r.get("embedding") or []
        if not isinstance(emb, list) or len(emb) < 8:
            continue
        embf = []
        try:
            embf = [float(x) for x in emb]
        except Exception:
            continue
        embn = _normalize(embf)
        if not embn:
            continue

        tickers = r.get("tickers") or []
        if not isinstance(tickers, list):
            tickers = []

        items.append(
            _Item(
                news_id=str(r.get("news_id", "")),
                title=str(r.get("title", "") or ""),
                summary=str(r.get("summary", "") or ""),
                category=str(r.get("category", "") or ""),
                importance=int(r.get("importance", 1) or 1),
                sentiment=str(r.get("sentiment", "") or "neutral"),
                tickers=[str(t) for t in tickers if str(t)],
                source_url=str(r.get("source_url", "") or ""),
                published_at=str(r.get("published_at", "") or ""),
                embedding=embn,
            )
        )

    _log(f"usable items (with embedding)={len(items)}")
    if not items:
        _notify("⏭️ 임베딩 사용 뉴스가 없어 스킵")
        return 0

    clusters, assigned_dist = build_clusters(items, threshold=threshold)
    _log(f"raw clusters={len(clusters)}")

    # Filter clusters by min size for storage, but keep map for all items (singletons are still mapped).
    filtered: list[_Cluster] = [c for c in clusters if len(c.members) >= min_size]
    _log(f"clusters kept (size>={min_size})={len(filtered)}")
    snapshot_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prev_state = _load_prev_cluster_state(lookback_hours=max(24, window * 7))

    cluster_rows: list[dict[str, Any]] = []
    state_rows: list[dict[str, Any]] = []
    map_rows: list[dict[str, Any]] = []

    for c in clusters:
        cid = _cluster_id_for(c.members)
        for m in c.members:
            map_rows.append(
                {
                    "news_id": m.news_id,
                    "cluster_id": cid,
                    "asof_ts": snapshot_ts,
                    "window_hours": window,
                    "distance": float(assigned_dist.get(m.news_id, 0.0)),
                }
            )

    for c in filtered:
        cid = _cluster_id_for(c.members)
        imax = max(m.importance for m in c.members)
        sp = sum(1 for m in c.members if m.sentiment == "positive")
        sn = sum(1 for m in c.members if m.sentiment == "negative")
        su = sum(1 for m in c.members if m.sentiment not in ("positive", "negative"))
        tickers_all: list[str] = []
        cats_all: list[str] = []
        for m in c.members:
            tickers_all.extend(m.tickers or [])
            if m.category:
                cats_all.append(m.category)

        top_tickers = _topn(Counter(tickers_all), 6)
        top_cats = _topn(Counter(cats_all), 4)
        example_titles = [m.title[:90] for m in sorted(c.members, key=lambda m: (m.importance, m.published_at), reverse=True)[:4]]
        summary, top_tickers2 = _summarize_cluster(c.members)
        if top_tickers2:
            top_tickers = top_tickers2

        sentiment_bias = float((sp - sn) / max(1, len(c.members)))
        prev = prev_state.get(cid, {})
        prev_n = prev.get("n_news")
        prev_bias = prev.get("sentiment_bias")
        try:
            prev_n_i = int(prev_n) if prev_n is not None else None
        except Exception:
            prev_n_i = None
        try:
            prev_bias_f = float(prev_bias) if prev_bias is not None else None
        except Exception:
            prev_bias_f = None

        delta_news = int(len(c.members) - (prev_n_i or 0)) if prev_n_i is not None else int(len(c.members))
        delta_sent = float(sentiment_bias - (prev_bias_f or 0.0)) if prev_bias_f is not None else float(sentiment_bias)
        state_label = _resolve_state(prev_n_i, prev_bias_f, len(c.members), sentiment_bias)
        prev_story = str(prev.get("storyline", "") or "")
        storyline = summary[:240]
        changed = 1
        if prev_n_i is not None:
            changed = 1 if (
                state_label != "stable"
                or abs(delta_news) >= 2
                or abs(delta_sent) >= 0.06
                or (prev_story[:160] != storyline[:160])
            ) else 0

        cluster_rows.append(
            {
                "cluster_id": cid,
                "asof_ts": snapshot_ts,
                "window_hours": window,
                "n_news": len(c.members),
                "importance_max": int(imax),
                "sentiment_pos": int(sp),
                "sentiment_neg": int(sn),
                "sentiment_neu": int(su),
                "tickers_top": top_tickers,
                "categories_top": top_cats,
                "example_titles": example_titles,
                "summary": summary[:240],
                "centroid": [float(x) for x in c.centroid],
            }
        )
        state_rows.append(
            {
                "cluster_id": cid,
                "asof_ts": snapshot_ts,
                "window_hours": window,
                "state_label": state_label,
                "n_news": int(len(c.members)),
                "importance_max": int(imax),
                "sentiment_bias": round(sentiment_bias, 4),
                "delta_news": int(delta_news),
                "delta_sentiment": round(delta_sent, 4),
                "storyline": storyline,
                "top_tickers": top_tickers[:8],
                "top_categories": top_cats[:6],
                "changed": int(changed),
            }
        )

    # Show a small preview for ops sanity.
    preview = sorted(cluster_rows, key=lambda r: (r["importance_max"], r["n_news"]), reverse=True)[:5]
    for p in preview:
        _log(f"preview cluster {p['cluster_id']} n={p['n_news']} imp={p['importance_max']} tickers={','.join(p['tickers_top'][:3])} summary={p['summary'][:80]}")
    state_preview = sorted(state_rows, key=lambda r: (r["changed"], r["importance_max"], r["n_news"]), reverse=True)[:5]
    for s in state_preview:
        _log(
            "state "
            f"{s['cluster_id']} label={s['state_label']} "
            f"dn={s['delta_news']} ds={s['delta_sentiment']:+.3f} "
            f"changed={s['changed']}"
        )

    if args.dry_run:
        _notify(
            f"🧪 dry-run: window={window} threshold={threshold:.3f} limit={limit} raw={len(rows)} "
            f"usable={len(items)} clusters={len(clusters)} kept={len(filtered)}"
        )
        _log("dry-run: skip inserts")
        return 0

    # Insert with chunking to avoid oversized HTTP payloads.
    _log(f"inserting clusters={len(cluster_rows)} cluster_states={len(state_rows)} maps={len(map_rows)}")
    for i in range(0, len(cluster_rows), 200):
        ch_insert_json_each_row("trading.news_clusters", cluster_rows[i : i + 200], timeout_sec=120)
    for i in range(0, len(state_rows), 200):
        ch_insert_json_each_row("trading.news_cluster_state", state_rows[i : i + 200], timeout_sec=120)
    for i in range(0, len(map_rows), 800):
        ch_insert_json_each_row("trading.news_cluster_map", map_rows[i : i + 800], timeout_sec=120)
    _notify(
        f"✅ 완료: window={window} threshold={threshold:.3f} "
        f"raw={len(items)} clusters={len(filtered)} rows(n_news={len(cluster_rows)}/{len(state_rows)}) maps={len(map_rows)}"
    )
    _log("done")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        _log("interrupted")
        raise SystemExit(130)
