#!/usr/bin/env python3
"""hidden_relation_scorer.py

구조화 이벤트 프레임 + 클러스터 상태를 이용해
"숨은 연관성 전이 점수(transfer score)"를 산출한다.

출력:
- trading.hidden_relation_signals (스냅샷 누적 저장)
- trading.v_hidden_relation_signals (latest 뷰에서 사용)
"""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import json
import math
import os
import re
import sys
from pathlib import Path
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()


def _log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"{ts} [hidden_relation] {msg}", flush=True)


def _notify(text: str) -> None:
    try:
        from telegram_notify import notify
    except Exception:
        return
    try:
        notify(f"🔗 <b>연관성 점수</b>\n{text}")
    except Exception as e:
        _log(f"telegram notify failed: {e}")


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

    url = os.getenv("CLICKHOUSE_URL", "http://default:trading@localhost:8123").strip()
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


def ensure_schema() -> None:
    """런타임 스키마 보강(하위호환)."""
    statements = [
        """
ALTER TABLE trading.hidden_relation_signals
ADD COLUMN IF NOT EXISTS relation_quality Float32 DEFAULT 0.5
""",
    ]
    for sql in statements:
        try:
            url, headers = _ch_url_and_headers()
            req = Request(url, data=sql.encode("utf-8"), headers=headers, method="POST")
            with urlopen(req, timeout=30) as r:
                _ = r.read()
        except Exception:
            # 테이블 미생성/권한 이슈 등은 다음 주기에 재시도
            continue


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _sign(x: float, eps: float = 1e-12) -> int:
    if x > eps:
        return 1
    if x < -eps:
        return -1
    return 0


def _is_ticker(v: str) -> bool:
    s = str(v or "").strip()
    return bool(re.fullmatch(r"\d{6}", s)) and s != "000000"


def _norm_name(v: str) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    s = re.sub(r"\(.*?\)", "", s).strip()
    s = s.replace(" ", "")
    return s.lower()


def _parse_dt(v: str) -> dt.datetime | None:
    s = (v or "").strip().replace("T", " ")
    if not s:
        return None
    if "." in s:
        s = s.split(".", 1)[0]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def _as_str_list(v: Any, max_items: int = 12, max_len: int = 24) -> list[str]:
    if not isinstance(v, list):
        return []
    out: list[str] = []
    for it in v[:max_items]:
        s = str(it or "").strip().replace("\n", " ")
        if not s:
            continue
        if len(s) > max_len:
            s = s[:max_len]
        out.append(s)
    return out


def _parse_entities_json(raw: Any) -> list[dict[str, Any]]:
    arr: list[Any] = []
    if isinstance(raw, list):
        arr = raw
    elif isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return []
        try:
            obj = json.loads(txt)
            if isinstance(obj, list):
                arr = obj
        except Exception:
            return []
    out: list[dict[str, Any]] = []
    for it in arr[:12]:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name", "") or "").strip()
        ticker = str(it.get("ticker", "") or "").strip()
        if not _is_ticker(ticker):
            ticker = ""
        role = str(it.get("role", "related") or "related").strip().lower()
        conf = _clamp(_to_float(it.get("confidence", 0.6), 0.6), 0.0, 1.0)
        if not name and not ticker:
            continue
        out.append({"name": name, "ticker": ticker, "role": role, "confidence": conf})
    return out


CHANNEL_WEIGHT = {
    "revenue": 1.00,
    "margin": 0.95,
    "demand": 0.95,
    "supply": 0.90,
    "cost": 0.88,
    "policy": 0.82,
    "risk": 0.80,
    "fx": 0.78,
    "commodity": 0.78,
    "rate": 0.78,
    "liquidity": 0.74,
    "valuation": 0.72,
    "sentiment": 0.70,
    "other": 0.68,
}

ROLE_WEIGHT = {
    "issuer": 1.00,
    "supplier": 0.90,
    "customer": 0.90,
    "competitor": 0.85,
    "regulator": 0.62,
    "macro": 0.55,
    "asset": 0.55,
    "related": 0.72,
}

ROLE_POLARITY = {
    "competitor": -1.0,
}

CO_MENTION_TRANSFER_WEIGHT = _clamp(
    _to_float(os.getenv("RELATION_CO_MENTION_WEIGHT", "0.12"), 0.12),
    0.0,
    0.5,
)


@dataclass
class _Agg:
    direct_score: float = 0.0
    transfer_score: float = 0.0
    cluster_score: float = 0.0
    calibration_sum: float = 0.0
    calibration_weight: float = 0.0
    support_event_ids: set[str] = field(default_factory=set)
    support_clusters: int = 0
    source_counter: Counter[str] = field(default_factory=Counter)
    role_counter: Counter[str] = field(default_factory=Counter)
    channel_counter: Counter[str] = field(default_factory=Counter)


def _channel_weight(channels: list[str]) -> float:
    chans = [c for c in channels if c in CHANNEL_WEIGHT]
    if not chans:
        return 0.75
    return sum(CHANNEL_WEIGHT[c] for c in chans) / max(1, len(chans))


def _load_ticker_name_map() -> dict[str, str]:
    out: dict[str, str] = _load_ticker_master_name_map()
    rows = ch_select(
        """
        SELECT ticker, ticker_name
        FROM trading.technical_signals
        WHERE date = (SELECT max(date) FROM trading.technical_signals)
        """,
        timeout_sec=30,
    )
    for r in rows:
        t = str(r.get("ticker", "")).strip()
        if _is_ticker(t):
            nm = str(r.get("ticker_name", "") or "").strip()
            if nm:
                out[t] = nm
    return out


def _build_name_ticker_map(ticker_name_map: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    ambiguous: set[str] = set()
    for t, name in ticker_name_map.items():
        if not _is_ticker(t):
            continue
        raw = str(name or "").strip()
        variants = {
            raw,
            raw.replace(" ", ""),
            re.sub(r"\(.*?\)", "", raw).strip(),
            re.sub(r"\(.*?\)", "", raw).strip().replace(" ", ""),
        }
        for v in variants:
            k = _norm_name(v)
            if not k:
                continue
            prev = out.get(k)
            if prev and prev != t:
                ambiguous.add(k)
            else:
                out[k] = t
    for k in ambiguous:
        out.pop(k, None)
    return out


def _load_ticker_master_name_map() -> dict[str, str]:
    """전종목 마스터(코드/종목명)를 로드한다.

    우선순위:
    1) ~/.openclaw/workspace/STOCKS.csv
    2) ~/.openclaw/data/krx_stocks.json (name->code 구조)
    """
    out: dict[str, str] = {}
    csv_path = Path.home() / ".openclaw" / "workspace" / "STOCKS.csv"
    if csv_path.exists():
        try:
            with csv_path.open("r", encoding="utf-8") as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row or len(row) < 2:
                        continue
                    code = str(row[0] or "").strip().zfill(6)
                    name = str(row[1] or "").strip()
                    if _is_ticker(code) and name:
                        out[code] = name
        except Exception:
            pass

    json_path = Path.home() / ".openclaw" / "data" / "krx_stocks.json"
    if json_path.exists():
        try:
            obj = json.loads(json_path.read_text(encoding="utf-8"))
            stocks = obj.get("stocks", {})
            if isinstance(stocks, dict):
                for name, code in stocks.items():
                    c = str(code or "").strip().zfill(6)
                    n = str(name or "").strip()
                    if _is_ticker(c) and n and c not in out:
                        out[c] = n
        except Exception:
            pass
    return out


def _load_quality_map() -> dict[tuple[str, str], tuple[float, float]]:
    rows = ch_select(
        """
        SELECT event_type, time_horizon, n, calibration_score
        FROM trading.v_event_memory_quality
        """,
        timeout_sec=30,
    )
    out: dict[tuple[str, str], tuple[float, float]] = {}
    for r in rows:
        et = str(r.get("event_type", "") or "other").strip()
        hz = str(r.get("time_horizon", "") or "1-3d").strip()
        n = _to_int(r.get("n", 0), 0)
        calib = _clamp(_to_float(r.get("calibration_score", 0.5), 0.5), 0.0, 1.0)
        if n >= 5:
            mem_w = 0.85 + 0.50 * calib
        elif n >= 2:
            mem_w = 0.90 + 0.30 * calib
        else:
            mem_w = 1.00
        out[(et, hz)] = (_clamp(mem_w, 0.70, 1.35), calib)
    return out


def _load_event_frames(lookback_hours: int, limit: int) -> list[dict[str, Any]]:
    lookback_hours = max(1, int(lookback_hours))
    limit = max(200, int(limit))
    return ch_select(
        f"""
        SELECT
            toString(published_at) AS published_at,
            importance,
            sentiment,
            analysis_confidence,
            event_type,
            time_horizon,
            event_signature,
            tickers,
            channels,
            entities_json
        FROM trading.news_event_frames
        WHERE collected_at >= now() - INTERVAL {lookback_hours} HOUR
          AND relevant = 1
          AND importance >= 2
        ORDER BY published_at DESC
        LIMIT {limit}
        """,
        timeout_sec=60,
    )


def _load_cluster_latest() -> list[dict[str, Any]]:
    return ch_select(
        """
        SELECT
            state_label,
            importance_max,
            sentiment_bias,
            delta_sentiment,
            changed,
            top_tickers
        FROM trading.news_cluster_state
        WHERE asof_ts = (SELECT max(asof_ts) FROM trading.news_cluster_state)
        """,
        timeout_sec=30,
    )


def _score_cluster_row(r: dict[str, Any]) -> float:
    state = str(r.get("state_label", "") or "stable").strip().lower()
    importance = _clamp(_to_float(r.get("importance_max", 1), 1.0) / 5.0, 0.2, 1.0)
    changed = 1 if _to_int(r.get("changed", 1), 1) > 0 else 0
    bias = _to_float(r.get("sentiment_bias", 0.0), 0.0)
    delta = _to_float(r.get("delta_sentiment", 0.0), 0.0)
    direction = _sign(bias) if abs(bias) > 1e-9 else _sign(delta)
    if direction == 0:
        return 0.0
    if state == "reinforcing":
        state_w = 0.35
    elif state == "emerging":
        state_w = 0.22
    elif state == "reversing":
        state_w = -0.24
    else:
        state_w = 0.08
    changed_mult = 1.0 if changed else 0.6
    return state_w * importance * float(direction) * changed_mult


def _relation_quality(a: _Agg, avg_calib: float) -> float:
    event_n = max(0, len(a.support_event_ids))
    cluster_n = max(0, int(a.support_clusters))
    src_n = max(0, len(a.source_counter))
    role_n = max(0, len(a.role_counter))

    event_q = _clamp(math.log1p(event_n) / math.log1p(16.0), 0.0, 1.0)
    cluster_q = _clamp(cluster_n / 8.0, 0.0, 1.0)
    source_q = _clamp(src_n / 8.0, 0.0, 1.0)
    role_q = _clamp(role_n / 4.0, 0.0, 1.0)
    calib_q = _clamp(avg_calib, 0.0, 1.0)

    quality = (
        event_q * 0.36
        + cluster_q * 0.18
        + source_q * 0.20
        + role_q * 0.10
        + calib_q * 0.16
    )
    return _clamp(quality, 0.05, 1.0)


def build_scores(lookback_hours: int, limit: int, max_tickers: int) -> list[dict[str, Any]]:
    now = dt.datetime.now()
    ticker_name_map = _load_ticker_name_map()
    name_ticker_map = _build_name_ticker_map(ticker_name_map)
    quality_map = _load_quality_map()
    frames = _load_event_frames(lookback_hours=lookback_hours, limit=limit)
    clusters = _load_cluster_latest()

    _log(f"frames={len(frames)} clusters={len(clusters)} quality_keys={len(quality_map)}")

    agg: dict[str, _Agg] = defaultdict(_Agg)

    for r in frames:
        published_at_s = str(r.get("published_at", "") or "")
        pub_dt = _parse_dt(published_at_s)
        if pub_dt is None:
            continue
        age_h = max(0.0, (now - pub_dt).total_seconds() / 3600.0)
        decay = max(0.20, math.exp(-age_h / 96.0))

        event_type = str(r.get("event_type", "") or "other").strip()
        horizon = str(r.get("time_horizon", "") or "1-3d").strip()
        sentiment = str(r.get("sentiment", "") or "neutral").strip().lower()
        if sentiment == "positive":
            sent_sign = 1.0
        elif sentiment == "negative":
            sent_sign = -1.0
        elif event_type in {"policy", "regulation", "supply_chain", "macro_data", "index_flow", "guidance"}:
            sent_sign = 0.18
        else:
            sent_sign = 0.0
        if sent_sign == 0.0:
            continue

        importance = _clamp(_to_float(r.get("importance", 2), 2.0) / 5.0, 0.2, 1.0)
        conf = _clamp(_to_float(r.get("analysis_confidence", 0.6), 0.6), 0.05, 0.99)
        conf_w = 0.50 + 0.50 * conf

        channels = _as_str_list(r.get("channels", []), max_items=10, max_len=24)
        channel_w = _channel_weight(channels)

        mem_w, calib = quality_map.get((event_type, horizon), (1.0, 0.5))
        base = sent_sign * importance * conf_w * decay * channel_w * mem_w

        tickers_raw = _as_str_list(r.get("tickers", []), max_items=12, max_len=6)
        direct_tickers = [t for t in tickers_raw if _is_ticker(t)]
        # order-preserving dedupe
        direct_tickers = list(dict.fromkeys(direct_tickers))

        event_id = str(r.get("event_signature", "") or "").strip()
        if not event_id:
            event_id = f"{published_at_s}|{event_type}|{horizon}"

        for t in direct_tickers:
            a = agg[t]
            a.direct_score += base
            a.support_event_ids.add(event_id)
            a.calibration_sum += calib
            a.calibration_weight += 1.0
            for ch in channels[:6]:
                a.channel_counter[ch] += 1

        # direct ticker가 여러 개인 이벤트는 약한 전이(co-mention) 신호를 만든다.
        if len(direct_tickers) >= 2 and CO_MENTION_TRANSFER_WEIGHT > 0:
            denom = max(1, len(direct_tickers) - 1)
            for src in direct_tickers:
                for dst in direct_tickers:
                    if src == dst:
                        continue
                    contrib = base * CO_MENTION_TRANSFER_WEIGHT / float(denom)
                    if abs(contrib) < 1e-12:
                        continue
                    a = agg[dst]
                    a.transfer_score += contrib
                    a.support_event_ids.add(event_id)
                    a.calibration_sum += calib * 0.35
                    a.calibration_weight += 0.35
                    a.source_counter[src] += 1
                    a.role_counter["co_mention"] += 1
                    for ch in channels[:6]:
                        a.channel_counter[ch] += 1

        entities = _parse_entities_json(r.get("entities_json", "[]"))
        if not entities:
            continue

        for ent in entities:
            t = str(ent.get("ticker", "") or "").strip()
            if not _is_ticker(t):
                nm = _norm_name(str(ent.get("name", "") or ""))
                t = name_ticker_map.get(nm, "")
            if not _is_ticker(t):
                continue
            if t in direct_tickers:
                continue
            role = str(ent.get("role", "related") or "related").strip().lower()
            role_w = ROLE_WEIGHT.get(role, ROLE_WEIGHT["related"])
            role_sign = ROLE_POLARITY.get(role, 1.0)
            ent_conf = _clamp(_to_float(ent.get("confidence", 0.6), 0.6), 0.0, 1.0)
            ent_w = 0.60 + 0.40 * ent_conf
            contrib = base * role_w * role_sign * ent_w
            if abs(contrib) < 1e-9:
                continue

            a = agg[t]
            a.transfer_score += contrib
            a.support_event_ids.add(event_id)
            # entity-link는 calibration 반영을 절반만 준다.
            a.calibration_sum += calib * 0.5
            a.calibration_weight += 0.5
            if direct_tickers:
                for src in direct_tickers[:4]:
                    a.source_counter[src] += 1
            else:
                a.source_counter["macro_event"] += 1
            a.role_counter[role] += 1
            for ch in channels[:6]:
                a.channel_counter[ch] += 1

    for r in clusters:
        tickers = [t for t in _as_str_list(r.get("top_tickers", []), max_items=12, max_len=6) if _is_ticker(t)]
        if not tickers:
            continue
        cscore = _score_cluster_row(r)
        if abs(cscore) < 1e-12:
            continue
        for t in tickers:
            a = agg[t]
            a.cluster_score += cscore
            a.support_clusters += 1

    rows: list[dict[str, Any]] = []
    for t, a in agg.items():
        if a.calibration_weight > 0:
            avg_calib = _clamp(a.calibration_sum / a.calibration_weight, 0.0, 1.0)
        else:
            avg_calib = 0.5
        calib_adj = (avg_calib - 0.5) * 0.30

        raw_total = (
            a.direct_score * 0.65
            + a.transfer_score * 1.00
            + a.cluster_score * 0.75
            + calib_adj
        )
        rel_quality = _relation_quality(a, avg_calib)
        quality_mult = 0.35 + 0.65 * rel_quality
        total = raw_total * quality_mult
        if total >= 0.12:
            bias = "positive"
        elif total <= -0.12:
            bias = "negative"
        else:
            bias = "neutral"

        srcs = [k for (k, _) in a.source_counter.most_common(8)]
        roles = [k for (k, _) in a.role_counter.most_common(6)]
        chans = [k for (k, _) in a.channel_counter.most_common(6)]

        rows.append(
            {
                "ticker": t,
                "ticker_name": ticker_name_map.get(t, ""),
                "direct_event_score": round(a.direct_score, 6),
                "transfer_event_score": round(a.transfer_score, 6),
                "cluster_state_score": round(a.cluster_score, 6),
                "memory_calibration_score": round(avg_calib, 4),
                "relation_quality": round(rel_quality, 4),
                "total_relation_score": round(total, 6),
                "relation_bias": bias,
                "support_events": int(len(a.support_event_ids)),
                "support_clusters": int(a.support_clusters),
                "source_tickers": srcs,
                "top_roles": roles,
                "top_channels": chans,
            }
        )

    rows.sort(key=lambda x: abs(_to_float(x.get("total_relation_score", 0.0), 0.0)), reverse=True)
    if max_tickers > 0:
        rows = rows[: max(1, int(max_tickers))]
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="숨은 연관성 전이 점수 스냅샷 생성")
    ap.add_argument("--lookback-hours", type=int, default=int(os.getenv("RELATION_LOOKBACK_HOURS", "168")))
    ap.add_argument("--limit", type=int, default=int(os.getenv("RELATION_FRAME_LIMIT", "6000")))
    ap.add_argument("--max-tickers", type=int, default=int(os.getenv("RELATION_MAX_TICKERS", "500")))
    ap.add_argument("--min-abs-score", type=float, default=float(os.getenv("RELATION_MIN_ABS_SCORE", "0.0")))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    lookback_hours = max(6, int(args.lookback_hours))
    limit = max(200, int(args.limit))
    max_tickers = max(1, int(args.max_tickers))
    min_abs_score = max(0.0, float(args.min_abs_score))

    _log(
        f"lookback_hours={lookback_hours} limit={limit} "
        f"max_tickers={max_tickers} min_abs_score={min_abs_score:.4f} dry_run={args.dry_run}"
    )

    ensure_schema()
    rows = build_scores(lookback_hours=lookback_hours, limit=limit, max_tickers=max_tickers)
    if min_abs_score > 0:
        rows = [r for r in rows if abs(_to_float(r.get("total_relation_score", 0.0), 0.0)) >= min_abs_score]
    _log(f"scored_tickers={len(rows)}")
    if not rows:
        _notify("⏭️ 계산 대상 없음")
        return 0

    snapshot_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out_rows: list[dict[str, Any]] = []
    for r in rows:
        out_rows.append(
            {
                "asof_ts": snapshot_ts,
                "lookback_hours": int(lookback_hours),
                **r,
            }
        )

    for r in out_rows[:12]:
        _log(
            f"  {r['ticker']} score={_to_float(r['total_relation_score'],0.0):+.4f} "
            f"direct={_to_float(r['direct_event_score'],0.0):+.4f} "
            f"transfer={_to_float(r['transfer_event_score'],0.0):+.4f} "
            f"cluster={_to_float(r['cluster_state_score'],0.0):+.4f} "
            f"events={_to_int(r['support_events'],0)}"
        )

    if args.dry_run:
        _notify(f"🧪 dry-run: candidate={len(rows)} kept={len(out_rows)}")
        _log("dry-run: skip insert")
        return 0

    for i in range(0, len(out_rows), 200):
        ch_insert_json_each_row("trading.hidden_relation_signals", out_rows[i : i + 200], timeout_sec=120)
    _log(f"inserted_rows={len(out_rows)} asof_ts={snapshot_ts}")
    _notify(f"✅ 완료: total={len(rows)} kept={len(out_rows)} inserted={len(out_rows)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        _log("interrupted")
        raise SystemExit(130)
