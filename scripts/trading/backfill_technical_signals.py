#!/usr/bin/env python3
"""Backfill trading.technical_signals for a historical date range.

This script reuses the live technical-signal rule shape and writes daily rows
for each ticker between --start-date and --end-date.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

import pandas as pd

try:
    import yfinance as yf
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"yfinance import failed: {exc}")


def _log(msg: str) -> None:
    print(f"{dt.datetime.now().strftime('%H:%M:%S')} [tech-backfill] {msg}", flush=True)


def _ch_url_and_headers() -> tuple[str, dict[str, str]]:
    host = os.getenv("CLICKHOUSE_HOST", "").strip()
    user = os.getenv("CLICKHOUSE_USER", "").strip()
    pw = os.getenv("CLICKHOUSE_PASS", "").strip()
    headers: dict[str, str] = {"Content-Type": "text/plain; charset=utf-8"}
    if host:
        if not user:
            user = "default"
        if not pw:
            pw = os.getenv("CLICKHOUSE_PASSWORD", "trading")
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


def ch_execute(sql: str, timeout_sec: int = 120) -> None:
    url, headers = _ch_url_and_headers()
    req = Request(url, data=sql.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        _ = r.read()


def ch_insert_json_each_row(table: str, rows: list[dict[str, Any]], timeout_sec: int = 120) -> None:
    if not rows:
        return
    url, headers = _ch_url_and_headers()
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    q = f"INSERT INTO {table} FORMAT JSONEachRow\n".encode("utf-8") + payload.encode("utf-8")
    req = Request(url, data=q, headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        _ = r.read()


def load_stocks_csv() -> dict[str, tuple[str, str]]:
    path = os.path.expanduser("~/.openclaw/workspace/STOCKS.csv")
    out: dict[str, tuple[str, str]] = {}
    if not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()
    if not lines:
        return out
    header = [h.strip() for h in lines[0].split(",")]
    idx = {k: i for i, k in enumerate(header)}
    if "Code" not in idx or "Name" not in idx:
        return out
    for line in lines[1:]:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) <= max(idx.values()):
            continue
        code = parts[idx["Code"]]
        name = parts[idx["Name"]]
        mkt = parts[idx.get("Market", -1)] if "Market" in idx else "KOSPI"
        if re.fullmatch(r"\d{6}", code):
            suffix = "KQ" if "KOSDAQ" in mkt.upper() else "KS"
            out[code] = (name, suffix)
    return out


CORE_WATCHLIST: dict[str, tuple[str, str]] = {
    "005930": ("삼성전자", "KS"),
    "000660": ("SK하이닉스", "KS"),
    "373220": ("LG에너지솔루션", "KS"),
    "207940": ("삼성바이오로직스", "KS"),
    "005380": ("현대차", "KS"),
    "068270": ("셀트리온", "KS"),
    "051910": ("LG화학", "KS"),
    "006400": ("삼성SDI", "KS"),
    "105560": ("KB금융", "KS"),
    "055550": ("신한지주", "KS"),
    "000270": ("기아", "KS"),
    "028260": ("삼성물산", "KS"),
    "012330": ("현대모비스", "KS"),
    "066570": ("LG전자", "KS"),
    "003550": ("LG", "KS"),
    "086790": ("하나금융지주", "KS"),
    "034730": ("SK", "KS"),
    "003670": ("포스코퓨처엠", "KS"),
    "042700": ("한미반도체", "KS"),
    "009150": ("삼성전기", "KS"),
    "034020": ("두산에너빌리티", "KS"),
    "402340": ("SK스퀘어", "KS"),
    "010130": ("고려아연", "KS"),
    "035420": ("NAVER", "KS"),
    "035720": ("카카오", "KS"),
    "329180": ("HD현대중공업", "KS"),
    "009540": ("HD한국조선해양", "KS"),
    "012450": ("한화에어로스페이스", "KS"),
    "042660": ("한화오션", "KS"),
    "003490": ("대한항공", "KS"),
    "247540": ("에코프로비엠", "KQ"),
    "086520": ("에코프로", "KQ"),
    "196170": ("알테오젠", "KQ"),
    "328130": ("루닛", "KQ"),
    "403870": ("HPSP", "KQ"),
}


@dataclass
class TickerJob:
    code: str
    name: str
    suffix: str


def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50.0)


def score_signal(row: pd.Series) -> tuple[int, str]:
    score = 0
    rsi = float(row.get("rsi14", 50.0))
    macd_hist = float(row.get("macd_hist", 0.0))
    bb_pct = float(row.get("bb_pct", 0.5))
    vol_ratio = float(row.get("vol_ratio", 1.0))
    ma5 = float(row.get("ma5", 0.0))
    ma20 = float(row.get("ma20", 0.0))
    close = float(row.get("close_price", 0.0))

    if rsi < 25:
        score += 2
    elif rsi < 35:
        score += 1
    elif rsi > 75:
        score -= 2
    elif rsi > 65:
        score -= 1

    if macd_hist > 0:
        score += 1
    elif macd_hist < 0:
        score -= 1

    if bb_pct < 0.1:
        score += 2
    elif bb_pct < 0.25:
        score += 1
    elif bb_pct > 0.9:
        score -= 2
    elif bb_pct > 0.75:
        score -= 1

    if ma5 > 0 and ma20 > 0 and close > 0:
        if close > ma20 and ma5 > ma20:
            score += 1
        elif close < ma20 and ma5 < ma20:
            score -= 1

    if vol_ratio > 3.0:
        if score > 0:
            score += 1
        elif score < 0:
            score -= 1

    score = max(-5, min(5, score))
    if score >= 3:
        return score, "strong_buy"
    if score >= 1:
        return score, "buy"
    if score >= -1:
        return score, "neutral"
    if score >= -3:
        return score, "sell"
    return score, "strong_sell"


def build_universe(top_n: int, explicit_tickers: list[str]) -> list[TickerJob]:
    stocks = load_stocks_csv()
    out: dict[str, TickerJob] = {}

    for code, (name, suffix) in CORE_WATCHLIST.items():
        out[code] = TickerJob(code=code, name=name, suffix=suffix)

    if top_n > 0:
        count = 0
        for code, (name, suffix) in stocks.items():
            if code in out:
                continue
            if name.endswith("우") or "스팩" in name:
                continue
            out[code] = TickerJob(code=code, name=name, suffix=suffix)
            count += 1
            if count >= top_n:
                break

    for code in explicit_tickers:
        if not re.fullmatch(r"\d{6}", code):
            continue
        if code in out:
            continue
        name, suffix = stocks.get(code, (f"종목_{code}", "KS"))
        out[code] = TickerJob(code=code, name=name, suffix=suffix)

    return list(out.values())


def fetch_history(code: str, suffix: str, start: dt.date, end: dt.date) -> tuple[pd.DataFrame, str]:
    end_plus = end + dt.timedelta(days=1)
    start_fetch = start - dt.timedelta(days=120)
    sym = f"{code}.{suffix}"
    hist = yf.Ticker(sym).history(start=start_fetch.isoformat(), end=end_plus.isoformat(), auto_adjust=False)
    if not hist.empty:
        return hist, suffix
    alt = "KQ" if suffix == "KS" else "KS"
    sym2 = f"{code}.{alt}"
    hist2 = yf.Ticker(sym2).history(start=start_fetch.isoformat(), end=end_plus.isoformat(), auto_adjust=False)
    return hist2, alt


def build_rows(job: TickerJob, hist: pd.DataFrame, start: dt.date, end: dt.date) -> list[dict[str, Any]]:
    if hist.empty:
        return []
    df = hist.copy()
    if "Close" not in df.columns or "Open" not in df.columns or "Volume" not in df.columns:
        return []
    df = df.rename(
        columns={
            "Close": "close_price",
            "Open": "open_price",
            "High": "high",
            "Low": "low",
            "Volume": "volume",
        }
    )
    df = df[["open_price", "high", "low", "close_price", "volume"]].dropna()
    if df.empty:
        return []

    close = df["close_price"].astype(float)
    df["ma5"] = close.rolling(5).mean()
    df["ma20"] = close.rolling(20).mean()
    df["ma60"] = close.rolling(60).mean()
    df["rsi14"] = compute_rsi(close, 14)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    df["bb_middle"] = close.rolling(20).mean()
    bb_std = close.rolling(20).std(ddof=0)
    df["bb_upper"] = df["bb_middle"] + 2.0 * bb_std
    df["bb_lower"] = df["bb_middle"] - 2.0 * bb_std
    width = (df["bb_upper"] - df["bb_lower"]).replace(0, pd.NA)
    df["bb_pct"] = ((close - df["bb_lower"]) / width).fillna(0.5)
    vol_mean = df["volume"].astype(float).rolling(20).mean().replace(0, pd.NA)
    df["vol_ratio"] = (df["volume"].astype(float) / vol_mean).fillna(1.0)
    df["change_pct"] = ((close - close.shift(1)) / close.shift(1) * 100.0).fillna(0.0)

    out: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        d = idx.date() if hasattr(idx, "date") else dt.datetime.strptime(str(idx)[:10], "%Y-%m-%d").date()
        if d < start or d > end:
            continue
        if pd.isna(row["close_price"]) or float(row["close_price"]) <= 0:
            continue
        if pd.isna(row["ma20"]) or pd.isna(row["ma60"]):
            continue
        market = "KOSDAQ" if job.suffix == "KQ" else "KOSPI"
        score, signal = score_signal(row)
        out.append(
            {
                "date": d.isoformat(),
                "ticker": job.code,
                "ticker_name": job.name,
                "market": market,
                "close_price": round(float(row["close_price"]), 2),
                "change_pct": round(float(row["change_pct"]), 2),
                "ma5": round(float(row["ma5"]), 2),
                "ma20": round(float(row["ma20"]), 2),
                "ma60": round(float(row["ma60"]), 2),
                "rsi14": round(float(row["rsi14"]), 2),
                "macd": round(float(row["macd"]), 4),
                "macd_signal": round(float(row["macd_signal"]), 4),
                "macd_hist": round(float(row["macd_hist"]), 4),
                "bb_upper": round(float(row["bb_upper"]), 2),
                "bb_middle": round(float(row["bb_middle"]), 2),
                "bb_lower": round(float(row["bb_lower"]), 2),
                "bb_pct": round(float(row["bb_pct"]), 4),
                "volume": int(float(row["volume"])),
                "vol_ratio": round(float(row["vol_ratio"]), 2),
                "signal": signal,
                "signal_score": int(score),
                "updated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill technical_signals for historical range")
    ap.add_argument("--start-date", default="2025-01-01")
    ap.add_argument("--end-date", default="2025-12-31")
    ap.add_argument("--top-n", type=int, default=220, help="extra tickers from STOCKS.csv")
    ap.add_argument("--tickers", default="", help="comma separated 6-digit tickers")
    ap.add_argument("--overwrite", action="store_true", help="delete existing rows in date range")
    ap.add_argument("--batch-size", type=int, default=2000)
    ap.add_argument("--sleep-sec", type=float, default=0.1)
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start_date)
    end = dt.date.fromisoformat(args.end_date)
    if start > end:
        raise SystemExit("start-date must be <= end-date")

    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    universe = build_universe(max(0, int(args.top_n)), tickers)
    _log(f"range={start}..{end} universe={len(universe)}")

    if args.overwrite:
        ch_execute(
            f"""
ALTER TABLE trading.technical_signals
DELETE WHERE date >= '{start.isoformat()}'
  AND date <= '{end.isoformat()}'
"""
        )
        _log("overwrite delete issued for range")

    inserted = 0
    failed = 0
    batch: list[dict[str, Any]] = []

    for idx, job in enumerate(universe, start=1):
        try:
            hist, resolved_suffix = fetch_history(job.code, job.suffix, start, end)
            if resolved_suffix != job.suffix:
                job.suffix = resolved_suffix
            rows = build_rows(job, hist, start, end)
            if not rows:
                failed += 1
                _log(f"[{idx}/{len(universe)}] {job.code} {job.name} no rows")
                continue
            batch.extend(rows)
            if len(batch) >= args.batch_size:
                ch_insert_json_each_row("trading.technical_signals", batch, timeout_sec=180)
                inserted += len(batch)
                batch = []
            _log(f"[{idx}/{len(universe)}] {job.code} {job.name} rows={len(rows)}")
            time.sleep(max(0.0, float(args.sleep_sec)))
        except Exception as exc:
            failed += 1
            _log(f"[{idx}/{len(universe)}] {job.code} failed: {exc}")

    if batch:
        ch_insert_json_each_row("trading.technical_signals", batch, timeout_sec=180)
        inserted += len(batch)

    _log(f"done inserted={inserted} failed_tickers={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
