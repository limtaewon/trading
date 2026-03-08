#!/usr/bin/env python3
"""Run a realistic daily backtest with current Stage-style decision logic.

Execution model:
- Decide at D close using D data
- Execute at D+1 close (conservative, no intraday lookahead)
- Apply slippage/fees/tax
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from env_bootstrap import bootstrap_openclaw_env
from llm_model_config import resolve_model

bootstrap_openclaw_env()

try:
    import yfinance as yf
except Exception:  # pragma: no cover
    yf = None

try:
    from codex_exec_guard import run_codex_cached
except Exception:  # pragma: no cover
    run_codex_cached = None


def _log(msg: str) -> None:
    print(f"{dt.datetime.now().strftime('%H:%M:%S')} [bt2025] {msg}", flush=True)


def _to_float(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return d


def _to_int(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _weighted_effective_score(
    scores: dict[str, float],
    weights: dict[str, float],
    enabled: list[str],
    penalty: float,
) -> tuple[float, float]:
    raw = 0.0
    for k, w in weights.items():
        raw += w * _to_float(scores.get(k), 0.0)
    raw = _clamp(raw - penalty, 0.0, 100.0)

    den = sum(weights.get(k, 0.0) for k in enabled)
    if den <= 0:
        return raw, raw
    eff = sum(weights.get(k, 0.0) * _to_float(scores.get(k), 0.0) for k in enabled) / den
    eff = _clamp(eff - penalty, 0.0, 100.0)
    return raw, eff


def _load_live_min_cash_ratio(default_value: float = 0.15) -> float:
    p = Path.home() / ".openclaw" / "state" / "adaptive_policy.json"
    try:
        if p.exists():
            obj = json.loads(p.read_text(encoding="utf-8"))
            v = _to_float(obj.get("min_cash_ratio"), default_value)
            return _clamp(v, 0.0, 0.90)
    except Exception:
        pass
    return _clamp(default_value, 0.0, 0.90)


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


def ch_select(sql: str, timeout_sec: int = 180) -> list[dict[str, Any]]:
    url, headers = _ch_url_and_headers()
    q = sql.strip() + "\nFORMAT JSON"
    req = Request(url, data=q.encode("utf-8"), headers=headers, method="POST")
    with urlopen(req, timeout=timeout_sec) as r:
        body = r.read().decode("utf-8", errors="replace")
    obj = json.loads(body)
    data = obj.get("data", [])
    return data if isinstance(data, list) else []


@dataclass
class Holding:
    qty: int
    avg_price: float
    entry_date: str


@dataclass
class PendingOrder:
    exec_date: str
    ticker: str
    side: str  # BUY / SELL
    notional: float = 0.0
    qty: int = 0
    reason: str = ""


def score_stage1(day: str, kospi_close_hist: dict[str, float], usd_hist: dict[str, float], rate_hist: dict[str, float]) -> tuple[float, bool, bool]:
    dates = sorted(kospi_close_hist.keys())
    if day not in kospi_close_hist:
        return 50.0, False, False
    idx = dates.index(day)
    if idx < 5:
        return 55.0, True, False
    kospi_5d = (kospi_close_hist[day] / kospi_close_hist[dates[idx - 5]] - 1.0) * 100.0

    usd_dates = sorted(d for d in usd_hist.keys() if d <= day)
    usdkrw_3d = 0.0
    if len(usd_dates) >= 4:
        u0 = usd_hist[usd_dates[-1]]
        u3 = usd_hist[usd_dates[-4]]
        if u3 > 0:
            usdkrw_3d = (u0 / u3 - 1.0) * 100.0

    rate_dates = sorted(d for d in rate_hist.keys() if d <= day)
    rate_5d_bp = 0.0
    if len(rate_dates) >= 6:
        rate_5d_bp = (rate_hist[rate_dates[-1]] - rate_hist[rate_dates[-6]]) * 100.0

    ma20 = sum(kospi_close_hist[d] for d in dates[max(0, idx - 19): idx + 1]) / min(20, idx + 1)
    ma60 = sum(kospi_close_hist[d] for d in dates[max(0, idx - 59): idx + 1]) / min(60, idx + 1)
    close = kospi_close_hist[day]
    trend_score = 8.0
    if close > ma20 and ma20 >= ma60:
        trend_score = 35.0
    elif close > ma20:
        trend_score = 20.0

    vol_score = 20.0
    fx_score = 20.0
    if usdkrw_3d > 2.0:
        fx_score = 0.0
    elif usdkrw_3d > 1.0:
        fx_score = 10.0
    elif usdkrw_3d > 0.3:
        fx_score = 15.0

    rates_score = 10.0
    if rate_5d_bp > 20:
        rates_score = 0.0
    elif rate_5d_bp > 10:
        rates_score = 5.0

    score = _clamp(trend_score + vol_score + fx_score + rates_score, 0, 100)
    hard_riskoff = (usdkrw_3d > 2.0) or (kospi_5d < -3.5)
    passed = score >= 55 and not hard_riskoff
    return round(score, 2), passed, hard_riskoff


def score_market_flow(
    day: str,
    market_flow_by_day: dict[str, dict[str, dict[str, float]]],
    fallback_market_score: float,
) -> tuple[float, bool, str, float, float]:
    dates = sorted(d for d in market_flow_by_day.keys() if d <= day)
    if not dates:
        return fallback_market_score, False, "MISSING", 0.0, 0.0
    last5 = dates[-5:]
    f_net = 0.0
    f_traded = 0.0
    i_net = 0.0
    i_traded = 0.0
    for d in last5:
        row_f = market_flow_by_day[d].get("FOREIGN", {})
        row_i = market_flow_by_day[d].get("INST", {})
        f_net += _to_float(row_f.get("net_buy"), 0.0)
        f_traded += _to_float(row_f.get("traded"), 0.0)
        i_net += _to_float(row_i.get("net_buy"), 0.0)
        i_traded += _to_float(row_i.get("traded"), 0.0)
    if f_traded <= 0 or i_traded <= 0:
        return fallback_market_score, False, "DENOM_ZERO", 0.0, 0.0
    fpct = (f_net / f_traded) * 100.0
    ipct = (i_net / i_traded) * 100.0
    shock_abs = max(abs(fpct), abs(ipct))
    shock = "PASS"
    if shock_abs > 12.0:
        shock = "EXTREME"
    elif shock_abs > 8.0:
        shock = "ALERT"
    elif shock_abs > 3.0:
        shock = "WARN"

    def s(p: float, foreign: bool) -> float:
        if foreign:
            if p >= 0.8:
                return 25.0
            if p >= 0.3:
                return 15.0
            if p > -0.3:
                return 8.0
            return 0.0
        if p >= 0.8:
            return 15.0
        if p >= 0.3:
            return 10.0
        if p > -0.3:
            return 5.0
        return 0.0

    score = s(fpct, True) + s(ipct, False)
    if shock == "WARN":
        score *= 0.9
    elif shock == "ALERT":
        score *= 0.7
    elif shock == "EXTREME":
        score *= 0.35
    return round(score, 2), True, shock, fpct, ipct


def score_stock_flow(
    day: str,
    ticker: str,
    stock_flow_by_day_ticker: dict[str, dict[str, dict[str, float]]],
    fallback_stock_score: float,
) -> tuple[float, bool, float, float]:
    per_day = stock_flow_by_day_ticker.get(ticker, {})
    dates = sorted(d for d in per_day.keys() if d <= day)
    if not dates:
        return fallback_stock_score, False, 0.0, 0.0
    last3 = dates[-3:]
    f_net = 0.0
    i_net = 0.0
    traded = 0.0
    f_pos_days = 0
    i_pos_days = 0
    for d in last3:
        fd = per_day[d].get("FOREIGN", {})
        idd = per_day[d].get("INST", {})
        fv = _to_float(fd.get("net_buy"), 0.0)
        iv = _to_float(idd.get("net_buy"), 0.0)
        tv = max(_to_float(fd.get("traded"), 0.0), _to_float(idd.get("traded"), 0.0))
        f_net += fv
        i_net += iv
        traded += tv
        if fv > 0:
            f_pos_days += 1
        if iv > 0:
            i_pos_days += 1
    if traded <= 0:
        return fallback_stock_score, False, 0.0, 0.0

    f_pct = (f_net / traded) * 100.0
    i_pct = (i_net / traded) * 100.0

    def sf(p: float, foreign: bool) -> float:
        if foreign:
            if p >= 6.0:
                return 30.0
            if p >= 2.0:
                return 20.0
            if p >= 0.0:
                return 10.0
            return 0.0
        if p >= 4.0:
            return 15.0
        if p >= 1.0:
            return 10.0
        if p >= 0.0:
            return 5.0
        return 0.0

    persistence = 0.0
    pos_days = min(3, f_pos_days + i_pos_days)
    if pos_days >= 3:
        persistence = 15.0
    elif pos_days == 2:
        persistence = 8.0
    elif pos_days == 1:
        persistence = 3.0
    return round(_clamp(sf(f_pct, True) + sf(i_pct, False) + persistence, 0, 60), 2), True, f_pct, i_pct


def score_stage3(
    day: str,
    ticker: str,
    event_daily: dict[str, dict[str, dict[str, float]]],
    market_event_daily: dict[str, dict[str, float]],
    mode: str,
) -> tuple[float, bool]:
    dates = sorted(d for d in event_daily.keys() if d <= day)
    if not dates:
        if mode == "neutral":
            return 55.0, True
        if mode == "balanced":
            return 50.0, True
        # strict라도 시장 공통 이벤트 근거가 충분하면 보조 패스로 허용
        md = sorted(d for d in market_event_daily.keys() if d <= day)
        if md:
            m_last3 = md[-3:]
            m_explain = sum(_to_int(market_event_daily.get(d, {}).get("explain_ready_cnt"), 0) for d in m_last3)
            m_imp = [_to_float(market_event_daily.get(d, {}).get("importance_avg"), 0.0) for d in m_last3]
            m_imp_avg = (sum(m_imp) / len(m_imp)) if m_imp else 0.0
            if m_explain >= 6 and m_imp_avg >= 2.0:
                return 52.0, True
        return 35.0, False
    last3 = dates[-3:]
    imp_values: list[float] = []
    event_cnt = 0
    explain_ready = 0
    for d in last3:
        e = event_daily.get(d, {}).get(ticker)
        if not e:
            continue
        imp_values.append(_to_float(e.get("importance_avg"), 0.0))
        event_cnt += _to_int(e.get("event_cnt"), 0)
        explain_ready += _to_int(e.get("explain_ready_cnt"), 0)
    if not imp_values:
        if mode == "neutral":
            return 55.0, True
        if mode == "balanced":
            return 50.0, True
        # ticker 이벤트가 없어도 시장 이벤트 밀도가 높으면 중립 패스
        md = sorted(d for d in market_event_daily.keys() if d <= day)
        if md:
            m_last3 = md[-3:]
            m_explain = sum(_to_int(market_event_daily.get(d, {}).get("explain_ready_cnt"), 0) for d in m_last3)
            m_imp = [_to_float(market_event_daily.get(d, {}).get("importance_avg"), 0.0) for d in m_last3]
            m_imp_avg = (sum(m_imp) / len(m_imp)) if m_imp else 0.0
            if m_explain >= 6 and m_imp_avg >= 2.0:
                return 52.0, True
        return 35.0, False
    imp_avg = sum(imp_values) / len(imp_values)
    cluster_score = 20.0
    event_score = _clamp((imp_avg / 5.0) * 30.0, 0, 30)
    relevance = 20.0
    if event_cnt <= 1:
        novelty = 15.0
    elif event_cnt <= 3:
        novelty = 10.0
    else:
        novelty = 5.0
    s3 = _clamp(cluster_score + event_score + relevance + novelty, 0, 100)
    if explain_ready <= 0:
        if mode == "neutral":
            s3 = max(s3, 55.0)
        elif mode == "balanced":
            s3 = max(s3, 50.0)
        else:
            md = sorted(d for d in market_event_daily.keys() if d <= day)
            if md:
                m_last3 = md[-3:]
                m_explain = sum(_to_int(market_event_daily.get(d, {}).get("explain_ready_cnt"), 0) for d in m_last3)
                m_imp = [_to_float(market_event_daily.get(d, {}).get("importance_avg"), 0.0) for d in m_last3]
                m_imp_avg = (sum(m_imp) / len(m_imp)) if m_imp else 0.0
                if m_explain >= 6 and m_imp_avg >= 2.0:
                    s3 = max(s3, 52.0)
                    return round(s3, 2), True
            s3 = min(s3, 35.0)
    if mode == "balanced" and s3 >= 50.0:
        return round(s3, 2), True
    return round(s3, 2), explain_ready > 0


def score_stage4(tech: dict[str, Any], stage1_score: float, hard_riskoff: bool, mode: str) -> tuple[float, bool, bool]:
    close = _to_float(tech.get("close"), 0.0)
    ma20 = _to_float(tech.get("ma20"), 0.0)
    ma60 = _to_float(tech.get("ma60"), 0.0)
    rsi = _to_float(tech.get("rsi14"), 50.0)
    vol_ratio = _to_float(tech.get("vol_ratio"), 1.0)
    signal_score = _to_float(tech.get("signal_score"), 0.0)
    bb_pct = _to_float(tech.get("bb_pct"), 0.5)

    trend_score = 5.0
    if close > ma20 > ma60:
        trend_score = 40.0
    elif close > ma20:
        trend_score = 28.0
    elif close > ma60:
        trend_score = 18.0

    mom_score = 4.0
    if 40 <= rsi <= 65:
        mom_score = 20.0
    elif 35 <= rsi <= 70:
        mom_score = 12.0
    if signal_score >= 3:
        mom_score += 5.0
    elif signal_score >= 1:
        mom_score += 3.0
    elif signal_score <= -2:
        mom_score -= 5.0
    mom_score = _clamp(mom_score, 0, 25)

    vol_score = 15.0 if 0.2 <= bb_pct <= 0.8 else 8.0
    vol_conf = 3.0
    if vol_ratio >= 1.5:
        vol_conf = 20.0
    elif vol_ratio >= 1.0:
        vol_conf = 15.0
    elif vol_ratio >= 0.7:
        vol_conf = 8.0

    s4 = _clamp(trend_score + mom_score + vol_score + vol_conf, 0, 100)
    regime = "RISK_OFF" if hard_riskoff else ("RISK_ON" if stage1_score >= 70 else "NEUTRAL")
    p1 = close > ma20 > ma60 > 0 and vol_ratio >= 1.0 and rsi <= 72
    p1_riskoff = close > ma20 > ma60 > 0 and vol_ratio >= 1.2 and rsi <= 68
    p2 = ma20 > ma60 > 0 and (ma20 * 0.975 <= close <= ma20 * 1.005) and (35 <= rsi <= 65)
    p3 = signal_score >= 3 and vol_ratio >= 1.3 and rsi <= 75 and close >= ma20 > 0

    if mode == "strict":
        if regime == "RISK_ON":
            passed = s4 >= 55 and (p1 or p2 or p3)
        elif regime == "NEUTRAL":
            passed = s4 >= 58 and (p1 or p2)
        else:
            passed = s4 >= 65 and p1_riskoff
    elif mode == "balanced":
        if regime == "RISK_ON":
            passed = s4 >= 52 and (p1 or p2 or p3)
        elif regime == "NEUTRAL":
            passed = s4 >= 55 and (p1 or p2)
        else:
            passed = s4 >= 62 and p1_riskoff
    else:
        if regime == "RISK_ON":
            passed = s4 >= 50 and (p1 or p2 or p3)
        elif regime == "NEUTRAL":
            passed = s4 >= 53 and (p1 or p2)
        else:
            passed = s4 >= 60 and p1_riskoff
    rsi_overheat = rsi > 75
    return round(s4, 2), passed, rsi_overheat


def score_stage5(liquidity_proxy: float, spread_bp: float, stage3_score: float, stage4_score: float, hard_riskoff: bool) -> tuple[float, bool, float]:
    if liquidity_proxy >= 5_000_000_000:
        s5 = 100.0
        exec_mult = 1.0
    elif liquidity_proxy >= 1_000_000_000:
        s5 = 80.0
        exec_mult = 0.7
    elif liquidity_proxy >= 500_000_000:
        s5 = 60.0
        exec_mult = 0.4
    elif liquidity_proxy >= 300_000_000:
        s5 = 40.0
        exec_mult = 0.2
    else:
        s5 = 20.0
        exec_mult = 0.0
    hard_block = liquidity_proxy < 300_000_000

    if spread_bp > 80:
        s5 = 0.0
        exec_mult = 0.0
        hard_block = True
    elif spread_bp > 50:
        s5 = max(0.0, s5 - 15.0)
        exec_mult = min(exec_mult, 0.6)

    # Step5는 기본적으로 사이징 엔진으로 사용.
    # 하드 차단은 초저유동(<3억) 또는 과대 스프레드(>80bp)만 적용한다.
    return round(s5, 2), (not hard_block), exec_mult


def calc_metrics(curve: list[dict[str, Any]], start_equity: float) -> dict[str, float]:
    if not curve:
        return {}
    equities = [_to_float(x.get("equity"), start_equity) for x in curve]
    rets = []
    for i in range(1, len(equities)):
        if equities[i - 1] > 0:
            rets.append(equities[i] / equities[i - 1] - 1.0)
    total_return = (equities[-1] / start_equity - 1.0) * 100.0 if start_equity > 0 else 0.0
    years = max(1e-9, len(curve) / 252.0)
    cagr = ((equities[-1] / start_equity) ** (1.0 / years) - 1.0) * 100.0 if start_equity > 0 else 0.0
    peak = equities[0]
    mdd = 0.0
    for v in equities:
        peak = max(peak, v)
        dd = (v / peak - 1.0) * 100.0
        mdd = min(mdd, dd)
    sharpe = 0.0
    if rets:
        mu = sum(rets) / len(rets)
        var = sum((r - mu) ** 2 for r in rets) / max(1, len(rets) - 1)
        sd = math.sqrt(max(var, 0.0))
        if sd > 0:
            sharpe = (mu / sd) * math.sqrt(252.0)
    return {
        "total_return_pct": round(total_return, 2),
        "cagr_pct": round(cagr, 2),
        "max_drawdown_pct": round(mdd, 2),
        "sharpe": round(sharpe, 3),
        "days": len(curve),
    }


def build_universe_for_day(
    day: str,
    day_tech: dict[str, dict[str, Any]],
    watchlist_rows: list[dict[str, Any]],
    universe_size: int,
) -> list[str]:
    if watchlist_rows:
        day_d = dt.date.fromisoformat(day)
        latest_by_ticker: dict[str, str] = {}
        for r in watchlist_rows:
            ticker = str(r.get("ticker", "") or "")
            d = str(r.get("d", "") or "")
            ts = str(r.get("ts", "") or "")
            if not ticker or not d:
                continue
            try:
                dd = dt.date.fromisoformat(d)
            except Exception:
                continue
            if dd > day_d or (day_d - dd).days > 3:
                continue
            prev = latest_by_ticker.get(ticker, "")
            if ts > prev:
                latest_by_ticker[ticker] = ts
        if latest_by_ticker:
            ranked = sorted(latest_by_ticker.items(), key=lambda kv: kv[1], reverse=True)
            out = [tk for tk, _ in ranked[: max(1, universe_size)] if tk in day_tech]
            if out:
                return out

    ranked = sorted(
        day_tech.items(),
        key=lambda kv: (_to_float(kv[1].get("signal_score"), 0.0), _to_float(kv[1].get("vol_ratio"), 1.0)),
        reverse=True,
    )
    return [t for t, _ in ranked[: max(1, universe_size)]]


def _ranked_tech_tickers(day_tech: dict[str, dict[str, Any]]) -> list[str]:
    ranked = sorted(
        day_tech.items(),
        key=lambda kv: (_to_float(kv[1].get("signal_score"), 0.0), _to_float(kv[1].get("vol_ratio"), 1.0)),
        reverse=True,
    )
    return [t for t, _ in ranked]


def expand_universe_after_prefilter(
    *,
    day_tech: dict[str, dict[str, Any]],
    base_universe: list[str],
    holdings: dict[str, Holding],
    target_non_holding: int,
    prefilter_liquidity_krw: float,
    max_universe: int = 600,
) -> list[str]:
    if target_non_holding <= 0 or prefilter_liquidity_krw <= 0:
        return base_universe
    out: list[str] = []
    seen: set[str] = set()
    for tk in base_universe:
        if tk in seen:
            continue
        out.append(tk)
        seen.add(tk)

    def _is_liq_ok(ticker: str) -> bool:
        tech = day_tech.get(ticker)
        if not tech:
            return False
        liq = max(0.0, _to_float(tech.get("close"), 0.0) * _to_float(tech.get("volume"), 0.0))
        return liq >= prefilter_liquidity_krw

    non_holding_liq_ok = sum(1 for tk in out if tk not in holdings and _is_liq_ok(tk))
    if non_holding_liq_ok >= target_non_holding:
        return out

    ranked_all = _ranked_tech_tickers(day_tech)
    for tk in ranked_all:
        if tk in seen:
            continue
        out.append(tk)
        seen.add(tk)
        if len(out) >= max_universe:
            break
        if tk not in holdings and _is_liq_ok(tk):
            non_holding_liq_ok += 1
            if non_holding_liq_ok >= target_non_holding:
                break
    return out


def llm_decide_actions_for_day(
    *,
    day: str,
    mode: str,
    s1_score: float,
    hard_riskoff: bool,
    shock: str,
    candidates: list[dict[str, Any]],
    holdings: dict[str, Holding],
    llm_model: str,
    llm_timeout_sec: int,
    llm_cache_ttl_sec: int,
    llm_candidate_limit: int,
) -> tuple[dict[str, dict[str, Any]], str]:
    if run_codex_cached is None:
        return {}, "llm_unavailable"
    schema_path = Path(__file__).resolve().parent / "backtest_llm_decision_schema.json"
    if not schema_path.exists():
        return {}, "schema_missing"
    top = sorted(candidates, key=lambda x: _to_float(x.get("total"), 0.0), reverse=True)[: max(5, llm_candidate_limit)]
    slim_candidates: list[dict[str, Any]] = []
    for c in top:
        slim_candidates.append(
            {
                "ticker": str(c.get("ticker", "")),
                "total_score": round(_to_float(c.get("total"), 0.0), 2),
                "s1": round(s1_score, 2),
                "s2": round(_to_float(c.get("s2"), 0.0), 2),
                "s3": round(_to_float(c.get("s3"), 0.0), 2),
                "s4": round(_to_float(c.get("s4"), 0.0), 2),
                "s5": round(_to_float(c.get("s5"), 0.0), 2),
                "s2_reference_only": True,
                "stage5_pass": bool(c.get("s5_pass", False)),
                "rsi_overheat": bool(c.get("rsi_overheat", False)),
                "rule_action": str(c.get("rule_action", "HOLD")),
            }
        )
    slim_holdings = sorted(list(holdings.keys()))
    prompt = (
        "너는 한국주식 백테스트 의사결정 보조 LLM이다.\n"
        "목표: 각 티커에 대해 BUY/HOLD/REDUCE 중 하나를 선택한다.\n"
        "중요 규칙:\n"
        "1) S2는 참고지표만 사용하고 차단 기준으로 사용하지 말 것.\n"
        "2) S4는 비활성 가정(진입 차단 근거로 사용 금지).\n"
        "3) hard_riskoff=true 또는 shock=EXTREME면 BUY 금지.\n"
        "4) stage5_pass=false 또는 rsi_overheat=true 티커는 BUY 금지.\n"
        "5) 출력은 JSON 스키마를 정확히 준수할 것.\n\n"
        f"[DAY] {day}\n"
        f"[MODE] {mode}\n"
        f"[MARKET] {json.dumps({'s1_score': round(s1_score,2), 'hard_riskoff': hard_riskoff, 'shock': shock}, ensure_ascii=False)}\n"
        f"[HOLDINGS] {json.dumps(slim_holdings, ensure_ascii=False)}\n"
        f"[CANDIDATES] {json.dumps(slim_candidates, ensure_ascii=False)}\n"
    )
    try:
        raw = run_codex_cached(
            prompt=prompt,
            codex_bin=os.getenv("CODEX_BIN", "openclaw"),
            model=llm_model,
            workdir=str(Path(__file__).resolve().parent),
            timeout_sec=max(30, int(llm_timeout_sec)),
            base_args=[],
            output_schema_path=str(schema_path),
            cache_dir=os.getenv("CODEX_EXEC_CACHE_DIR", os.path.expanduser("~/.openclaw/cache/codex-exec")),
            cache_ttl_sec=max(0, int(llm_cache_ttl_sec)),
        )
    except Exception as exc:
        return {}, f"llm_call_failed:{type(exc).__name__}"
    obj = _extract_json_obj(raw)
    if not isinstance(obj, dict):
        return {}, "llm_parse_failed"
    decisions = obj.get("decisions", [])
    if not isinstance(decisions, list):
        return {}, "llm_invalid_payload"
    out: dict[str, dict[str, Any]] = {}
    for it in decisions:
        if not isinstance(it, dict):
            continue
        ticker = str(it.get("ticker", "") or "").strip()
        action = str(it.get("action", "HOLD") or "HOLD").upper().strip()
        conf = _clamp(_to_float(it.get("confidence"), 0.0), 0.0, 1.0)
        reason = str(it.get("reason", "") or "").strip()
        if not ticker:
            continue
        if action not in {"BUY", "HOLD", "REDUCE"}:
            action = "HOLD"
        out[ticker] = {"action": action, "confidence": conf, "reason": reason}
    return out, "ok"


def main() -> int:
    ap = argparse.ArgumentParser(description="2025 realistic backtest with stage-style logic")
    ap.add_argument("--start-date", default="2025-01-01")
    ap.add_argument("--end-date", default="2025-12-31")
    ap.add_argument("--initial-cash", type=float, default=100_000_000)
    ap.add_argument("--universe-size", type=int, default=120)
    ap.add_argument("--max-positions", type=int, default=10)
    ap.add_argument("--fee-bps", type=float, default=3.0)
    ap.add_argument("--sell-tax-bps", type=float, default=18.0)
    ap.add_argument("--slippage-bps", type=float, default=10.0)
    ap.add_argument("--fallback-market-score", type=float, default=15.0)
    ap.add_argument("--fallback-stock-score", type=float, default=28.0)
    ap.add_argument("--buy-threshold", type=float, default=0.0, help="0이면 mode 기본값 사용")
    ap.add_argument("--neutral-stage2-min", type=float, default=40.0)
    ap.add_argument(
        "--mode",
        default="strict",
        choices=["strict", "balanced", "neutral"],
        help="실운영 decision pipeline과 동일한 모드",
    )
    ap.add_argument("--universe", default="watchlist", choices=["watchlist", "technical"])
    ap.add_argument("--disable-stage4", action="store_true", help="S4 타이밍 레이어를 완전히 비활성화")
    ap.add_argument("--s2-reference-only", action="store_true", help="S2를 차단 게이트가 아닌 참고지표로만 사용")
    ap.add_argument("--llm-decision", action="store_true", help="일자별 LLM 판단으로 BUY/HOLD/REDUCE 결정")
    ap.add_argument("--llm-model", default=resolve_model("CODEX_MODEL"))
    ap.add_argument("--llm-timeout-sec", type=int, default=90)
    ap.add_argument("--llm-cache-ttl-sec", type=int, default=0)
    ap.add_argument("--llm-candidate-limit", type=int, default=20)
    ap.add_argument("--llm-min-confidence", type=float, default=0.60)
    ap.add_argument("--prefilter-liquidity-krw", type=float, default=1_000_000_000.0, help="watchlist/후보 사전 필터 최소 유동성(원)")
    ap.add_argument("--max-new-buys-per-day", type=int, default=0, help="0이면 제한 없음, 양수면 일일 신규매수 최대 건수")
    ap.add_argument("--min-cash-ratio", type=float, default=-1.0, help="-1이면 실운영(adaptive_policy) 값 사용")
    ap.add_argument("--daily-deploy-cap-ratio", type=float, default=0.12, help="하루 신규 투입 상한(자산 대비 비율), 0 이하면 비활성")
    ap.add_argument("--force-liquidate-on-end", action="store_true", help="백테스트 마지막 거래일 종가에 전량 강제청산")
    ap.add_argument("--min-trade-days", type=int, default=40)
    ap.add_argument("--max-trade-days", type=int, default=0, help="0이면 전체, 양수면 최근 N거래일만 사용")
    ap.add_argument("--output-json", default=os.path.expanduser("~/.openclaw/data/backtest_2025_result.json"))
    args = ap.parse_args()
    mode_cfg = {
        "strict": {"buy_threshold": 70.0, "stage2_min": 50.0},
        "balanced": {"buy_threshold": 65.0, "stage2_min": 45.0},
        "neutral": {"buy_threshold": 60.0, "stage2_min": 40.0},
    }[args.mode]
    buy_threshold = float(args.buy_threshold) if float(args.buy_threshold) > 0 else float(mode_cfg["buy_threshold"])
    default_min_cash = _load_live_min_cash_ratio(float(os.getenv("DEFAULT_MIN_CASH_RATIO", "0.15")))
    if float(args.min_cash_ratio) >= 0.0:
        min_cash_ratio = _clamp(float(args.min_cash_ratio), 0.0, 0.90)
    else:
        min_cash_ratio = default_min_cash
    daily_deploy_cap_ratio = _clamp(float(args.daily_deploy_cap_ratio), 0.0, 1.0)

    start = args.start_date
    end = args.end_date
    _log(f"load range={start}..{end}")

    tech_rows = ch_select(
        f"""
SELECT
  toString(date) AS d, ticker, any(ticker_name) AS ticker_name,
  any(market) AS market, any(close_price) AS close_price, any(ma20) AS ma20,
  any(ma60) AS ma60, any(rsi14) AS rsi14, any(vol_ratio) AS vol_ratio,
  any(signal_score) AS signal_score, any(bb_pct) AS bb_pct, any(volume) AS volume
FROM trading.technical_signals
WHERE date >= '{start}' AND date <= '{end}'
  AND match(ticker, '^[0-9]{{6}}$')
GROUP BY d, ticker
"""
    )
    if not tech_rows:
        raise SystemExit("no technical_signals rows in range. run backfill_technical_signals.py first")

    tech_by_day: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    close_by_day: dict[str, dict[str, float]] = defaultdict(dict)
    for r in tech_rows:
        d = str(r.get("d"))
        t = str(r.get("ticker"))
        row = {
            "ticker_name": str(r.get("ticker_name", "")),
            "market": str(r.get("market", "")),
            "close": _to_float(r.get("close_price"), 0.0),
            "ma20": _to_float(r.get("ma20"), 0.0),
            "ma60": _to_float(r.get("ma60"), 0.0),
            "rsi14": _to_float(r.get("rsi14"), 50.0),
            "vol_ratio": _to_float(r.get("vol_ratio"), 1.0),
            "signal_score": _to_float(r.get("signal_score"), 0.0),
            "bb_pct": _to_float(r.get("bb_pct"), 0.5),
            "volume": _to_int(r.get("volume"), 0),
        }
        tech_by_day[d][t] = row
        close_by_day[d][t] = row["close"]

    trade_days = sorted(tech_by_day.keys())
    if int(args.max_trade_days) > 0:
        trade_days = trade_days[-int(args.max_trade_days):]
    if len(trade_days) < max(1, int(args.min_trade_days)):
        raise SystemExit(f"not enough trade days: {len(trade_days)}")
    _log(f"trade_days={len(trade_days)}")

    market_idx_rows = ch_select(
        f"""
SELECT toString(date) AS d, index_code, close_price
FROM trading.market_index
WHERE date >= addDays(toDate('{start}'), -20)
  AND date <= '{end}'
  AND index_code = 'KOSPI'
"""
    )
    fx_rows = ch_select(
        f"""
SELECT toString(date) AS d, close_rate
FROM trading.exchange_rate
WHERE date >= addDays(toDate('{start}'), -20)
  AND date <= '{end}'
  AND currency_pair = 'USDKRW'
"""
    )
    rate_rows = ch_select(
        f"""
SELECT toString(date) AS d, rate_value
FROM trading.interest_rate
WHERE date >= addDays(toDate('{start}'), -30)
  AND date <= '{end}'
"""
    )
    kospi_hist = {str(r.get("d")): _to_float(r.get("close_price"), 0.0) for r in market_idx_rows}
    usd_hist = {str(r.get("d")): _to_float(r.get("close_rate"), 0.0) for r in fx_rows}
    rate_hist = {str(r.get("d")): _to_float(r.get("rate_value"), 0.0) for r in rate_rows}

    # Fallback: if DB history is sparse, use yfinance daily history for stage1.
    if yf is not None and len(kospi_hist) < 120:
        try:
            h = yf.Ticker("^KS11").history(
                start=(dt.date.fromisoformat(start) - dt.timedelta(days=30)).isoformat(),
                end=(dt.date.fromisoformat(end) + dt.timedelta(days=1)).isoformat(),
            )
            for idx, row in h.iterrows():
                d = idx.strftime("%Y-%m-%d")
                c = _to_float(row.get("Close"), 0.0)
                if c > 0:
                    kospi_hist[d] = c
            _log(f"stage1 fallback: yfinance KOSPI rows={len(h)}")
        except Exception as exc:
            _log(f"stage1 fallback KOSPI failed: {exc}")
    if yf is not None and len(usd_hist) < 120:
        try:
            h = yf.Ticker("KRW=X").history(
                start=(dt.date.fromisoformat(start) - dt.timedelta(days=30)).isoformat(),
                end=(dt.date.fromisoformat(end) + dt.timedelta(days=1)).isoformat(),
            )
            for idx, row in h.iterrows():
                d = idx.strftime("%Y-%m-%d")
                c = _to_float(row.get("Close"), 0.0)
                if c > 0:
                    usd_hist[d] = c
            _log(f"stage1 fallback: yfinance USDKRW rows={len(h)}")
        except Exception as exc:
            _log(f"stage1 fallback USDKRW failed: {exc}")

    market_flow_rows = ch_select(
        f"""
SELECT
  toString(trade_date) AS d,
  investor_type,
  sum(net_buy_value_krw) AS net_buy,
  sum(market_traded_value_krw) AS traded,
  anyHeavy(market_traded_value_krw_source) AS source,
  max(market_traded_value_krw_universe_n) AS universe_n
FROM trading.market_flow_daily
WHERE trade_date >= addDays(toDate('{start}'), -7)
  AND trade_date <= '{end}'
  AND market = 'ALL'
GROUP BY d, investor_type
"""
    )
    market_flow_by_day: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for r in market_flow_rows:
        market_flow_by_day[str(r.get("d"))][str(r.get("investor_type"))] = {
            "net_buy": _to_float(r.get("net_buy"), 0.0),
            "traded": _to_float(r.get("traded"), 0.0),
            "source": str(r.get("source", "") or ""),
            "universe_n": _to_int(r.get("universe_n"), 0),
        }

    stock_flow_rows = ch_select(
        f"""
SELECT toString(trade_date) AS d, ticker, investor_type,
       sum(net_buy_value_krw) AS net_buy, max(traded_value_krw) AS traded
FROM trading.stock_flow_daily
WHERE trade_date >= addDays(toDate('{start}'), -7)
  AND trade_date <= '{end}'
GROUP BY d, ticker, investor_type
"""
    )
    stock_flow_by_day_ticker: dict[str, dict[str, dict[str, dict[str, float]]]] = defaultdict(lambda: defaultdict(dict))
    for r in stock_flow_rows:
        t = str(r.get("ticker"))
        d = str(r.get("d"))
        inv = str(r.get("investor_type"))
        stock_flow_by_day_ticker[t][d][inv] = {"net_buy": _to_float(r.get("net_buy"), 0.0), "traded": _to_float(r.get("traded"), 0.0)}

    event_rows = ch_select(
        f"""
SELECT
  toString(toDate(published_at)) AS d,
  arrayJoin(tickers) AS ticker,
  avg(toFloat64(importance)) AS importance_avg,
  max(toFloat64(importance)) AS importance_max,
  count() AS event_cnt,
  countIf(thesis_path != '' AND evidence_json != '[]') AS explain_ready_cnt
FROM trading.news_event_frames
WHERE published_at >= addDays(toDateTime('{start} 00:00:00'), -3)
  AND published_at <= toDateTime('{end} 23:59:59')
  AND relevant = 1
GROUP BY d, ticker
HAVING match(ticker, '^[0-9]{{6}}$')
"""
    )
    event_daily: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    for r in event_rows:
        event_daily[str(r.get("d"))][str(r.get("ticker"))] = {
            "importance_avg": _to_float(r.get("importance_avg"), 0.0),
            "importance_max": _to_float(r.get("importance_max"), 0.0),
            "event_cnt": _to_int(r.get("event_cnt"), 0),
            "explain_ready_cnt": _to_int(r.get("explain_ready_cnt"), 0),
        }
    market_event_rows = ch_select(
        f"""
SELECT
  toString(toDate(published_at)) AS d,
  avg(toFloat64(importance)) AS importance_avg,
  count() AS event_cnt,
  countIf(thesis_path != '' AND evidence_json != '[]') AS explain_ready_cnt
FROM trading.news_event_frames
WHERE published_at >= addDays(toDateTime('{start} 00:00:00'), -3)
  AND published_at <= toDateTime('{end} 23:59:59')
  AND relevant = 1
GROUP BY d
"""
    )
    market_event_daily: dict[str, dict[str, float]] = {}
    for r in market_event_rows:
        market_event_daily[str(r.get("d"))] = {
            "importance_avg": _to_float(r.get("importance_avg"), 0.0),
            "event_cnt": _to_int(r.get("event_cnt"), 0),
            "explain_ready_cnt": _to_int(r.get("explain_ready_cnt"), 0),
        }

    watchlist_rows: list[dict[str, Any]] = []
    if args.universe == "watchlist":
        try:
            watchlist_rows = ch_select(
                f"""
SELECT
  toString(ts) AS ts,
  toString(toDate(ts)) AS d,
  ticker
FROM trading.interest_watchlist
WHERE ts >= addDays(toDateTime('{start} 00:00:00'), -5)
  AND ts <= toDateTime('{end} 23:59:59')
  AND match(ticker, '^[0-9]{{6}}$')
ORDER BY ts
"""
            )
        except Exception:
            watchlist_rows = []

    holdings: dict[str, Holding] = {}
    pending: list[PendingOrder] = []
    cash = float(args.initial_cash)
    curve: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    fallback_market_days = 0
    fallback_stock_cnt = 0
    diag: Counter[str] = Counter()

    w = {"s1": 0.25, "s2": 0.30, "s3": 0.25, "s4": 0.15, "s5": 0.05}
    lots: dict[str, list[tuple[int, float]]] = defaultdict(list)  # qty, unit_cost_with_buy_fee
    realized_pnl = 0.0
    fees_paid = 0.0
    taxes_paid = 0.0
    closed_trade_count = 0
    closed_win_count = 0

    for i, day in enumerate(trade_days):
        price_map = close_by_day.get(day, {})
        if not price_map:
            continue

        # Execute pending orders at today's close.
        remain: list[PendingOrder] = []
        for od in pending:
            if od.exec_date != day:
                remain.append(od)
                continue
            px = _to_float(price_map.get(od.ticker), 0.0)
            if px <= 0:
                continue
            slip = float(args.slippage_bps) / 10000.0
            fee = float(args.fee_bps) / 10000.0
            sell_tax = float(args.sell_tax_bps) / 10000.0
            if od.side == "BUY":
                exec_px = px * (1.0 + slip)
                qty = od.qty if od.qty > 0 else int(max(0.0, od.notional) / exec_px)
                if qty <= 0:
                    continue
                gross = qty * exec_px
                cost = gross * fee
                total = gross + cost
                if total > cash:
                    continue
                cash -= total
                fees_paid += cost
                h = holdings.get(od.ticker)
                if h:
                    new_qty = h.qty + qty
                    avg = (h.avg_price * h.qty + exec_px * qty) / max(1, new_qty)
                    holdings[od.ticker] = Holding(qty=new_qty, avg_price=avg, entry_date=h.entry_date)
                else:
                    holdings[od.ticker] = Holding(qty=qty, avg_price=exec_px, entry_date=day)
                unit_cost = total / max(1, qty)
                lots[od.ticker].append((qty, unit_cost))
                trades.append({"date": day, "ticker": od.ticker, "side": "BUY", "qty": qty, "price": round(exec_px, 2), "reason": od.reason})
            else:
                h = holdings.get(od.ticker)
                if not h or h.qty <= 0:
                    continue
                exec_px = px * (1.0 - slip)
                qty = h.qty if od.qty <= 0 else min(h.qty, od.qty)
                gross = qty * exec_px
                fee_cost = gross * fee
                tax_cost = gross * sell_tax
                net = gross - fee_cost - tax_cost
                cash += net
                fees_paid += fee_cost
                taxes_paid += tax_cost
                remain_qty = h.qty - qty
                if remain_qty <= 0:
                    del holdings[od.ticker]
                else:
                    holdings[od.ticker] = Holding(qty=remain_qty, avg_price=h.avg_price, entry_date=h.entry_date)

                remaining_qty = qty
                sell_unit_net = net / max(1, qty)
                pnl_this_sell = 0.0
                fifo = lots.get(od.ticker, [])
                new_fifo: list[tuple[int, float]] = []
                for lot_qty, lot_cost in fifo:
                    if remaining_qty <= 0:
                        new_fifo.append((lot_qty, lot_cost))
                        continue
                    take = min(remaining_qty, lot_qty)
                    pnl_this_sell += (sell_unit_net - lot_cost) * take
                    left = lot_qty - take
                    if left > 0:
                        new_fifo.append((left, lot_cost))
                    remaining_qty -= take
                if remaining_qty > 0:
                    pnl_this_sell -= sell_unit_net * remaining_qty
                lots[od.ticker] = new_fifo
                realized_pnl += pnl_this_sell
                closed_trade_count += 1
                if pnl_this_sell > 0:
                    closed_win_count += 1
                trades.append({"date": day, "ticker": od.ticker, "side": "SELL", "qty": qty, "price": round(exec_px, 2), "reason": od.reason})
        pending = remain

        # Mark-to-market
        position_value = sum(h.qty * _to_float(price_map.get(tk), 0.0) for tk, h in holdings.items())
        equity = cash + position_value
        curve.append(
            {
                "date": day,
                "equity": round(equity, 2),
                "cash": round(cash, 2),
                "positions": len(holdings),
                "gross_exposure": round(position_value / equity, 6) if equity > 0 else 0.0,
            }
        )

        # Last day: no new orders
        if i >= len(trade_days) - 1:
            continue
        next_day = trade_days[i + 1]

        s1, s1_pass, hard_riskoff = score_stage1(day, kospi_hist, usd_hist, rate_hist)
        if hard_riskoff:
            diag["hard_riskoff_days"] += 1
        mkt_s2, market_valid, shock, mkt_foreign_pct, mkt_inst_pct = score_market_flow(
            day, market_flow_by_day, float(args.fallback_market_score)
        )
        if not market_valid:
            fallback_market_days += 1

        day_tech = tech_by_day.get(day, {})
        if not day_tech:
            continue
        universe = build_universe_for_day(day, day_tech, watchlist_rows, int(args.universe_size))
        universe = expand_universe_after_prefilter(
            day_tech=day_tech,
            base_universe=universe,
            holdings=holdings,
            target_non_holding=int(args.universe_size),
            prefilter_liquidity_krw=float(args.prefilter_liquidity_krw),
        )
        for tk in list(holdings.keys()):
            if tk not in universe:
                universe.append(tk)

        candidates: list[dict[str, Any]] = []
        for tk in universe:
            tech = day_tech.get(tk)
            if not tech:
                continue
            liquidity = max(0.0, _to_float(tech.get("close"), 0.0) * _to_float(tech.get("volume"), 0.0))
            # 후보 단계에서 저유동 종목을 먼저 제외해 Step5 병목을 줄인다.
            # 단, 이미 보유 중인 종목은 청산/감산 판단을 위해 계속 평가한다.
            if tk not in holdings and float(args.prefilter_liquidity_krw) > 0 and liquidity < float(args.prefilter_liquidity_krw):
                diag["prefilter_low_liquidity"] += 1
                continue

            s2_stock, stock_valid, stk_foreign_pct, stk_inst_pct = score_stock_flow(
                day, tk, stock_flow_by_day_ticker, float(args.fallback_stock_score)
            )
            if not stock_valid:
                fallback_stock_cnt += 1
            s2 = _clamp(mkt_s2 + s2_stock, 0, 100)
            if args.mode == "neutral":
                if (not market_valid) or (not stock_valid):
                    s2 = max(s2, float(args.neutral_stage2_min))
                s2_pass = (shock != "EXTREME") and s2 >= 40
            elif args.mode == "balanced":
                if (not market_valid) or (not stock_valid):
                    s2 = max(s2, 45.0)
                s2_pass = (shock != "EXTREME") and s2 >= 45
            else:
                s2_pass = (shock != "EXTREME") and s2 >= float(mode_cfg["stage2_min"])
            distribution_block = stk_foreign_pct <= -6.0 and stk_inst_pct <= -3.0
            if distribution_block:
                s2_pass = False
            if args.s2_reference_only:
                # 사용자는 S2를 참고지표로만 쓰길 원함: 하드차단은 EXTREME만 유지
                s2_pass = shock != "EXTREME"

            s3, s3_explain = score_stage3(day, tk, event_daily, market_event_daily, args.mode)
            s3_pass = s3 >= 50
            s4, s4_pass, rsi_overheat = score_stage4(tech, s1, hard_riskoff, args.mode)
            if args.disable_stage4:
                s4_pass = True
                rsi_overheat = False
            s5, s5_pass, s5_exec_mult = score_stage5(liquidity, 0.0, s3, s4, hard_riskoff)
            if not s1_pass:
                diag["fail_s1"] += 1
            if not s2_pass:
                diag["fail_s2"] += 1
            if not s3_pass:
                diag["fail_s3"] += 1
            if not s4_pass:
                diag["fail_s4"] += 1
            if not s5_pass:
                diag["fail_s5"] += 1

            total = (
                w["s1"] * s1
                + w["s2"] * s2
                + w["s3"] * s3
                + w["s4"] * s4
                + w["s5"] * s5
            )
            penalty = 0.0
            if shock == "ALERT":
                penalty += 10.0
            if shock == "EXTREME":
                penalty += 20.0
            if distribution_block:
                penalty += 15.0
            if rsi_overheat:
                penalty += 10.0
            scores = {"s1": s1, "s2": s2, "s3": s3, "s4": s4, "s5": s5}
            enabled = ["s1", "s3", "s5"]
            if not bool(args.s2_reference_only):
                enabled.append("s2")
            if not bool(args.disable_stage4):
                enabled.append("s4")
            total, effective_total = _weighted_effective_score(scores, w, enabled, penalty)

            abs_block = hard_riskoff or (shock == "EXTREME") or rsi_overheat
            all_pass = s1_pass and s2_pass and s3_pass and s4_pass and s5_pass and not abs_block
            if all_pass:
                diag["all_pass"] += 1
            else:
                diag["all_fail"] += 1
            if effective_total >= buy_threshold:
                diag["score_ge_buy_threshold"] += 1
            rule_action = "HOLD"
            if all_pass and effective_total >= buy_threshold:
                rule_action = "BUY"
            elif total <= 35 or hard_riskoff:
                rule_action = "REDUCE"

            candidates.append(
                {
                    "ticker": tk,
                    "total": round(total, 2),
                    "effective_total": round(effective_total, 2),
                    "action": rule_action,
                    "rule_action": rule_action,
                    "s2": round(s2, 2),
                    "s3": round(s3, 2),
                    "s3_explain": s3_explain,
                    "s4": round(s4, 2),
                    "s5": round(s5, 2),
                    "shock": shock,
                    "s5_exec_mult": round(s5_exec_mult, 3),
                    "all_pass": bool(all_pass),
                    "s5_pass": bool(s5_pass),
                    "rsi_overheat": bool(rsi_overheat),
                    "distribution_block": bool(distribution_block),
                }
            )

        if args.llm_decision and candidates:
            llm_map, llm_status = llm_decide_actions_for_day(
                day=day,
                mode=args.mode,
                s1_score=s1,
                hard_riskoff=hard_riskoff,
                shock=shock,
                candidates=candidates,
                holdings=holdings,
                llm_model=str(args.llm_model),
                llm_timeout_sec=int(args.llm_timeout_sec),
                llm_cache_ttl_sec=int(args.llm_cache_ttl_sec),
                llm_candidate_limit=int(args.llm_candidate_limit),
            )
            diag[f"llm_status_{llm_status}"] += 1
            min_conf = _clamp(_to_float(args.llm_min_confidence, 0.6), 0.0, 1.0)
            for c in candidates:
                dec = llm_map.get(str(c.get("ticker", "")))
                if not dec:
                    continue
                conf = _clamp(_to_float(dec.get("confidence"), 0.0), 0.0, 1.0)
                action = str(dec.get("action", "HOLD") or "HOLD").upper().strip()
                if conf < min_conf:
                    action = "HOLD"
                # hard guardrails
                if action == "BUY":
                    if hard_riskoff or shock == "EXTREME" or (not bool(c.get("s5_pass", False))) or bool(c.get("rsi_overheat", False)):
                        action = "HOLD"
                if action == "REDUCE" and c["ticker"] not in holdings:
                    action = "HOLD"
                if action not in {"BUY", "HOLD", "REDUCE"}:
                    action = "HOLD"
                c["action"] = action
                c["llm_confidence"] = round(conf, 4)
                c["llm_reason"] = str(dec.get("reason", "") or "")[:220]

        for c in candidates:
            action = str(c.get("action", "HOLD"))
            if action == "BUY":
                diag["action_buy"] += 1
            elif action == "REDUCE":
                diag["action_sell"] += 1
            else:
                diag["action_hold"] += 1

        # Exit first
        for c in candidates:
            tk = c["ticker"]
            if tk in holdings and c["action"] == "REDUCE":
                pending.append(PendingOrder(exec_date=next_day, ticker=tk, side="SELL", qty=0, reason="rule_reduce"))

        # Buy candidates
        if not hard_riskoff and shock != "EXTREME":
            open_slots = max(0, int(args.max_positions) - len(holdings))
            if open_slots <= 0:
                diag["days_no_open_slots"] += 1
            if open_slots > 0:
                buy_cands = [c for c in candidates if c["action"] == "BUY" and c["ticker"] not in holdings]
                if not buy_cands:
                    diag["days_no_buy_candidates"] += 1
                buy_cands.sort(key=lambda x: x.get("effective_total", x["total"]), reverse=True)
                if int(args.max_new_buys_per_day) > 0:
                    open_slots = min(open_slots, int(args.max_new_buys_per_day))
                reserve_cash = equity * min_cash_ratio
                investable_cash = max(0.0, cash - reserve_cash)
                if investable_cash <= 0:
                    diag["days_cash_locked_by_min_cash"] += 1
                    continue
                day_deploy_cap = investable_cash
                if daily_deploy_cap_ratio > 0:
                    day_deploy_cap = min(day_deploy_cap, equity * daily_deploy_cap_ratio)
                planned_deploy = 0.0
                for c in buy_cands[:open_slots]:
                    buy_mult = _clamp((s1 - 45.0) / 25.0, 0.0, 1.0)
                    if buy_mult <= 0:
                        diag["buy_skipped_buy_mult"] += 1
                        continue
                    if c["shock"] == "EXTREME":
                        m_shock = 0.0
                    elif c["shock"] == "ALERT":
                        m_shock = 0.35
                    elif c["shock"] == "WARN":
                        m_shock = 0.70
                    else:
                        m_shock = 1.0
                    score_for_size = _to_float(c.get("effective_total"), _to_float(c.get("total"), 0.0))
                    base_weight = _clamp(0.03 + max(0.0, score_for_size - buy_threshold) / 200.0, 0.0, 0.10)
                    target_notional = equity * base_weight * buy_mult * m_shock * _to_float(c.get("s5_exec_mult"), 1.0)
                    remaining_budget = min(investable_cash - planned_deploy, day_deploy_cap - planned_deploy)
                    if remaining_budget <= 0:
                        diag["buy_skipped_daily_deploy_cap"] += 1
                        break
                    order_notional = min(target_notional, remaining_budget)
                    if order_notional < 500_000:
                        diag["buy_skipped_notional"] += 1
                        continue
                    pending.append(
                        PendingOrder(
                            exec_date=next_day,
                            ticker=c["ticker"],
                            side="BUY",
                            notional=order_notional,
                            reason="score_buy",
                        )
                    )
                    planned_deploy += order_notional

        if (i + 1) % 20 == 0:
            _log(
                f"progress {i+1}/{len(trade_days)} day={day} equity={equity:,.0f} holdings={len(holdings)} pending={len(pending)}"
            )

    # Final mark-to-market / optional force liquidation
    if trade_days:
        last = trade_days[-1]
        px = close_by_day.get(last, {})
        if args.force_liquidate_on_end and holdings:
            slip = float(args.slippage_bps) / 10000.0
            fee = float(args.fee_bps) / 10000.0
            sell_tax = float(args.sell_tax_bps) / 10000.0
            for tk, h in list(holdings.items()):
                if h.qty <= 0:
                    continue
                p = _to_float(px.get(tk), 0.0)
                if p <= 0:
                    continue
                exec_px = p * (1.0 - slip)
                qty = h.qty
                gross = qty * exec_px
                fee_cost = gross * fee
                tax_cost = gross * sell_tax
                net = gross - fee_cost - tax_cost
                cash += net
                fees_paid += fee_cost
                taxes_paid += tax_cost

                sell_unit_net = net / max(1, qty)
                pnl_this_sell = 0.0
                remaining_qty = qty
                fifo = lots.get(tk, [])
                new_fifo: list[tuple[int, float]] = []
                for lot_qty, lot_cost in fifo:
                    if remaining_qty <= 0:
                        new_fifo.append((lot_qty, lot_cost))
                        continue
                    take = min(remaining_qty, lot_qty)
                    pnl_this_sell += (sell_unit_net - lot_cost) * take
                    left = lot_qty - take
                    if left > 0:
                        new_fifo.append((left, lot_cost))
                    remaining_qty -= take
                lots[tk] = new_fifo
                realized_pnl += pnl_this_sell
                closed_trade_count += 1
                if pnl_this_sell > 0:
                    closed_win_count += 1
                trades.append({"date": last, "ticker": tk, "side": "SELL", "qty": qty, "price": round(exec_px, 2), "reason": "force_liquidate_end"})
                del holdings[tk]
            final_equity = cash
            if curve:
                curve[-1]["equity"] = round(final_equity, 2)
                curve[-1]["cash"] = round(cash, 2)
                curve[-1]["positions"] = 0
                curve[-1]["gross_exposure"] = 0.0
        else:
            final_equity = cash + sum(h.qty * _to_float(px.get(tk), 0.0) for tk, h in holdings.items())
    else:
        final_equity = cash

    metrics = calc_metrics(curve, float(args.initial_cash))
    unrealized_pnl = 0.0
    if trade_days:
        last = trade_days[-1]
        px = close_by_day.get(last, {})
        for tk, fifo in lots.items():
            p = _to_float(px.get(tk), 0.0)
            if p <= 0:
                continue
            for lot_qty, lot_cost in fifo:
                if lot_qty <= 0:
                    continue
                unrealized_pnl += (p - lot_cost) * lot_qty
    buy_order_count = sum(1 for tr in trades if tr.get("side") == "BUY")
    sell_order_count = sum(1 for tr in trades if tr.get("side") == "SELL")
    avg_exposure = 0.0
    if curve:
        avg_exposure = sum(_to_float(x.get("gross_exposure"), 0.0) for x in curve) / len(curve)
    unique_tickers = sorted({str(tr.get("ticker", "")) for tr in trades if str(tr.get("ticker", ""))})
    buy_by_day = Counter(str(tr.get("date", "")) for tr in trades if str(tr.get("side", "")) == "BUY")
    max_buys_per_day = max(buy_by_day.values()) if buy_by_day else 0
    avg_buys_per_day = (sum(buy_by_day.values()) / max(1, len(buy_by_day))) if buy_by_day else 0.0

    result = {
        "config": {
            "start_date": start,
            "end_date": end,
            "initial_cash": args.initial_cash,
            "mode": args.mode,
            "universe": args.universe,
            "disable_stage4": bool(args.disable_stage4),
            "s2_reference_only": bool(args.s2_reference_only),
            "llm_decision": bool(args.llm_decision),
            "llm_model": str(args.llm_model),
            "llm_min_confidence": _to_float(args.llm_min_confidence, 0.6),
            "prefilter_liquidity_krw": float(args.prefilter_liquidity_krw),
            "max_new_buys_per_day": int(args.max_new_buys_per_day),
            "min_cash_ratio": float(min_cash_ratio),
            "daily_deploy_cap_ratio": float(daily_deploy_cap_ratio),
            "force_liquidate_on_end": bool(args.force_liquidate_on_end),
            "buy_threshold": buy_threshold,
            "universe_size": args.universe_size,
            "max_positions": args.max_positions,
            "fee_bps": args.fee_bps,
            "sell_tax_bps": args.sell_tax_bps,
            "slippage_bps": args.slippage_bps,
        },
        "metrics": {
            **metrics,
            "final_equity": round(final_equity, 2),
            "order_count": len(trades),
            "buy_order_count": buy_order_count,
            "sell_order_count": sell_order_count,
            "round_trip_count": closed_trade_count,
            "win_rate_closed_pct": round((closed_win_count / closed_trade_count * 100.0), 2) if closed_trade_count > 0 else 0.0,
            "realized_pnl_krw": round(realized_pnl, 2),
            "unrealized_pnl_end_krw": round(unrealized_pnl, 2),
            "fees_paid_krw": round(fees_paid, 2),
            "taxes_paid_krw": round(taxes_paid, 2),
            "avg_gross_exposure": round(avg_exposure, 4),
            "max_buys_per_day": int(max_buys_per_day),
            "avg_buys_per_day": round(avg_buys_per_day, 3),
            "unique_ticker_count": len(unique_tickers),
        },
        "quality": {
            "flow_market_fallback_days": fallback_market_days,
            "flow_stock_fallback_count": fallback_stock_cnt,
            "trade_days": len(trade_days),
            "diagnostics": dict(diag),
        },
        "trades": trades,
        "sample_trades": trades[:40],
        "traded_tickers": unique_tickers[:80],
        "equity_curve_head": curve[:10],
        "equity_curve_tail": curve[-10:],
    }

    out_path = args.output_json
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    _log(
        f"done final={final_equity:,.0f} return={metrics.get('total_return_pct', 0):+.2f}% "
        f"mdd={metrics.get('max_drawdown_pct', 0):.2f}% sharpe={metrics.get('sharpe', 0):.3f} trades={len(trades)}"
    )
    _log(f"result saved: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
