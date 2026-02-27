#!/usr/bin/env python3
"""market_realtime.py

실시간 시장 스냅샷 보조 모듈.
- KOSPI/KOSDAQ: Naver polling API
- USDKRW: Naver marketindex HTML 파싱
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Optional

try:
    import requests
except ImportError:
    from _requests_compat import requests


NAVER_RT_URL = "https://polling.finance.naver.com/api/realtime"
NAVER_USDKRW_URL = "https://finance.naver.com/marketindex/exchangeDetail.naver?marketindexCd=FX_USDKRW"
REQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Referer": "https://finance.naver.com/",
}


def _to_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def _strip_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s or "")


def _extract_number(s: str) -> Optional[float]:
    m = re.search(r"([0-9][0-9,]*\.[0-9]+)", s or "")
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except Exception:
        return None


def fetch_naver_realtime_indices(timeout_sec: int = 8) -> Dict[str, Dict[str, Any]]:
    """KOSPI/KOSDAQ 실시간 시세 반환."""
    out: Dict[str, Dict[str, Any]] = {}
    try:
        r = requests.get(
            NAVER_RT_URL,
            params={"query": "SERVICE_INDEX:KOSPI,KOSDAQ"},
            headers=REQ_HEADERS,
            timeout=timeout_sec,
        )
        r.raise_for_status()
        j = r.json()
        result = j.get("result", {}) if isinstance(j, dict) else {}
        areas = result.get("areas", []) if isinstance(result, dict) else []
        datas = []
        if areas and isinstance(areas[0], dict):
            datas = areas[0].get("datas", []) or []
        asof_ms = result.get("time")
        for d in datas:
            if not isinstance(d, dict):
                continue
            code = str(d.get("cd", "")).strip().upper()
            if code not in {"KOSPI", "KOSDAQ"}:
                continue
            nv = _to_float(d.get("nv"))
            if nv is None:
                continue
            out[code] = {
                "price": nv / 100.0,
                "change_pct": _to_float(d.get("cr")),
                "market_state": str(d.get("ms", "") or ""),
                "asof_epoch_ms": asof_ms,
                "source": "naver_polling",
            }
    except Exception:
        return {}
    return out


def fetch_naver_usdkrw(timeout_sec: int = 8) -> Dict[str, Any]:
    """USDKRW 실시간(고시) 시세 반환."""
    try:
        r = requests.get(NAVER_USDKRW_URL, headers=REQ_HEADERS, timeout=timeout_sec)
        r.raise_for_status()
        raw = r.content
        try:
            html = raw.decode("cp949", errors="ignore")
        except Exception:
            html = raw.decode("utf-8", errors="ignore")
    except Exception:
        return {}

    today_block = re.search(r'(?s)<p class="no_today">.*?</p>', html)
    exday_block = re.search(r'(?s)<p class="no_exday">.*?</p>', html)
    date_block = re.search(r'<span class="date">([^<]+)</span>', html)

    price = _extract_number(_strip_tags(today_block.group(0)) if today_block else "")
    change_pct: Optional[float] = None
    if exday_block:
        exday_html = exday_block.group(0)
        text = re.sub(r"\s+", " ", _strip_tags(exday_html))
        m = re.search(r"\(\s*([+\-]?\d+\.\d+)\s*%\s*\)", text)
        if m:
            change_pct = _to_float(m.group(1))
        else:
            # 괄호 파싱 실패 시 class로 부호 추론
            pct = _extract_number(text)
            if pct is not None:
                sign = -1.0 if ("ico minus" in exday_html or "ico down" in exday_html) else 1.0
                change_pct = sign * pct

    observed_at = ""
    if date_block:
        observed_at = date_block.group(1).strip()
        # "2026.02.23 08:51" -> ISO 유사 포맷으로도 추가 제공
        try:
            dt = datetime.strptime(observed_at, "%Y.%m.%d %H:%M")
            observed_at = dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

    if price is None:
        return {}
    return {
        "pair": "USDKRW",
        "price": price,
        "change_pct": change_pct,
        "observed_at": observed_at,
        "source": "naver_marketindex",
    }
