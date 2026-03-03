#!/usr/bin/env python3
from __future__ import annotations

"""Lightweight web market signal fetcher.

No API key required. Uses Google News RSS queries to supplement macro context
when DB coverage is thin.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any
from urllib.parse import quote_plus
from urllib.request import Request, urlopen
import re
import xml.etree.ElementTree as ET


TOPIC_QUERIES: list[tuple[str, str]] = [
    ("geopolitics", "중동 전쟁 이란 이스라엘 원유 when:2d"),
    ("oil", "WTI 브렌트 유가 급등 호르무즈 when:2d"),
    ("fx", "원달러 환율 달러인덱스 DXY when:2d"),
    ("korea_market", "코스피 코스닥 급락 반등 전망 when:2d"),
    ("rates", "미국 국채 금리 FOMC 인플레이션 when:2d"),
]

TOPIC_KEYWORDS: dict[str, list[str]] = {
    "geopolitics": ["이란", "이스라엘", "중동", "전쟁", "공습", "분쟁", "war", "missile", "conflict"],
    "oil": ["유가", "원유", "브렌트", "wti", "호르무즈", "opec", "crude", "oil"],
    "shipping": ["해운", "운임", "항로", "수에즈", "물류", "shipping", "freight"],
    "sanctions": ["관세", "제재", "수출통제", "엠바고", "sanction", "tariff"],
    "fx": ["환율", "원달러", "달러인덱스", "dxy", "usdkrw", "dollar"],
    "rates": ["금리", "국채", "fomc", "yield", "채권"],
    "korea_market": ["코스피", "코스닥", "kospi", "kosdaq", "증시"],
}


@dataclass
class WebSignal:
    topic: str
    title: str
    source_url: str
    source_name: str
    published_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "title": self.title,
            "source_url": self.source_url,
            "source_name": self.source_name,
            "published_at": self.published_at,
            "importance": 3,
            "from_web": 1,
        }


def _text_norm(s: str) -> str:
    txt = unescape(str(s or ""))
    txt = re.sub(r"\s+", " ", txt)
    return txt.strip()


def _infer_topic(title: str, default_topic: str) -> str:
    low = _text_norm(title).lower()
    for topic, kws in TOPIC_KEYWORDS.items():
        for kw in kws:
            if kw.lower() in low:
                return topic
    return default_topic


def _parse_pubdate(pub_date: str) -> str:
    if not pub_date:
        return ""
    try:
        dt = parsedate_to_datetime(pub_date)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _fetch_query(topic: str, query: str, timeout_sec: int = 5) -> list[WebSignal]:
    q = quote_plus(query)
    url = f"https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=timeout_sec) as resp:  # nosec B310
        raw = resp.read()
    root = ET.fromstring(raw)
    out: list[WebSignal] = []
    for item in root.findall("./channel/item")[:12]:
        title = _text_norm(item.findtext("title", default=""))
        link = _text_norm(item.findtext("link", default=""))
        src = _text_norm(item.findtext("source", default=""))
        pub = _parse_pubdate(item.findtext("pubDate", default=""))
        if not title or not link:
            continue
        out.append(
            WebSignal(
                topic=_infer_topic(title, topic),
                title=title,
                source_url=link,
                source_name=src,
                published_at=pub,
            )
        )
    return out


def fetch_web_market_signals(limit: int = 12, timeout_sec: int = 5) -> list[dict[str, Any]]:
    """Fetch macro web headlines for supplementary context.

    Returns deduplicated list with stable keys. Never raises.
    """
    lim = max(1, int(limit))
    all_items: list[WebSignal] = []
    for topic, query in TOPIC_QUERIES:
        try:
            all_items.extend(_fetch_query(topic=topic, query=query, timeout_sec=timeout_sec))
        except Exception:
            continue

    dedup: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for sig in all_items:
        key = (sig.title, sig.source_url)
        if key in seen:
            continue
        seen.add(key)
        dedup.append(sig.as_dict())

    dedup.sort(key=lambda x: str(x.get("published_at", "")), reverse=True)
    return dedup[:lim]


if __name__ == "__main__":
    import json

    print(json.dumps(fetch_web_market_signals(limit=10), ensure_ascii=False, indent=2))
