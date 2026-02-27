#!/usr/bin/env python3
"""ticker_mapper.py

KRX 종목 마스터: 회사명 → 종목코드 매핑

데이터 소스 (우선순위):
  1. STOCKS.csv (2,885종목 전체 마스터)
  2. 캐시 krx_stocks.json (7일 유효)
  3. 네이버 금융 (주말에도 동작)
  4. 내장 매핑 26종목 (최후 폴백)

의존성: 표준 라이브러리만 사용(urllib).
캐시: ~/.openclaw/data/krx_stocks.json
마스터: ~/.openclaw/workspace/STOCKS.csv
"""

import csv
import json
import time
import logging
import re
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

log = logging.getLogger("ticker-mapper")

CACHE_DIR = Path.home() / ".openclaw" / "data"
CACHE_FILE = CACHE_DIR / "krx_stocks.json"
STOCKS_CSV = Path.home() / ".openclaw" / "workspace" / "STOCKS.csv"
CACHE_TTL = 7 * 86400  # 7일 (주말 커버)


def http_get_text(url: str, headers=None, timeout=15) -> str:
    headers = dict(headers or {})
    req = Request(url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except HTTPError as e:
        raise RuntimeError(f"HTTP {e.code}: {url}") from e
    except URLError as e:
        raise RuntimeError(f"URL error: {url} {e}") from e


class TickerMapper:
    def __init__(self):
        self.stocks = {}
        self.aliases = {}
        self._load()

    def _load(self):
        # 1순위: STOCKS.csv (전체 2,885종목)
        self._load_stocks_csv()
        if self.stocks:
            return
        # 2순위: 캐시 (krx_stocks.json)
        if self._cache_valid():
            self._load_cache()
            if self.stocks:
                return
        # 3순위: 네이버 금융 스크래핑
        self._fetch_naver()
        if not self.stocks:
            self._load_cache()  # 만료된 캐시라도 사용
        if not self.stocks:
            self._use_builtin()

    def _load_stocks_csv(self):
        """STOCKS.csv에서 전체 종목 로드 (가장 우선)"""
        if not STOCKS_CSV.exists():
            return
        try:
            stocks = {}
            with open(STOCKS_CSV, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = row.get("Code", "").strip()
                    name = row.get("Name", "").strip()
                    if code and name and len(code) == 6:
                        stocks[name] = code
            if stocks:
                self.stocks = stocks
                self._build_aliases()
                # 캐시도 갱신
                self._save_cache()
                log.info(f"STOCKS.csv 로드: {len(self.stocks)}종목")
        except Exception as e:
            log.warning(f"STOCKS.csv 읽기 실패: {e}")

    def _cache_valid(self):
        if not CACHE_FILE.exists():
            return False
        return (time.time() - CACHE_FILE.stat().st_mtime) < CACHE_TTL

    def _load_cache(self):
        try:
            data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            self.stocks = data.get("stocks", {})
            self.aliases = data.get("aliases", {})
            if self.stocks:
                log.info(f"종목 캐시 로드: {len(self.stocks)}종목")
        except Exception:
            pass

    def _save_cache(self):
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        CACHE_FILE.write_text(
            json.dumps(
                {
                    "updated": datetime.now().isoformat(),
                    "count": len(self.stocks),
                    "stocks": self.stocks,
                    "aliases": self.aliases,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    # ─── 네이버 금융에서 다운로드 ────────────────────────────
    def _fetch_naver(self):
        """네이버 금융 시가총액 페이지에서 전 종목 수집 (주말에도 동작)"""
        log.info("네이버 금융에서 종목 마스터 다운로드 중...")
        all_stocks = {}

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }

        # sosok=0: 코스피, sosok=1: 코스닥
        for sosok, label in [(0, "코스피"), (1, "코스닥")]:
            page = 1
            max_pages = 50  # 안전장치

            while page <= max_pages:
                try:
                    url = (
                        "https://finance.naver.com/sise/sise_market_sum.naver"
                        f"?sosok={sosok}&page={page}"
                    )
                    html = http_get_text(url, headers=headers, timeout=15)

                    # 종목코드와 이름 추출: /item/main.naver?code=005930
                    pattern = re.compile(
                        r'href="/item/main\.naver\?code=(\d{6})"[^>]*>'
                        r"\s*<a[^>]*>\s*([^<]+?)\s*</a>",
                        re.DOTALL,
                    )
                    matches = pattern.findall(html)

                    # 더 간단한 패턴도 시도
                    if not matches:
                        pattern2 = re.compile(
                            r'class="tltle"[^>]*href="/item/main\.naver\?code=(\d{6})"'
                            r"[^>]*>\s*([^<]+?)\s*</a>",
                            re.DOTALL,
                        )
                        matches = pattern2.findall(html)

                    # 최종 패턴: 가장 일반적
                    if not matches:
                        codes = re.findall(r"code=(\d{6})", html)
                        names = re.findall(r'class="tltle"[^>]*>([^<]+)</a>', html)
                        if codes and names:
                            matches = list(zip(codes[: len(names)], names))

                    if not matches:
                        break  # 더 이상 페이지 없음

                    before = len(all_stocks)
                    for code, name in matches:
                        name = (name or "").strip()
                        if name and len(code) == 6:
                            all_stocks[name] = code

                    # 새로 추가된 게 없으면 마지막 페이지
                    if len(all_stocks) == before:
                        break

                    page += 1
                    time.sleep(0.25)

                except Exception as e:
                    log.warning(f"  {label} page {page} 실패: {e}")
                    break

            log.info(f"  {label}: {len(all_stocks)}종목 (누적)")

        if all_stocks:
            self.stocks = all_stocks
            self._build_aliases()
            self._save_cache()
            log.info(f"종목 마스터: {len(self.stocks)}종목")
        else:
            log.warning("네이버 금융 데이터 없음")

    # ─── 별칭 사전 ────────────────────────────────────────
    def _build_aliases(self):
        self.aliases = {}
        for name in self.stocks:
            self.aliases[name] = name
            ns = name.replace(" ", "")
            if ns != name:
                self.aliases[ns] = name
            base = re.sub(r"\(.*?\)", "", name).strip()
            if base and base != name and base not in self.aliases:
                self.aliases[base] = name
            for prefix in ["HD", "LG", "SK", "KT"]:
                if name.startswith(prefix) and len(name) > len(prefix):
                    short = name[len(prefix) :]
                    if short not in self.aliases:
                        self.aliases[short] = name

    # ─── 폴백 ────────────────────────────────────────────
    def _use_builtin(self):
        log.info("내장 매핑 사용 (26종목)")
        self.stocks = {
            "삼성전자": "005930",
            "SK하이닉스": "000660",
            "현대차": "005380",
            "기아": "000270",
            "LG에너지솔루션": "373220",
            "삼성SDI": "006400",
            "POSCO홀딩스": "005490",
            "네이버": "035420",
            "카카오": "035720",
            "셀트리온": "068270",
            "KB금융": "105560",
            "신한지주": "055550",
            "하나금융지주": "086790",
            "우리금융지주": "316140",
            "한화에어로스페이스": "012450",
            "HD현대중공업": "329180",
            "HD한국조선해양": "009540",
            "LG전자": "066570",
            "SK이노베이션": "096770",
            "에코프로비엠": "247540",
            "한화솔루션": "009830",
            "현대모비스": "012330",
            "삼성바이오로직스": "207940",
            "LG화학": "051910",
            "카카오뱅크": "323410",
            "크래프톤": "259960",
        }
        self._build_aliases()

    # ─── 매핑 ─────────────────────────────────────────────
    def lookup(self, company_name: str):
        name = (company_name or "").strip()
        if not name:
            return None
        if name in self.stocks:
            return (name, self.stocks[name])
        if name in self.aliases:
            official = self.aliases[name]
            return (official, self.stocks[official])
        if len(name) >= 3:
            candidates = [(sn, code) for sn, code in self.stocks.items() if name in sn or sn in name]
            if len(candidates) == 1:
                return candidates[0]
            if candidates:
                candidates.sort(key=lambda x: abs(len(x[0]) - len(name)))
                return candidates[0]
        return None

    def map_companies(self, companies: list) -> list:
        results = []
        for name in companies:
            r = self.lookup(name)
            if r:
                results.append(r)
        return results

    def companies_to_tickers(self, companies: list) -> list:
        return [code for _, code in self.map_companies(companies)]


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    mapper = TickerMapper()
    print(f"\n총 {len(mapper.stocks)}종목 로드\n")

    names = sys.argv[1:] or [
        "삼성전자",
        "SK하이닉스",
        "에코프로비엠",
        "카카오뱅크",
        "현대중공업",
        "한국조선해양",
        "POSCO홀딩스",
        "없는회사",
    ]

    print(f"{'입력':20s} → {'정식명':20s}  {'코드':8s}")
    print("-" * 55)
    for name in names:
        result = mapper.lookup(name)
        if result:
            print(f"{name:20s} → {result[0]:20s}  {result[1]}")
        else:
            print(f"{name:20s} → ❌ 매핑 실패")
