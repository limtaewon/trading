#!/usr/bin/env python3
"""collect_dart.py

DART OpenAPI에서 공시 정보를 수집하여 ClickHouse에 저장.
OpenClaw gpt-5.2가 매매 판단 시 공시 기반 리스크를 파악할 수 있게 한다.

DART API 키 발급: https://opendart.fss.or.kr/ → 인증키 신청 (무료, 일 1만건)

사용법:
  python3 collect_dart.py                     # 최근 3일 공시
  python3 collect_dart.py --days 7            # 최근 7일
  python3 collect_dart.py --bgn_de 20260201   # 특정 시작일

환경변수:
  DART_API_KEY: DART OpenAPI 인증키 (필수)
"""
from __future__ import annotations

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import requests
except ImportError:
    from _requests_compat import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dart-collector")

try:
    from env_bootstrap import bootstrap_openclaw_env

    bootstrap_openclaw_env(override=False)
except Exception:
    pass


def _resolve_clickhouse_conn() -> tuple[str, tuple[str, str] | None]:
    raw = (
        os.environ.get("CLICKHOUSE_URL", "").strip()
        or os.environ.get("CLICKHOUSE_HOST", "").strip()
        or "http://localhost:8123"
    )
    user = os.environ.get("CLICKHOUSE_USER", "").strip()
    password = os.environ.get("CLICKHOUSE_PASS", os.environ.get("CLICKHOUSE_PASSWORD", "")).strip()

    p = urlparse(raw)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    if not user:
        user = (p.username or q.get("user") or "").strip()
    if not password:
        password = (p.password or q.get("password") or "").strip()

    safe_query = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if k.lower() not in ("user", "password")]
    netloc = p.hostname or "localhost"
    if p.port:
        netloc = f"{netloc}:{p.port}"
    safe_url = urlunparse((p.scheme or "http", netloc, p.path or "", p.params, urlencode(safe_query), p.fragment))
    auth = (user, password) if user else None
    return safe_url, auth


CLICKHOUSE_URL, CLICKHOUSE_AUTH = _resolve_clickhouse_conn()
DART_API_KEY = os.environ.get("DART_API_KEY", "")
DART_API_URL = "https://opendart.fss.or.kr/api/list.json"

# 중요 공시 유형 분류 + 자동 중요도
REPORT_CATEGORIES = {
    # 실적 관련 (importance 4)
    "사업보고서": ("earnings", 4),
    "반기보고서": ("earnings", 4),
    "분기보고서": ("earnings", 3),

    # 자본 변동 (importance 4-5)
    "증권신고서": ("capital", 4),
    "유상증자": ("capital", 5),
    "무상증자": ("capital", 3),
    "전환사채": ("capital", 4),
    "신주인수권부사채": ("capital", 4),
    "자기주식": ("capital", 3),
    "자사주": ("capital", 3),

    # 내부자 거래 (importance 3)
    "임원": ("insider", 3),
    "주요주주": ("insider", 3),
    "특정증권등소유상황": ("insider", 3),

    # 경영 관련 (importance 3-5)
    "주요사항보고서": ("management", 4),
    "관리종목": ("management", 5),
    "투자주의": ("management", 5),
    "상장폐지": ("management", 5),
    "거래정지": ("management", 5),
    "합병": ("management", 4),
    "분할": ("management", 4),
    "영업양수도": ("management", 4),
    "공개매수": ("management", 5),

    # 기업설명회
    "기업설명회": ("ir", 2),
    "IR": ("ir", 2),
}


def classify_report(report_nm: str, rm: str = "") -> tuple[str, int]:
    """보고서명으로 카테고리 + 중요도 분류"""
    combined = report_nm + " " + rm
    for keyword, (cat, imp) in REPORT_CATEGORIES.items():
        if keyword in combined:
            # 정정 공시는 중요도 +1
            if "정정" in combined:
                imp = min(imp + 1, 5)
            return cat, imp
    return "other", 2


def fetch_dart_list(bgn_de: str, end_de: str, corp_cls: str = "Y", page: int = 1) -> list[dict]:
    """DART 공시 목록 조회"""
    params = {
        "crtfc_key": DART_API_KEY,
        "bgn_de": bgn_de,
        "end_de": end_de,
        "corp_cls": corp_cls,
        "page_no": page,
        "page_count": 100,
        "sort": "date",
        "sort_mth": "desc",
    }
    try:
        resp = requests.get(DART_API_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status", "")
        if status == "000":
            return data.get("list", [])
        elif status == "013":
            log.info(f"  DART 조회 결과 없음 ({corp_cls})")
            return []
        else:
            log.warning(f"  DART API 상태: {status} - {data.get('message', '')}")
            return []
    except Exception as e:
        log.error(f"DART API 실패: {e}")
        return []


def save_to_clickhouse(disclosures: list[dict]) -> int:
    """ClickHouse에 공시 저장"""
    if not disclosures:
        return 0

    def esc(s):
        return (s or "").replace("\\", "\\\\").replace("'", "\\'")

    values = []
    for d in disclosures:
        values.append(
            f"('{d['rcept_dt']}', '{esc(d['rcept_no'])}', '{esc(d['corp_code'])}', "
            f"'{esc(d['corp_name'])}', '{esc(d['stock_code'])}', '{esc(d['report_nm'])}', "
            f"'{esc(d['corp_cls'])}', '{esc(d['flr_nm'])}', '{esc(d['rm'])}', "
            f"{d['importance']}, '{esc(d['category'])}', now())"
        )

    sql = (
        "INSERT INTO trading.dart_disclosure "
        "(rcept_dt, rcept_no, corp_code, corp_name, stock_code, report_nm, "
        "corp_cls, flr_nm, rm, importance, category, collected_at) VALUES "
        + ",".join(values)
    )

    try:
        resp = requests.post(CLICKHOUSE_URL, data=sql.encode("utf-8"), timeout=15, auth=CLICKHOUSE_AUTH)
        resp.raise_for_status()
        return len(disclosures)
    except Exception as e:
        log.error(f"ClickHouse 저장 실패: {e}")
        return 0


def get_existing_rcept_nos(bgn_de: str) -> set:
    """이미 저장된 접수번호 조회 (중복 방지)"""
    q = f"SELECT rcept_no FROM trading.dart_disclosure WHERE rcept_dt >= '{bgn_de}'"
    try:
        resp = requests.get(CLICKHOUSE_URL, params={"query": q}, timeout=10, auth=CLICKHOUSE_AUTH)
        resp.raise_for_status()
        return set(line.strip() for line in resp.text.strip().splitlines() if line.strip())
    except Exception:
        return set()


def main():
    start = time.time()
    days = 3
    bgn_de = None

    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--days" and i < len(sys.argv) - 1:
            days = int(sys.argv[i + 1])
        elif arg == "--bgn_de" and i < len(sys.argv) - 1:
            bgn_de = sys.argv[i + 1]

    if not DART_API_KEY:
        log.error("=" * 60)
        log.error("DART_API_KEY 미설정!")
        log.error("발급: https://opendart.fss.or.kr/ → 인증키 신청 (무료)")
        log.error("설정: export DART_API_KEY='발급받은키'")
        log.error("=" * 60)
        sys.exit(1)

    if not bgn_de:
        bgn_de = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    end_de = datetime.now().strftime("%Y%m%d")

    log.info("=" * 60)
    log.info(f"DART 공시 수집: {bgn_de} ~ {end_de}")
    log.info("=" * 60)

    # 기존 접수번호 (중복 방지)
    existing = get_existing_rcept_nos(bgn_de[:4] + "-" + bgn_de[4:6] + "-" + bgn_de[6:8])
    log.info(f"  기존 DB: {len(existing)}건")

    all_disclosures = []

    # 유가증권(Y) + 코스닥(K) 수집
    for corp_cls, label in [("Y", "유가증권"), ("K", "코스닥")]:
        log.info(f"\n  [{label}] 수집 중...")
        page = 1
        total = 0

        while page <= 10:  # 최대 10페이지 (1000건)
            items = fetch_dart_list(bgn_de, end_de, corp_cls, page)
            if not items:
                break

            for item in items:
                rcept_no = item.get("rcept_no", "")
                if rcept_no in existing:
                    continue

                report_nm = item.get("report_nm", "")
                rm = item.get("rm", "")
                category, importance = classify_report(report_nm, rm)

                # stock_code 정리 (빈 값이면 빈 문자열)
                stock_code = item.get("stock_code", "")
                if stock_code == " " or not stock_code:
                    stock_code = ""

                # rcept_dt 포맷: YYYYMMDD → YYYY-MM-DD
                raw_dt = item.get("rcept_dt", "")
                if len(raw_dt) == 8:
                    rcept_dt = f"{raw_dt[:4]}-{raw_dt[4:6]}-{raw_dt[6:8]}"
                else:
                    rcept_dt = raw_dt

                disc = {
                    "rcept_dt": rcept_dt,
                    "rcept_no": rcept_no,
                    "corp_code": item.get("corp_code", ""),
                    "corp_name": item.get("corp_name", ""),
                    "stock_code": stock_code,
                    "report_nm": report_nm,
                    "corp_cls": corp_cls,
                    "flr_nm": item.get("flr_nm", ""),
                    "rm": rm,
                    "importance": importance,
                    "category": category,
                }
                all_disclosures.append(disc)
                total += 1

            log.info(f"    페이지 {page}: {len(items)}건 (신규: {total}건)")

            if len(items) < 100:
                break
            page += 1
            time.sleep(0.5)

    log.info("-" * 60)
    log.info(f"  수집 합계: {len(all_disclosures)}건")

    # 중요도별 통계
    by_importance = {}
    for d in all_disclosures:
        imp = d["importance"]
        by_importance[imp] = by_importance.get(imp, 0) + 1

    for imp in sorted(by_importance.keys(), reverse=True):
        log.info(f"  중요도 {imp}: {by_importance[imp]}건")

    # 카테고리별 통계
    by_category = {}
    for d in all_disclosures:
        cat = d["category"]
        by_category[cat] = by_category.get(cat, 0) + 1

    for cat, cnt in sorted(by_category.items(), key=lambda x: -x[1]):
        log.info(f"  카테고리 [{cat}]: {cnt}건")

    # 중요 공시 출력
    important = [d for d in all_disclosures if d["importance"] >= 4]
    if important:
        log.info("\n  === 주요 공시 ===")
        for d in important[:20]:
            log.info(
                f"  [{d['rcept_dt']}] {'★' * d['importance']} "
                f"{d['corp_name']} - {d['report_nm'][:50]} "
                f"({d['category']})"
            )

    # 저장
    if all_disclosures:
        inserted = save_to_clickhouse(all_disclosures)
        log.info(f"\n  ClickHouse 저장: {inserted}건")
    else:
        log.info("\n  저장할 공시 없음")

    elapsed = time.time() - start
    log.info("=" * 60)
    log.info(f"완료 ({elapsed:.1f}초)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
