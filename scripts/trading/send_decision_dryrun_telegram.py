#!/usr/bin/env python3
"""send_decision_dryrun_telegram.py

최신 decision_run 결과를 사람이 읽기 쉬운 한국어 요약으로 만들어
텔레그램으로 전송한다.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import html
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()


def _log(msg: str) -> None:
    print(f"{dt.datetime.now().strftime('%H:%M:%S')} [dryrun-report] {msg}", flush=True)


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


def _float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _parse_json_obj(v: Any) -> dict[str, Any]:
    if isinstance(v, dict):
        return v
    if not isinstance(v, str):
        return {}
    s = v.strip()
    if not s:
        return {}
    try:
        obj = json.loads(s)
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _to_eok_from_krw(v: float) -> float:
    return _float(v, 0.0) / 100_000_000.0


def _to_star(importance: float) -> str:
    n = max(1, min(5, int(round(_float(importance, 1.0)))))
    return "★" * n + "☆" * (5 - n)


def _state_label(state: str) -> str:
    s = (state or "").strip().lower()
    return {
        "emerging": "신규 확산",
        "reinforcing": "강화",
        "stable": "지속",
        "reversing": "반전 신호",
        "decaying": "약화",
    }.get(s, s or "미분류")


def _state_interpretation(state: str) -> str:
    s = (state or "").strip().lower()
    return {
        "emerging": "새로운 이슈가 시장에 확산되는 구간입니다.",
        "reinforcing": "기존 이슈가 재강화되며 가격 영향이 커지는 구간입니다.",
        "stable": "이미 반영된 이슈가 유지되는 구간입니다.",
        "reversing": "기존 방향이 꺾이는 초기 신호로, 추세 재확인이 필요합니다.",
        "decaying": "영향력이 줄어드는 소멸 구간입니다.",
    }.get(s, "상태 해석 정보가 부족합니다.")


def _theme_from_storyline(text: str) -> str:
    s = (text or "").lower()
    if any(k in s for k in ["관세", "무역", "대법원", "수출"]):
        return "대외무역/관세 리스크"
    if any(k in s for k in ["가계빚", "가계부채", "대출", "연체"]):
        return "가계부채/내수 부담"
    if any(k in s for k in ["공매도", "인버스", "포지션", "수급"]):
        return "수급 포지셔닝 변화"
    if any(k in s for k in ["ai", "반도체", "hbm", "메모리"]):
        return "AI/반도체 모멘텀"
    return "일반 매크로/섹터 이슈"


def _impact_sentence(state: str, importance: float, storyline: str) -> str:
    s = (state or "").lower()
    imp = _float(importance, 0.0)
    t = (storyline or "").lower()
    risk_kw = ["관세", "소송", "규제", "가계빚", "부채", "급락", "충격", "불안"]
    has_risk = any(k in t for k in risk_kw)
    if has_risk and imp >= 4:
        return "리스크 민감 업종 비중은 보수적으로 유지하는 편이 유리합니다."
    if s == "reversing":
        return "방향 전환 초입 가능성이 있어, 추격 매수보다 확인 후 대응이 적절합니다."
    if s in {"emerging", "reinforcing"} and imp >= 3:
        return "뉴스 확산 속도가 빨라 단기 변동성 확대를 전제로 대응해야 합니다."
    if s == "stable":
        return "새로운 재료보다는 기존 시나리오 재평가 관점으로 보는 것이 적절합니다."
    return "중립적으로 관찰하되, 후속 뉴스/수급 확인이 필요합니다."


def _shorten(s: str, max_len: int = 78) -> str:
    t = (s or "").strip()
    if len(t) <= max_len:
        return t
    return t[: max_len - 3] + "..."


def _html_link(url: str, label: str) -> str:
    u = (url or "").strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        return ""
    return f'<a href="{html.escape(u, quote=True)}">{html.escape(label)}</a>'


def _safe_ch_select(sql: str, timeout_sec: int = 30) -> list[dict]:
    try:
        return ch_select(sql, timeout_sec=timeout_sec)
    except Exception as e:
        _log(f"query 실패: {type(e).__name__}: {e}")
        return []


def _sql_quote(s: str) -> str:
    return "'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _sql_in_strings(items: list[str]) -> str:
    vals = [_sql_quote(x) for x in items if str(x).strip()]
    return "(" + ",".join(vals) + ")" if vals else "('')"


def _table_exists(table_name: str) -> bool:
    rows = _safe_ch_select(
        f"""
SELECT count() AS cnt
FROM system.tables
WHERE database='trading' AND name={_sql_quote(table_name)}
""",
        timeout_sec=15,
    )
    if not rows:
        return False
    try:
        return int(rows[0].get("cnt") or 0) > 0
    except Exception:
        return False


def _column_exists(table_name: str, column_name: str) -> bool:
    rows = _safe_ch_select(
        f"""
SELECT count() AS cnt
FROM system.columns
WHERE database='trading'
  AND table={_sql_quote(table_name)}
  AND name={_sql_quote(column_name)}
""",
        timeout_sec=15,
    )
    if not rows:
        return False
    return int(_float(rows[0].get("cnt"), 0.0)) > 0


_SECTOR_CACHE: dict[str, str] = {}


def _get_sector_by_kis(ticker: str) -> str:
    tk = str(ticker or "").strip()
    if not re.fullmatch(r"\d{6}", tk):
        return "-"
    if tk in _SECTOR_CACHE:
        return _SECTOR_CACHE[tk]

    try:
        cmd = [
            "mcporter",
            "call",
            f'kis-trading.inquery-stock-price(symbol: "{tk}")',
            "--output",
            "json",
        ]
        proc = subprocess.run(cmd, text=True, capture_output=True, timeout=8, check=False)
        if proc.returncode != 0:
            _SECTOR_CACHE[tk] = "-"
            return "-"
        obj = json.loads(proc.stdout or "{}")
        sector = str(obj.get("bstp_kor_isnm") or "-").strip() or "-"
        _SECTOR_CACHE[tk] = sector
        return sector
    except Exception:
        _SECTOR_CACHE[tk] = "-"
        return "-"


def _describe_action_hint(action: str, total_score: float, blocks: list[str], global_wait: bool = False) -> str:
    a = (action or "").upper()
    if blocks:
        return f"{a} 유지(제약 존재: {', '.join(blocks)})"
    if global_wait:
        if a == "BUY":
            return "관찰 후보(전역 보류 상태)"
        if a == "HOLD" and total_score >= 70:
            return "관찰 후보(전역 보류 상태)"
    if a == "BUY":
        return "신규매수 검토(분할 접근)"
    if a == "REDUCE":
        return "비중 축소/리스크 관리"
    if a == "HOLD":
        if total_score >= 70:
            return "신규매수 검토(근거 추가 확인 필요)"
        return "관찰(조건 일부 미달)"
    return "보유/관찰"


def _describe_technical_signal(rsi: float, vol_ratio: float, pct: float, bb_pct: float, rel_score: float) -> str:
    if rsi >= 70:
        rsi_txt = "RSI가 높아 단기 과열 신호가 존재"
    elif rsi >= 50:
        rsi_txt = "RSI가 중립~강세권으로 급격한 변동은 덜 뚜렷"
    elif rsi >= 35:
        rsi_txt = "RSI가 낮아 눌림 구간에서 반등 여지가 존재"
    else:
        rsi_txt = "RSI가 매우 낮아 추가 하락 방어가 필요"

    if vol_ratio >= 1.5:
        vol_txt = "거래량 동조가 큰 변화 구간"
    elif vol_ratio >= 1.0:
        vol_txt = "거래량은 보통보다 약간 높은 수준"
    else:
        vol_txt = "거래량이 약해 가격 신호 신뢰도가 떨어질 수 있음"

    if pct >= 1.5:
        price_txt = "최근 가격이 강하게 올라온 구간"
    elif pct <= -1.5:
        price_txt = "최근 가격이 꾸준히 눌리는 구간"
    else:
        price_txt = "가격 변동이 완만한 구간"

    if bb_pct > 1.05:
        bb_txt = "밴드 확장으로 변동성 확대"
    elif bb_pct < 0.85:
        bb_txt = "밴드 압축으로 변동성 둔화"
    else:
        bb_txt = "밴드 기준은 비교적 안정적"

    if rel_score >= 0.1:
        rel_txt = "클러스터 연계가 비교적 우호적"
    elif rel_score <= -0.05:
        rel_txt = "클러스터 연계 영향이 부정적으로 작동할 여지"
    else:
        rel_txt = "클러스터 연계는 중립"

    return f"{rsi_txt}, {vol_txt}, {price_txt}, {bb_txt}, {rel_txt}"


def _one_line_pick(signal_score: float, rsi: float, bb_pct: float, vol_ratio: float) -> str:
    reasons: list[str] = []
    if signal_score >= 3:
        reasons.append("기술점수 상위")
    elif signal_score >= 2:
        reasons.append("기술점수 양호")
    if 40 <= rsi <= 65:
        reasons.append("RSI 과열 아님")
    elif rsi < 40:
        reasons.append("저점권 반등 후보")
    if bb_pct < 1.0:
        reasons.append("볼린저 과열 아님")
    if vol_ratio >= 1.5:
        reasons.append("거래량 유입")
    return ", ".join(reasons) if reasons else "기술지표 종합상 상대 우위"


def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "")
    if not v:
        return default
    return v.strip().lower() in {"1", "true", "yes", "on", "y"}


def _parse_first_json_object(raw: str) -> dict[str, Any] | None:
    txt = (raw or "").strip()
    if not txt:
        return None
    try:
        obj = json.loads(txt)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    m = re.search(r"\{.*\}", txt, re.DOTALL)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _run_llm_summary(context: dict[str, Any], timeout_sec: int = 90) -> tuple[dict[str, Any] | None, str]:
    here = os.path.dirname(os.path.abspath(__file__))
    if here not in sys.path:
        sys.path.insert(0, here)

    try:
        from codex_exec_guard import run_codex_cached  # type: ignore
    except Exception as e:
        return None, f"import_failed:{type(e).__name__}:{e}"

    codex_bin = os.getenv("CODEX_BIN", os.getenv("OPENCLAW_BIN", "openclaw")).strip() or "openclaw"
    resolved = shutil.which(codex_bin) or codex_bin
    if not shutil.which(resolved) and not Path(resolved).exists():
        return None, f"codex_not_found:{resolved}"

    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "market_view": {"type": "string"},
            "flow_view": {"type": "string"},
            "overall_judgment": {"type": "string"},
            "trade_plan": {"type": "string"},
            "key_risks": {"type": "array", "items": {"type": "string"}},
            "candidate_notes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "ticker": {"type": "string"},
                        "name": {"type": "string"},
                        "view": {"type": "string"},
                    },
                    "required": ["ticker", "name", "view"],
                },
            },
            "news_linkage": {"type": "string"},
        },
        "required": [
            "market_view",
            "flow_view",
            "overall_judgment",
            "trade_plan",
            "key_risks",
            "candidate_notes",
            "news_linkage",
        ],
    }
    prompt = (
        "너는 한국 주식 트레이딩 리포트의 최종 해석 작성자다.\n"
        "아래 JSON 지표를 근거로만 해석하라. 없는 사실/수치/뉴스를 만들지 마라.\n"
        "숫자 단위/자릿수는 입력값을 변경하지 마라. 특히 금액 단위(억/KRW)와 비율(%)은 재계산/재포맷 금지.\n"
        "LLM 해석 섹션에서는 숫자를 재서술하지 말고, 방향성/제약/우선순위만 정성적으로 작성하라.\n"
        "macro_top3(글로벌 지정학/금리·달러/국내 수급)를 해석의 기준 축으로 반드시 반영하라.\n"
        "각 필드는 핵심만 간결히 작성하라(항목당 1~2문장, 불필요한 반복 금지).\n"
        "출력은 JSON만 반환한다.\n\n"
        "[INPUT_JSON]\n"
        + json.dumps(context, ensure_ascii=False, indent=2)
    )
    schema_path = ""
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as sf:
            schema_path = sf.name
            json.dump(schema, sf, ensure_ascii=False)

        raw = run_codex_cached(
            prompt=prompt,
            codex_bin=resolved,
            model=os.getenv("CODEX_MODEL", "openai-codex/gpt-5.3-codex-spark"),
            workdir=None,
            timeout_sec=max(30, int(timeout_sec)),
            base_args=["--skip-git-repo-check", "--full-auto"],
            output_schema_path=schema_path,
            cache_dir=os.getenv("CODEX_EXEC_CACHE_DIR", os.path.expanduser("~/.openclaw/cache/codex-exec")),
            cache_ttl_sec=int(os.getenv("STOCK_REPORT_CODEX_CACHE_TTL", os.getenv("CODEX_EXEC_CACHE_TTL", "180"))),
            cache_lock_wait_sec=int(
                os.getenv("STOCK_REPORT_CODEX_CACHE_LOCK_WAIT", os.getenv("CODEX_EXEC_CACHE_LOCK_WAIT", "20"))
            ),
        )
        obj = _parse_first_json_object(raw)
        if not obj:
            return None, "llm_json_parse_failed"
        return obj, ""
    except Exception as e:
        return None, f"llm_error:{type(e).__name__}:{e}"
    finally:
        if schema_path:
            try:
                Path(schema_path).unlink(missing_ok=True)
            except Exception:
                pass


def _trade_interpretation(action: str, score: float, state: str, imp: float) -> str:
    a = (action or "").upper()
    if a == "BUY":
        return f"매수 후보입니다. 다만 분할 진입 원칙으로 접근하는 것이 좋습니다. (총점 {score:.1f})"
    if a == "REDUCE":
        return f"리스크 관리 구간입니다. 보유 비중 축소 또는 방어적 대응이 우선입니다. (총점 {score:.1f})"
    if a == "HOLD":
        return f"즉시 진입보다 관찰 우선입니다. 수급/추세 재확인 뒤 판단하는 편이 적절합니다. (총점 {score:.1f})"
    s = (state or "").lower()
    if s == "reversing" and imp >= 3:
        return "반전 신호가 있어 신규 진입은 보수적으로 접근하는 편이 유리합니다."
    if s in {"emerging", "reinforcing"} and imp >= 4:
        return "모멘텀 후보군으로 모니터링하되, 변동성 확대를 감안해 분할 접근이 적절합니다."
    return "중립 관찰 구간으로, 추가 데이터 확인 후 매매 판단이 필요합니다."


def _format_flow_lines(rows: list[dict]) -> list[str]:
    inv = {"foreign": "외국인", "institution": "기관", "individual": "개인"}
    out: list[str] = []
    for r in rows:
        net_amount = _float(r.get("net_amount"), 0.0)
        eok = net_amount / 100.0
        sign = "+" if eok >= 0 else ""
        out.append(f"- {r.get('market')} {inv.get(str(r.get('investor_type')), str(r.get('investor_type')))}: {sign}{eok:,.0f}억")
    return out


def _has_meaningful_flow(rows: list[dict]) -> bool:
    if not rows:
        return False
    for r in rows:
        if abs(_float(r.get("net_amount"), 0.0)) > 0:
            return True
    return False


def _market_direction_interpretation(
    kospi_pct: float,
    kosdaq_pct: float,
    flow_rows: list[dict],
    stage2_debug: dict[str, Any] | None = None,
) -> list[str]:
    flows: dict[tuple[str, str], float] = {}
    by_investor: dict[str, float] = {"foreign": 0.0, "institution": 0.0, "individual": 0.0}
    by_market: dict[str, float] = {"KOSPI": 0.0, "KOSDAQ": 0.0}
    has_flow = _has_meaningful_flow(flow_rows)

    for r in flow_rows:
        market = str(r.get("market") or "").upper()
        inv = str(r.get("investor_type") or "").lower()
        eok = _float(r.get("net_amount"), 0.0) / 100.0
        flows[(market, inv)] = eok
        if inv in by_investor:
            by_investor[inv] += eok
        if market in by_market:
            by_market[market] += eok

    lines: list[str] = []

    if kospi_pct < 0 < kosdaq_pct:
        lines.append("- 지수 해석: 대형주 중심 코스피 약세, 코스닥 상대강세로 종목장 성격이 나타납니다.")
    elif kospi_pct > 0 and kosdaq_pct > 0:
        lines.append("- 지수 해석: 코스피·코스닥 동반 상승으로 위험선호가 개선된 흐름입니다.")
    elif kospi_pct < 0 and kosdaq_pct < 0:
        lines.append("- 지수 해석: 양 시장 동반 약세로 단기 위험회피 성향이 우세합니다.")
    else:
        lines.append("- 지수 해석: 양 시장 방향이 엇갈려 업종/테마별 차별화가 큰 장세입니다.")

    if has_flow:
        foreign_total = by_investor["foreign"]
        inst_total = by_investor["institution"]
        indiv_total = by_investor["individual"]
        if foreign_total < 0 and indiv_total > 0:
            lines.append(
                f"- 수급 해석: 외국인 순매도({foreign_total:,.0f}억)를 개인 순매수({indiv_total:,.0f}억)가 받는 역방향 수급입니다."
            )
        elif foreign_total > 0 and inst_total > 0:
            lines.append(
                f"- 수급 해석: 외국인·기관 동반 순매수(외국인 {foreign_total:,.0f}억, 기관 {inst_total:,.0f}억)로 위험자산 선호 신호입니다."
            )
        else:
            lines.append(
                f"- 수급 해석: 외국인 {foreign_total:,.0f}억 / 기관 {inst_total:,.0f}억 / 개인 {indiv_total:,.0f}억으로 혼조세입니다."
            )
    else:
        s2 = stage2_debug or {}
        f5 = _to_eok_from_krw(_float(s2.get("foreign_net_krw_5d"), 0.0))
        i5 = _to_eok_from_krw(_float(s2.get("inst_net_krw_5d"), 0.0))
        lines.append(
            f"- 수급 해석: 당일 장마감 수급 데이터가 아직 없어 5영업일 기준으로 해석합니다(외국인 {f5:+,.0f}억 / 기관 {i5:+,.0f}억)."
        )

    kf = flows.get(("KOSPI", "foreign"), 0.0)
    qf = flows.get(("KOSDAQ", "foreign"), 0.0)
    if has_flow:
        if kf < 0 and qf > 0:
            lines.append(
                f"- 시장별 해석: 외국인은 코스피({kf:,.0f}억) 매도, 코스닥({qf:,.0f}억) 매수로 대형주보다 중소형/테마주 선호가 상대적으로 강합니다."
            )
        elif kf < 0 and qf <= 0:
            lines.append("- 시장별 해석: 외국인 자금이 양 시장 모두에서 유출되어 방어적 대응이 필요합니다.")
        else:
            lines.append("- 시장별 해석: 외국인 수급은 시장 간 방향성이 뚜렷하지 않아 추가 확인이 필요합니다.")
    else:
        lines.append("- 시장별 해석: 당일 시장별 수급 데이터 미수집으로 세부 시장 비교 해석은 보류합니다.")

    return lines


def _verdict_text(run: dict) -> str:
    blocks = run.get("absolute_block_reason") or []
    if blocks:
        return "신규 매수 보류"
    if _float(run.get("total_score"), 0.0) >= 70:
        return "조건부 신규 매수 가능"
    if _float(run.get("total_score"), 0.0) <= 35:
        return "비중 축소/리스크 관리 우선"
    return "관망"


def _overall_reason_text(run: dict) -> str:
    total = _float(run.get("total_score"), 0.0)
    s1 = _float(run.get("stage1_score"), 0.0)
    s2 = _float(run.get("stage2_score"), 0.0)
    s3 = _float(run.get("stage3_score"), 0.0)
    s4 = _float(run.get("stage4_score"), 0.0)
    s5 = _float(run.get("stage5_score"), 0.0)
    blocks = [str(x) for x in (run.get("absolute_block_reason") or [])]

    reasons: list[str] = []
    if blocks:
        reasons.append(f"절대 블록({', '.join(blocks)})이 있어 신규 매수를 제한합니다")
    if total < 70:
        reasons.append(f"총점 {total:.2f}로 매수 기준(70점) 미달입니다")
    if s2 < 55:
        reasons.append(f"수급 점수({s2:.1f})가 약해 추세 확신이 부족합니다")
    if s5 < 60:
        reasons.append(f"리스크/집행 점수({s5:.1f})가 낮아 공격적 진입이 어렵습니다")

    # 보조 설명(강한 항목도 함께 보여 균형감 제공)
    strong = []
    if s1 >= 70:
        strong.append(f"레짐 {s1:.1f}")
    if s3 >= 60:
        strong.append(f"뉴스 {s3:.1f}")
    if s4 >= 70:
        strong.append(f"타이밍 {s4:.1f}")
    if strong:
        reasons.append("강점은 " + ", ".join(strong) + "이나, 상기 제약이 우선 적용됩니다")

    if not reasons:
        reasons.append("핵심 스테이지가 혼조라 방향성 확인이 더 필요합니다")
    return ". ".join(reasons) + "."


def _global_wait_mode(run: dict) -> bool:
    verdict = _verdict_text(run)
    return verdict in {"신규 매수 보류", "관망", "비중 축소/리스크 관리 우선"}


def _trust_label(coverage_pct: float, feature_age_min: float) -> tuple[str, str]:
    cov = _float(coverage_pct, 0.0)
    age = _float(feature_age_min, 99999.0)
    if cov < 50.0 or age > 120.0:
        return "낮음", f"feature_snapshot_coverage_pct={cov:.1f}%, liquidity_snapshot_age_minutes={int(age):,}"
    if cov < 80.0 or age > 60.0:
        return "보통", f"feature_snapshot_coverage_pct={cov:.1f}%, liquidity_snapshot_age_minutes={int(age):,}"
    return "높음", f"feature_snapshot_coverage_pct={cov:.1f}%, liquidity_snapshot_age_minutes={int(age):,}"


def _llm_rule_alignment(overall_judgment: str, rule_verdict: str) -> tuple[str, str]:
    txt = str(overall_judgment or "").lower()
    wait_words = ("관망", "보류", "대기", "신규 매수 없이", "진입 보류")
    buy_words = ("매수", "진입", "공격적", "추가 매수")
    llm_wait = any(w in txt for w in wait_words)
    llm_buy = any(w in txt for w in buy_words)

    rule_wait = rule_verdict in {"신규 매수 보류", "관망", "비중 축소/리스크 관리 우선"}
    if rule_wait and llm_buy and not llm_wait:
        return "부분충돌", "LLM이 매수 뉘앙스를 제시했으나 룰 기준으로 보수적 액션을 적용"
    if (not rule_wait) and llm_wait:
        return "부분충돌", "LLM이 보수적으로 해석했으며 룰 액션은 유지"
    return "정합", "룰 결론과 LLM 해석의 방향이 일치"


def _block_reason_text(codes: list[str]) -> str:
    m = {
        "STAGE0_FAIL": "데이터 품질 게이트 미통과",
        "LOW_LIQUIDITY_REAL": "유동성 제약(실제 거래대금 부족)",
        "MISSING_LIQUIDITY_SNAPSHOT": "유동성 스냅샷 결손(데이터 미수집/조인 실패)",
        "SPREAD_WIDE": "스프레드 과대",
        "SPREAD_TOO_WIDE": "스프레드 과대(절대 차단)",
        "HARD_RISK_OFF": "시장 하드 리스크오프",
        "FLOW_DENOM_INVALID": "수급 분모 검증 실패",
        "FLOW_DISTRIBUTION_BLOCK": "수급 분배(매도 우위) 신호",
        "FLOW_DISTRIBUTION_WARN": "수급 분배(매도 우위) 경고",
        "TECH_OVERHEAT_RSI": "기술적 과열(RSI) 신호",
        "DART_REDFLAG": "공시 레드플래그",
        "EVENT_REDFLAG": "뉴스 이벤트 레드플래그",
    }
    out: list[str] = []
    for c in codes or []:
        key = str(c)
        out.append(m.get(key, key))
    return ", ".join(out) if out else "추가 제약 없음"


def _candidate_news_reason(stage3_score: float, cluster_state: str, imp: float) -> str:
    if stage3_score >= 80:
        base = "중요도 높은 이벤트가 누적되어 뉴스 점수가 매우 강합니다."
    elif stage3_score >= 70:
        base = "핵심 뉴스 이벤트가 다수 반영되어 뉴스 점수가 높은 편입니다."
    elif stage3_score >= 60:
        base = "단기 모멘텀 뉴스가 유지되어 뉴스 점수가 양호합니다."
    else:
        base = "뉴스 점수는 중립 수준으로, 후속 기사 확인이 필요합니다."
    state_txt = _state_label(cluster_state)
    return f"{base} (클러스터 상태: {state_txt}, 중요도: {_to_star(imp)})"


def _candidate_timing_reason(tech: dict, stage4_score: float) -> str:
    close = _float(tech.get("close_price"), 0.0)
    ma20 = _float(tech.get("ma20"), 0.0)
    ma60 = _float(tech.get("ma60"), 0.0)
    rsi = _float(tech.get("rsi14"), 50.0)
    vol_ratio = _float(tech.get("vol_ratio"), 1.0)
    signal_score = _float(tech.get("signal_score"), 0.0)

    parts: list[str] = [f"타이밍 점수 {stage4_score:.1f}."]
    if close > ma20 > ma60 > 0:
        parts.append("가격이 MA20·MA60 위에 있어 상승 추세 구조입니다.")
    elif ma20 > ma60 > 0:
        parts.append("중기 추세는 상방이나 가격 위치 확인이 추가로 필요합니다.")
    else:
        parts.append("추세 구조가 강하지 않아 신중 접근이 필요합니다.")

    if 45 <= rsi <= 65:
        parts.append(f"RSI({rsi:.1f})가 과열 구간이 아니라 진입 부담이 낮습니다.")
    elif rsi > 70:
        parts.append(f"RSI({rsi:.1f}) 과열로 추격 진입은 비보수적입니다.")
    else:
        parts.append(f"RSI({rsi:.1f}) 기준 모멘텀은 보통 수준입니다.")

    if vol_ratio >= 1.2:
        parts.append(f"거래량 배수({vol_ratio:.2f})가 높아 추세 확인이 강합니다.")
    elif vol_ratio >= 1.0:
        parts.append(f"거래량 배수({vol_ratio:.2f})가 기준 이상으로 추세를 보조합니다.")
    else:
        parts.append(f"거래량 배수({vol_ratio:.2f})가 낮아 신호 신뢰도는 제한적입니다.")

    if signal_score >= 3:
        parts.append(f"기술 신호 점수({signal_score:.1f})도 강한 편입니다.")
    return " ".join(parts)


def _summarize_stage5_failures(cands: list[dict[str, Any]]) -> tuple[dict[str, int], dict[str, int]]:
    counts: dict[str, int] = {}
    exec_zero: dict[str, int] = {}
    for c in cands:
        raw_codes = c.get("stage5_fail_codes")
        codes: list[str] = []
        exec_mult = _float(c.get("stage5_exec_multiplier"), 1.0)
        if isinstance(raw_codes, list):
            codes = [str(x) for x in raw_codes if str(x).strip()]
        if not codes:
            # 하위 호환: 절대 블록에서 Stage5 관련 코드만 추출
            for x in c.get("absolute_block_reason") or []:
                sx = str(x)
                if sx in {"LOW_LIQUIDITY", "LOW_LIQUIDITY_REAL", "MISSING_LIQUIDITY_SNAPSHOT", "SPREAD_WIDE", "SPREAD_TOO_WIDE"}:
                    codes.append(sx)
        for code in set(codes):
            counts[code] = counts.get(code, 0) + 1
            if exec_mult <= 0.0:
                exec_zero[code] = exec_zero.get(code, 0) + 1
    return counts, exec_zero


def _stage5_seed_text(stage_debug: dict[str, Any]) -> str:
    stage5 = stage_debug.get("stage5") if isinstance(stage_debug.get("stage5"), dict) else {}
    policy = stage5.get("policy") if isinstance(stage5.get("policy"), dict) else {}
    tiers = policy.get("liquidity_tiers_krw") if isinstance(policy.get("liquidity_tiers_krw"), dict) else {}
    spread = policy.get("spread_bp") if isinstance(policy.get("spread_bp"), dict) else {}
    g50 = _to_eok_from_krw(_float(tiers.get("gte_50eok"), 0.0))
    g10 = _to_eok_from_krw(_float(tiers.get("gte_10eok"), 0.0))
    g5 = _to_eok_from_krw(_float(tiers.get("gte_5eok"), 0.0))
    g3 = _to_eok_from_krw(_float(tiers.get("gte_3eok"), 0.0))
    warn = _float(spread.get("warn"), 50.0)
    block = _float(spread.get("abs_block"), 80.0)
    if g50 <= 0 or g10 <= 0 or g5 <= 0 or g3 <= 0:
        return "- Stage5 기준(Seed): 유동성 tier(50억/10억/5억/3억), 스프레드 경고 50bp·차단 80bp"
    return (
        "- Stage5 기준(Seed): "
        f"유동성 tier >= {g50:.0f}억(100) / >= {g10:.0f}억(80) / >= {g5:.0f}억(60) / >= {g3:.0f}억(40), "
        f"< {g3:.0f}억은 LOW_LIQUIDITY_REAL 차단, 스프레드 {warn:.0f}bp 경고·{block:.0f}bp 차단"
    )


def _feature_snapshot_health(tickers: list[str]) -> dict[str, float]:
    uniq = sorted({str(t).strip() for t in tickers if re.fullmatch(r"\d{6}", str(t or "").strip())})
    if not uniq or not _table_exists("feature_snapshot"):
        return {"coverage_pct": 0.0, "covered": 0.0, "total": float(len(uniq)), "age_min": 99999.0}
    in_expr = _sql_in_strings(uniq)
    rows = _safe_ch_select(
        f"""
SELECT
    count() AS covered_cnt,
    if(count()=0, 99999, greatest(dateDiff('minute', max(last_ts), now()), 0)) AS age_min
FROM
(
    SELECT
        symbol,
        if(regular_liq > 0, last_regular_ts, last_any_ts) AS last_ts,
        if(
          regular_liq > 0,
          regular_liq,
          any_liq
        ) AS liq
    FROM
    (
        SELECT
            symbol,
            maxIf(ts, session='REGULAR') AS last_regular_ts,
            max(ts) AS last_any_ts,
            argMaxIf(liquidity_krw, ts, session='REGULAR') AS regular_liq,
            argMax(liquidity_krw, ts) AS any_liq
        FROM trading.feature_snapshot
        WHERE symbol IN {in_expr}
          AND ts >= now() - INTERVAL 5 DAY
        GROUP BY symbol
    )
    HAVING liq > 0
)
"""
    )
    covered = _float((rows[0] if rows else {}).get("covered_cnt"), 0.0)
    age_min = _float((rows[0] if rows else {}).get("age_min"), 99999.0)
    total = float(len(uniq))
    coverage_pct = (100.0 * covered / total) if total > 0 else 0.0
    return {"coverage_pct": round(coverage_pct, 2), "covered": covered, "total": total, "age_min": age_min}


def _macro_top3_lines(
    stage1_debug: dict[str, Any],
    stage2_debug: dict[str, Any],
    flow_rows: list[dict[str, Any]],
    dedup_clusters: list[dict[str, Any]],
) -> list[str]:
    geo_line = "최근 클러스터 기준 초대형 지정학 이벤트 직접 매핑은 제한적입니다."
    geo_keywords = ["이란", "중동", "전쟁", "공습", "분쟁", "관세", "제재", "원유", "호르무즈"]
    geo_pick: dict[str, Any] | None = None
    for c in dedup_clusters:
        st = str(c.get("storyline") or "")
        if any(k in st for k in geo_keywords):
            geo_pick = c
            break
    if geo_pick:
        geo_line = f"{_shorten(str(geo_pick.get('storyline') or ''), 80)} (상태: {_state_label(str(geo_pick.get('state_label') or ''))}, 중요도: {_to_star(_float(geo_pick.get('importance_max'), 1.0))})"

    fx_rows = _safe_ch_select(
        """
SELECT currency_pair, close_rate, date
FROM trading.exchange_rate
WHERE currency_pair IN ('USDKRW')
ORDER BY date DESC
LIMIT 1
"""
    )
    rt_rows = _safe_ch_select(
        """
SELECT rate_code, rate_value, date
FROM trading.interest_rate
WHERE rate_code IN ('KR_TB10Y','KR_TB3Y','BOK_BASE')
ORDER BY date DESC
LIMIT 20
"""
    )
    usdkrw = _float((fx_rows[0] if fx_rows else {}).get("close_rate"), 0.0)
    tb10 = 0.0
    bok = 0.0
    for r in rt_rows:
        code = str(r.get("rate_code") or "")
        if code == "KR_TB10Y" and tb10 <= 0:
            tb10 = _float(r.get("rate_value"), 0.0)
        if code == "BOK_BASE" and bok <= 0:
            bok = _float(r.get("rate_value"), 0.0)
    posture = str(stage1_debug.get("action_posture") or "normal")
    stress_flags = str(stage1_debug.get("stress_flags") or "").strip()
    rates_line = f"USDKRW {usdkrw:,.2f}, KR10Y {tb10:.2f}% / 기준금리 {bok:.2f}% (posture: {posture})"
    if stress_flags:
        rates_line += f", stress={stress_flags}"

    has_flow = _has_meaningful_flow(flow_rows)
    by_inv = {"foreign": 0.0, "institution": 0.0, "individual": 0.0}
    if has_flow:
        for r in flow_rows:
            inv = str(r.get("investor_type") or "").lower()
            by_inv[inv] = by_inv.get(inv, 0.0) + (_float(r.get("net_amount"), 0.0) / 100.0)
    shock = str(stage2_debug.get("shock_level") or "UNKNOWN")
    f5 = _to_eok_from_krw(_float(stage2_debug.get("foreign_net_krw_5d"), 0.0))
    i5 = _to_eok_from_krw(_float(stage2_debug.get("inst_net_krw_5d"), 0.0))
    if has_flow:
        flow_line = (
            f"외국인 5영업일 {f5:+,.0f}억 / 기관 {i5:+,.0f}억, "
            f"당일(참고) 외국인 {by_inv.get('foreign', 0.0):+,.0f}억·기관 {by_inv.get('institution', 0.0):+,.0f}억·개인 {by_inv.get('individual', 0.0):+,.0f}억 "
            f"(Stage2={shock})"
        )
    else:
        flow_line = (
            f"외국인 5영업일 {f5:+,.0f}억 / 기관 {i5:+,.0f}억, 당일(참고) 장마감 수급 미수집 "
            f"(Stage2={shock})"
        )

    return [geo_line, rates_line, flow_line]


def build_message(decision_id: str, top_n: int, clusters_n: int) -> str:
    run_rows = _safe_ch_select(
        f"""
SELECT
    decision_id, decision_time, stage0_pass, stage0_score,
    stage1_pass, stage1_score, stage2_pass, stage2_score,
    stage3_pass, stage3_score, stage4_pass, stage4_score,
    stage5_pass, stage5_score, total_score, absolute_block_reason,
    data_freshness_json, stage_debug_json
FROM trading.decision_run
WHERE decision_id = '{decision_id}'
ORDER BY decision_time DESC
LIMIT 1
"""
    )
    if not run_rows:
        run_rows = _safe_ch_select(
            f"""
SELECT
    decision_id, decision_time, stage0_pass, stage0_score,
    stage1_pass, stage1_score, stage2_pass, stage2_score,
    stage3_pass, stage3_score, stage4_pass, stage4_score,
    stage5_pass, stage5_score, total_score, absolute_block_reason,
    data_freshness_json
FROM trading.decision_run
WHERE decision_id = '{decision_id}'
ORDER BY decision_time DESC
LIMIT 1
"""
        )
    if not run_rows:
        raise RuntimeError(f"decision_id not found: {decision_id}")
    run = run_rows[0]
    stage_debug = _parse_json_obj(run.get("stage_debug_json"))
    mode_debug = stage_debug.get("mode") if isinstance(stage_debug.get("mode"), dict) else {}
    stage2_debug = stage_debug.get("stage2") if isinstance(stage_debug.get("stage2"), dict) else {}

    idx_rows = ch_select(
        """
SELECT
    index_code,
    any(index_name) AS index_name,
    argMax(close_price, date) AS close_price,
    argMax(change_pct, date) AS change_pct,
    max(date) AS dt
FROM trading.market_index
WHERE index_code IN ('KOSPI', 'KOSDAQ')
GROUP BY index_code
ORDER BY index_code
"""
    )
    idx_map = {str(r.get("index_code")): r for r in idx_rows}

    flow_rows = _safe_ch_select(
        """
SELECT date, market, investor_type, net_amount
FROM trading.investor_flow
WHERE date = (SELECT max(date) FROM trading.investor_flow)
  AND market IN ('KOSPI', 'KOSDAQ')
ORDER BY market, investor_type
"""
    )

    liq_src_select = "any(c.liquidity_source) AS liquidity_source" if _column_exists("decision_candidate", "liquidity_source") else "'' AS liquidity_source"
    cand_rows_all = _safe_ch_select(
        f"""
SELECT
    c.ticker,
    any(t.ticker_name) AS ticker_name,
    c.action,
    c.total_score,
    c.stage2_stock_flow_score,
    c.stage3_event_score,
    c.stage4_timing_score,
    c.stage5_risk_score,
    c.absolute_block_reason,
    c.stage5_fail_codes,
    c.stage5_exec_multiplier,
    {liq_src_select},
    c.stage3_evidence_count,
    c.stage3_score_capped,
    c.primary_cluster_id
FROM trading.decision_candidate c
LEFT JOIN trading.technical_signals t ON c.ticker = t.ticker
WHERE c.decision_id = '{decision_id}'
GROUP BY
    c.ticker, c.action, c.total_score,
    c.stage2_stock_flow_score, c.stage3_event_score, c.stage4_timing_score, c.stage5_risk_score,
    c.absolute_block_reason, c.stage5_fail_codes, c.stage5_exec_multiplier, c.stage3_evidence_count, c.stage3_score_capped,
    c.primary_cluster_id
ORDER BY c.total_score DESC
LIMIT {int(max(top_n * 6, 60))}
"""
    )
    if not cand_rows_all:
        cand_rows_all = _safe_ch_select(
        f"""
SELECT
    c.ticker,
    any(t.ticker_name) AS ticker_name,
    c.action,
    c.total_score,
    c.stage2_stock_flow_score,
    c.stage3_event_score,
    c.stage4_timing_score,
    c.stage5_risk_score,
    c.absolute_block_reason,
    c.primary_cluster_id
FROM trading.decision_candidate c
LEFT JOIN trading.technical_signals t ON c.ticker = t.ticker
WHERE c.decision_id = '{decision_id}'
GROUP BY
    c.ticker, c.action, c.total_score,
    c.stage2_stock_flow_score, c.stage3_event_score, c.stage4_timing_score, c.stage5_risk_score,
    c.absolute_block_reason, c.primary_cluster_id
ORDER BY c.total_score DESC
LIMIT {int(max(top_n * 6, 60))}
"""
        )
    cand_rows = cand_rows_all[: max(1, int(top_n))]
    candidate_tickers = {
        str(r.get("ticker") or "")
        for r in cand_rows
        if re.fullmatch(r"\d{6}", str(r.get("ticker") or ""))
    }
    candidate_metric_map: dict[str, dict[str, float]] = {}
    if candidate_tickers:
        in_sql = _sql_in_strings(sorted(candidate_tickers))
        metric_rows = _safe_ch_select(
            f"""
WITH
  news_agg AS (
    SELECT
      arrayJoin(tickers) AS ticker,
      countIf(sentiment='positive') AS pos,
      countIf(sentiment='negative') AS neg,
      count() AS news_cnt
    FROM trading.news
    WHERE published_at >= now() - INTERVAL 3 DAY
    GROUP BY ticker
  ),
  frame_agg AS (
    SELECT
      arrayJoin(tickers) AS ticker,
      countIf(relevant=1 AND thesis_path!='' AND evidence_json!='[]') AS explain_ready_3d
    FROM trading.news_event_frames
    WHERE published_at >= now() - INTERVAL 3 DAY
    GROUP BY ticker
  )
SELECT
  coalesce(n.ticker, f.ticker) AS ticker,
  ifNull(n.pos, 0) AS pos,
  ifNull(n.neg, 0) AS neg,
  ifNull(n.news_cnt, 0) AS news_cnt,
  ifNull(f.explain_ready_3d, 0) AS explain_ready_3d
FROM news_agg n
FULL OUTER JOIN frame_agg f ON n.ticker = f.ticker
WHERE coalesce(n.ticker, f.ticker) IN {in_sql}
"""
        )
        candidate_metric_map = {
            str(r.get("ticker") or ""): {
                "pos": _float(r.get("pos"), 0.0),
                "neg": _float(r.get("neg"), 0.0),
                "news_cnt": _float(r.get("news_cnt"), 0.0),
                "explain_ready_3d": _float(r.get("explain_ready_3d"), 0.0),
                "rel_score": 0.0,
            }
            for r in metric_rows
        }

        if _table_exists("hidden_relation_signals"):
            rel_rows = _safe_ch_select(
                f"""
WITH latest_rel AS (SELECT max(asof_ts) AS ts FROM trading.hidden_relation_signals)
SELECT ticker, total_relation_score
FROM trading.hidden_relation_signals
WHERE asof_ts = (SELECT ts FROM latest_rel)
  AND ticker IN {in_sql}
"""
            )
            for r in rel_rows:
                tk = str(r.get("ticker") or "")
                if tk not in candidate_metric_map:
                    candidate_metric_map[tk] = {
                        "pos": 0.0,
                        "neg": 0.0,
                        "news_cnt": 0.0,
                        "explain_ready_3d": 0.0,
                        "rel_score": 0.0,
                    }
                candidate_metric_map[tk]["rel_score"] = _float(r.get("total_relation_score"), 0.0)

    cluster_rows = ch_select(
        f"""
SELECT
    cluster_id, state_label, importance_max, top_tickers, storyline, asof_ts
FROM trading.news_cluster_state
ORDER BY asof_ts DESC
LIMIT {int(max(clusters_n * 2, 6))}
"""
    )
    seen: set[str] = set()
    dedup_clusters: list[dict] = []
    for row in cluster_rows:
        cid = str(row.get("cluster_id", "") or "")
        if not cid or cid in seen:
            continue
        seen.add(cid)
        dedup_clusters.append(row)
        if len(dedup_clusters) >= clusters_n:
            break

    ticker_names: dict[str, str] = {}
    cluster_tickers: set[str] = set()
    for c in dedup_clusters:
        for t in c.get("top_tickers", []) or []:
            s = str(t)
            if re.fullmatch(r"\d{6}", s):
                cluster_tickers.add(s)
    if cluster_tickers:
        tickers_csv = ",".join(f"'{t}'" for t in sorted(cluster_tickers))
        name_rows = ch_select(
            f"""
SELECT ticker, any(ticker_name) AS ticker_name
FROM trading.technical_signals
WHERE ticker IN ({tickers_csv})
GROUP BY ticker
"""
        )
        ticker_names = {str(r.get("ticker")): str(r.get("ticker_name") or "") for r in name_rows}

    news_title_by_ticker: dict[str, str] = {}
    news_url_by_ticker: dict[str, str] = {}
    if cluster_tickers:
        tickers_csv = ",".join(f"'{t}'" for t in sorted(cluster_tickers))
        news_rows = ch_select(
            f"""
SELECT
    ticker,
    argMax(title, published_at) AS title,
    argMax(source_url, published_at) AS source_url
FROM
(
    SELECT
        arrayJoin(tickers) AS ticker,
        title,
        source_url,
        published_at
    FROM trading.news
    WHERE collected_at >= now() - INTERVAL 3 DAY
)
WHERE ticker IN ({tickers_csv})
GROUP BY ticker
"""
        )
        news_title_by_ticker = {str(r.get("ticker") or ""): str(r.get("title") or "") for r in news_rows}
        news_url_by_ticker = {str(r.get("ticker") or ""): str(r.get("source_url") or "") for r in news_rows}

    candidate_news_map: dict[str, list[dict[str, str]]] = {}
    if candidate_tickers:
        for tk in sorted(candidate_tickers):
            if not re.fullmatch(r"\d{6}", tk):
                continue
            rows = ch_select(
                f"""
SELECT title, source_url, sentiment, importance
FROM trading.news
WHERE collected_at >= now() - INTERVAL 3 DAY
  AND has(tickers, '{tk}')
ORDER BY importance DESC, published_at DESC
LIMIT 2
"""
            )
            items: list[dict[str, str]] = []
            seen_keys: set[str] = set()
            for r in rows:
                t = str(r.get("title") or "").strip()
                u = str(r.get("source_url") or "").strip()
                sent = str(r.get("sentiment") or "").strip()
                imp = str(r.get("importance") or "").strip()
                if not t and not u:
                    continue
                key = f"{t}|{u}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                items.append({"title": t, "url": u, "sentiment": sent, "importance": imp})
            if items:
                candidate_news_map[tk] = items

    tech_map: dict[str, dict] = {}
    if candidate_tickers:
        tickers_csv = ",".join(f"'{t}'" for t in sorted(candidate_tickers))
        tech_rows = ch_select(
            f"""
SELECT
    ticker,
    any(ticker_name) AS ticker_name,
    any(signal) AS signal,
    max(close_price) AS close_price,
    max(change_pct) AS change_pct,
    max(ma20) AS ma20,
    max(ma60) AS ma60,
    max(rsi14) AS rsi14,
    max(bb_pct) AS bb_pct,
    max(vol_ratio) AS vol_ratio,
    max(signal_score) AS signal_score
FROM trading.technical_signals
WHERE date = (SELECT max(date) FROM trading.technical_signals)
  AND ticker IN ({tickers_csv})
GROUP BY ticker
"""
        )
        tech_map = {str(r.get("ticker") or ""): r for r in tech_rows}

    cluster_meta_by_id: dict[str, dict] = {
        str(r.get("cluster_id") or ""): r for r in dedup_clusters if str(r.get("cluster_id") or "")
    }
    cand_cluster_ids = {
        str(r.get("primary_cluster_id") or "")
        for r in cand_rows_all
        if str(r.get("primary_cluster_id") or "")
    }
    missing_cluster_ids = [c for c in cand_cluster_ids if c and c not in cluster_meta_by_id]
    if missing_cluster_ids:
        ids_csv = ",".join(f"'{c}'" for c in missing_cluster_ids)
        cmeta_rows = ch_select(
            f"""
SELECT
    cluster_id,
    argMax(state_label, asof_ts) AS state_label,
    max(toFloat64(importance_max)) AS importance_max,
    argMax(storyline, asof_ts) AS storyline
FROM trading.news_cluster_state
WHERE cluster_id IN ({ids_csv})
GROUP BY cluster_id
"""
        )
        for r in cmeta_rows:
            cluster_meta_by_id[str(r.get("cluster_id") or "")] = r

    now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    k = idx_map.get("KOSPI", {})
    q = idx_map.get("KOSDAQ", {})
    kp = _float(k.get("change_pct"), 0.0)
    qp = _float(q.get("change_pct"), 0.0)
    kps = "+" if kp >= 0 else ""
    qps = "+" if qp >= 0 else ""

    lines: list[str] = []
    rule_verdict = _verdict_text(run)
    global_wait = _global_wait_mode(run)
    lines.append(f"📌 <b>매매 파이프라인 요약</b> ({now_str})")
    if _bool_env("DRYRUN_REPORT_SHOW_DECISION_ID", False):
        lines.append(f"- decision_id: <code>{decision_id}</code>")
    lines.append(f"- 종합판단: <b>{rule_verdict}</b> (총점 {_float(run.get('total_score')):.2f})")
    lines.append(
        "- Stage 점수: "
        f"S0 {_float(run.get('stage0_score')):.1f} / "
        f"S1 {_float(run.get('stage1_score')):.1f} / "
        f"S2 {_float(run.get('stage2_score')):.1f} / "
        f"S3 {_float(run.get('stage3_score')):.1f} / "
        f"S4 {_float(run.get('stage4_score')):.1f} / "
        f"S5 {_float(run.get('stage5_score')):.1f}"
    )
    lines.append("- Stage 설명: S0 데이터품질 / S1 시장레짐 / S2 수급(보조, EXTREME만 차단) / S3 뉴스·이벤트(보조) / S4 기술타이밍(보조) / S5 리스크·집행")
    universe_forced = bool(mode_debug.get("universe_forced_watchlist", True))
    stage2_extreme_only = bool(mode_debug.get("stage2_extreme_only_block", True))
    stage3_gate = bool(mode_debug.get("stage3_gate_enabled", False))
    stage4_gate = bool(mode_debug.get("stage4_gate_enabled", False))
    watchlist_source = str(mode_debug.get("watchlist_active_source") or "enrich_data")
    policy_parts = [
        f"universe={'watchlist-only' if universe_forced else 'configured'}",
        f"watchlist_source={watchlist_source}",
        f"stage2_block={'EXTREME-only' if stage2_extreme_only else 'score+shock'}",
        f"stage3_gate={'ON' if stage3_gate else 'OFF'}",
        f"stage4_gate={'ON' if stage4_gate else 'OFF'}",
    ]
    lines.append(f"- 운영 정책: {' / '.join(policy_parts)}")
    lines.append(f"- Absolute Block: {', '.join([str(x) for x in (run.get('absolute_block_reason') or [])]) or '-'}")
    stage5_fail_summary = (
        stage_debug.get("stage5", {}).get("fail_summary")
        if isinstance(stage_debug.get("stage5"), dict)
        else {}
    )
    stage5_exec_zero_summary = (
        stage_debug.get("stage5", {}).get("exec_zero_summary")
        if isinstance(stage_debug.get("stage5"), dict)
        else {}
    )
    if not isinstance(stage5_fail_summary, dict):
        stage5_fail_summary = {}
    if not isinstance(stage5_exec_zero_summary, dict):
        stage5_exec_zero_summary = {}
    if not stage5_fail_summary:
        stage5_fail_summary, stage5_exec_zero_summary = _summarize_stage5_failures(cand_rows_all)
    if stage5_fail_summary:
        ordered = sorted(stage5_fail_summary.items(), key=lambda x: (-int(x[1]), str(x[0])))
        parts: list[str] = []
        for code, v in ordered[:3]:
            z = int(stage5_exec_zero_summary.get(code, 0))
            if z > 0:
                parts.append(f"{code}:{int(v)}(exec=0:{z})")
            else:
                parts.append(f"{code}:{int(v)}")
        txt = " / ".join(parts)
        lines.append(f"- Stage5 제약 요약: {txt}")
    lines.append(_stage5_seed_text(stage_debug))
    stage0_debug = stage_debug.get("stage0") if isinstance(stage_debug.get("stage0"), dict) else {}
    freshness_map = stage0_debug.get("freshness_map") if isinstance(stage0_debug.get("freshness_map"), dict) else _parse_json_obj(run.get("data_freshness_json"))
    feature_age_min = _float((freshness_map or {}).get("feature_snapshot"), 99999.0)
    all_cand_tickers = [str(c.get("ticker") or "") for c in cand_rows_all if str(c.get("ticker") or "").strip()]
    fs_health = _feature_snapshot_health(all_cand_tickers)
    coverage_pct = _float(fs_health.get("coverage_pct"), 0.0)
    covered = int(_float(fs_health.get("covered"), 0.0))
    total = int(_float(fs_health.get("total"), 0.0))
    if _float(feature_age_min, 99999.0) >= 99999.0:
        feature_age_min = _float(fs_health.get("age_min"), 99999.0)
    lines.append(
        f"- 데이터 품질: liquidity_snapshot_age_minutes={int(feature_age_min):,}, "
        f"feature_snapshot_coverage_pct={coverage_pct:.1f}% ({covered}/{total})"
    )
    trust_label, trust_reason = _trust_label(coverage_pct=coverage_pct, feature_age_min=feature_age_min)
    lines.append("")
    lines.append("<b>요약(비개발자용)</b>")
    lines.append(f"- 오늘 결론(룰 기준): <b>{rule_verdict}</b>")
    if global_wait:
        lines.append("- 오늘 행동 3가지: ① 신규매수 보류 ② 관찰 후보 모니터링 ③ 실행제약/데이터 복구 우선")
    else:
        lines.append("- 오늘 행동 3가지: ① 상위 후보 분할 접근 ② 충격 신호 재확인 ③ 제약 없는 종목만 실행")
    lines.append(f"- 브리핑 신뢰도: <b>{trust_label}</b> ({trust_reason})")
    lines.append("")
    lines.append("<b>시장 방향</b>")
    lines.append(
        f"- KOSPI {_float(k.get('close_price')):,.2f} ({kps}{kp:.2f}%) [{k.get('dt','-')}] / "
        f"KOSDAQ {_float(q.get('close_price')):,.2f} ({qps}{qp:.2f}%) [{q.get('dt','-')}]"
    )
    if stage2_debug:
        s2_valid = bool(stage2_debug.get("valid"))
        s2_flags = stage2_debug.get("flags") if isinstance(stage2_debug.get("flags"), list) else []
        s2_source = str(stage2_debug.get("source") or "UNKNOWN")
        s2_uni = int(_float(stage2_debug.get("universe_n"), 0.0))
        s2_shock_level = str(stage2_debug.get("shock_level") or "UNKNOWN")
        s2_shock_abs = _float(stage2_debug.get("shock_abs_ratio_pct"), 0.0)
        thr = stage2_debug.get("shock_threshold_pct") if isinstance(stage2_debug.get("shock_threshold_pct"), dict) else {}
        pass_max = _float(thr.get("pass_max"), 3.0)
        warn_max = _float(thr.get("warn_max"), 8.0)
        denom_krw = max(
            _float(stage2_debug.get("foreign_traded_krw_5d"), 0.0),
            _float(stage2_debug.get("inst_traded_krw_5d"), 0.0),
        )
        denom_eok = _to_eok_from_krw(denom_krw)
        f_net = _to_eok_from_krw(_float(stage2_debug.get("foreign_net_krw_5d"), 0.0))
        i_net = _to_eok_from_krw(_float(stage2_debug.get("inst_net_krw_5d"), 0.0))
        f_pct = _float(stage2_debug.get("foreign_net_pct_turnover_5d"), 0.0)
        i_pct = _float(stage2_debug.get("inst_net_pct_turnover_5d"), 0.0)
        lines.append(
            f"- Stage2 기준 수급(5영업일): source={s2_source}, universe_n={s2_uni}, denom={denom_eok:,.0f}억"
        )
        lines.append(f"- 외국인 {f_net:+,.0f}억 (ratio={f_pct:+.2f}%)")
        lines.append(f"- 기관 {i_net:+,.0f}억 (ratio={i_pct:+.2f}%)")
        lines.append(
            f"- Stage2 충격 레벨: {s2_shock_level} "
            f"(|ratio|max={s2_shock_abs:.2f}%, PASS<= {pass_max:.1f} / WARN<= {warn_max:.1f} / ALERT> {warn_max:.1f})"
        )
        lines.append(f"- Stage2 분모 검증: {'PASS' if s2_valid else 'FAIL'} / flags: {', '.join([str(x) for x in s2_flags]) or '-'}")
    flow_has_data = _has_meaningful_flow(flow_rows)
    if flow_has_data:
        lines.append("- 장마감 수급(참고):")
        lines.extend(_format_flow_lines(flow_rows))
    else:
        lines.append("- 장마감 수급(참고): 데이터 미수집/유효값 없음(당일 장마감 수급 해석 보류)")
    lines.extend(_market_direction_interpretation(kp, qp, flow_rows, stage2_debug))
    lines.append("")
    lines.append("<b>🚀 유망주 요약</b>")
    if not cand_rows:
        lines.append("- 후보 데이터 없음")
    else:
        market_s2 = _float(stage2_debug.get("market_score"), _float(run.get("stage2_score"), 0.0))
        for i, c in enumerate(cand_rows, 1):
            nm = str(c.get("ticker_name") or c.get("ticker") or "")
            tk = str(c.get("ticker") or "")
            action = str(c.get("action") or "")
            total = _float(c.get("total_score"), 0.0)
            s2 = _float(c.get("stage2_stock_flow_score"), 0.0)
            s2_stock = max(0.0, s2 - market_s2)
            s3 = _float(c.get("stage3_event_score"), 0.0)
            s4 = _float(c.get("stage4_timing_score"), 0.0)
            s5 = _float(c.get("stage5_risk_score"), 0.0)
            blocks = [str(x) for x in (c.get("absolute_block_reason") or [])]
            stage5_codes_raw = c.get("stage5_fail_codes")
            stage5_codes = [str(x) for x in stage5_codes_raw] if isinstance(stage5_codes_raw, list) else []
            stage5_exec_mult = _float(c.get("stage5_exec_multiplier"), 1.0)
            liquidity_source = str(c.get("liquidity_source") or "").strip()
            stage3_evidence_count = int(_float(c.get("stage3_evidence_count"), 0.0))
            stage3_score_capped = int(_float(c.get("stage3_score_capped"), 0.0)) == 1
            cluster_id = str(c.get("primary_cluster_id") or "")

            tech = tech_map.get(tk, {})
            signal = str(tech.get("signal") or "").strip()
            pct = _float(tech.get("change_pct"), 0.0)
            close = _float(tech.get("close_price"), 0.0)
            ma20 = _float(tech.get("ma20"), 0.0)
            ma60 = _float(tech.get("ma60"), 0.0)
            rsi = _float(tech.get("rsi14"), 0.0)
            bb_pct = _float(tech.get("bb_pct"), 0.5)
            vol_ratio = _float(tech.get("vol_ratio"), 0.0)
            signal_score = _float(tech.get("signal_score"), 0.0)
            m = candidate_metric_map.get(
                tk,
                {"pos": 0.0, "neg": 0.0, "news_cnt": 0.0, "explain_ready_3d": 0.0, "rel_score": 0.0},
            )
            pos = int(m.get("pos", 0.0))
            neg = int(m.get("neg", 0.0))
            news_cnt = int(m.get("news_cnt", 0.0))
            explain_ready = int(m.get("explain_ready_3d", 0.0))
            rel_score = _float(m.get("rel_score"), 0.0)
            evidence_ready = max(stage3_evidence_count, explain_ready)
            sector = _get_sector_by_kis(tk)
            action_txt = _describe_action_hint(action, total, blocks, global_wait=global_wait)
            tech_txt = _describe_technical_signal(rsi=rsi, vol_ratio=vol_ratio, pct=pct, bb_pct=bb_pct, rel_score=rel_score)
            one_line = _one_line_pick(signal_score=signal_score, rsi=rsi, bb_pct=bb_pct, vol_ratio=vol_ratio)

            news_items = candidate_news_map.get(tk, [])
            is_fallback_only = evidence_ready <= 0
            if not news_items:
                fallback_title = news_title_by_ticker.get(tk) or "관련 뉴스 추출 데이터 없음"
                fallback_url = news_url_by_ticker.get(tk) or ""
                news_items = [{"title": fallback_title, "url": fallback_url, "sentiment": "-", "importance": "-"}]
                is_fallback_only = True

            lines.append(f"{i}) <b>{nm}({tk})</b>")
            lines.append(f"   - 업종: {sector}")
            lines.append(f"   - 판단: {action_txt} (파이프라인 {action}, 총점 {total:.1f})")
            lines.append(f"   - 해석: {tech_txt}")
            lines.append(
                f"   - 뉴스/근거: 호재 {pos}건, 악재 {neg}건, 총 이슈 {news_cnt}건, "
                f"근거 충족 {evidence_ready}건, 연관점수 {rel_score:+.3f}"
            )
            if stage3_score_capped:
                lines.append("   - Stage3 보정: 근거 미충족으로 뉴스 점수 상한(cap) 적용")
            lines.append(f"   - 한 줄 판단: {one_line}")
            lines.append(
                f"   - 점수 요약: 수급 {s2:.1f} (시장 {market_s2:.1f} + 종목 {s2_stock:.1f}) / "
                f"뉴스 {s3:.1f} / 타이밍 {s4:.1f} / 집행 {s5:.1f} / signal {signal or '-'}"
            )
            lines.append(
                f"   - 기술 지표: close {close:,.2f} / MA20 {ma20:,.2f} / MA60 {ma60:,.2f} / "
                f"RSI {rsi:.1f} / BB {bb_pct:.3f} / VOL {vol_ratio:.2f} / pct {pct:+.2f}%"
            )
            if news_items:
                for ni in news_items[:2]:
                    sent = str(ni.get("sentiment") or "?").strip()
                    imp = str(ni.get("importance") or "?").strip()
                    title = _shorten(str(ni.get("title") or ""), 92)
                    prefix = "참고뉴스" if is_fallback_only else "관련뉴스"
                    lines.append(f"   - {prefix}: [{sent}/{imp}] {title}")
                    lines.append(f"     링크: {str(ni.get('url') or '-').strip() or '-'}")
            else:
                lines.append("   - 관련뉴스: 없음")
            lines.append(
                f"   - 집행 제약: {', '.join(stage5_codes) if stage5_codes else '-'} / exec x{stage5_exec_mult:.2f}"
            )
            if liquidity_source:
                lines.append(f"   - 집행 데이터 소스: liquidity_source={liquidity_source}")
            lines.append(f"   - 제약 코드: {', '.join(blocks) if blocks else '-'}")
            lines.append(f"   - cluster_id: {cluster_id or '-'}")
            if i < len(cand_rows):
                lines.append("")

    lines.append("")
    lines.append("<b>주요 뉴스/연관 지표</b>")

    if not dedup_clusters:
        lines.append("- 클러스터 데이터가 부족합니다.")
    else:
        picked_clusters = dedup_clusters[: max(3, top_n)]
        candidate_ticker_set = {str(c.get("ticker") or "") for c in cand_rows_all}
        macro_themes = {"일반 매크로/섹터 이슈", "대외무역/관세 리스크", "가계부채/내수 부담"}
        for i, c in enumerate(picked_clusters, 1):
            cid = str(c.get("cluster_id") or "")
            state = str(c.get("state_label") or "")
            imp = _float(c.get("importance_max"), 1.0)
            storyline = str(c.get("storyline") or "")
            theme = _theme_from_storyline(storyline)

            tickers = [
                str(t)
                for t in (c.get("top_tickers", []) or [])
                if re.fullmatch(r"\d{6}", str(t))
            ][:5]
            if theme in macro_themes and tickers:
                strict_mapped = [tk for tk in tickers if tk in candidate_ticker_set]
                if strict_mapped:
                    tickers = strict_mapped
                else:
                    tickers = []
            related_names = []
            for tk in tickers:
                nm = ticker_names.get(tk, "")
                related_names.append(f"{nm}({tk})" if nm else tk)
            related_text = ", ".join(related_names) if related_names else (
                "직접 매핑 근거 부족(보수적 제외)" if theme in macro_themes else "관련 종목 매핑 없음"
            )

            major_news = ""
            for tk in tickers:
                t = (news_title_by_ticker.get(tk) or "").strip()
                if t:
                    major_news = t
                    break
            if not major_news:
                major_news = storyline or "관련 주요 뉴스 미확인"

            lines.append(f"{i}.")
            lines.append(f"- 주요뉴스: {_shorten(major_news, 92)}")
            lines.append(f"- 테마/상태: {theme} / {_state_label(state)}")
            lines.append(f"- 중요도: {_to_star(imp)}")
            lines.append(f"- cluster_id: {cid or '-'}")
            lines.append(f"- 연관된 종목 명: {related_text}")
            lines.append("")

    llm_enabled = _bool_env("DRYRUN_REPORT_LLM_ENABLED", True)
    lines.append("<b>LLM 해석</b>")
    stage1_debug = stage_debug.get("stage1") if isinstance(stage_debug.get("stage1"), dict) else {}
    macro_top3 = _macro_top3_lines(
        stage1_debug=stage1_debug if isinstance(stage1_debug, dict) else {},
        stage2_debug=stage2_debug if isinstance(stage2_debug, dict) else {},
        flow_rows=flow_rows,
        dedup_clusters=dedup_clusters,
    )
    lines.append("<b>0) 오늘의 매크로 Top-3</b>")
    lines.append(f"- 글로벌 지정학: {macro_top3[0] if len(macro_top3) > 0 else '-'}")
    lines.append(f"- 금리/달러: {macro_top3[1] if len(macro_top3) > 1 else '-'}")
    lines.append(f"- 국내 수급: {macro_top3[2] if len(macro_top3) > 2 else '-'}")
    lines.append("")
    if not llm_enabled:
        lines.append("- 비활성화됨(DRYRUN_REPORT_LLM_ENABLED=0)")
        return "\n".join(lines)

    llm_stage2 = {}
    if stage2_debug:
        llm_stage2 = {
            "source": str(stage2_debug.get("source") or "UNKNOWN"),
            "valid": bool(stage2_debug.get("valid")),
            "denom_eok_5d": round(
                _to_eok_from_krw(
                    max(
                        _float(stage2_debug.get("foreign_traded_krw_5d"), 0.0),
                        _float(stage2_debug.get("inst_traded_krw_5d"), 0.0),
                    )
                ),
                0,
            ),
            "foreign_net_eok_5d": round(_to_eok_from_krw(_float(stage2_debug.get("foreign_net_krw_5d"), 0.0)), 0),
            "inst_net_eok_5d": round(_to_eok_from_krw(_float(stage2_debug.get("inst_net_krw_5d"), 0.0)), 0),
            "foreign_ratio_pct_5d": round(_float(stage2_debug.get("foreign_net_pct_turnover_5d"), 0.0), 2),
            "inst_ratio_pct_5d": round(_float(stage2_debug.get("inst_net_pct_turnover_5d"), 0.0), 2),
            "flags": [str(x) for x in (stage2_debug.get("flags") or [])] if isinstance(stage2_debug.get("flags"), list) else [],
        }

    llm_context = {
        "decision_id": decision_id,
        "decision_time": str(run.get("decision_time") or ""),
        "verdict": rule_verdict,
        "total_score": round(_float(run.get("total_score"), 0.0), 2),
        "stage_scores": {
            "s0": round(_float(run.get("stage0_score"), 0.0), 2),
            "s1": round(_float(run.get("stage1_score"), 0.0), 2),
            "s2": round(_float(run.get("stage2_score"), 0.0), 2),
            "s3": round(_float(run.get("stage3_score"), 0.0), 2),
            "s4": round(_float(run.get("stage4_score"), 0.0), 2),
            "s5": round(_float(run.get("stage5_score"), 0.0), 2),
        },
        "stage2_debug": llm_stage2,
        "macro_top3": {
            "geopolitics": macro_top3[0] if len(macro_top3) > 0 else "",
            "rates_fx": macro_top3[1] if len(macro_top3) > 1 else "",
            "domestic_flow": macro_top3[2] if len(macro_top3) > 2 else "",
        },
        "stage5_fail_summary": stage5_fail_summary,
        "absolute_block_reason": [str(x) for x in (run.get("absolute_block_reason") or [])],
        "market": {
            "kospi": {"close": _float(k.get("close_price"), 0.0), "pct": kp, "date": str(k.get("dt") or "")},
            "kosdaq": {"close": _float(q.get("close_price"), 0.0), "pct": qp, "date": str(q.get("dt") or "")},
            "investor_flow": [
                {
                    "market": str(r.get("market") or ""),
                    "investor_type": str(r.get("investor_type") or ""),
                    "net_amount_eok": round(_float(r.get("net_amount"), 0.0) / 100.0, 1),
                }
                for r in flow_rows
            ],
        },
        "candidates": [
            {
                "ticker": str(c.get("ticker") or ""),
                "name": str(c.get("ticker_name") or ""),
                "action": str(c.get("action") or ""),
                "total_score": round(_float(c.get("total_score"), 0.0), 2),
                "flow_score": round(_float(c.get("stage2_stock_flow_score"), 0.0), 2),
                "news_score": round(_float(c.get("stage3_event_score"), 0.0), 2),
                "timing_score": round(_float(c.get("stage4_timing_score"), 0.0), 2),
                "block_codes": [str(x) for x in (c.get("absolute_block_reason") or [])],
                "stage5_fail_codes": [str(x) for x in (c.get("stage5_fail_codes") or [])]
                if isinstance(c.get("stage5_fail_codes"), list)
                else [],
                "stage5_exec_multiplier": round(_float(c.get("stage5_exec_multiplier"), 1.0), 2),
                "stage3_evidence_count": int(_float(c.get("stage3_evidence_count"), 0.0)),
                "stage3_score_capped": int(_float(c.get("stage3_score_capped"), 0.0)),
                "cluster_id": str(c.get("primary_cluster_id") or ""),
                "top_news": (
                    (candidate_news_map.get(str(c.get("ticker") or ""), [{"title": ""}]) or [{"title": ""}])[0].get("title", "")
                    if int(_float(c.get("stage3_evidence_count"), 0.0)) > 0
                    else ""
                ),
                "top_news_url": (
                    (candidate_news_map.get(str(c.get("ticker") or ""), [{"url": ""}]) or [{"url": ""}])[0].get("url", "")
                    if int(_float(c.get("stage3_evidence_count"), 0.0)) > 0
                    else ""
                ),
            }
            for c in cand_rows
        ],
        "clusters": [
            {
                "cluster_id": str(c.get("cluster_id") or ""),
                "theme": _theme_from_storyline(str(c.get("storyline") or "")),
                "state": _state_label(str(c.get("state_label") or "")),
                "importance": round(_float(c.get("importance_max"), 0.0), 2),
                "storyline": str(c.get("storyline") or ""),
            }
            for c in dedup_clusters[: max(3, top_n)]
        ],
    }
    llm_timeout = int(os.getenv("DRYRUN_REPORT_LLM_TIMEOUT", "90"))
    llm_obj, llm_err = _run_llm_summary(llm_context, timeout_sec=max(30, llm_timeout))
    if not llm_obj:
        lines.append(f"- LLM 해석 생성 실패: {llm_err}")
        return "\n".join(lines)

    lines.append("<b>1) 시장/수급 요약</b>")
    lines.append(f"- 시장: {str(llm_obj.get('market_view') or '-').strip()}")
    lines.append(f"- 수급: {str(llm_obj.get('flow_view') or '-').strip()}")
    lines.append("")

    lines.append("<b>2) 종합 판단</b>")
    lines.append(f"- 결론: {str(llm_obj.get('overall_judgment') or '-').strip()}")
    lines.append("")

    lines.append("<b>3) 매매 가이드</b>")
    lines.append(f"- 실행 전략: {str(llm_obj.get('trade_plan') or '-').strip()}")
    lines.append("")

    lines.append("<b>4) 뉴스/연관 해석</b>")
    lines.append(f"- 해석: {str(llm_obj.get('news_linkage') or '-').strip()}")
    lines.append("")

    lines.append("<b>5) 핵심 리스크</b>")
    risks = llm_obj.get("key_risks") if isinstance(llm_obj.get("key_risks"), list) else []
    if risks:
        for r in risks[:5]:
            txt = str(r).strip()
            if txt:
                lines.append(f"- {txt}")
    else:
        lines.append("- -")
    lines.append("")

    notes = llm_obj.get("candidate_notes") if isinstance(llm_obj.get("candidate_notes"), list) else []
    if notes:
        lines.append("<b>6) 후보별 코멘트</b>")
        for i, n in enumerate(notes[: max(3, top_n)], 1):
            ticker = str((n or {}).get("ticker") or "").strip()
            name = str((n or {}).get("name") or "").strip()
            view = str((n or {}).get("view") or "").strip()
            label = f"{name}({ticker})" if name and ticker else (ticker or name or "-")
            lines.append(f"{i}. <b>{label}</b>")
            lines.append(f"- 코멘트: {view or '-'}")
            lines.append("")

    align_label, align_desc = _llm_rule_alignment(
        overall_judgment=str(llm_obj.get("overall_judgment") or ""),
        rule_verdict=rule_verdict,
    )
    lines.append("<b>LLM-룰 정합성 체크</b>")
    lines.append(f"- 상태: {align_label}")
    lines.append(f"- 해석: {align_desc}")
    lines.append("")

    lines.append("<b>오늘 금지사항</b>")
    abs_blocks = [str(x) for x in (run.get("absolute_block_reason") or [])]
    if abs_blocks:
        lines.append(f"- Absolute Block 발생 시 신규매수 금지 ({', '.join(abs_blocks)})")
    if stage2_debug:
        shock_level = str(stage2_debug.get("shock_level") or "").upper()
        if shock_level in {"ALERT", "EXTREME"}:
            lines.append(f"- Stage2 충격레벨 {shock_level} 구간: 추격매수 금지")
    if "MISSING_LIQUIDITY_SNAPSHOT" in stage5_fail_summary:
        lines.append("- MISSING_LIQUIDITY_SNAPSHOT 종목 신규매수 금지")
    elif "LOW_LIQUIDITY" in stage5_fail_summary:
        lines.append("- LOW_LIQUIDITY 종목 신규매수 금지 또는 극소액만 허용")
    if len(lines) > 0 and lines[-1] == "<b>오늘 금지사항</b>":
        lines.append("- 추가 금지사항 없음")

    return "\n".join(lines)


def _load_telegram_notify():
    here = os.path.dirname(os.path.abspath(__file__))
    parent = os.path.dirname(here)
    if parent not in sys.path:
        sys.path.insert(0, parent)
    try:
        from telegram_notify import notify, notify_plain  # type: ignore
    except Exception as e:
        raise RuntimeError(f"telegram_notify import 실패: {e}") from e
    return notify, notify_plain


def _strip_html_for_plain(text: str) -> str:
    s = str(text or "")
    # 링크는 라벨(URL) 형태로 평문화
    s = re.sub(r'<a href="([^"]+)">([^<]+)</a>', r"\2 (\1)", s)
    # 기본 태그 제거
    s = s.replace("<b>", "").replace("</b>", "")
    s = s.replace("<code>", "").replace("</code>", "")
    # 남은 태그 안전 제거
    s = re.sub(r"</?[^>]+>", "", s)
    return s


def _build_compact_message(decision_id: str) -> str:
    run_rows = ch_select(
        f"""
SELECT
  total_score,
  stage0_pass,
  stage1_pass,
  stage2_score,
  stage5_score,
  absolute_block_reason
FROM trading.decision_run
WHERE decision_id = '{decision_id}'
LIMIT 1
"""
    )
    cand_rows = ch_select(
        f"""
SELECT action, count() AS n
FROM trading.decision_candidate
WHERE decision_id = '{decision_id}'
GROUP BY action
ORDER BY action
"""
    )
    if not run_rows:
        return f"📌 매매판단 요약\n- decision_id: {decision_id}\n- 상태: 데이터 조회 실패"

    r = run_rows[0]
    blocks = [str(x).strip() for x in (r.get("absolute_block_reason") or []) if str(x).strip()]
    block_txt = ", ".join(blocks) if blocks else "-"
    action_txt = " / ".join(
        f"{str(x.get('action') or '-')} {int(float(x.get('n') or 0))}" for x in cand_rows
    ) or "-"
    return "\n".join(
        [
            "📌 매매판단 요약",
            f"- decision_id: {decision_id}",
            f"- 총점: {float(r.get('total_score') or 0):.2f}",
            f"- Stage: S0={int(r.get('stage0_pass') or 0)} S1={int(r.get('stage1_pass') or 0)} S2={float(r.get('stage2_score') or 0):.1f} S5={float(r.get('stage5_score') or 0):.1f}",
            f"- 후보 집계: {action_txt}",
            f"- Absolute Block: {block_txt}",
        ]
    )


def resolve_decision_id(decision_id: str) -> str:
    if decision_id:
        return decision_id
    rows = ch_select(
        """
SELECT decision_id
FROM trading.decision_run
ORDER BY decision_time DESC
LIMIT 1
"""
    )
    if not rows:
        raise RuntimeError("decision_run 데이터가 없습니다.")
    return str(rows[0].get("decision_id") or "")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--decision-id", default="", help="전송할 decision_id (기본: 최신)")
    ap.add_argument("--top-candidates", type=int, default=5)
    ap.add_argument("--clusters", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true", help="텔레그램 전송 없이 콘솔 출력만")
    args = ap.parse_args()

    decision_id = resolve_decision_id(args.decision_id.strip())
    msg = build_message(decision_id=decision_id, top_n=max(1, args.top_candidates), clusters_n=max(1, args.clusters))
    print(msg, flush=True)

    if args.dry_run:
        _log("dry-run 모드: 텔레그램 전송 스킵")
        return 0

    notify_html, notify_plain = _load_telegram_notify()
    ok = bool(notify_html(msg))
    if ok:
        _log("텔레그램 전송 성공(HTML)")
        return 0

    # HTML 파싱 오류(예: '<', '<=' 등) 시 평문으로 자동 재시도
    plain_msg = _strip_html_for_plain(msg)
    ok_plain = bool(notify_plain(plain_msg))
    if ok_plain:
        _log("텔레그램 전송 성공(plain fallback)")
        return 0

    compact_msg = _build_compact_message(decision_id)
    ok_compact = bool(notify_plain(compact_msg))
    if ok_compact:
        _log("텔레그램 전송 성공(compact fallback)")
        return 0

    _log("텔레그램 전송 실패")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
