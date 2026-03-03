#!/usr/bin/env python3
from __future__ import annotations

"""
Dooray 브리핑 전송기
- 매매 체결/잔고 데이터 제외
- 유망주 후보 + 관련 뉴스 + 최근 속보 전송
- 브리핑 직전 최신 지수/환율 데이터 저장 후 사용
- 코스피/코스닥은 네이버 실시간 조회 우선
"""

import os
import csv
import json
import hashlib
import subprocess
import argparse
import re
import html
import time
import sys
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import requests

from env_bootstrap import bootstrap_openclaw_env
from market_realtime import fetch_naver_realtime_indices, fetch_naver_usdkrw

bootstrap_openclaw_env()

def _resolve_clickhouse():
    raw_url = (
        os.environ.get("CLICKHOUSE_URL", "").strip()
        or os.environ.get("CLICKHOUSE_HOST", "").strip()
        or "http://localhost:8123"
    )
    user = os.environ.get("CLICKHOUSE_USER", "").strip()
    pw = os.environ.get("CLICKHOUSE_PASS", os.environ.get("CLICKHOUSE_PASSWORD", "")).strip()
    sp = urlsplit(raw_url)
    if sp.username and not user:
        user = sp.username
        pw = sp.password or pw
    if sp.username:
        netloc = sp.hostname or "localhost"
        if sp.port:
            netloc = f"{netloc}:{sp.port}"
        raw_url = urlunsplit((sp.scheme or "http", netloc, sp.path or "", sp.query, sp.fragment))
    if not user:
        user = "default"
    return raw_url, user, pw


CLICKHOUSE_HTTP, CH_USER, CH_PASSWORD = _resolve_clickhouse()
CH_DB = os.environ.get("CLICKHOUSE_DB", "trading").strip() or "trading"

WEBHOOK = os.environ.get("DOORAY_WEBHOOK_URL", "").strip()
STATE_PATH = os.path.expanduser("~/.openclaw/state/dooray_briefing_state.json")
STOCKS_CSV = os.path.expanduser("~/.openclaw/workspace/STOCKS.csv")
PUBLIC_BASE_URL = os.environ.get("STOCK_REPORT_PUBLIC_BASE_URL", "").strip().rstrip("/")
URGENT_CONTEXT_PATH = os.path.expanduser("~/.openclaw/state/news_urgent_context.json")
MACRO_TOPIC_KEYWORDS = {
    "geopolitics": ["이란", "중동", "이스라엘", "전쟁", "공습", "분쟁", "war", "strike", "conflict"],
    "war": ["전쟁", "공습", "폭격", "미사일", "war", "missile", "strike"],
    "oil": ["원유", "유가", "브렌트", "wti", "opec", "석유", "호르무즈", "oil", "crude"],
    "shipping": ["해운", "운임", "항로", "수에즈", "물류", "shipping", "freight"],
    "sanctions": ["제재", "관세", "수출통제", "엠바고", "금수", "sanction", "tariff"],
}

CODE_LABELS = {
    "HARD_RISK_OFF": "전역 위험차단",
    "STAGE0_FAIL": "데이터 품질 미통과",
    "LOW_LIQUIDITY_REAL": "실제 유동성 부족",
    "LOW_LIQUIDITY": "유동성 부족",
    "MISSING_LIQUIDITY_SNAPSHOT": "유동성 데이터 누락",
    "MISSING_FEATURE_SNAPSHOT": "시세 스냅샷 누락",
    "MISSING_SNAPSHOT": "스냅샷 누락",
    "SPREAD_WIDE": "호가 스프레드 확장",
    "SPREAD_TOO_WIDE": "호가 스프레드 과대",
    "FLOW_DENOM_INVALID": "수급 기준값 오류",
    "FLOW_SHOCK_ALERT": "수급 충격 경고",
    "FLOW_DISTRIBUTION_BLOCK": "수급 분배 경고",
    "EXTREME-only": "극단 충격만 차단",
    "ALERT": "경고",
    "EXTREME": "극단",
    "FEATURE_SNAPSHOT_ANY_SESSION": "최근 시세 스냅샷",
    "ADV20_FALLBACK": "20일 평균 거래대금 대체값",
    "GEOPOLITICAL_RISK": "지정학 리스크",
    "OIL_SHOCK_RISK": "유가 충격 리스크",
    "SHIPPING_DISRUPTION_RISK": "해운 교란 리스크",
    "FOREIGN_1D<=-2T": "외국인 하루 순매도 과다",
    "stage2_block=": "수급 차단 기준=",
    "stage3_gate=": "뉴스 게이트=",
    "stage4_gate=": "타이밍 게이트=",
    "OFF": "비활성",
    "PASS": "정상",
    "WARN": "주의",
    "FAIL": "실패",
    "signal buy": "신호 매수",
    "signal neutral": "신호 중립",
    "signal sell": "신호 매도",
}

ACTION_LABELS = {
    "BUY": "매수 가능",
    "HOLD": "관찰 유지",
    "REDUCE": "비중 축소",
    "SELL": "매도 검토",
    "NEUTRAL": "중립",
}


def _f(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _parse_first_json_object(raw: str) -> dict | None:
    txt = str(raw or "").strip()
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
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _to_eok(v_krw: float) -> float:
    return _f(v_krw, 0.0) / 100_000_000.0


def _norm_ticker(v: object) -> str:
    s = str(v or "").strip()
    m = re.search(r"(\d{6})", s)
    return m.group(1) if m else ""


def _fmt_eok(v_krw: float) -> str:
    vv = _to_eok(v_krw)
    sign = "+" if vv >= 0 else ""
    return f"{sign}{vv:,.0f}억"


def _run_briefing_llm(context: dict, timeout_sec: int = 80) -> tuple[dict | None, str]:
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
            "key_event": {"type": "string"},
            "market_summary": {"type": "string"},
            "sector_summary": {"type": "string"},
            "final_judgment": {"type": "string"},
            "trade_note": {"type": "string"},
        },
        "required": ["key_event", "market_summary", "sector_summary", "final_judgment", "trade_note"],
    }
    prompt = (
        "너는 한국 주식 운영 브리핑 작성자다.\n"
        "입력 JSON 숫자/사실만 사용해서 한국어로 짧고 명확하게 요약한다.\n"
        "없는 수치/뉴스를 만들지 말고, 단위(억, %, 원)를 바꾸지 말라.\n"
        "영문 필드명/코드명(예: absolute_blocks, shock_level)을 본문에 노출하지 말고 한국어 의미로만 작성하라.\n"
        "핵심 이벤트 1개, 시장요약 1문장, 섹터요약 1문장, 종합판단 1문장, 매매노트 1문장만 작성.\n"
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


def _bucket_candidate(action: str, total_score: float, rsi: float, abs_blocks: list[str], exec_mult: float) -> str:
    if abs_blocks or exec_mult <= 0.0 or rsi >= 70.0:
        return "red"
    a = str(action or "").upper()
    if a == "BUY" and total_score >= 65.0:
        return "green"
    if total_score >= 70.0:
        return "green"
    return "yellow"


def run_cmd(cmd: str):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)


def _dooray_text_from_html(src: str) -> str:
    t = str(src or "")
    t = re.sub(
        r'<a\s+href="([^"]+)">([^<]+)</a>',
        lambda m: f"{m.group(2)}: {m.group(1)}",
        t,
        flags=re.IGNORECASE,
    )
    t = re.sub(r"</?(b|code)>", "", t, flags=re.IGNORECASE)
    t = html.unescape(t)
    return t


def _humanize_codes(s: str) -> str:
    out = str(s or "")
    for k, v in CODE_LABELS.items():
        out = out.replace(k, v)
    return out


def _label_codes(codes) -> list[str]:
    out = []
    for c in (codes or []):
        k = str(c or "").strip()
        if not k:
            continue
        out.append(CODE_LABELS.get(k, k))
    return out


def _build_pipeline_reason_lines(decision_id: str) -> list[str]:
    did = str(decision_id or "").strip()
    if not did:
        return []
    rows = ch_query(
        f"""
SELECT
  decision_id,
  stage0_pass,
  stage1_pass,
  stage2_pass,
  stage5_pass,
  absolute_block_reason,
  stage_debug_json
FROM trading.decision_run
WHERE decision_id = {sql_quote(did)}
LIMIT 1
"""
    )
    if not rows:
        return []
    row = rows[0]
    try:
        stage_debug = json.loads(str(row.get("stage_debug_json") or "{}"))
    except Exception:
        stage_debug = {}

    abs_blocks = row.get("absolute_block_reason") or []
    if not isinstance(abs_blocks, list):
        abs_blocks = []

    s1 = stage_debug.get("stage1") if isinstance(stage_debug, dict) else {}
    s2 = stage_debug.get("stage2") if isinstance(stage_debug, dict) else {}
    s5 = stage_debug.get("stage5") if isinstance(stage_debug, dict) else {}

    lines = ["🧾 결론 사유"]
    if abs_blocks:
        lines.append(f"- 전역 제약: {', '.join(_label_codes(abs_blocks[:3]))}")

    if isinstance(s1, dict):
        hard = bool(s1.get("hard_riskoff"))
        posture = str(s1.get("action_posture") or "").strip()
        stress = str(s1.get("stress_flags") or "").strip()
        if hard:
            if stress:
                lines.append(f"- 시장 리스크: {CODE_LABELS.get('HARD_RISK_OFF','전역 위험차단')} ({_humanize_codes(stress)})")
            else:
                lines.append(f"- 시장 리스크: {CODE_LABELS.get('HARD_RISK_OFF','전역 위험차단')}")
        elif posture:
            lines.append(f"- 시장 행동강도: {posture}")

    if isinstance(s2, dict):
        shock = str(s2.get("shock_level") or "").strip().upper()
        if shock in {"WARN", "ALERT", "EXTREME"}:
            ratio = float(s2.get("shock_abs_ratio_pct") or 0.0)
            lines.append(f"- 수급 상태: {CODE_LABELS.get(shock, shock)} (충격 {ratio:.2f}%)")

    if isinstance(s5, dict):
        fail_summary = s5.get("fail_summary") if isinstance(s5.get("fail_summary"), dict) else {}
        if fail_summary:
            top = sorted(fail_summary.items(), key=lambda kv: int(kv[1]), reverse=True)[:2]
            pretty = []
            for k, v in top:
                pretty.append(f"{CODE_LABELS.get(str(k), str(k))}:{int(v)}")
            lines.append(f"- 집행 제약: {', '.join(pretty)}")

    # 사유가 너무 빈약하면 섹션 생략
    if len(lines) <= 1:
        return []
    return lines


def _insert_reason_section(msg: str, reason_lines: list[str]) -> str:
    if not reason_lines:
        return msg
    src = str(msg or "")
    marker = "📈 시장 방향"
    if marker in src:
        head, tail = src.split(marker, 1)
        reason_block = "\n".join(reason_lines).rstrip() + "\n\n"
        return head.rstrip() + "\n\n" + reason_block + marker + tail
    return src.rstrip() + "\n\n" + "\n".join(reason_lines)


def _beautify_pipeline_text(msg: str) -> str:
    lines = []
    for raw in str(msg or "").splitlines():
        line = str(raw)
        striped = line.strip()
        if "cluster_id:" in striped:
            continue
        if striped == "요약(비개발자용)":
            line = "🧭 요약(비개발자용)"
        elif striped == "시장 방향":
            line = "📈 시장 방향"
        elif striped == "주요 뉴스/연관 지표":
            line = "📰 주요 뉴스/연관 지표"
        elif striped == "LLM 해석":
            line = "🧠 LLM 해석"
        elif striped == "LLM-룰 정합성 체크":
            line = "🤝 LLM-룰 정합성 체크"
        elif striped == "오늘 금지사항":
            line = "⛔ 오늘 금지사항"
        line = line.replace("- Absolute Block:", "- 절대 차단 사유:")
        line = line.replace("- 제약 코드:", "- 제약 사유:")
        line = line.replace("liquidity_source=", "")
        line = _humanize_codes(line)
        lines.append(line)
    return "\n".join(lines)


def _action_label(action: str) -> str:
    a = str(action or "").strip().upper()
    return ACTION_LABELS.get(a, a or "-")


def _build_pipeline_message_legacy(decision_id: str = "", top_candidates: int = 5, clusters: int = 3):
    from send_decision_dryrun_telegram import resolve_decision_id, build_message  # type: ignore

    did = resolve_decision_id((decision_id or "").strip())
    html_msg = build_message(
        decision_id=did,
        top_n=max(1, int(top_candidates)),
        clusters_n=max(1, int(clusters)),
    )
    msg = _beautify_pipeline_text(_dooray_text_from_html(html_msg))
    reason_lines = _build_pipeline_reason_lines(did)
    msg = _insert_reason_section(msg, reason_lines)
    digest_rows = build_macro_digest_rows(hours=24, top_n=3)
    if digest_rows:
        msg = msg + "\n\n🌍 매크로 24h Digest"
        for d in digest_rows:
            msg += f"\n- [{d.get('topic','macro')}] {d.get('title','-')}"
            if d.get("source_url"):
                msg += f"\n  링크: {d.get('source_url')}"

    raw = {
        "mode": "pipeline",
        "decision_id": did,
        "top_candidates": max(1, int(top_candidates)),
        "clusters": max(1, int(clusters)),
        "macro_digest": digest_rows,
    }
    return msg, raw


def _build_pipeline_message_claude_style(decision_id: str = "", top_candidates: int = 5, clusters: int = 3):
    from send_decision_dryrun_telegram import resolve_decision_id  # type: ignore

    did = resolve_decision_id((decision_id or "").strip())
    run_rows = ch_query(
        f"""
SELECT
  decision_id,
  toString(decision_time) AS decision_time_s,
  total_score,
  stage0_score,
  stage1_score,
  stage2_score,
  stage3_score,
  stage4_score,
  stage5_score,
  absolute_block_reason,
  stage_debug_json
FROM trading.decision_run
WHERE decision_id = {sql_quote(did)}
LIMIT 1
"""
    )
    if not run_rows:
        return "현재 의사결정 데이터가 없습니다.", {"mode": "pipeline_claude", "decision_id": did}
    run = run_rows[0]
    try:
        stage_debug = json.loads(str(run.get("stage_debug_json") or "{}"))
    except Exception:
        stage_debug = {}

    idx_rows = ch_query(
        """
SELECT
  (SELECT close_price FROM trading.market_index WHERE index_code='KOSPI' ORDER BY date DESC LIMIT 1) AS kospi,
  (SELECT change_pct FROM trading.market_index WHERE index_code='KOSPI' ORDER BY date DESC LIMIT 1) AS kospi_chg,
  (SELECT toString(date) FROM trading.market_index WHERE index_code='KOSPI' ORDER BY date DESC LIMIT 1) AS kospi_dt,
  (SELECT close_price FROM trading.market_index WHERE index_code='KOSDAQ' ORDER BY date DESC LIMIT 1) AS kosdaq,
  (SELECT change_pct FROM trading.market_index WHERE index_code='KOSDAQ' ORDER BY date DESC LIMIT 1) AS kosdaq_chg,
  (SELECT toString(date) FROM trading.market_index WHERE index_code='KOSDAQ' ORDER BY date DESC LIMIT 1) AS kosdaq_dt,
  (SELECT close_price FROM trading.market_index WHERE index_code='VIX' ORDER BY date DESC LIMIT 1) AS vix,
  (SELECT close_rate FROM trading.exchange_rate WHERE currency_pair='USDKRW' ORDER BY date DESC LIMIT 1) AS usdkrw
"""
    )
    idx = idx_rows[0] if idx_rows else {}

    regime_rows = ch_query(
        """
SELECT
  regime_label,
  ifNull(action_posture, 'normal') AS action_posture,
  ifNull(arrayStringConcat(stress_flags, ', '), '') AS stress_flags,
  ifNull(guide_text, '') AS guide_text
FROM trading.market_regime
ORDER BY date DESC, updated_at DESC
LIMIT 1
"""
    )
    regime = regime_rows[0] if regime_rows else {}

    cand_rows = ch_query(
        f"""
WITH
  latest_ts AS (SELECT max(date) AS d FROM trading.technical_signals),
  latest_rel AS (SELECT max(asof_ts) AS ts FROM trading.hidden_relation_signals)
SELECT
  c.ticker AS ticker,
  any(ts.ticker_name) AS ticker_name,
  c.action,
  c.total_score,
  c.stage2_stock_flow_score,
  c.stage3_event_score,
  c.stage4_timing_score,
  c.absolute_block_reason,
  c.stage5_fail_codes,
  c.stage5_exec_multiplier,
  any(c.primary_cluster_id) AS primary_cluster_id,
  any(ts.signal) AS signal,
  max(toFloat64(ts.signal_score)) AS signal_score,
  max(toFloat64(ts.rsi14)) AS rsi14,
  max(toFloat64(ts.vol_ratio)) AS vol_ratio,
  max(toFloat64(ts.close_price)) AS close_price,
  max(toFloat64(ts.ma20)) AS ma20,
  max(toFloat64(ts.ma60)) AS ma60,
  max(toFloat64(ifNull(hrs.total_relation_score, 0))) AS rel_score,
  any(ifNull(hrs.relation_bias, 'neutral')) AS rel_bias,
  argMax(toFloat64(vfs.foreign_net_flow), vfs.ts) AS foreign_flow,
  argMax(toFloat64(vfs.inst_net_flow), vfs.ts) AS inst_flow
FROM trading.decision_candidate c
LEFT JOIN trading.technical_signals ts
  ON ts.ticker = c.ticker
 AND ts.date = (SELECT d FROM latest_ts)
LEFT JOIN trading.v_feature_snapshot vfs
  ON vfs.symbol = c.ticker
 AND vfs.ts >= now() - INTERVAL 2 DAY
LEFT JOIN trading.hidden_relation_signals hrs
  ON hrs.ticker = c.ticker
 AND hrs.asof_ts = (SELECT ts FROM latest_rel)
WHERE c.decision_id = {sql_quote(did)}
GROUP BY
  c.ticker, c.action, c.total_score, c.stage2_stock_flow_score, c.stage3_event_score, c.stage4_timing_score,
  c.absolute_block_reason, c.stage5_fail_codes, c.stage5_exec_multiplier
ORDER BY c.total_score DESC
LIMIT {max(5, int(top_candidates) * 5)}
"""
    )
    cand_tickers = []
    for r in cand_rows:
        tk = _norm_ticker(r.get("ticker"))
        if is_valid_ticker(tk):
            cand_tickers.append(tk)
    news_map = get_candidate_news_map(cand_tickers, per_ticker=2) if cand_tickers else {}

    cluster_ids = sorted(
        {
            str(r.get("primary_cluster_id") or "").strip()
            for r in cand_rows
            if str(r.get("primary_cluster_id") or "").strip()
        }
    )
    cluster_meta: dict[str, dict] = {}
    if cluster_ids:
        c_rows = ch_query(
            f"""
SELECT
  cluster_id,
  argMax(storyline, asof_ts) AS storyline,
  argMax(state_label, asof_ts) AS state_label,
  max(toFloat64(importance_max)) AS importance_max
FROM trading.news_cluster_state
WHERE cluster_id IN {sql_in_strings(cluster_ids)}
GROUP BY cluster_id
"""
        )
        cluster_meta = {str(r.get("cluster_id") or ""): r for r in c_rows}

    digest_rows = build_macro_digest_rows(hours=48, top_n=max(3, int(clusters)))
    stage2 = stage_debug.get("stage2") if isinstance(stage_debug.get("stage2"), dict) else {}
    stage1 = stage_debug.get("stage1") if isinstance(stage_debug.get("stage1"), dict) else {}
    abs_blocks = [str(x) for x in (run.get("absolute_block_reason") or []) if str(x).strip()]

    llm_context = {
        "decision_id": did,
        "decision_time": str(run.get("decision_time_s") or ""),
        "total_score": round(_f(run.get("total_score")), 2),
        "stage_scores": {
            "s0": round(_f(run.get("stage0_score")), 2),
            "s1": round(_f(run.get("stage1_score")), 2),
            "s2": round(_f(run.get("stage2_score")), 2),
            "s3": round(_f(run.get("stage3_score")), 2),
            "s4": round(_f(run.get("stage4_score")), 2),
            "s5": round(_f(run.get("stage5_score")), 2),
        },
        "absolute_blocks": abs_blocks,
        "regime": {
            "label": str(regime.get("regime_label") or ""),
            "posture": str(regime.get("action_posture") or ""),
            "stress_flags": str(regime.get("stress_flags") or ""),
            "guide_text": str(regime.get("guide_text") or ""),
        },
        "market": {
            "kospi": {"price": _f(idx.get("kospi")), "chg_pct": _f(idx.get("kospi_chg")), "date": str(idx.get("kospi_dt") or "")},
            "kosdaq": {"price": _f(idx.get("kosdaq")), "chg_pct": _f(idx.get("kosdaq_chg")), "date": str(idx.get("kosdaq_dt") or "")},
            "vix": _f(idx.get("vix")),
            "usdkrw": _f(idx.get("usdkrw")),
        },
        "stage2": {
            "shock_level": str(stage2.get("shock_level") or ""),
            "foreign_net_eok_5d": round(_to_eok(_f(stage2.get("foreign_net_krw_5d"))), 1),
            "inst_net_eok_5d": round(_to_eok(_f(stage2.get("inst_net_krw_5d"))), 1),
            "foreign_ratio_pct_5d": _f(stage2.get("foreign_net_pct_turnover_5d")),
            "inst_ratio_pct_5d": _f(stage2.get("inst_net_pct_turnover_5d")),
        },
        "macro_digest": digest_rows,
        "candidates": [
            {
                "ticker": _norm_ticker(r.get("ticker")),
                "name": str(r.get("ticker_name") or ""),
                "action": str(r.get("action") or ""),
                "score": round(_f(r.get("total_score")), 2),
                "rsi": round(_f(r.get("rsi14")), 2),
                "vol_ratio": round(_f(r.get("vol_ratio")), 2),
                "rel_score": round(_f(r.get("rel_score")), 3),
                "flow": {"foreign": _f(r.get("foreign_flow")), "inst": _f(r.get("inst_flow"))},
            }
            for r in cand_rows[:15]
        ],
    }
    llm_obj, llm_err = _run_briefing_llm(llm_context, timeout_sec=int(os.getenv("DOORAY_BRIEF_LLM_TIMEOUT", "80")))
    key_event = str((llm_obj or {}).get("key_event") or "").strip()
    market_summary = str((llm_obj or {}).get("market_summary") or "").strip()
    sector_summary = str((llm_obj or {}).get("sector_summary") or "").strip()
    final_judgment = str((llm_obj or {}).get("final_judgment") or "").strip()
    trade_note = str((llm_obj or {}).get("trade_note") or "").strip()
    if not key_event:
        key_event = str((digest_rows[0] if digest_rows else {}).get("title") or "핵심 거시 이벤트 모니터링")

    greens: list[dict] = []
    yellows: list[dict] = []
    reds: list[dict] = []
    for r in cand_rows:
        rsi = _f(r.get("rsi14"))
        bucket = _bucket_candidate(
            action=str(r.get("action") or ""),
            total_score=_f(r.get("total_score")),
            rsi=rsi,
            abs_blocks=[str(x) for x in (r.get("absolute_block_reason") or []) if str(x).strip()],
            exec_mult=_f(r.get("stage5_exec_multiplier"), 1.0),
        )
        if bucket == "green":
            greens.append(r)
        elif bucket == "yellow":
            yellows.append(r)
        else:
            reds.append(r)

    def _candidate_block(row: dict) -> list[str]:
        tk = _norm_ticker(row.get("ticker"))
        name = str(row.get("ticker_name") or tk)
        score = _f(row.get("total_score"))
        action = str(row.get("action") or "").upper()
        signal = str(row.get("signal") or "-").strip().lower()
        signal_ko = {"buy": "매수신호", "neutral": "중립신호", "sell": "매도신호"}.get(signal, signal or "-")
        rsi = _f(row.get("rsi14"))
        vol = _f(row.get("vol_ratio"))
        rel = _f(row.get("rel_score"))
        ff = _f(row.get("foreign_flow"))
        inf = _f(row.get("inst_flow"))
        rel_bias = str(row.get("rel_bias") or "neutral").strip().lower()
        rel_bias_ko = {"positive": "긍정", "neutral": "중립", "negative": "부정"}.get(rel_bias, rel_bias)
        absb = [str(x) for x in (row.get("absolute_block_reason") or []) if str(x).strip()]
        stage5_codes = [str(x) for x in (row.get("stage5_fail_codes") or []) if str(x).strip()] if isinstance(row.get("stage5_fail_codes"), list) else []
        cid = str(row.get("primary_cluster_id") or "").strip()
        cm = cluster_meta.get(cid, {})
        storyline = str(cm.get("storyline") or "").strip()
        n_rows = news_map.get(tk, [])
        top_news = str((n_rows[0] if n_rows else {}).get("title") or storyline or "관련 뉴스 추적 중")
        exec_note = ", ".join(_label_codes(absb[:1] + stage5_codes[:1])) if (absb or stage5_codes) else "실행 제약 없음"
        out = [
            f"{name} ({tk}) — decision score {score:.1f}",
            f"- 기술: {signal_ko}, RSI {rsi:.1f}, 거래량비율 {vol:.2f}",
            f"- 수급: 외인 지표 {ff:+,.1f}, 기관 지표 {inf:+,.1f}",
            f"- 연관: 점수 {rel:+.2f}, {rel_bias_ko}",
            f"- 뉴스: {_short(top_news, 96)}",
            f"- 매매판단: {_action_label(action)} / {exec_note}",
        ]
        return out

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"📌 매매 브리핑 ({now})", ""]
    lines.append("현재 시장 상황 요약")
    lines.append(f"핵심 이벤트: {key_event}")
    lines.append(
        f"- KOSPI {_f(idx.get('kospi')):,.2f} ({_f(idx.get('kospi_chg')):+.2f}%) / "
        f"KOSDAQ {_f(idx.get('kosdaq')):,.2f} ({_f(idx.get('kosdaq_chg')):+.2f}%)"
    )
    lines.append(f"- VIX {_f(idx.get('vix')):,.2f}, USD/KRW {_f(idx.get('usdkrw')):,.2f}")
    if isinstance(stage2, dict) and stage2:
        lines.append(
            f"- 최근 5영업일 수급: 외국인 {_fmt_eok(_f(stage2.get('foreign_net_krw_5d')))}, "
            f"기관 {_fmt_eok(_f(stage2.get('inst_net_krw_5d')))} "
            f"(충격레벨: {_humanize_codes(str(stage2.get('shock_level') or '-'))})"
        )
    posture = str(stage1.get("action_posture") or regime.get("action_posture") or "normal")
    stress = str(stage1.get("stress_flags") or regime.get("stress_flags") or "").strip()
    guide = str(regime.get("guide_text") or "").strip()
    posture_ko = {"defensive": "방어", "cautious": "주의", "aggressive": "공격", "normal": "중립"}.get(str(posture).lower(), posture)
    lines.append(f"- 시장 행동강도: {posture_ko}")
    if stress:
        lines.append(f"- 스트레스 신호: {_humanize_codes(stress)}")
    if guide:
        lines.append(f"- 가이드: {_humanize_codes(guide)}")
    if market_summary:
        lines.append(f"- 해석: {market_summary}")
    lines.append("")

    lines.append("내일 유망주 분석 (섹터별)")
    lines.append("🟢 상승 유력 — 상대강세/우선 관찰")
    if greens:
        for c in greens[: max(2, int(top_candidates))]:
            lines.extend(_candidate_block(c))
            lines.append("")
    else:
        lines.append("- 해당 없음")
        lines.append("")

    lines.append("🟡 관망/선별 — 조건 확인 후 대응")
    if yellows:
        for c in yellows[: max(2, int(top_candidates // 2 or 1))]:
            lines.extend(_candidate_block(c))
            lines.append("")
    else:
        lines.append("- 해당 없음")
        lines.append("")

    lines.append("🔴 주의/회피")
    if reds:
        for c in reds[: max(2, int(top_candidates // 2 or 1))]:
            lines.extend(_candidate_block(c))
            lines.append("")
    else:
        lines.append("- 해당 없음")
        lines.append("")

    lines.append("종합 판단")
    if final_judgment:
        lines.append(final_judgment)
    else:
        lines.append(f"현재 총점 {_f(run.get('total_score')):.2f} 기준으로 신규 매수는 보수적으로 접근하는 것이 적절합니다.")
    if sector_summary:
        lines.append(f"- 섹터 요약: {sector_summary}")
    if abs_blocks:
        lines.append(f"- 시스템 제약: {', '.join(_label_codes(abs_blocks))}")
    if trade_note:
        lines.append(f"- 실행 노트: {trade_note}")
    lines.append(f"- 기준 decision_id: {did}")
    if llm_err:
        lines.append(f"- LLM 요약 상태: 실패({llm_err}) → 규칙 기반 요약 사용")

    raw = {
        "mode": "pipeline_claude",
        "decision_id": did,
        "top_candidates": max(1, int(top_candidates)),
        "clusters": max(1, int(clusters)),
        "macro_digest": digest_rows,
        "llm_summary": llm_obj or {},
    }
    return _humanize_codes("\n".join(lines).strip()), raw


def build_pipeline_message(decision_id: str = "", top_candidates: int = 5, clusters: int = 3):
    style = os.environ.get("DOORAY_PIPELINE_STYLE", "claude").strip().lower()
    if style in {"legacy", "old", "v1"}:
        return _build_pipeline_message_legacy(
            decision_id=decision_id,
            top_candidates=top_candidates,
            clusters=clusters,
        )
    return _build_pipeline_message_claude_style(
        decision_id=decision_id,
        top_candidates=top_candidates,
        clusters=clusters,
    )


def _short(s: str, n: int = 96) -> str:
    txt = str(s or "").strip()
    if len(txt) <= n:
        return txt
    return txt[: max(0, n - 1)] + "…"


def _classify_exec_possible(action: str, abs_blocks: list[str], fail_codes: list[str]) -> bool:
    if abs_blocks:
        return False
    hard_fail = {"MISSING_LIQUIDITY_SNAPSHOT", "LOW_LIQUIDITY", "LOW_LIQUIDITY_REAL"}
    if any(str(c) in hard_fail for c in (fail_codes or [])):
        return False
    return action.upper() in {"BUY", "REDUCE", "HOLD"}


def _action_delta(action: str, exec_possible: bool, fail_codes: list[str], abs_blocks: list[str]) -> str:
    a = str(action or "").upper()
    if abs_blocks:
        reasons = ", ".join([CODE_LABELS.get(x, x) for x in abs_blocks[:2]])
        return f"관찰 유지 ({reasons})"
    if not exec_possible:
        if fail_codes:
            reasons = ", ".join([CODE_LABELS.get(str(x), str(x)) for x in fail_codes[:2]])
            return f"관찰 유지 ({reasons})"
        return "관찰 유지 (실행 제약)"
    if a == "BUY":
        return "매수 가능 (조건 충족)"
    if a == "REDUCE":
        return "비중 축소 (리스크 관리)"
    return "관찰 유지 (조건 관찰)"


def _ticker_name_map(tickers: list[str]) -> dict[str, str]:
    if not tickers:
        return {}
    in_sql = sql_in_strings(tickers)
    rows = ch_query(
        f"""
SELECT ticker, any(ticker_name) AS ticker_name
FROM trading.technical_signals
WHERE date = (SELECT max(date) FROM trading.technical_signals)
  AND ticker IN {in_sql}
GROUP BY ticker
"""
    )
    out = {str(r.get("ticker") or ""): str(r.get("ticker_name") or "").strip() for r in rows}
    return {k: v for k, v in out.items() if k and v}


def build_relation_plus_a_message(decision_id: str, top_candidates: int = 3, top_hypothesis: int = 3):
    did = str(decision_id or "").strip()
    run_rows = ch_query(
        f"""
SELECT
  decision_id,
  toString(decision_time) AS decision_time_s,
  toString(absolute_block_reason) AS abs_blocks_s,
  stage_debug_json
FROM trading.decision_run
WHERE decision_id = {sql_quote(did)}
LIMIT 1
"""
    )
    if not run_rows:
        return "", {}
    run = run_rows[0]
    cand_rows = ch_query(
        f"""
SELECT
  ticker,
  action,
  total_score,
  toString(absolute_block_reason) AS abs_blocks_s,
  toString(stage5_fail_codes) AS fail_codes_s,
  primary_reasoning_id,
  primary_cluster_id
FROM trading.decision_candidate
WHERE decision_id = {sql_quote(did)}
ORDER BY total_score DESC
LIMIT {max(3, int(top_candidates) * 4)}
"""
    )
    if not cand_rows:
        return "", {}

    tickers = [str(r.get("ticker") or "").strip() for r in cand_rows if is_valid_ticker(str(r.get("ticker") or ""))]
    ticker_names = _ticker_name_map(tickers)

    in_sql = sql_in_strings(tickers[:80])
    reasoning_rows = ch_query(
        f"""
SELECT
  ticker,
  toString(asof_ts) AS asof_ts_s,
  summary,
  causal_chain,
  confidence,
  time_horizon,
  source_cluster,
  evidence_titles
FROM trading.hidden_relation_reasoning
WHERE ticker IN {in_sql}
  AND asof_ts >= now() - INTERVAL 7 DAY
ORDER BY asof_ts DESC, updated_at DESC
LIMIT 800
"""
    )
    by_ticker: dict[str, list[dict]] = {}
    for r in reasoning_rows:
        tk = str(r.get("ticker") or "").strip()
        if not tk:
            continue
        by_ticker.setdefault(tk, []).append(r)

    def pick_reasoning(tk: str, primary_reasoning_id: str) -> dict:
        arr = by_ticker.get(tk, [])
        if not arr:
            return {}
        target = str(primary_reasoning_id or "").strip()
        if target:
            for row in arr:
                if str(row.get("asof_ts_s") or "").strip() == target:
                    return row
        return arr[0]

    cluster_ids = sorted({str(r.get("primary_cluster_id") or "").strip() for r in cand_rows if str(r.get("primary_cluster_id") or "").strip()})
    cluster_map = {}
    if cluster_ids:
        c_rows = ch_query(
            f"""
SELECT
  cluster_id,
  argMax(storyline, asof_ts) AS storyline,
  argMax(state_label, asof_ts) AS state_label,
  max(toFloat64(importance_max)) AS importance_max
FROM trading.news_cluster_state
WHERE cluster_id IN {sql_in_strings(cluster_ids)}
GROUP BY cluster_id
"""
        )
        cluster_map = {str(r.get("cluster_id") or ""): r for r in c_rows}

    top_cards = []
    for r in cand_rows:
        tk = str(r.get("ticker") or "").strip()
        if not is_valid_ticker(tk):
            continue
        action = str(r.get("action") or "").strip().upper()
        abs_blocks = [x.strip().strip("'\"") for x in str(r.get("abs_blocks_s") or "").strip("[]").split(",") if x.strip()]
        fail_codes = [x.strip().strip("'\"") for x in str(r.get("fail_codes_s") or "").strip("[]").split(",") if x.strip()]
        exec_possible = _classify_exec_possible(action, abs_blocks, fail_codes)
        reason = pick_reasoning(tk, str(r.get("primary_reasoning_id") or ""))
        cid = str(r.get("primary_cluster_id") or "").strip()
        cmeta = cluster_map.get(cid, {})
        evidence_titles = reason.get("evidence_titles", [])
        if not isinstance(evidence_titles, list):
            evidence_titles = []
        top_cards.append(
            {
                "ticker": tk,
                "name": ticker_names.get(tk, tk),
                "action": action,
                "score": float(r.get("total_score") or 0.0),
                "exec_possible": exec_possible,
                "delta": _action_delta(action, exec_possible, fail_codes, abs_blocks),
                "summary": str(reason.get("summary") or "").strip(),
                "causal_chain": str(reason.get("causal_chain") or "").strip(),
                "confidence": float(reason.get("confidence") or 0.0),
                "time_horizon": str(reason.get("time_horizon") or "").strip(),
                "source_cluster": str(reason.get("source_cluster") or cid),
                "evidence_titles": [str(x) for x in evidence_titles[:2]],
                "fail_codes": fail_codes,
                "abs_blocks": abs_blocks,
                "cluster_storyline": str(cmeta.get("storyline") or "").strip(),
                "cluster_state": str(cmeta.get("state_label") or "").strip(),
                "cluster_importance": float(cmeta.get("importance_max") or 0.0),
            }
        )
        if len(top_cards) >= max(1, int(top_candidates)):
            break

    hypotheses = []
    for c in top_cards:
        chain = c["causal_chain"] or c["summary"] or c["cluster_storyline"]
        if not chain:
            continue
        hypotheses.append(
            {
                "text": chain,
                "ticker": c["ticker"],
                "name": c["name"],
                "confidence": c["confidence"],
                "horizon": c["time_horizon"] or "1-3d",
            }
        )
    hypotheses = sorted(hypotheses, key=lambda x: float(x.get("confidence") or 0.0), reverse=True)[: max(1, int(top_hypothesis))]

    stage_debug = {}
    try:
        stage_debug = json.loads(str(run.get("stage_debug_json") or "{}"))
    except Exception:
        stage_debug = {}
    all_cand_tickers = [str(r.get("ticker") or "").strip() for r in cand_rows if is_valid_ticker(str(r.get("ticker") or ""))]
    fs_health = {}
    try:
        from send_decision_dryrun_telegram import _feature_snapshot_health  # type: ignore
        fs_health = _feature_snapshot_health(all_cand_tickers)
    except Exception:
        fs_health = {"covered": 0.0, "total": float(len(all_cand_tickers)), "coverage_pct": 0.0}
    relation_cov_num = sum(1 for c in top_cards if c.get("summary") or c.get("causal_chain"))
    relation_cov_den = max(1, len(top_cards))
    relation_cov_pct = (relation_cov_num / relation_cov_den) * 100.0
    feature_cov_pct = float(fs_health.get("coverage_pct", 0.0) or 0.0)

    quality = "PASS"
    if feature_cov_pct < 80.0 or relation_cov_pct < 66.0:
        quality = "WARN"
    if feature_cov_pct < 50.0 or relation_cov_pct < 34.0:
        quality = "FAIL"

    abs_blocks = str(run.get("abs_blocks_s") or "")
    if abs_blocks and abs_blocks not in {"[]", ""}:
        core_line = "연관 신호는 존재하지만 실행 제약이 우선입니다."
    else:
        core_line = "연관 강도 상위 종목 중심으로 선별 대응이 유효합니다."

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"🧠 연관관계 +A 브리핑 ({now})", ""]
    lines.append("🧭 핵심 한줄")
    lines.append(f"- {core_line}")
    lines.append("")
    lines.append(f"🧠 Top 가설 {max(1, int(top_hypothesis))}개")
    if hypotheses:
        for h in hypotheses:
            lines.append(
                f"- {_short(h['text'], 88)} → {h['name']}({h['ticker']}) | "
                f"신뢰도 {float(h.get('confidence') or 0.0):.2f} | 시계열 {h.get('horizon') or '1-3d'}"
            )
    else:
        lines.append("- 유효한 연관 가설이 부족합니다.")
    lines.append("")
    lines.append("⚠️ 액션 영향(기존판단 대비)")
    if top_cards:
        for c in top_cards:
            lines.append(
                f"- {c['name']}({c['ticker']}): {c['delta']}"
            )
    else:
        lines.append("- 후보 카드 없음")
    lines.append("")
    lines.append("👀 종목별 연관 카드")
    for c in top_cards:
        lines.append(
            f"- {c['name']}({c['ticker']}) | 최종액션 {_action_label(c['action'])} | 실행가능 {'예' if c['exec_possible'] else '아니오'}"
        )
        chain = c["causal_chain"] or c["summary"] or c["cluster_storyline"] or "-"
        lines.append(f"  인과사슬: {_short(chain, 96)}")
        e1 = c["evidence_titles"][0] if len(c["evidence_titles"]) > 0 else "-"
        e2 = c["evidence_titles"][1] if len(c["evidence_titles"]) > 1 else "-"
        lines.append(f"  근거: {_short(e1, 54)}, {_short(e2, 54)}")
        lines.append(
            f"  신뢰도: {c['confidence']:.2f} | 시간지평: {c['time_horizon'] or '1-3d'} | 액션보정: {c['delta']}"
        )
    lines.append("")
    lines.append("🔁 무효화 조건")
    lines.append("- Stage2 충격레벨 ALERT 이상 지속 또는 거래량/후속근거 약화 시 가설 즉시 약화")
    lines.append("")
    lines.append("📊 신뢰도 스탬프")
    lines.append(
        f"- data_quality: {quality} (feature_snapshot_coverage {int(fs_health.get('covered',0))}/{int(fs_health.get('total',0))}, "
        f"relation_coverage {relation_cov_num}/{relation_cov_den})"
    )

    raw = {
        "mode": "relation_plus_a",
        "decision_id": did,
        "core_line": core_line,
        "hypotheses": hypotheses,
        "cards": top_cards,
        "quality": quality,
        "feature_snapshot_health": fs_health,
        "relation_coverage": {"num": relation_cov_num, "den": relation_cov_den, "pct": relation_cov_pct},
    }
    return _humanize_codes("\n".join(lines)), raw


def refresh_market_data():
    run_cmd("python3 ~/.openclaw/scripts/trading/collect_market_data.py --only index --days 2")
    run_cmd("python3 ~/.openclaw/scripts/trading/collect_market_data.py --only fx --days 2")


def ch_query(sql: str):
    auth = (CH_USER, CH_PASSWORD) if CH_USER else None
    resp = requests.post(
        CLICKHOUSE_HTTP,
        params={"database": CH_DB, "default_format": "JSON"},
        data=(sql + "\n").encode("utf-8"),
        timeout=30,
        auth=auth,
    )
    resp.raise_for_status()
    payload = resp.json() if resp.text else {}
    return payload.get("data", [])


def _news_has_column(col: str) -> bool:
    try:
        safe_col = str(col).replace("'", "\\'")
        rows = ch_query(
            f"""
SELECT count() AS c
FROM system.columns
WHERE database = '{CH_DB}'
  AND table = 'news'
  AND name = '{safe_col}'
"""
        )
        return bool(rows and int((rows[0] or {}).get("c", 0) or 0) > 0)
    except Exception:
        return False


def load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def load_urgent_context(context_path=URGENT_CONTEXT_PATH):
    try:
        with open(context_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def load_ticker_map():
    m = {}
    try:
        with open(STOCKS_CSV, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                code = row[0].strip()
                name = row[1].strip() if len(row) > 1 else ""
                if code and name:
                    m[code] = name
    except Exception:
        pass
    return m


def build_macro_digest_rows(hours: int = 24, top_n: int = 3):
    h = max(6, int(hours))
    n = max(1, int(top_n))
    try:
        summary_expr = "summary" if _news_has_column("summary") else "'' AS summary"
        source_expr = "source_url" if _news_has_column("source_url") else "'' AS source_url"
        rows = ch_query(
            f"""
SELECT
    toString(published_at) AS published_at_s,
    title,
    {summary_expr},
    importance,
    {source_expr}
FROM trading.news
WHERE published_at >= now() - INTERVAL {h} HOUR
  AND importance >= 3
ORDER BY importance DESC, published_at DESC
LIMIT 250
"""
        )
    except Exception:
        return []
    out = []
    seen_topic = set()
    for r in rows:
        title = str(r.get("title", "") or "").strip()
        summary = str(r.get("summary", "") or "").strip()
        text = f"{title} {summary}".lower()
        picked = None
        for topic, kws in MACRO_TOPIC_KEYWORDS.items():
            if any(k.lower() in text for k in kws):
                picked = topic
                break
        if not picked:
            continue
        if picked in seen_topic:
            continue
        seen_topic.add(picked)
        out.append(
            {
                "topic": picked,
                "title": title,
                "importance": int(r.get("importance", 0) or 0),
                "source_url": str(r.get("source_url", "") or "").strip(),
                "published_at": str(r.get("published_at_s", "") or "").strip(),
            }
        )
        if len(out) >= n:
            break
    return out


def get_realtime_indices():
    out = {
        "kospi_rt": None,
        "kosdaq_rt": None,
        "usdkrw_rt": None,
        "usdkrw_rt_time": "",
    }
    idx = fetch_naver_realtime_indices(timeout_sec=8)
    if "KOSPI" in idx:
        out["kospi_rt"] = idx["KOSPI"].get("price")
    if "KOSDAQ" in idx:
        out["kosdaq_rt"] = idx["KOSDAQ"].get("price")
    fx = fetch_naver_usdkrw(timeout_sec=8)
    if fx:
        out["usdkrw_rt"] = fx.get("price")
        out["usdkrw_rt_time"] = fx.get("observed_at", "")
    return out


def fmt_tickers_with_name(raw, tmap):
    if not raw:
        return "-"
    arr = raw if isinstance(raw, list) else [str(raw)]
    out = []
    for t in arr[:5]:
        code = str(t).strip().strip("[]'\"")
        name = tmap.get(code, "?")
        out.append(f"{name}({code})")
    return ", ".join(out) if out else "-"


def get_sector_by_kis(ticker: str):
    try:
        cmd = f'mcporter call "kis-trading.inquery-stock-price(symbol: \\\"{ticker}\\\")" --output json'
        r = run_cmd(cmd)
        if r.returncode != 0:
            return "-"
        data = json.loads(r.stdout)
        return data.get("bstp_kor_isnm", "-") or "-"
    except Exception:
        return "-"


def explain_pick(p):
    reasons = []
    score = p.get("score", 0)
    rsi = float(p.get("rsi", 0))
    bb = float(p.get("bb", 0))
    vol = float(p.get("vol_r", 0))
    if score >= 3:
        reasons.append("기술점수 상위")
    elif score >= 2:
        reasons.append("기술점수 양호")
    if 40 <= rsi <= 65:
        reasons.append("RSI 과열 아님")
    elif rsi < 40:
        reasons.append("저점권 반등 후보")
    if bb < 1.0:
        reasons.append("볼린저 과열 아님")
    if vol >= 1.5:
        reasons.append("거래량 유입")
    return ", ".join(reasons) if reasons else "기술지표 종합상 상대 우위"


def describe_action_hint(p):
    action = classify_action_hint(p)
    if action == "BUY_REVIEW":
        return "신규매수 검토(근거 추가 확인 필요)"
    if action == "WATCH(근거보강)":
        return "보수적 매수 후보(근거 보강 대기)"
    if action == "WATCH":
        return "관찰(조건 일부 미달)"
    if action == "AVOID(RSI>70)":
        return "과열 구간(추가 상승 추격 비권장)"
    return "보유/관찰"


def describe_technical_signal(p):
    rsi = float(p.get("rsi", 0) or 0)
    bb = float(p.get("bb", 0) or 0)
    vol = float(p.get("vol_r", 0) or 0)
    pct = float(p.get("pct", 0) or 0)
    rel = float(p.get("rel_score", 0) or 0)

    if rsi >= 70:
        rsi_txt = "RSI가 높아 단기 과열 신호가 존재"
    elif rsi >= 50:
        rsi_txt = "RSI가 중립~강세권으로 급격한 변동은 덜 뚜렷"
    elif rsi >= 35:
        rsi_txt = "RSI가 낮아 눌림 구간에서 반등 여지가 존재"
    else:
        rsi_txt = "RSI가 매우 낮아 추가 하락 방어가 필요"

    if vol >= 1.5:
        vol_txt = "거래량 동조가 큰 변화 구간"
    elif vol >= 1.0:
        vol_txt = "거래량은 보통보다 약간 높은 수준"
    else:
        vol_txt = "거래량이 약해 가격 신호 신뢰도가 떨어질 수 있음"

    if pct >= 1.5:
        price_txt = "최근 가격이 강하게 올라온 구간"
    elif pct <= -1.5:
        price_txt = "최근 가격이 꾸준히 눌리는 구간"
    else:
        price_txt = "가격 변동이 완만한 구간"

    if bb > 1.05:
        bb_txt = "밴드 확장으로 변동성 확대"
    elif bb < 0.85:
        bb_txt = "밴드 압축으로 변동성 둔화"
    else:
        bb_txt = "밴드 기준은 비교적 안정적"

    if rel >= 0.1:
        rel_txt = "클러스터 연계가 비교적 우호적"
    elif rel <= -0.05:
        rel_txt = "클러스터 연계 영향이 부정적으로 작동할 여지"
    else:
        rel_txt = "클러스터 연계는 중립"

    return f"{rsi_txt}, {vol_txt}, {price_txt}, {bb_txt}, {rel_txt}"


def sql_quote(s: str) -> str:
    return "'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"


def sql_in_strings(items):
    vals = []
    for x in items:
        if not x:
            continue
        vals.append(sql_quote(str(x)))
    if not vals:
        return "('')"
    return "(" + ",".join(vals) + ")"


def news_fingerprint(item: dict) -> tuple:
    if not isinstance(item, dict):
        return ("", "", "")
    return (
        str(item.get("source_url", "")).strip(),
        str(item.get("title", "")).strip(),
        str(item.get("published_at_s", "")).strip(),
    )


def dedupe_news(items, max_items=None):
    out = []
    seen = set()
    max_items = None if max_items is None else int(max_items)
    for item in items:
        if not isinstance(item, dict):
            continue
        fp = news_fingerprint(item)
        if fp not in seen:
            seen.add(fp)
            out.append(item)
        if max_items is not None and len(out) >= max_items:
            break
    return out


def is_valid_ticker(value: str) -> bool:
    return bool(re.match(r"^\d{6}$", str(value)))


def classify_action_hint(p):
    score = float(p.get("score", 0) or 0)
    rsi = float(p.get("rsi", 0) or 0)
    explain_ready = int(p.get("explain_ready_3d", 0) or 0)
    rel_score = float(p.get("rel_score", 0) or 0)
    if rsi > 70:
        return "AVOID(RSI>70)"
    if score >= 2 and explain_ready > 0 and rel_score >= 0:
        return "BUY_REVIEW"
    if score >= 2 and explain_ready == 0:
        return "WATCH(근거보강)"
    if score >= 1:
        return "WATCH"
    return "HOLD"


def get_breaking_ticker_snapshots(tickers):
    if not tickers:
        return {}
    in_sql = sql_in_strings(tickers)
    rows = ch_query(f"""
        WITH
          latest_date AS (SELECT max(date) AS d FROM technical_signals),
          latest_rel AS (SELECT max(asof_ts) AS ts FROM hidden_relation_signals),
          news_agg AS (
            SELECT
              arrayJoin(tickers) AS ticker,
              countIf(sentiment='positive') AS pos,
              countIf(sentiment='negative') AS neg,
              count() AS news_cnt
            FROM news
            WHERE published_at >= now() - INTERVAL 3 DAY
            GROUP BY ticker
          ),
          frame_agg AS (
            SELECT
              arrayJoin(tickers) AS ticker,
              countIf(relevant=1 AND thesis_path!='' AND evidence_json!='[]') AS explain_ready_3d
            FROM news_event_frames
            WHERE published_at >= now() - INTERVAL 3 DAY
            GROUP BY ticker
          )
        SELECT
          ts.ticker AS ticker,
          ts.ticker_name AS ticker_name,
          ts.signal AS signal,
          ts.signal_score AS score,
          round(ts.rsi14, 2) AS rsi,
          round(ts.bb_pct, 4) AS bb,
          round(ts.vol_ratio, 2) AS vol_r,
          round(ts.change_pct, 2) AS pct,
          ifNull(na.pos, 0) AS pos,
          ifNull(na.neg, 0) AS neg,
          ifNull(na.news_cnt, 0) AS news_cnt,
          ifNull(fa.explain_ready_3d, 0) AS explain_ready_3d,
          round(ifNull(hrs.total_relation_score, 0), 6) AS rel_score,
          ifNull(hrs.relation_bias, 'neutral') AS rel_bias
        FROM technical_signals ts
        LEFT JOIN news_agg na ON na.ticker = ts.ticker
        LEFT JOIN frame_agg fa ON fa.ticker = ts.ticker
        LEFT JOIN hidden_relation_signals hrs ON hrs.ticker = ts.ticker AND hrs.asof_ts = (SELECT ts FROM latest_rel)
        WHERE ts.date = (SELECT d FROM latest_date)
          AND ts.ticker IN {in_sql}
        ORDER BY ts.ticker
    """)
    out = {}
    for r in rows:
        p = dict(r)
        p["action_hint"] = classify_action_hint(p)
        p["why"] = explain_pick(p)
        out[str(p.get("ticker", "")).strip()] = p
    return out


def get_pick_candidates(limit=6):
    """API 스타일 후보 추출: 기술 + 뉴스 + explainability + relation."""
    limit = max(1, int(limit))
    rows = ch_query(f"""
        WITH
          latest_date AS (SELECT max(date) AS d FROM technical_signals),
          latest_rel AS (SELECT max(asof_ts) AS ts FROM hidden_relation_signals),
          news_agg AS (
            SELECT
              arrayJoin(tickers) AS ticker,
              countIf(sentiment='positive') AS pos,
              countIf(sentiment='negative') AS neg,
              count() AS news_cnt
            FROM news
            WHERE published_at >= now() - INTERVAL 3 DAY
            GROUP BY ticker
          ),
          frame_agg AS (
            SELECT
              arrayJoin(tickers) AS ticker,
              countIf(relevant=1 AND thesis_path!='' AND evidence_json!='[]') AS explain_ready_3d
            FROM news_event_frames
            WHERE published_at >= now() - INTERVAL 3 DAY
            GROUP BY ticker
          )
        SELECT
          ts.ticker AS ticker,
          ts.ticker_name AS ticker_name,
          ts.signal AS signal,
          ts.signal_score AS score,
          round(ts.rsi14, 2) AS rsi,
          round(ts.bb_pct, 4) AS bb,
          round(ts.vol_ratio, 2) AS vol_r,
          round(ts.change_pct, 2) AS pct,
          ifNull(na.pos, 0) AS pos,
          ifNull(na.neg, 0) AS neg,
          ifNull(na.news_cnt, 0) AS news_cnt,
          ifNull(fa.explain_ready_3d, 0) AS explain_ready_3d,
          round(ifNull(hrs.total_relation_score, 0), 6) AS rel_score,
          ifNull(hrs.relation_bias, 'neutral') AS rel_bias,
          round(
            (ts.signal_score * 1.6)
            + (ifNull(na.pos,0)-ifNull(na.neg,0)) * 0.25
            + least(ifNull(na.news_cnt,0),10) * 0.10
            + ifNull(hrs.total_relation_score,0) * 2
            + if(ifNull(fa.explain_ready_3d,0) > 0, 1.0, -0.4)
            + if(ts.rsi14 BETWEEN 45 AND 65, 0.4, 0.0),
            4
          ) AS composite_score
        FROM technical_signals ts
        LEFT JOIN news_agg na ON na.ticker = ts.ticker
        LEFT JOIN frame_agg fa ON fa.ticker = ts.ticker
        LEFT JOIN hidden_relation_signals hrs ON hrs.ticker = ts.ticker AND hrs.asof_ts = (SELECT ts FROM latest_rel)
        WHERE ts.date = (SELECT d FROM latest_date)
          AND ts.ticker_name != ''
          AND ts.signal_score >= 1
          AND ts.rsi14 <= 70
        ORDER BY composite_score DESC, score DESC, vol_r DESC
        LIMIT {limit}
    """)

    out = []
    for r in rows:
        p = dict(r)
        p["action_hint"] = classify_action_hint(p)
        p["why"] = explain_pick(p)
        out.append(p)
    return out


def get_candidate_news_map(tickers, per_ticker=2):
    """각 후보 종목의 최신 관련 뉴스(링크 포함)."""
    if not tickers:
        return {}
    in_sql = sql_in_strings(tickers)
    per_ticker = max(1, int(per_ticker))
    rows = ch_query(f"""
        SELECT ticker, toString(published_at) AS published_at_s, sentiment, importance, title, source_url
        FROM (
          SELECT
            ticker,
            published_at,
            sentiment,
            importance,
            title,
            source_url,
            row_number() OVER (PARTITION BY ticker ORDER BY published_at DESC, importance DESC) AS rn
          FROM (
            SELECT
              arrayJoin(tickers) AS ticker,
              published_at,
              sentiment,
              importance,
              title,
              source_url
            FROM news
            WHERE published_at >= now() - INTERVAL 3 DAY
          )
          WHERE ticker IN {in_sql}
        )
        WHERE rn <= {per_ticker}
        ORDER BY ticker, published_at_s DESC
    """)
    raw_map = {}
    for r in rows:
        t = str(r.get("ticker", "")).strip()
        if not t:
            continue
        raw_map.setdefault(t, []).append(r)
    m = {}
    for t, news_rows in raw_map.items():
        m[t] = dedupe_news(news_rows, max_items=per_ticker)
    return m


def build_breaking_trade_section(urgent_context, tmap):
    alerts = urgent_context.get("alerts", [])
    if not isinstance(alerts, list):
        alerts = []

    deduped_alerts = []
    seen_alerts = set()
    for alert in alerts:
        if not isinstance(alert, dict):
            continue
        key = (
            str(alert.get("url", "")).strip(),
            str(alert.get("title", "")).strip(),
            str(alert.get("summary", ""))[:120].strip(),
        )
        if key in seen_alerts:
            continue
        seen_alerts.add(key)
        deduped_alerts.append(alert)
    alerts = deduped_alerts

    if not isinstance(alerts, list) or not alerts:
        return ["🧭 속보 해석: 매매 연관 종목 없음", "- 현재 처리할 속보 항목 없음"]

    holdings = urgent_context.get("holdings", [])
    holdings = [str(x) for x in holdings if is_valid_ticker(x)]
    holdings_set = set(holdings)

    trading_tickers = []
    for alert in alerts:
        raw = alert.get("tickers", [])
        if not isinstance(raw, list):
            continue
        for t in raw:
            t = str(t).strip()
            if is_valid_ticker(t) and t not in trading_tickers:
                trading_tickers.append(t)

    ticker_snapshots = get_breaking_ticker_snapshots(trading_tickers)
    lines = [
        "🧭 속보 해석(매매 관점)",
        f"- 보유종목 매칭 수: {len([t for t in trading_tickers if t in holdings_set])} / {len(trading_tickers)}개",
    ]

    for alert in alerts[:6]:
        imp = int(alert.get("importance", 0) or 0)
        sentiment = alert.get("sentiment", "neutral")
        impact = alert.get("impact_type", "-")
        title = str(alert.get("title", "-"))[:70]
        summary = str(alert.get("summary", "-"))
        tickers = [str(t) for t in alert.get("tickers", []) if is_valid_ticker(t)]
        lines.append("")
        lines.append(f"- [{impact}/{sentiment}][중요도 {imp}] {title}")
        if summary:
            if len(summary) > 110:
                summary = summary[:107] + "..."
            lines.append(f"  요약: {summary}")
        if tickers:
            rel_parts = [f"{tmap.get(t, '-')}({t})" for t in tickers[:4]]
            lines.append("  관련종목: " + " / ".join(rel_parts))
    lines.append("")

    if not trading_tickers:
        lines.append("- 유효한 매매연결 종목 없음")
        return lines

    lines.append("")
    lines.append("📈 매매 후보 해석")
    for t in trading_tickers[:10]:
        snapshot = ticker_snapshots.get(t, {})
        name = tmap.get(t, "?")
        held = "보유" if t in holdings_set else "미보유"
        if not snapshot:
            lines.append(f"- {name}({t}) | 보유:{held} | 데이터 미수집")
            continue
        action = snapshot.get("action_hint", "-")
        lines.append(
            f"- {name}({t}) | {held} | action={action} | "
            f"signal={snapshot.get('signal','-')} {snapshot.get('score', '-')}"
        )
        lines.append("  ---")
        lines.append(
            f"  지표: RSI={float(snapshot.get('rsi',0)):.1f}, BB={float(snapshot.get('bb',0)):.3f}, "
            f"VOL={float(snapshot.get('vol_r',0)):.2f}, rel={float(snapshot.get('rel_score',0)):+.3f}"
        )
        lines.append(
            f"  뉴스: pos={int(snapshot.get('pos',0))}, neg={int(snapshot.get('neg',0))}, "
            f"news={int(snapshot.get('news_cnt',0))}, explain_ready={int(snapshot.get('explain_ready_3d',0))}"
        )
        lines.append(f"  해석: {snapshot.get('why','-')}")
        if snapshot.get("pct") is not None:
            lines.append(f"  가격변동: {float(snapshot.get('pct',0)):+.2f}%")
    return lines


def build_message(urgent_context=None):
    tmap = load_ticker_map()
    rt = get_realtime_indices()
    urgent_context = urgent_context if isinstance(urgent_context, dict) else {}

    snapshot = ch_query("""
        SELECT
          (SELECT close_price FROM market_index WHERE index_code='KOSPI' ORDER BY date DESC LIMIT 1) AS kospi_db,
          (SELECT close_price FROM market_index WHERE index_code='KOSDAQ' ORDER BY date DESC LIMIT 1) AS kosdaq_db,
          (SELECT close_price FROM market_index WHERE index_code='VIX' ORDER BY date DESC LIMIT 1) AS vix,
          (SELECT close_rate FROM exchange_rate WHERE currency_pair='USDKRW' ORDER BY date DESC LIMIT 1) AS usdkrw_db,
          (SELECT toString(max(date)) FROM market_index WHERE index_code IN ('KOSPI','KOSDAQ','VIX')) AS index_dt,
          (SELECT toString(max(date)) FROM exchange_rate WHERE currency_pair='USDKRW') AS fx_dt
    """)

    regime = ch_query("""
        SELECT
          date,
          regime_label,
          summary,
          ifNull(action_posture, 'normal') AS action_posture,
          ifNull(arrayStringConcat(stress_flags, ', '), '') AS stress_flags,
          ifNull(guide_text, '') AS guide_text
        FROM market_regime
        ORDER BY date DESC, updated_at DESC
        LIMIT 1
    """)

    picks = get_pick_candidates(limit=6)
    pick_tickers = [p.get("ticker", "") for p in picks if p.get("ticker")]
    pick_news_map = get_candidate_news_map(pick_tickers, per_ticker=2)

    major_news = dedupe_news(
        ch_query("""
        SELECT toString(published_at) AS published_at_s, title, sentiment, importance, source_url, tickers
        FROM news
        WHERE published_at > now() - INTERVAL 180 MINUTE
          AND importance >= 3
        ORDER BY importance DESC, published_at DESC
        LIMIT 7
    """),
        max_items=6,
    )

    breaking = dedupe_news(
        ch_query("""
        SELECT toString(published_at) AS published_at_s, title, sentiment, importance, source_url, tickers
        FROM news
        WHERE published_at > now() - INTERVAL 60 MINUTE
          AND importance >= 4
        ORDER BY published_at DESC
        LIMIT 7
    """),
        max_items=6,
    )

    major_keys = {news_fingerprint(n) for n in major_news}
    breaking = [b for b in breaking if news_fingerprint(b) not in major_keys]

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"📌 장중 브리핑 ({now})", ""]

    if snapshot:
        s = snapshot[0]
        kospi_db = float(s.get("kospi_db") or 0)
        kosdaq_db = float(s.get("kosdaq_db") or 0)
        usdkrw_db = float(s.get("usdkrw_db") or 0)
        kospi_show = rt.get("kospi_rt") if rt.get("kospi_rt") else kospi_db
        kosdaq_show = rt.get("kosdaq_rt") if rt.get("kosdaq_rt") else kosdaq_db
        usdkrw_show = rt.get("usdkrw_rt") if rt.get("usdkrw_rt") else usdkrw_db

        lines.append("🌡 시장 스냅샷")
        lines.append(
            f"- KOSPI {kospi_show:,.2f} (RT 우선, DB {kospi_db:,.2f}) | "
            f"KOSDAQ {kosdaq_show:,.2f} (RT 우선, DB {kosdaq_db:,.2f})"
        )
        usd_rt_meta = ""
        if rt.get("usdkrw_rt"):
            usd_rt_meta = f", RT시각 {rt.get('usdkrw_rt_time','-')}"
        lines.append(
            f"- VIX {float(s.get('vix') or 0):,.2f} | USDKRW {usdkrw_show:,.2f} "
            f"(RT 우선, DB {usdkrw_db:,.2f}{usd_rt_meta}) | "
            f"DB기준일 index={s.get('index_dt','-')} fx={s.get('fx_dt','-')}"
        )
    else:
        lines.append("🌡 시장 스냅샷: 데이터 없음")

    if regime:
        r = regime[0]
        lines.append(f"- 레짐: {r.get('regime_label','-')}")
        lines.append(f"- 행동강도: {r.get('action_posture','normal')}")
        flags = str(r.get("stress_flags", "") or "").strip()
        if flags:
            lines.append(f"- 스트레스 플래그: {flags}")
        guide_text = str(r.get("guide_text", "") or "").strip()
        if guide_text:
            lines.append(f"- 행동 가이드: {guide_text}")

    lines.append("")
    lines.append("🚀 유망주 요약")
    enriched = []
    if picks:
        for p in picks:
            sector = get_sector_by_kis(p["ticker"]) if str(p.get("ticker", "")).strip() else "-"
            why = explain_pick(p)
            rel_news_rows = pick_news_map.get(str(p.get("ticker", "")).strip(), [])
            enriched.append(
                {
                    **p,
                    "sector": sector,
                    "why": why,
                    "rel_news_rows": rel_news_rows,
                }
            )

        for i, p in enumerate(enriched, 1):
            lines.append(f"{i}) {p['ticker_name']}({p['ticker']})")
            lines.append(f"   - 업종: {p.get('sector','-')}")
            lines.append(f"   - 판단: {describe_action_hint(p)}")
            lines.append(f"   - 해석: {describe_technical_signal(p)}")
            lines.append(
                f"   - 뉴스/근거: 호재 {int(p.get('pos',0))}건, 악재 {int(p.get('neg',0))}건, 총 이슈 {int(p.get('news_cnt',0))}건, "
                f"근거 충족 {int(p.get('explain_ready_3d',0))}건"
            )
            lines.append(f"   - 한 줄 판단: {p['why']}")
            rel_rows = p.get("rel_news_rows", []) or []
            if rel_rows:
                for rn in rel_rows[:2]:
                    lines.append(
                        f"   - 관련뉴스: [{rn.get('sentiment','?')}/{rn.get('importance','?')}] {rn.get('title','')}"
                    )
                    lines.append(f"     링크: {rn.get('source_url','-')}")
            else:
                lines.append("   - 관련뉴스: 없음")
            if i < len(enriched):
                lines.append("")
            if PUBLIC_BASE_URL:
                lines.append(
                    f"   - 상세리포트: {PUBLIC_BASE_URL}/api/v1/stock-report?q={p['ticker']}&include_llm=1"
                )
    else:
        lines.append("- 현재 조건(기술점수/RSI) 충족 종목 없음")

    lines.append("")
    lines.append("🗞 주요 뉴스 요약")
    if major_news:
        for n in major_news[:3]:
            lines.append(
                f"- [{n['sentiment']}/{n['importance']}] {n['title']} "
                f"(관련종목: {fmt_tickers_with_name(n.get('tickers'), tmap)})"
            )
            lines.append(f"  링크: {n.get('source_url','-')}")
    else:
        lines.append("- 최근 3시간 주요 뉴스 없음")

    lines.append("")
    lines.append("📰 최근 속보")
    if breaking:
        for b in breaking[:3]:
            lines.append(
                f"- [{b['sentiment']}/{b['importance']}] {b['title']} (관련종목: {fmt_tickers_with_name(b.get('tickers'), tmap)})"
            )
            lines.append(f"  링크: {b.get('source_url','-')}")
    else:
        lines.append("- 최근 1시간 중요 속보 없음")

    if urgent_context:
        lines.append("")
        lines.extend(build_breaking_trade_section(urgent_context, tmap))

    lines.append("")
    lines.append("※ 브리핑 전용: 체결/잔고/주문 데이터 제외")

    raw = {
        "rt": rt,
        "snapshot": snapshot,
        "regime": regime,
        "picks": enriched,
        "major_news": major_news,
        "breaking": breaking,
        "urgent_context": urgent_context,
    }
    return "\n".join(lines), raw


def main():
    ap = argparse.ArgumentParser(description="Dooray 브리핑 전송기")
    ap.add_argument("--dry-run", action="store_true", help="웹훅 전송 없이 메시지만 출력")
    ap.add_argument("--breaking", action="store_true", help="속보 기반 매매 해석 브리핑 모드")
    ap.add_argument("--context-file", default=URGENT_CONTEXT_PATH, help="속보 컨텍스트 JSON 경로")
    ap.add_argument("--decision-id", default="", help="파이프라인 브리핑 decision_id(기본: 최신)")
    ap.add_argument("--top-candidates", type=int, default=5, help="유망주 표시 개수")
    ap.add_argument("--clusters", type=int, default=3, help="클러스터 표시 개수")
    ap.add_argument("--legacy-format", action="store_true", help="기존 도레이 브리핑 포맷 사용")
    ap.add_argument("--relation-plus-a", action="store_true", help="+A 연관관계 브리핑도 함께 전송")
    args = ap.parse_args()
    if not WEBHOOK and not args.dry_run:
        raise SystemExit("DOORAY_WEBHOOK_URL 환경변수가 없습니다.")

    refresh_market_data()
    use_pipeline = os.environ.get("DOORAY_USE_PIPELINE_BRIEFING", "1") == "1" and not args.legacy_format
    plus_a_enabled = (
        args.relation_plus_a
        or (os.environ.get("DOORAY_SEND_RELATION_PLUS_A", "1") == "1")
    )
    plus_a_delay = max(0, int(os.environ.get("DOORAY_RELATION_PLUS_A_DELAY_SEC", "2")))
    plus_a_top = max(1, int(os.environ.get("DOORAY_RELATION_PLUS_A_TOP", "3")))
    plus_a_hypothesis = max(1, int(os.environ.get("DOORAY_RELATION_PLUS_A_HYPOTHESIS", "3")))

    if use_pipeline:
        msg, raw = build_pipeline_message(
            decision_id=args.decision_id,
            top_candidates=args.top_candidates,
            clusters=args.clusters,
        )
    else:
        urgent_context = load_urgent_context(args.context_file) if args.breaking else {}
        msg, raw = build_message(urgent_context)

    if args.dry_run:
        print(msg)
        if use_pipeline and plus_a_enabled:
            plus_a_msg, _ = build_relation_plus_a_message(
                decision_id=str(raw.get("decision_id", "")),
                top_candidates=plus_a_top,
                top_hypothesis=plus_a_hypothesis,
            )
            if plus_a_msg:
                print("\n\n" + "=" * 48 + "\n")
                print(plus_a_msg)
        return

    digest = hashlib.sha256(msg.encode("utf-8")).hexdigest()
    news_digest = digest
    relation_msg = ""
    relation_raw = {}
    relation_digest = ""
    if use_pipeline and plus_a_enabled:
        relation_msg, relation_raw = build_relation_plus_a_message(
            decision_id=str(raw.get("decision_id", "")),
            top_candidates=plus_a_top,
            top_hypothesis=plus_a_hypothesis,
        )
        if relation_msg:
            relation_digest = hashlib.sha256(relation_msg.encode("utf-8")).hexdigest()

    state = load_state()
    if state.get("last_news_digest") == news_digest:
        return
    if state.get("last_digest") == digest:
        return

    resp = requests.post(WEBHOOK, json={"text": msg}, timeout=10)
    resp.raise_for_status()

    if relation_msg and relation_digest:
        if state.get("last_relation_digest") != relation_digest:
            if plus_a_delay > 0:
                time.sleep(plus_a_delay)
            r2 = requests.post(WEBHOOK, json={"text": relation_msg}, timeout=10)
            r2.raise_for_status()

    next_state = dict(state)
    next_state.update({
        "last_digest": digest,
        "last_news_digest": news_digest,
        "sent_at": datetime.now().isoformat(),
    })
    if use_pipeline:
        next_state["last_mode"] = "pipeline"
        next_state["last_decision_id"] = str(raw.get("decision_id", ""))
        if relation_digest:
            next_state["last_relation_digest"] = relation_digest
            next_state["last_relation_decision_id"] = str(relation_raw.get("decision_id", ""))
    else:
        next_state["last_mode"] = "legacy"
    save_state(next_state)


if __name__ == "__main__":
    main()
