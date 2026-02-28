#!/usr/bin/env python3
"""Prompt sanity checker for macOS runtime migration.

Checks:
- main trading prompt includes required sections and no known wrong mapping text
- position manager prompt includes required governance/context sections
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str]) -> tuple[int, str, str]:
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


def contains_all(text: str, patterns: list[str]) -> list[str]:
    return [p for p in patterns if p not in text]


def main() -> int:
    here = Path(__file__).resolve().parent
    main_prompt = Path("/tmp/gpt_prompt_sanity.txt")
    pm_prompt = Path("/tmp/position_manager_prompt_sanity.txt")

    rc, out, err = run([sys.executable, str(here / "prepare_gpt_prompt.py"), "--output", str(main_prompt)])
    if rc != 0:
        print(json.dumps({"status": "error", "step": "prepare_gpt_prompt", "stderr": err[-400:]}, ensure_ascii=False))
        return 1

    rc, out, err = run([
        sys.executable,
        str(here / "manage_positions.py"),
        "--skip-llm",
        "--dump-prompt",
        str(pm_prompt),
    ])
    if rc != 0:
        print(json.dumps({"status": "error", "step": "manage_positions_dump", "stderr": err[-400:]}, ensure_ascii=False))
        return 1

    main_text = main_prompt.read_text(encoding="utf-8") if main_prompt.exists() else ""
    pm_text = pm_prompt.read_text(encoding="utf-8") if pm_prompt.exists() else ""

    main_required = [
        "## 실행 엔진 컨텍스트",
        "## 이번 실행 트리거",
        "## 운영 프로토콜 원문 (HEARTBEAT.md)",
        "## 정체성/투자 철학 원문 (SOUL.md)",
        "## 절대 규칙 (반드시 준수)",
        "## 응답 형식 (반드시 아래 JSON으로만 응답하세요)",
        "시장/종목 정규화 수급 기준 테이블: market_flow_daily, stock_flow_daily",
    ]
    main_forbidden = [
        "feature_snapshot.news_event_score",
    ]

    pm_required = [
        "[ENGINE_CONTEXT]",
        "[SYSTEM_EVENT]",
        "[PERSISTENT_MEMORY]",
        "[HEARTBEAT]",
        "[SOUL]",
        "[MARKET_REGIME]",
        "[POLICY]",
        "[POSITIONS]",
    ]

    missing_main = contains_all(main_text, main_required)
    missing_pm = contains_all(pm_text, pm_required)
    forbidden_hits = [p for p in main_forbidden if p in main_text]

    result = {
        "status": "ok" if (not missing_main and not missing_pm and not forbidden_hits) else "fail",
        "main_prompt_bytes": len(main_text),
        "position_prompt_bytes": len(pm_text),
        "missing_main": missing_main,
        "missing_position": missing_pm,
        "forbidden_hits": forbidden_hits,
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
