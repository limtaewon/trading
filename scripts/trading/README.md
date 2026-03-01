# trading scripts (stock)

이 폴더는 실사용 주식 트레이딩 스크립트의 실제 위치다.
기존 경로 호환을 위해 `~/.openclaw/scripts/<file>` 는 `trading/<file>`로 향하는 심볼릭 링크를 유지한다.

핵심 실행 흐름:
- codex_cron_router.sh -> codex_brain.sh -> prepare_gpt_prompt.py -> execute_gpt_orders.py
- 보유 포지션 동적 관리: manage_positions.py -> execute_gpt_orders.py
- 뉴스/데이터 파이프라인: collect_news.py, monitor_news.py, cluster_news.py, llm_relation_reasoner.py
- P0 의사결정 로그 파이프라인:
  enrich_data.sh -> refresh_interest_watchlist.py -> decision_operating_pipeline.py
- Decision 운영 정책(실거래 정렬):
  universe는 `watchlist-only`로 강제(technical/feature fallback 미사용)
  하드게이트는 Stage0/Stage1/Stage2(EXTREME shock만)/Stage5
  Stage3/Stage4는 총점/설명용 보조지표로 사용(하드 차단 아님)
- Stage2 수급 분모 정책:
  market_flow_daily 분모는 `MARKET_TOTAL`(market_index.traded_value_krw) 우선
  분모 품질 문제는 경고/보조 처리하며, 매수 차단은 EXTREME shock에서만 적용
- 브리핑 파이프라인:
  send_dooray_briefing.py(pipeline mode) -> send_decision_dryrun_telegram.py
  (decision_run / decision_candidate 기반)
- 인증/환경 부트스트랩:
  핵심 실행기(`prepare_gpt_prompt.py`, `execute_gpt_orders.py`, `manage_positions.py`,
  `decision_operating_pipeline.py`, `send_decision_dryrun_telegram.py`, `send_dooray_briefing.py`)는
  시작 시 `env_bootstrap.py`로 `~/.openclaw/.env.trading`/`~/.openclaw/.env`를 자동 로드한다.
  따라서 수동 실행에서도 ClickHouse 401 재발 가능성을 줄인다.
- 프롬프트 무결성 점검:
  prompt_sanity_check.py (메인/포지션 프롬프트 섹션/금지문구 자동 검사)

정리 정책:
- 불필요 백업 파일(*.bak) 삭제
- LLM 실행은 openclaw agent 우선 + codex exec fallback 허용
- 실행 경로는 `~/.openclaw/scripts/trading/...` 기준으로 통일
