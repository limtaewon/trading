# trading scripts (stock)

이 폴더는 실사용 주식 트레이딩 스크립트의 실제 위치다.
기존 경로 호환을 위해 `~/.openclaw/scripts/<file>` 는 `trading/<file>`로 향하는 심볼릭 링크를 유지한다.

핵심 실행 흐름:
- codex_cron_router.sh -> codex_brain.sh -> prepare_gpt_prompt.py -> execute_gpt_orders.py
- 보유 포지션 동적 관리: manage_positions.py -> execute_gpt_orders.py
- 뉴스/데이터 파이프라인: collect_news.py, monitor_news.py, cluster_news.py, llm_relation_reasoner.py
- P0 의사결정 로그 파이프라인:
  enrich_data.sh -> refresh_interest_watchlist.py -> decision_operating_pipeline.py
- Stage2 수급 분모 정책:
  market_flow_daily 분모는 `MARKET_TOTAL`(market_index.traded_value_krw) 우선,
  없으면 `SAMPLE_STOCK_FLOW_SUM`으로 fallback하며 이 경우 Stage2는 fail-closed 처리
- 브리핑 파이프라인:
  send_dooray_briefing.py(pipeline mode) -> send_decision_dryrun_telegram.py
  (decision_run / decision_candidate 기반)
- 프롬프트 무결성 점검:
  prompt_sanity_check.py (메인/포지션 프롬프트 섹션/금지문구 자동 검사)

정리 정책:
- 불필요 백업 파일(*.bak) 삭제
- LLM 실행은 openclaw agent 우선 + codex exec fallback 허용
- 실행 경로는 `~/.openclaw/scripts/trading/...` 기준으로 통일
