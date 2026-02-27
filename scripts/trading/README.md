# trading scripts (stock)

이 폴더는 실사용 주식 트레이딩 스크립트의 실제 위치다.
기존 경로 호환을 위해 `~/.openclaw/scripts/<file>` 는 `trading/<file>`로 향하는 심볼릭 링크를 유지한다.

핵심 실행 흐름:
- codex_cron_router.sh -> codex_brain.sh -> prepare_gpt_prompt.py -> execute_gpt_orders.py
- 뉴스/데이터 파이프라인: collect_news.py, monitor_news.py, cluster_news.py, llm_relation_reasoner.py

정리 정책:
- 불필요 백업 파일(*.bak) 삭제
- LLM 실행은 openclaw agent 전용(codex exec fallback 제거)
- 실행 경로는 `~/.openclaw/scripts/trading/...` 기준으로 통일
