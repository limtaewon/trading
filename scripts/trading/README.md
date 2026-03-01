# trading scripts (stock)

이 폴더는 실사용 주식 트레이딩 스크립트의 실제 위치다.
기존 경로 호환을 위해 `~/.openclaw/scripts/<file>` 는 `trading/<file>`로 향하는 심볼릭 링크를 유지한다.

핵심 실행 흐름:
- codex_cron_router.sh -> codex_brain.sh -> prepare_gpt_prompt.py -> execute_gpt_orders.py
- 보유 포지션 동적 관리: manage_positions.py -> execute_gpt_orders.py
- 뉴스/데이터 파이프라인: collect_news.py, monitor_news.py, cluster_news.py, llm_relation_reasoner.py
- P0 의사결정 로그 파이프라인:
  enrich_data.sh -> refresh_interest_watchlist.py -> decision_operating_pipeline.py
- Watchlist 운영 보조:
  monitor_watchlist_runs.py (run 메타 헬스체크/알림)
  prune_interest_watchlist.py (append snapshot retention prune)
- watchlist 산출 정책:
  후보 유니버스는 `technical_signals ∪ news ∪ news_event_frames ∪ hidden_relation_signals` 합집합
  candidate_pool(기본 200) 다중 버킷 선별 후 최종 limit(기본 30)만 저장
  저장은 append snapshot 방식(최신 ts 조회)
  오래된 스냅샷 정리는 `prune_interest_watchlist.py` 전용 잡에서 수행
  (`WATCHLIST_RETENTION_DAYS`, `WATCHLIST_RUN_RETENTION_DAYS`)
  `WATCHLIST_ADAPTIVE_WEIGHTING=1` + `WATCHLIST_EVENT_RULE_FLOOR`로 이벤트 기반 종목 결손 보정
  run 메타(`interest_watchlist_runs`)를 기록하고 decision은 최신 정상 run 기준으로 watchlist를 로드
  run 헬스 모니터는 `monitor_watchlist_runs.py` 전용 잡에서 점검/알림
- Decision 운영 정책(실거래 정렬):
  universe는 `watchlist-only`로 강제(technical/feature fallback 미사용)
  watchlist 조회 source는 `WATCHLIST_ACTIVE_SOURCE`로 강제(기본: `enrich_data`)
  하드게이트는 Stage0/Stage1/Stage2(EXTREME shock만)/Stage5
  Stage3/Stage4는 총점/설명용 보조지표로 사용(하드 차단 아님)
- Stage2 수급 분모 정책:
  market_flow_daily 분모는 `MARKET_TOTAL`(market_index.traded_value_krw) 우선
  분모 품질 문제는 경고/보조 처리하며, 매수 차단은 EXTREME shock에서만 적용
- 브레인 후보 정책:
  prepare_gpt_prompt.py는 watchlist 최신 스냅샷(활성 source 필터) 기반으로 후보를 구성
  `PROMPT_WATCHLIST_STRICT=1` 기본값에서 dashboard fallback 비활성
- 수급 스냅샷 대상 정책:
  collect_market_data.py는 watchlist 우선 + dashboard 보강 방식으로 feature_snapshot 종목을 선택
  feature_snapshot 의미: foreign_flow=외국인 보유비중(%), news_event_score=외국인 순매수 수량 proxy, inst_flow=기관 순매수 수량
  읽기 경로는 `v_feature_snapshot` 표준 컬럼(`foreign_ownership_pct`, `foreign_net_flow`, `inst_net_flow`) 사용 권장
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
