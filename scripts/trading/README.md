# trading scripts (stock)

이 폴더는 실사용 주식 트레이딩 스크립트의 실제 위치다.
기존 경로 호환을 위해 `~/.openclaw/scripts/<file>` 는 `trading/<file>`로 향하는 심볼릭 링크를 유지한다.

핵심 실행 흐름:
- codex_cron_router.sh -> codex_brain.sh -> prepare_gpt_prompt.py -> execute_gpt_orders.py
- refresh_execution_mode.py -> market_execution_mode.py -> adaptive_policy.json / market_execution_mode.json
- 보유 포지션 동적 관리: manage_positions.py -> execute_gpt_orders.py
- 뉴스/데이터 파이프라인: collect_news.py, monitor_news.py, cluster_news.py, llm_relation_reasoner.py
- 연관 정량 스코어 파이프라인: hidden_relation_scorer.py (cluster 직후, watchlist 직전)
- 매크로 리스크 플래그: market_regime.py가 24h 매크로 토픽(geopolitics/war/oil/shipping/sanctions)을 감지해 stress_flags에 반영
- P0 의사결정 로그 파이프라인:
  enrich_data.sh -> hidden_relation_scorer.py -> refresh_interest_watchlist.py -> decision_operating_pipeline.py
- Watchlist 운영 보조:
  monitor_watchlist_runs.py (run 메타 헬스체크/알림)
  prune_interest_watchlist.py (append snapshot retention prune)
- watchlist 산출 정책:
  후보 유니버스는 `technical_signals ∪ news ∪ news_event_frames ∪ hidden_relation_signals ∪ news_research(직/간접)` 합집합
  candidate_pool(기본 200) 다중 버킷 선별 후 최종 limit(기본 30)만 저장
  저장은 append snapshot 방식(최신 ts 조회)
  오래된 스냅샷 정리는 `prune_interest_watchlist.py` 전용 잡에서 수행
  (`WATCHLIST_RETENTION_DAYS`, `WATCHLIST_RUN_RETENTION_DAYS`)
  `WATCHLIST_ADAPTIVE_WEIGHTING=1` + `WATCHLIST_EVENT_RULE_FLOOR`로 이벤트 기반 종목 결손 보정
  액션 분류는 `technical_score` 단일이 아니라 `final/rule score + explain_ready + relation support(+quality) + research weighted signal` 결합 기준
  run 메타(`interest_watchlist_runs`)를 기록하고 decision은 최신 정상 run 기준으로 watchlist를 로드
  run 헬스 모니터는 `monitor_watchlist_runs.py` 전용 잡에서 점검/알림
  ClickHouse 접속은 `CLICKHOUSE_URL=http://user:pass@host:8123` 형식도 자동 정규화(인증 분리)해 401/404 재발을 방지
- Decision 운영 정책(실거래 정렬):
  universe는 execution mode 기준 `watchlist / shock_core / recovery_core`로 분기
  watchlist 조회 source는 `WATCHLIST_ACTIVE_SOURCE`로 강제(기본: `enrich_data`)
  Stage0/Stage1/Stage2 신호는 execution mode 산출과 executor 하드게이트에 함께 반영
  Stage3/Stage4는 총점/설명용 보조지표 성격이 더 강함
- Stage2 수급 분모 정책:
  market_flow_daily 분모는 `MARKET_TOTAL`(market_index.traded_value_krw) 우선
  분모 품질 문제는 경고/보조 처리하며, 매수 차단은 EXTREME shock에서만 적용
- 브레인 후보 정책:
  prepare_gpt_prompt.py는 execution mode를 읽어 `watchlist / shock_core / recovery_core`를 상위 제약으로 후보를 구성
  `response_enricher.py`가 mode/playbook 필드를 보정하고, `codex_brain.sh`는 `jsonschema + response_validator.py`로 이중 검증
- 수급 스냅샷 대상 정책:
  collect_market_data.py는 watchlist 우선 + dashboard 보강 방식으로 feature_snapshot 종목을 선택
  feature_snapshot 의미: foreign_flow=외국인 보유비중(%), news_event_score=외국인 순매수 수량 proxy, inst_flow=기관 순매수 수량
  feature_snapshot 수집 시 KIS quote(`acml_tr_pbmn`, `aspr_unit`)로 `liquidity_krw`, `spread_bp`를 실측 채움(0 고정 제거)
  장후(AFTER) 스냅샷 수집은 `INVESTOR_FLOW_ALLOW_AFTER`로 제어(기본 허용)
  `sync_normalized_flow_daily.py`는 `ticker_master_kr`(STOCKS.csv/krx_stocks.json 기반)를 함께 갱신하여
  watchlist 누락 종목을 `MASTER_FILL` row로 보강(종목 커버리지 안정화)
  읽기 경로는 `v_feature_snapshot` 표준 컬럼(`foreign_ownership_pct`, `foreign_net_flow`, `inst_net_flow`) 사용 권장
- 브리핑 파이프라인:
  send_dooray_briefing.py(pipeline mode, decision_run/decision_candidate 기반)
  기본 스타일(`DOORAY_PIPELINE_STYLE=claude`)은
  "현재 시장 상황 요약 → 내일 유망주 분석(섹터별) → 종합 판단" 템플릿으로 생성
  (LLM 요약은 보조, 액션/점수/제약은 decision 결과를 그대로 사용)
  `DOORAY_PIPELINE_STYLE=legacy`로 기존 상세형 포맷으로 롤백 가능
  pipeline 모드에서는 "매크로 24h Digest" 데이터를 함께 사용해 티커 매핑 실패 뉴스도 노출
  `DOORAY_SEND_RELATION_PLUS_A=1`(기본)일 때 기본 브리핑 직후 `연관관계 +A 브리핑`을 2차 전송
  (`DOORAY_RELATION_PLUS_A_DELAY_SEC`, `DOORAY_RELATION_PLUS_A_TOP`, `DOORAY_RELATION_PLUS_A_HYPOTHESIS`로 조정)
- 채널 분리:
  owner Telegram은 내부 운영 채널이다.
  여기에는 morning_briefing.py, execute_gpt_orders.py order brief, watchlist/news health alert, weekly_market_report.py 같은 내부 메시지가 남는다.
  public broadcast는 공개 Telegram + Dooray 2개 방을 하나의 묶음으로 보고 같은 메시지를 동시 전송한다.
  public daily/weekly sender는 send_public_telegram_report.py, send_public_weekly_review.py, send_public_weekly_outlook.py 이다.
  public 전송은 public_report_delivery.py가 맡고, Telegram public + Dooray webhook 2개로 동일 본문을 보낸다.
  public daily는 shared payload + web_market_signals + LLM longform renderer(public_daily_report_llm.py) 경로를 우선 사용한다.
- 연관 매핑 정책:
  hidden_relation_scorer.py는 technical_signals뿐 아니라
  `~/.openclaw/workspace/STOCKS.csv` + `~/.openclaw/data/krx_stocks.json`
  전종목 마스터를 함께 사용해 엔티티→티커 매핑 커버리지를 보강
  relation_quality(0~1)를 함께 산출해 저품질 연관 신호는 total_relation_score에 자동 감쇠 반영
- news_research 워커 정책:
  analyze_news_research.py는 기본 단일 워커 락(`NEWS_RESEARCH_SINGLE_WORKER_LOCK=1`)으로 동시 처리 레이스를 방지
  락 파일: `~/.openclaw/state/news_research_worker.lock`
  큐 적체 시 backlog 기반 동적 처리량(`NEWS_RESEARCH_MAX_DYNAMIC_LIMIT`, `NEWS_RESEARCH_MAX_ITEMS_PER_RUN`)으로 드레인
  codex_jobs `data-news-research-15m`(주간 표준) + `data-news-research-night-drain-10m`(야간 백로그 드레인) 이중 운용
  `news_research`/`news_research_queue` 시계열은 UTC 저장 기준으로 표준화
  LLM 기본모델은 `gpt-5.4`, 폴백도 `OPENCLAW_FALLBACK_MODEL` 또는 `gpt-5.4`
- 뉴스 파이프라인 헬스체크:
  monitor_news_pipeline_health.py
  점검항목: market_regime/news/news_cluster_state/news_event_frames/hidden_relation_signals/interest_watchlist_runs
  codex_jobs: `data-news-pipeline-health-20m`
- 인증/환경 부트스트랩:
  핵심 실행기(`prepare_gpt_prompt.py`, `execute_gpt_orders.py`, `manage_positions.py`,
  `decision_operating_pipeline.py`, `send_decision_dryrun_telegram.py`, `send_dooray_briefing.py`)는
  시작 시 `env_bootstrap.py`로 `~/.openclaw/.env.trading`/`~/.openclaw/.env`를 자동 로드한다.
  따라서 수동 실행에서도 ClickHouse 401 재발 가능성을 줄인다.
- 프롬프트 무결성 점검:
  prompt_sanity_check.py (메인/포지션 프롬프트 섹션/금지문구 자동 검사)

정리 정책:
- 불필요 백업 파일(*.bak) 삭제
- LLM 실행은 openclaw agent 우선, `codex exec` fallback은 기본 비활성
- 실행 경로는 `~/.openclaw/scripts/trading/...` 기준으로 통일
