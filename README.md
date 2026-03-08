# trading

주식 트레이딩봇 운영 코드의 단일 관리 저장소이다.
실행 대상은 `~/.openclaw/scripts/trading`이며, 이 저장소가 소스 오브 트루스 역할을 한다.

## 1) 시스템 목적
- 장중/장전 스케줄에 맞춰 자동으로 시장 데이터를 수집한다.
- OpenClaw Agent(`gpt-5.4`)로 매매 판단 JSON을 생성한다.
- JSON 주문안을 규칙 기반 검증 후 KIS MCP로 실제 주문한다.
- 긴급 속보 발생 시 즉시 판단 루프를 재트리거한다.

## 2) 핵심 실행 흐름
`cron -> codex_cron_router.sh -> codex_brain.sh -> prepare_gpt_prompt.py -> execute_gpt_orders.py`

보유 포지션 동적 관리 루프:
`cron(command) -> manage_positions.py -> execute_gpt_orders.py`

시장 모드 제어 루프:
`refresh_execution_mode.py -> market_execution_mode.json + adaptive_policy.json -> decision/position/executor`

### 2-1. `codex_cron_router.sh`
- 잡 단위 락으로 중복 실행을 방지한다.
- `payload.kind`별 분기 실행:
- `systemEvent`: 트레이딩 브레인 실행 + 주문 실행
- `agentTurn`: 메시지 생성 후 텔레그램 전송
- `command`: 셸 커맨드 실행
- 시작/종료 이벤트를 `~/.openclaw/state/codex_brain/events.jsonl`에 기록한다.
- 실패 시 텔레그램 에러 알림을 보낸다.

### 2-2. `codex_brain.sh`
- 프롬프트를 생성한 뒤 OpenClaw Agent를 호출한다.
- 동일 프롬프트 해시 캐시(TTL)와 락을 사용해 중복 호출을 줄인다.
- 응답은 `response_enricher.py`로 mode/playbook 필드를 보정한 뒤 `jsonschema + response_validator.py`로 이중 검증한다.
- 응답 JSON 유효성을 확인하고 `/tmp/gpt_response.json`에 저장한다.
- 기본 실행은 `openclaw agent`이며, `codex exec` 폴백은 기본 비활성 상태다.
- 기본 모델은 공식 확인된 `gpt-5.4`다.

### 2-3. `prepare_gpt_prompt.py`
- ClickHouse, KIS(mcporter), 워크스페이스 메모리 파일을 합쳐 판단 프롬프트를 만든다.
- 시장 레짐, watchlist 후보, 최근 뉴스, 공시, 잔고/미체결, 정책 파일을 프롬프트에 포함한다.
- 평시에는 `watchlist`, 충격장/복구장에는 execution mode가 지정한 `shock_core/recovery_core`를 상위 제약으로 사용한다.
- `PROMPT_WATCHLIST_STRICT=1`(기본)일 때도 execution mode가 `shock/recovery`면 allowlist 구조가 우선한다.

### 2-4. `execute_gpt_orders.py`
- 주문 JSON을 파싱 후 규칙 검증(신뢰도/리스크/데이터 신선도/계좌 상태)을 수행한다.
- `execution_mode` 기반 BUY 하드게이트, 물타기 차단, pending exit replay, 수량 재동기화, 재가격산정을 적용한다.
- 하드 스탑로스, 하드 테이크프로핏, 포지션/현금/일일 주문 제한 등 강제 가드레일을 적용한다.
- 검증 통과 주문만 KIS MCP로 실행하고 실행 이력을 상태 파일에 남긴다.
- 결과 JSON과 journal에는 skip reason 집계와 pending-exit 상태를 남긴다.
- 주문 입력 스키마(`trading_response_schema.json`)는 strict 모드로 관리한다.

### 2-5. `manage_positions.py`
- 보유종목만 대상으로 LLM 기반 동적 관리 판단(HOLD/REDUCE/EXIT/ADD/TIGHTEN_STOP/TAKE_PROFIT_PARTIAL)을 수행한다.
- 판단 결과를 `trading_response` 포맷으로 변환해 `execute_gpt_orders.py` 가드레일을 그대로 통과시킨다.
- 포지션 상태(thesis/action/cooldown/next trigger)를 `~/.openclaw/state/position_manager_state.json`에 저장한다.
- `shock/close_only`에선 ADD를 차단하고, fallback 임계값도 더 빠른 청산 쪽으로 조정한다.
- 리뷰 로그를 `position_review_run`, `position_review_action` 테이블에 기록한다.

## 3) 뉴스/이벤트 파이프라인

### 3-1. `collect_news.py`
- Naver 뉴스 수집 -> 중복 제거(L1 URL, L2 임베딩, L3 relevant 필터) -> LLM 분석 -> ClickHouse 적재.
- `morning`, `trading`, `backfill` 흐름을 지원한다.
- LLM 장애 시에도 파이프라인이 멈추지 않도록 보수적 fallback 레코드를 적재한다.
- LLM 출력은 `news_analysis_response_schema.json`으로 스키마 강제한다.
- 분석/임베딩 결과를 `trading.news`, `trading.news_event_frames`, `trading.event_memory`에 기록한다.

### 3-2. `monitor_news.py`
- 5분 주기로 속보 후보를 수집하고 중요도 높은 건을 선별한다.
- 보유종목 연관 중요뉴스가 감지되면 `news-urgent-trigger` 잡을 즉시 실행한다.
- 긴급 컨텍스트 파일(`~/.openclaw/state/news_urgent_context.json`)을 저장한다.
- 필요 시 두레이 브리핑 전송 스크립트를 백그라운드로 호출한다.
- 속보 판별 LLM 출력은 `breaking_news_response_schema.json`으로 스키마 강제한다.

### 3-3. 후속 해석/연관 분석
- `cluster_news.py`: 뉴스 클러스터링
- `hidden_relation_scorer.py`: 이벤트/클러스터 기반 정량 연관 점수(`hidden_relation_signals`) 스냅샷 생성
- `hidden_relation_scorer.py` 엔티티→티커 매핑은 `technical_signals` + 전종목 마스터(`~/.openclaw/workspace/STOCKS.csv`, `~/.openclaw/data/krx_stocks.json`)를 함께 사용해 커버리지를 보강한다.
- `llm_relation_reasoner.py`: 연관 종목/관계 추론
- `analyze_news_research.py`: 중요 뉴스 심층 연구 및 구조화 저장
- `collect_news.py`가 중요 뉴스(`importance>=3`)를 `news_research_queue`에 enqueue한다.
- `analyze_news_research.py`는 `news_research_queue`의 `pending/retry`를 dequeue해 비동기 심층 분석을 수행한다.
- `analyze_news_research.py`는 비동기 강화 레이어로 운영하며, `status/retry_count/next_retry_at` 기반 재시도(backoff) 정책을 사용한다.
- `analyze_news_research.py`는 기본 단일 워커 락(`NEWS_RESEARCH_SINGLE_WORKER_LOCK=1`)으로 멀티 워커 동시 dequeue 레이스를 방지한다.
- `status='ok'`인 레코드만 완료로 간주하고, `fallback/error`는 다음 주기 재분석 대상으로 유지한다.
- `refresh_interest_watchlist.py`는 `news_research`의 `direct_tickers/source_verdict/confidence`를 후보 유니버스 및 점수에 반영해 실운영 후보 선별에 사용한다.

## 4) 보조 데이터 강화 파이프라인

### 4-1. `enrich_data.sh`
- `collect_market_data.py`, `technical_indicators.py`, `market_regime.py`, `collect_dart.py`를 오케스트레이션한다.
- `sync_normalized_flow_daily.py`로 `stock_flow_daily`/`market_flow_daily`를 정규화 갱신한다.
- `sync_ticker_sector.py`로 섹터/테마 스냅샷(`ticker_sector`)을 갱신한다.
- `sync_position_snapshot.py`로 KIS 잔고+포지션매니저 상태를 `position_snapshot`에 동기화한다.
- `collect_earnings_calendar.py`로 DART/뉴스 기반 실적·이벤트 캘린더(`earnings_calendar`)를 갱신한다.
- 장전 시간대(09:00 이전)에는 뉴스 신선도 보장을 위해 `cluster_news.py -> hidden_relation_scorer.py -> llm_relation_reasoner.py` 체인을 선행 실행한다.
- 장전 신선도 체인은 `FORCE_RELATION_CHAIN=1`로 강제 실행 가능하며, `PREMARKET_RELATION_CHAIN_STRICT=1`(기본)일 때 실패 시 파이프라인을 중단한다.
- `hidden_relation_scorer.py`로 최신 연관 점수 스냅샷(`hidden_relation_signals`)을 watchlist 산출 직전에 갱신한다.
- `refresh_interest_watchlist.py`로 동적 watchlist를 재산출한다(룰 + LLM 리랭크).
- watchlist 산출은 `candidate_pool`(기본 200)에서 후보를 먼저 수집하고, 최종 `limit`(기본 30)만 저장한다.
- `decision_operating_pipeline.py`로 Stage 기반 판단 로그(`decision_run`, `decision_candidate`)를 생성한다.
- 장전/장중 빠른 갱신 모드(`--quick`)를 지원한다.

### 4-2. 핵심 데이터 산출물
- 지수/환율/금리/원자재/수급 데이터
- 종목별 기술지표(RSI, MACD, BB, 거래량비율 등)
- 시장 레짐(trend, volatility, risk_appetite, regime_label)
- 시장 레짐 행동강도(action_posture) 및 스트레스 플래그(stress_flags). 24h 매크로 토픽(geopolitics/war/oil/shipping/sanctions) 기반 리스크 플래그를 함께 반영한다.
- 공시 데이터 및 브리핑용 가공 데이터

## 5) 스케줄 관리
- 실제 잡 정의 파일:
- `cron/codex_jobs.json`
- `cron/jobs.json`
- 생성기:
- `scripts/build_codex_jobs_manifest.py`
- 보유 포지션 동적 관리는 `position-manager-20m` command 잡(평일 09:00~15:59, 20분 주기)으로 실행한다.
- `data-execution-mode-2m`로 execution mode를 2분마다 갱신한다.
- `shock-position-review-3m`로 shock/recovery 구간에서만 빠른 포지션 리뷰를 추가 실행한다.
- `pending-exit-replay-open`으로 장 시작 직후 queued exit를 우선 재생한다.
- `data-news-research-15m` command 잡(평일 08:00~16:59, 15분 간격)으로 심층 뉴스 연구 워커를 비동기 실행한다.
- 기본 처리량: `NEWS_RESEARCH_LIMIT=16`, `NEWS_RESEARCH_BATCH=4`, `NEWS_RESEARCH_WINDOW_HOURS=24`
- `data-news-pipeline-health-20m` command 잡으로 뉴스/클러스터/프레임/연관/watchlist 런의 헬스를 주기 점검한다.
- 연관 파이프라인은 장중 오프셋 순서로 실행한다:
  - `data-news-cluster-hourly` (`*/20`)
  - `data-news-relation-score-20m` (`3,23,43`)
  - `data-news-relation-reasoning-20m` (`7,27,47`)
- 주식 파이프라인 경로는 `~/.openclaw/scripts/trading/...`로 통일되어 있다.
- 코인/바이빗 작업은 별도 스크립트 경로를 유지한다.

## 6) 디렉터리 구조
- `scripts/trading/`: 주식 트레이딩 실사용 런타임 스크립트
- `scripts/build_codex_jobs_manifest.py`: 잡 매니페스트 생성기
- `scripts/ops/`: 런타임 <-> 저장소 동기화 스크립트
- `cron/`: 잡 설정 JSON
- `docs/`: 운영 메모/설정 문서

## 7) 운영 동기화 방법

### 런타임 -> 저장소
```bash
bash scripts/ops/sync_from_runtime.sh
```

### 저장소 -> 런타임
```bash
bash scripts/ops/deploy_to_runtime.sh
```

## 8) 현재 정리 원칙
- 주식 로직은 `scripts/trading` 중심으로만 관리한다.
- 레거시/백업 성격 파일은 지속적으로 제거한다.
- LLM 실행은 OpenClaw Agent를 우선 사용하고, 실행 제어권은 executor와 execution mode state가 가진다.

## 11) 유망주 선정/브리핑 기준(최신)

### 11-1. 유망주 데이터 소스
- 두레이/파이프라인 브리핑의 유망주는 `trading.decision_candidate`를 기준으로 표시한다.
- `decision_candidate`는 `decision_operating_pipeline.py`가 생성하며, 기본 universe는 `execution mode` 기준 `watchlist / shock_core / recovery_core` 중 하나다.
- decision의 watchlist 로딩은 append 스냅샷 최신 `ts` 1개를 고정하고 `rank ASC` 우선으로 후보를 선택한다.
- `watchlist` 소스는 `trading.interest_watchlist`이고, `refresh_interest_watchlist.py`가 아래 신호를 합성해 갱신한다.
- watchlist 후보 유니버스는 `technical_signals ∪ news_tickers ∪ news_event_frame_tickers ∪ hidden_relation_tickers ∪ news_research_tickers` 합집합으로 구성한다.
- watchlist는 후보풀(`WATCHLIST_CANDIDATE_POOL`, 기본 200)에서 다중 버킷 합집합 선별 후 최종 저장(`--limit`, 기본 30)으로 확정한다.
- 저장 방식은 `append snapshot`이며, 조회 시 최신 `ts`를 사용한다.
- `refresh_interest_watchlist.py`는 `run_id` 단위 idempotency를 지원한다. 동일 `run_id` 재실행 시 기본은 중복 insert를 스킵하고, `--replace-existing-run`일 때만 해당 run 스냅샷을 교체한다.
- 스냅샷/런 메타 retention 정리는 `prune_interest_watchlist.py` 전용 잡에서 수행한다.
  (`WATCHLIST_RETENTION_DAYS`, `WATCHLIST_RUN_RETENTION_DAYS`)
- `interest_watchlist_runs` 메타 테이블에 run 상태(삽입행수/기준행수/오류)를 기록하고, decision은 최신 정상 run 기준으로 universe를 선택한다.
- `monitor_watchlist_runs.py`가 `interest_watchlist_runs`를 주기 점검해 stale/partial/행수미달 상태를 경고한다.
- 기술: `technical_signals` (signal_score, RSI, BB, 거래량)
- 뉴스: `news`, `news_event_frames` (pos/neg, 뉴스건수, explain_ready)
- 연관: `hidden_relation_signals` (relation_score, bias, source_tickers/channels)
- 리서치: `news_research` (direct_tickers, source_verdict, confidence, thesis)
- 수급: `feature_snapshot`
  `foreign_flow`(외국인 보유비중%), `news_event_score`(외국인 순매수 수량 proxy), `inst_flow`(기관 순매수 수량)

### 11-1-1. `news_research` 조인/반영 방식(실운영)
- `refresh_interest_watchlist.py`의 후보 SQL에서 `news_research`를 뉴스ID가 아닌 **티커 기준**으로 조인한다.
- 조인 키 생성:
  - `direct/secondary/tertiary` 3계층 티커를 각각 펼친다.
  - 가중치: `direct=1.0`, `secondary=0.55`, `tertiary=0.30`
  - 6자리 종목코드/`000000` 제외 필터를 적용한다.
- 유니버스 확장(`research_tickers`):
  - 최근 `WATCHLIST_RESEARCH_LOOKBACK_DAYS` 기간
  - `status IN ('ok','fallback')`
  - `source_verdict != 'conflict'`
  - `confidence >= WATCHLIST_RESEARCH_MIN_CONF`
  - 조건을 만족한 ticker를 유니버스에 `UNION DISTINCT`로 편입한다.
- 점수 반영(`research_agg`):
  - `research_direct_cnt`, `research_secondary_cnt`, `research_tertiary_cnt`를 집계한다.
  - `research_weighted_refs`, `research_weighted_conf`를 가중 집계한다.
  - `research_last_hours`는 `greatest(dateDiff(...), 0)`로 음수 latency를 제거한다.
  - `LEFT JOIN research_agg nr ON nr.ticker = u.ticker`로 결합한다.
  - 후보 생존 조건과 `composite_score` 가점/감점에 함께 반영한다.
- 결과 전파:
  - research 집계값을 `interest_watchlist.request_json.context`에 저장한다.
  - `prepare_gpt_prompt.py`는 해당 context를 읽어 후보 테이블의 `Research(건/유효/충돌/conf)` 컬럼으로 LLM 입력에 노출한다.

### 11-2. LLM 반영 방식
- 후보군은 룰 기반 `composite_score`로 1차 정렬한다.
- LLM 입력은 버킷 균형 샘플링(rule/news+explain/relation/reaction)으로 편향을 줄여 `llm_score/verdict/reason/risk_flags/catalysts`를 얻는다.
- 최종 점수는 `rule_weight` + `llm_weight` 가중합으로 산출한다.
- `WATCHLIST_ADAPTIVE_WEIGHTING=1`이면 기술 결손 + 이벤트 근거 종목에서 LLM 비중을 자동 확대한다.
- `WATCHLIST_EVENT_RULE_FLOOR`(기본 40)로 explain_ready 기반 이벤트 종목 최소 점수 바닥을 적용한다.
- LLM 점수 반영은 `rule_score + llm_weight_effective * (llm_score-50)` 형태로 적용해 중립값(50) 편향을 제거한다.
- LLM 호출 실패 시 자동으로 룰 기반 점수만 사용한다.
- 액션 분류는 기술점수 단일 기준이 아니라 `final/rule score + explain_ready + relation support(+quality) + research weighted signal` 결합으로 판정한다.

### 11-2-1. 시간축 표준화(UTC)
- `collect_news.py`, `analyze_news_research.py`는 `news_research/news_research_queue` 적재 시 UTC 기준으로 저장한다.
- `published_at`이 미래시각(시간대 오인)으로 들어오는 경우 자동 보정해 음수 latency를 방지한다.
- 표시 레이어(브리핑/리포트)는 필요 시 로컬 타임존으로 변환해 보여준다.
- 기존 과거 레코드는 키컬럼 제약으로 직접 UPDATE가 어려워 소비 쿼리에서 `greatest(dateDiff(...), 0)`로 음수 latency를 방어한다.

### 11-2-2. 수급 스냅샷 보강 정책
- `collect_market_data.py`의 종목 수급 스냅샷(`feature_snapshot`) 대상은 watchlist 최신 종목을 우선 사용한다.
- 부족분만 `v_trading_dashboard` 상위로 보강한다.
- 관련 파라미터: `INVESTOR_FLOW_SYMBOL_LIMIT`(기본 30), `INVESTOR_FLOW_WATCHLIST_MULTIPLIER`(기본 3)

### 11-2-3. 공통 안정화(LLM/ClickHouse)
- LLM 호출 기본모델은 `gpt-5.4`이며, 폴백도 `OPENCLAW_FALLBACK_MODEL` 또는 `gpt-5.4`를 따른다.
- ClickHouse 연결은 `CLICKHOUSE_URL=http://user:pass@host:8123`와 `CLICKHOUSE_HOST + USER/PASS` 두 형식을 모두 정규화해 인증 오류 재발을 줄인다.

### 11-3. 두레이 보고 흐름
- 기본값(`DOORAY_USE_PIPELINE_BRIEFING=1`)에서 `send_dooray_briefing.py`는 파이프라인 모드를 사용한다.
- 파이프라인 모드는 `send_decision_dryrun_telegram.py`를 재사용해 `decision_run/decision_candidate` 기반 메시지를 생성한다.
- 파이프라인 모드 메시지에 `매크로 24h Digest`를 추가해 티커 매핑이 없는 지정학/전쟁/유가 이슈도 노출한다.
- 브리핑 LLM은 `scripts/trading/prompts/dooray_briefing_main_prompt.txt`를 기본 템플릿으로 사용하고, 공통 판단 프레임워크(`scripts/trading/prompts/shared_trading_framework_kr.txt`)를 동일하게 주입한다.
- DB 매크로 데이터가 빈약할 때는 `web_market_signals.py`를 통해 Google News RSS 기반 웹 보강 신호를 추가해 브리핑 누락을 줄인다(수치 판단은 DB 우선).
- 유망주 상세(뉴스 링크/연관 해석/타이밍 근거)는 브리핑 생성 시 `news`, `news_event_frames`, `hidden_relation_signals`, `technical_signals`를 추가 조회해 보강한다.
- `DOORAY_SEND_RELATION_PLUS_A=1`일 때 기본 브리핑 뒤에 `연관관계 +A 브리핑`을 2차 전송한다.
  - 조정 파라미터: `DOORAY_RELATION_PLUS_A_DELAY_SEC`(기본 2초), `DOORAY_RELATION_PLUS_A_TOP`(기본 3), `DOORAY_RELATION_PLUS_A_HYPOTHESIS`(기본 3)

### 11-3-1. 매매판단 프롬프트와 브리핑 프롬프트 정합성
- `prepare_gpt_prompt.py`도 동일한 공통 프레임워크 파일을 로드해 LLM 매매판단 컨텍스트에 포함한다.
- `prepare_gpt_prompt.py`는 DB 신호(`news/news_event_frames/news_research/relation/수급/기술`)를 최대치로 제공하고, 옵션으로 웹 보강 신호(`PROMPT_WEB_SIGNALS_ENABLE=1`)를 함께 제공한다.
- 결과적으로 매매판단과 브리핑이 같은 판단 순서(매크로→뉴스/이벤트→의사결정→기술→수급→연관→종합)를 공유한다.

### 11-4. 운영 실행 예시
```bash
# 0) 프롬프트 무결성 점검(엔진 전환 시 권장)
set -a; source ~/.openclaw/.env.trading; set +a
python3 ~/.openclaw/scripts/trading/prompt_sanity_check.py

# 1) 전체 데이터/의사결정 파이프라인
set -a; source ~/.openclaw/.env.trading; set +a
bash ~/.openclaw/scripts/trading/enrich_data.sh all

# 2) watchlist run 헬스 점검(수동)
python3 ~/.openclaw/scripts/trading/monitor_watchlist_runs.py --source "${WATCHLIST_ACTIVE_SOURCE:-enrich_data}"

# 3) watchlist retention prune(수동)
python3 ~/.openclaw/scripts/trading/prune_interest_watchlist.py --retention-days 21 --run-retention-days 45

# 4) 최신 decision_id 기준 두레이 보고
python3 ~/.openclaw/scripts/trading/send_dooray_briefing.py

# 5) 보유 포지션 동적 관리 실행(실주문)
python3 ~/.openclaw/scripts/trading/manage_positions.py --execute

# 6) Replay 검증(최근 20개 decision 재집계 일치성 점검)
python3 ~/.openclaw/scripts/trading/replay_decision.py --lookback-days 14 --limit 20

# 7) Outcome 집계(최근 decision 후보의 1/3/5일 성과 연결)
python3 ~/.openclaw/scripts/trading/build_decision_outcome.py --lookback-days 45 --limit-decisions 150 --horizons 1,3,5

# 8) Outcome A/B 비교(적용 전후 구간 성능 비교)
python3 ~/.openclaw/scripts/trading/report_decision_outcome_ab.py --lookback-days 30 --horizon 3

# 9) 보고서 확장 데이터 동기화(수동)
python3 ~/.openclaw/scripts/trading/sync_ticker_sector.py --limit 260
python3 ~/.openclaw/scripts/trading/sync_position_snapshot.py
python3 ~/.openclaw/scripts/trading/collect_earnings_calendar.py --days 45
```

## 9) 보안 운영
- 민감값은 코드 하드코딩 금지, `.env` 기반으로만 주입한다.
- 샘플 환경 파일: `.env.example`
- 키 교체/점검 절차: `docs/SECURITY_ROTATION_CHECKLIST.md`

## 9-1) 운영 개선 백로그
- 다음 단계 개선 로드맵(Replay/Outcome/리스크/체결/관측성):
- `docs/TRADING_OS_UPGRADE_BACKLOG.md`

## 10) ClickHouse 실사용 테이블 총정리
아래 목록은 현재 주식 런타임 코드(`scripts/trading/*`)에서 실제 참조되는 `trading` DB 테이블만 정리한 것이다.

### 10-1. 주문/의사결정/리스크 감사
| 테이블 | 역할 | 주 생성/갱신 스크립트 | 주 사용 스크립트 |
|---|---|---|---|
| `decision_log` | LLM 판단 원문/검증 입력 로그 저장 | `execute_gpt_orders.py`(INSERT) | `stock_rag_report_api.py` |
| `decision_run` | Stage0~5 실행 점수/패스/차단사유 로그 | `decision_operating_pipeline.py`(INSERT) | 운영 점검/전략 튜닝 |
| `decision_candidate` | 티커별 행동(BUY/HOLD/REDUCE) 및 근거코드 | `decision_operating_pipeline.py`(INSERT) | 운영 점검/전략 튜닝 |
| `decision_replay` | `decision_run` 재집계 일치성(PASS/FAIL) 검증 로그 | `replay_decision.py`(INSERT) | 운영 감사/리플레이 검증 |
| `decision_outcome` | 후보별 사후 성과(N일 수익률/MDD/변동성) 로그 | `build_decision_outcome.py`(INSERT) | 성과분석/가중치 튜닝 |
| `report_prediction` | 브리핑 시점 추천 종목/방향 스냅샷 | `send_dooray_briefing.py`(INSERT) | 브리핑 적중률/사후 검증 |
| `order_log` | 주문 시도/성공/스킵 사유 감사 로그 | `execute_gpt_orders.py`(INSERT) | `stock_rag_report_api.py` |
| `execution_pred` | 체결확률/슬리피지 추정치 기록 | `execute_gpt_orders.py`(INSERT) | 운영 감사/분석 |
| `kill_switch_event` | kill-switch/가드레일 발동 이력 | `execute_gpt_orders.py`(INSERT) | 운영 감사/리스크 추적 |
| `position_review_run` | 포지션 매니저 실행 단위 리뷰 로그 | `manage_positions.py`(INSERT) | 보유관리 회고/튜닝 |
| `position_review_action` | 포지션 매니저 티커별 액션 로그 | `manage_positions.py`(INSERT) | 보유관리 회고/튜닝 |
| `position_snapshot` | 보유 종목 수량/손익/TP·SL 시점 스냅샷 | `sync_position_snapshot.py`(INSERT) | 프롬프트/브리핑/운영 점검 |
| `session_calendar` | 장 세션(정규/경매/NXT) 판정 기준 | 운영 기준 테이블 | `execute_gpt_orders.py` |

### 10-2. 뉴스/이벤트 파이프라인
| 테이블 | 역할 | 주 생성/갱신 스크립트 | 주 사용 스크립트 |
|---|---|---|---|
| `news_raw` | 원천 뉴스 원본/중간 저장소 | `collect_news.py` | `collect_news.py`, `prepare_gpt_prompt.py`, `execute_gpt_orders.py`, `trading_preflight.sh` |
| `news` | 정제/분석 완료 뉴스 본 테이블 | `collect_news.py`, `monitor_news.py`(INSERT) | 프롬프트/브리핑/리스크 판정 전반 |
| `news_event_frames` | 이벤트를 구조화한 프레임(근거/경로) | `collect_news.py` | `prepare_gpt_prompt.py`, `llm_relation_reasoner.py`, `refresh_interest_watchlist.py`, `send_dooray_briefing.py` |
| `event_memory` | 이벤트 장기 메모리/회고 데이터 | `collect_news.py` | `stock_rag_report_api.py` |
| `news_clusters` | 임베딩 기반 뉴스 클러스터 집계 | `cluster_news.py` | `prepare_gpt_prompt.py` |
| `news_cluster_state` | 클러스터 상태(emerging/reinforcing 등) | `cluster_news.py` | `prepare_gpt_prompt.py`, `llm_relation_reasoner.py`, `stock_rag_report_api.py` |
| `news_cluster_map` | 뉴스-클러스터 매핑 상세 | `cluster_news.py` | `stock_rag_report_api.py` |
| `news_research_queue` | 심층 연구 비동기 작업 큐(`pending/retry/done/dead`) | `collect_news.py`, `analyze_news_research.py` | `analyze_news_research.py` |
| `news_research` | 중요 뉴스 심층 연구 결과 저장(`status/retry/backoff` 포함) | `analyze_news_research.py` | `refresh_interest_watchlist.py`(유니버스/점수 조인), `prepare_gpt_prompt.py`(watchlist context 노출), 운영 분석 |

### 10-3. 연관성/관심종목 레이어
| 테이블 | 역할 | 주 생성/갱신 스크립트 | 주 사용 스크립트 |
|---|---|---|---|
| `hidden_relation_signals` | 종목 간 숨은 연관성 점수/바이어스/품질(`relation_quality`) | 연관성 파이프라인 산출물 | `prepare_gpt_prompt.py`, `refresh_interest_watchlist.py`, `send_dooray_briefing.py`, `execute_gpt_orders.py` |
| `hidden_relation_reasoning` | 연관성 인과체인 텍스트 추론 결과 | `llm_relation_reasoner.py`(CREATE/INSERT) | `prepare_gpt_prompt.py`, `execute_gpt_orders.py`(뷰 경유 포함) |
| `interest_watchlist` | 점수 기반 관심종목 스냅샷 | `refresh_interest_watchlist.py` | 운영 모니터링/브리핑 |
| `ticker_sector` | 티커별 섹터/서브섹터/테마 태그 스냅샷 | `sync_ticker_sector.py`(INSERT) | 프롬프트/브리핑/섹터 분석 |

### 10-4. 시장/기술/거시 데이터
| 테이블 | 역할 | 주 생성/갱신 스크립트 | 주 사용 스크립트 |
|---|---|---|---|
| `technical_signals` | 종목 기술지표(RSI/MACD/BB/score) | `technical_indicators.py` | 프롬프트/주문검증/브리핑/리포트 전반 |
| `feature_snapshot` | 종목별 수급/보조 피처 스냅샷 | `collect_market_data.py` | `prepare_gpt_prompt.py`, `execute_gpt_orders.py`, `refresh_interest_watchlist.py` |
| `market_regime` | 시장 레짐(추세/변동성/리스크성향) | `market_regime.py` | `prepare_gpt_prompt.py`, `execute_gpt_orders.py`, 브리핑/리포트 |
| `market_index` | 지수 시계열(KOSPI/KOSDAQ/VIX 등) | `collect_market_data.py` | `market_regime.py`, 브리핑/리포트 |
| `exchange_rate` | 환율 시계열(USDKRW 등) | `collect_market_data.py` | `market_regime.py`, 브리핑/리포트 |
| `interest_rate` | 금리 시계열 | `collect_market_data.py` | `market_briefing.py` |
| `commodity` | 원자재 시계열 | `collect_market_data.py` | `market_briefing.py`, 뉴스 분석 보조 |
| `stock_flow_daily` | 종목 일별 정규화 수급(외국인/기관, 금액/수량/회전율) | `sync_normalized_flow_daily.py` | `decision_operating_pipeline.py` |
| `market_flow_daily` | 시장 일별 정규화 수급(시장/전체 집계) | `sync_normalized_flow_daily.py` | `decision_operating_pipeline.py` |
| `investor_flow` | 레거시 투자주체 수급(브리핑 호환) | 외부 수집 또는 `sync_normalized_flow_daily.py --sync-legacy-investor-flow` | `market_briefing.py` |
| `dart_disclosure` | DART 공시 원천/정규화 저장 | `collect_dart.py` | `prepare_gpt_prompt.py`, `technical_indicators.py` |
| `earnings_calendar` | 실적발표/실적 관련 이벤트 캘린더(D-day) | `collect_earnings_calendar.py`(INSERT) | 프롬프트/브리핑/이벤트 대응 |

`feature_snapshot` 컬럼 의미(운영 기준):
- `foreign_flow`: 외국인 보유비중(%)
- `news_event_score`: 외국인 순매수 수량 proxy
- `inst_flow`: 기관 순매수 수량

### 10-5. 참고: 조회용 뷰(테이블 아님)
- `v_regime`: 최신 시장 레짐 뷰
- `v_trading_dashboard`: 종목 종합 대시보드 뷰
- `v_feature_snapshot`: `feature_snapshot` 수급 컬럼 표준화 뷰
  (`foreign_ownership_pct`, `foreign_net_flow`, `inst_net_flow`)
- `v_stock_signals`: 종목 신호 뷰
- `v_recent_disclosures`: 최근 공시 요약 뷰
- `v_hidden_relation_signals`: 연관성 시그널 뷰
- `v_hidden_relation_reasoning`: 연관성 추론 뷰
- `v_event_memory_quality`: 이벤트 메모리 품질 뷰

### 10-6. 조건부 테이블 메모
- `execute_gpt_orders.py`는 EPS 필터를 위해 `stock_fundamentals`/`fundamentals`/`financial_metrics` 중 존재 테이블을 동적으로 조회한다.
- 해당 테이블이 없으면 EPS 체크는 \"가용 데이터 없음\"으로 처리된다(주문은 다른 가드레일 기준으로 진행).
