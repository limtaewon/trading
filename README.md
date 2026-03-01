# trading

주식 트레이딩봇 운영 코드의 단일 관리 저장소이다.
실행 대상은 `~/.openclaw/scripts/trading`이며, 이 저장소가 소스 오브 트루스 역할을 한다.

## 1) 시스템 목적
- 장중/장전 스케줄에 맞춰 자동으로 시장 데이터를 수집한다.
- OpenClaw Agent(`gpt-5.3-codex-spark`)로 매매 판단 JSON을 생성한다.
- JSON 주문안을 규칙 기반 검증 후 KIS MCP로 실제 주문한다.
- 긴급 속보 발생 시 즉시 판단 루프를 재트리거한다.

## 2) 핵심 실행 흐름
`cron -> codex_cron_router.sh -> codex_brain.sh -> prepare_gpt_prompt.py -> execute_gpt_orders.py`

보유 포지션 동적 관리 루프:
`cron(command) -> manage_positions.py -> execute_gpt_orders.py`

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
- 응답 JSON 유효성을 확인하고 `/tmp/gpt_response.json`에 저장한다.
- 기본 실행은 `openclaw agent`이며, 실패 시 `codex exec` 폴백 경로를 사용한다.

### 2-3. `prepare_gpt_prompt.py`
- ClickHouse, KIS(mcporter), 워크스페이스 메모리 파일을 합쳐 판단 프롬프트를 만든다.
- 시장 레짐, watchlist 후보, 최근 뉴스, 공시, 잔고/미체결, 정책 파일을 프롬프트에 포함한다.
- 매수/매도 후보는 `trading.interest_watchlist` 최신 스냅샷(`WATCHLIST_ACTIVE_SOURCE`)을 우선 사용한다.
- `PROMPT_WATCHLIST_STRICT=1`(기본)일 때 watchlist가 비어도 dashboard fallback 없이 엄격 모드로 동작한다.

### 2-4. `execute_gpt_orders.py`
- 주문 JSON을 파싱 후 규칙 검증(신뢰도/리스크/데이터 신선도/계좌 상태)을 수행한다.
- 하드 스탑로스, 하드 테이크프로핏, 포지션/현금/일일 주문 제한 등 강제 가드레일을 적용한다.
- 검증 통과 주문만 KIS MCP로 실행하고 실행 이력을 상태 파일에 남긴다.
- 주문 입력 스키마(`trading_response_schema.json`)는 strict 모드로 관리한다.

### 2-5. `manage_positions.py`
- 보유종목만 대상으로 LLM 기반 동적 관리 판단(HOLD/REDUCE/EXIT/ADD/TIGHTEN_STOP/TAKE_PROFIT_PARTIAL)을 수행한다.
- 판단 결과를 `trading_response` 포맷으로 변환해 `execute_gpt_orders.py` 가드레일을 그대로 통과시킨다.
- 포지션 상태(thesis/action/cooldown/next trigger)를 `~/.openclaw/state/position_manager_state.json`에 저장한다.
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
- `llm_relation_reasoner.py`: 연관 종목/관계 추론
- `analyze_news_research.py`: 중요 뉴스 심층 연구 및 구조화 저장

## 4) 보조 데이터 강화 파이프라인

### 4-1. `enrich_data.sh`
- `collect_market_data.py`, `technical_indicators.py`, `market_regime.py`, `collect_dart.py`를 오케스트레이션한다.
- `sync_normalized_flow_daily.py`로 `stock_flow_daily`/`market_flow_daily`를 정규화 갱신한다.
- `refresh_interest_watchlist.py`로 동적 watchlist를 재산출한다(룰 + LLM 리랭크).
- watchlist 산출은 `candidate_pool`(기본 200)에서 후보를 먼저 수집하고, 최종 `limit`(기본 30)만 저장한다.
- `decision_operating_pipeline.py`로 Stage 기반 판단 로그(`decision_run`, `decision_candidate`)를 생성한다.
- 장전/장중 빠른 갱신 모드(`--quick`)를 지원한다.

### 4-2. 핵심 데이터 산출물
- 지수/환율/금리/원자재/수급 데이터
- 종목별 기술지표(RSI, MACD, BB, 거래량비율 등)
- 시장 레짐(trend, volatility, risk_appetite, regime_label)
- 공시 데이터 및 브리핑용 가공 데이터

## 5) 스케줄 관리
- 실제 잡 정의 파일:
- `cron/codex_jobs.json`
- `cron/jobs.json`
- 생성기:
- `scripts/build_codex_jobs_manifest.py`
- 보유 포지션 동적 관리는 `position-manager-20m` command 잡(평일 09:00~15:59, 20분 주기)으로 실행한다.
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
- LLM 실행은 OpenClaw Agent를 우선 사용하고, 장애 시 Codex fallback을 허용한다.

## 11) 유망주 선정/브리핑 기준(최신)

### 11-1. 유망주 데이터 소스
- 두레이/파이프라인 브리핑의 유망주는 `trading.decision_candidate`를 기준으로 표시한다.
- `decision_candidate`는 `decision_operating_pipeline.py`가 생성하며, 기본 universe는 `watchlist`다.
- decision의 watchlist 로딩은 append 스냅샷 최신 `ts` 1개를 고정하고 `rank ASC` 우선으로 후보를 선택한다.
- `watchlist` 소스는 `trading.interest_watchlist`이고, `refresh_interest_watchlist.py`가 아래 신호를 합성해 갱신한다.
- watchlist 후보 유니버스는 `technical_signals ∪ news_tickers ∪ news_event_frame_tickers ∪ hidden_relation_tickers` 합집합으로 구성한다.
- watchlist는 후보풀(`WATCHLIST_CANDIDATE_POOL`, 기본 200)에서 다중 버킷 합집합 선별 후 최종 저장(`--limit`, 기본 30)으로 확정한다.
- 저장 방식은 `append snapshot`이며, 조회 시 최신 `ts`를 사용한다. 오래된 스냅샷은 `WATCHLIST_RETENTION_DAYS`(기본 21일) 기준으로 정리한다.
- 기술: `technical_signals` (signal_score, RSI, BB, 거래량)
- 뉴스: `news`, `news_event_frames` (pos/neg, 뉴스건수, explain_ready)
- 연관: `hidden_relation_signals` (relation_score, bias, source_tickers/channels)
- 수급: `feature_snapshot`
  `foreign_flow`(외국인 보유비중%), `news_event_score`(외국인 순매수 수량 proxy), `inst_flow`(기관 순매수 수량)

### 11-2. LLM 반영 방식
- 후보군은 룰 기반 `composite_score`로 1차 정렬한다.
- LLM 입력은 버킷 균형 샘플링(rule/news+explain/relation/reaction)으로 편향을 줄여 `llm_score/verdict/reason/risk_flags/catalysts`를 얻는다.
- 최종 점수는 `rule_weight` + `llm_weight` 가중합으로 산출한다.
- `WATCHLIST_ADAPTIVE_WEIGHTING=1`이면 기술 결손 + 이벤트 근거 종목에서 LLM 비중을 자동 확대한다.
- `WATCHLIST_EVENT_RULE_FLOOR`(기본 40)로 explain_ready 기반 이벤트 종목 최소 점수 바닥을 적용한다.
- LLM 호출 실패 시 자동으로 룰 기반 점수만 사용한다.

### 11-2-1. 수급 스냅샷 보강 정책
- `collect_market_data.py`의 종목 수급 스냅샷(`feature_snapshot`) 대상은 watchlist 최신 종목을 우선 사용한다.
- 부족분만 `v_trading_dashboard` 상위로 보강한다.
- 관련 파라미터: `INVESTOR_FLOW_SYMBOL_LIMIT`(기본 30), `INVESTOR_FLOW_WATCHLIST_MULTIPLIER`(기본 3)

### 11-3. 두레이 보고 흐름
- 기본값(`DOORAY_USE_PIPELINE_BRIEFING=1`)에서 `send_dooray_briefing.py`는 파이프라인 모드를 사용한다.
- 파이프라인 모드는 `send_decision_dryrun_telegram.py`를 재사용해 `decision_run/decision_candidate` 기반 메시지를 생성한다.
- 유망주 상세(뉴스 링크/연관 해석/타이밍 근거)는 브리핑 생성 시 `news`, `news_event_frames`, `hidden_relation_signals`, `technical_signals`를 추가 조회해 보강한다.

### 11-4. 운영 실행 예시
```bash
# 0) 프롬프트 무결성 점검(엔진 전환 시 권장)
set -a; source ~/.openclaw/.env.trading; set +a
python3 ~/.openclaw/scripts/trading/prompt_sanity_check.py

# 1) 전체 데이터/의사결정 파이프라인
set -a; source ~/.openclaw/.env.trading; set +a
bash ~/.openclaw/scripts/trading/enrich_data.sh all

# 2) 최신 decision_id 기준 두레이 보고
python3 ~/.openclaw/scripts/trading/send_dooray_briefing.py

# 3) 보유 포지션 동적 관리 실행(실주문)
python3 ~/.openclaw/scripts/trading/manage_positions.py --execute

# 4) Replay 검증(최근 20개 decision 재집계 일치성 점검)
python3 ~/.openclaw/scripts/trading/replay_decision.py --lookback-days 14 --limit 20

# 5) Outcome 집계(최근 decision 후보의 1/3/5일 성과 연결)
python3 ~/.openclaw/scripts/trading/build_decision_outcome.py --lookback-days 45 --limit-decisions 150 --horizons 1,3,5
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
| `order_log` | 주문 시도/성공/스킵 사유 감사 로그 | `execute_gpt_orders.py`(INSERT) | `stock_rag_report_api.py` |
| `execution_pred` | 체결확률/슬리피지 추정치 기록 | `execute_gpt_orders.py`(INSERT) | 운영 감사/분석 |
| `kill_switch_event` | kill-switch/가드레일 발동 이력 | `execute_gpt_orders.py`(INSERT) | 운영 감사/리스크 추적 |
| `position_review_run` | 포지션 매니저 실행 단위 리뷰 로그 | `manage_positions.py`(INSERT) | 보유관리 회고/튜닝 |
| `position_review_action` | 포지션 매니저 티커별 액션 로그 | `manage_positions.py`(INSERT) | 보유관리 회고/튜닝 |
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
| `news_research` | 중요 뉴스 심층 연구 결과 저장 | `analyze_news_research.py` | 운영 분석/확장 로직 |

### 10-3. 연관성/관심종목 레이어
| 테이블 | 역할 | 주 생성/갱신 스크립트 | 주 사용 스크립트 |
|---|---|---|---|
| `hidden_relation_signals` | 종목 간 숨은 연관성 점수/바이어스 | 연관성 파이프라인 산출물 | `prepare_gpt_prompt.py`, `refresh_interest_watchlist.py`, `send_dooray_briefing.py`, `execute_gpt_orders.py` |
| `hidden_relation_reasoning` | 연관성 인과체인 텍스트 추론 결과 | `llm_relation_reasoner.py`(CREATE/INSERT) | `prepare_gpt_prompt.py`, `execute_gpt_orders.py`(뷰 경유 포함) |
| `interest_watchlist` | 점수 기반 관심종목 스냅샷 | `refresh_interest_watchlist.py` | 운영 모니터링/브리핑 |

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
