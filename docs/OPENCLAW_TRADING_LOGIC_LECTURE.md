# OpenClaw 강의용 트레이딩 로직 정리

이 문서는 `/Users/imtaewon/trading` 기준의 실운영 주식 트레이딩 로직을 OpenClaw에게 설명하기 위한 강의 노트다.
설명 기준은 README가 아니라 실제 실행 스크립트와 크론 구성이다.

## 1. 한 줄 요약

이 시스템은 단일 LLM 매매봇이 아니다.

- 데이터 파이프라인이 먼저 시장/뉴스/공시/연관성/watchlist/decision 로그를 만든다.
- 브레인 루프는 그 데이터를 프롬프트로 묶어 LLM에게 주문안을 받는다.
- 실제 체결은 `execute_gpt_orders.py`가 하드 가드레일로 다시 필터링한 뒤에만 실행된다.
- 별도로 `manage_positions.py`가 보유종목만 대상으로 동적 관리 액션을 만든다.

즉, 구조는 `데이터 레이어 -> 후보 선정 -> LLM 판단 -> 규칙 검증 -> 주문 실행`이다.

## 2. 최상위 실행 구조

### 2-1. 정기 브레인 루프

주 실행 체인:

`cron -> codex_cron_router.sh -> codex_brain.sh -> prepare_gpt_prompt.py -> execute_gpt_orders.py`

역할:

- `codex_cron_router.sh`
  - 잡 이름으로 `codex_jobs.json`을 읽는다.
  - job lock을 잡아 중복 실행을 막는다.
  - `systemEvent`, `agentTurn`, `command`를 분기한다.
  - 시작/종료 이벤트를 `~/.openclaw/state/codex_brain/events.jsonl`에 남긴다.
- `codex_brain.sh`
  - `prepare_gpt_prompt.py`로 프롬프트 생성.
  - 동일 프롬프트 해시 캐시를 사용.
  - 기본은 `openclaw agent`, 실패 시 `codex exec` 폴백.
  - 응답 JSON을 `/tmp/gpt_response*.json`에 저장.
- `execute_gpt_orders.py`
  - 응답 JSON을 파싱.
  - 실제 매수/매도 가능 여부를 하드룰로 재검증.
  - 통과한 주문만 KIS MCP(`mcporter`)로 전송.

### 2-2. 포지션 매니저 루프

보유 포지션 전용 체인:

`cron(command) -> manage_positions.py --execute -> execute_gpt_orders.py`

이 루프는 신규 종목 발굴이 아니라 이미 들고 있는 종목의 유지/축소/청산/부분익절/추가매수 여부를 판단한다.

## 3. 운영 철학

OpenClaw에게 설명할 때 가장 먼저 강조할 점:

- LLM은 최종 체결 권한자가 아니다.
- watchlist와 decision pipeline이 이미 1차 구조화/선별을 끝낸다.
- 주문 실행기는 stale data, session, cash, weight, liquidity, explainability 같은 현실 제약을 강제한다.
- 따라서 이 시스템은 "LLM에게 다 맡긴다"가 아니라 "LLM을 규칙 엔진 사이에 끼워 넣은 하이브리드 운영체제"에 가깝다.

## 4. 데이터 레이어

### 4-1. 장전/장중 보조 데이터 오케스트레이션

`enrich_data.sh`가 핵심 선행 작업을 묶는다.

기본 순서:

1. `collect_market_data.py`
2. `technical_indicators.py`
3. `market_regime.py`
4. `collect_dart.py`
5. `sync_normalized_flow_daily.py`
6. `hidden_relation_scorer.py`
7. `refresh_interest_watchlist.py`
8. `sync_ticker_sector.py`
9. `sync_position_snapshot.py`
10. `collect_earnings_calendar.py`
11. `decision_operating_pipeline.py`

장전에는 추가로 아래 신선도 체인을 먼저 돌린다.

`cluster_news.py -> hidden_relation_scorer.py -> llm_relation_reasoner.py`

이 체인은 뉴스가 단순 headline 수준이 아니라 "이슈 클러스터 -> 종목 간 숨은 연관 -> 인과 설명"까지 올라오게 만든다.

### 4-2. 시장 레짐

`market_regime.py`는 다음을 조합한다.

- 추세
- 변동성
- 리스크 선호
- 매크로 토픽 스트레스

중요 포인트:

- `regime_label`: `BULL_CALM`, `BULL_VOL`, `BEAR_CALM`, `BEAR_VOL`, `SIDEWAYS`
- `action_posture`: normal/cautious/defensive 성격
- `stress_flags`: `geopolitics`, `war`, `oil`, `shipping`, `sanctions` 같은 매크로 리스크 플래그

이 값은 브레인 프롬프트와 decision stage에서 공통으로 소비된다.

## 5. 뉴스/이벤트 파이프라인

### 5-1. 일반 뉴스 수집

`collect_news.py`의 역할:

- Naver 뉴스 수집
- URL 중복 제거
- 임베딩 기반 유사 기사 제거
- LLM 구조화 분석
- ClickHouse 적재

핵심 적재 대상:

- `trading.news`
- `trading.news_event_frames`
- `trading.event_memory`
- `trading.news_research_queue`

중요 규칙:

- L1: URL 중복 제거
- L2: 임베딩 중복 제거
- L3: LLM이 `relevant=false`면 탈락
- relevant=true 이벤트는 `evidence`와 `thesis_path`가 없으면 다시 false로 내린다.
- LLM 실패 시에도 보수적 fallback 레코드를 넣어 파이프라인이 멈추지 않게 한다.

### 5-2. 심층 뉴스 연구

`analyze_news_research.py`는 `news_research_queue`를 비동기로 처리한다.

의미:

- 일반 뉴스 분석보다 더 깊은 "직접 종목/간접 종목/3차 연관 종목" 해석을 수행한다.
- 결과는 나중에 watchlist 후보 유니버스 확장과 점수 가산에 쓰인다.

### 5-3. 속보 감시

`monitor_news.py`는 5분 주기로 속보 후보를 본다.

역할:

- 중요도 높은 breaking news 선별
- 보유종목 연관 여부 확인
- 해당 시 `news-urgent-trigger` systemEvent 재기동
- `~/.openclaw/state/news_urgent_context.json` 저장

즉, 뉴스가 중요하면 다음 정기 잡을 기다리지 않고 브레인 루프를 재트리거한다.

## 6. 숨은 연관성과 watchlist

### 6-1. 숨은 연관성

`hidden_relation_scorer.py`와 `llm_relation_reasoner.py`는 다음 역할을 나눈다.

- scorer:
  - 이벤트/클러스터를 기반으로 종목 간 정량 점수 산출
  - `relation_quality`까지 계산
- reasoner:
  - 왜 이 종목이 연결되는지 인과 설명을 보강

이 연관 신호는 단순 theme tagging보다 강하다.
watchlist와 주문 가드레일에서 둘 다 참고한다.

### 6-2. watchlist 후보 유니버스

`refresh_interest_watchlist.py`는 유니버스를 다음 합집합으로 만든다.

- `technical_signals`
- 최근 `news`
- `news_event_frames`
- `hidden_relation_signals`
- `news_research`에서 파생된 direct/secondary/tertiary tickers

중요한 점은 "기술지표 상위종목만 고르는 시스템"이 아니라는 것이다.
뉴스/연관/리서치만으로도 후보 유입이 가능하다.

### 6-3. watchlist 룰 점수

기본 `composite_score`는 대략 다음을 합성한다.

- 기술 신호 점수
- 뉴스 긍정/부정 밸런스
- 뉴스 건수
- explain_ready 이벤트 수
- 연관 점수와 relation_quality
- research weighted refs/confidence
- conflict penalty
- RSI 중립 구간 보너스

그 뒤 추가 정책:

- multi-bucket candidate pool
  - rule 상위
  - 뉴스/설명가능성 상위
  - relation 상위
  - 가격반응/거래량 상위
- adaptive weighting
  - 기술 결손인데 이벤트/리서치 근거가 강하면 LLM 비중을 높임
- event rule floor
  - explain_ready 이벤트 종목이 기술지표 부재 때문에 완전히 밀리지 않게 최소 점수 바닥 부여

### 6-4. watchlist에서 LLM의 위치

LLM은 watchlist에서도 "후보 리랭크" 용도다.

- 룰 점수로 candidate pool 생성
- 일부 균형 샘플만 LLM 입력
- `final_score = rule_score_effective + llm_weight_effective * (llm_score - 50)`

즉, LLM이 50이면 중립이고, 50 초과/미만만 rule score를 미세 조정한다.
LLM 실패 시 watchlist는 룰 기반으로 계속 간다.

## 7. decision_operating_pipeline의 의미

watchlist와 decision은 다르다.

- watchlist: "무엇을 볼지"
- decision pipeline: "그 후보를 실제로 얼마나 밀지"

`decision_operating_pipeline.py`는 watchlist를 입력으로 받아 `decision_run`, `decision_candidate`를 적재한다.

### 7-1. Stage 구조

- Stage0: 데이터 품질/신선도
- Stage1: 시장 레짐, risk-on/off 성향
- Stage2: 시장 수급 충격 + 종목별 수급
- Stage3: 이벤트/뉴스/클러스터 근거
- Stage4: 기술적 타이밍
- Stage5: 유동성/스프레드/실행 리스크

### 7-2. 현재 운영 정책의 핵심

가장 중요한 운영 포인트:

- 현재는 `DECISION_STAGE0_ONLY_CONSTRAINTS=1`이 기본이다.
- 즉 하드 차단은 사실상 Stage0 중심이다.
- Stage1~Stage5는 총점과 설명, sizing에 주로 반영된다.
- 예외적으로 Stage2는 market shock가 `EXTREME`일 때만 강하게 해석된다.

이건 README 문장보다 코드가 더 분명하다.
현재 decision pipeline은 "다단계 스코어링 엔진"이고, 그중 대부분 stage는 설명/사이징용이다.

### 7-3. BUY 액션 산출

후보별로:

- stage score들을 가중합해서 total score 생성
- block reason이 없고
- total이 mode threshold 이상이면 BUY
- total이 아주 낮으면 REDUCE

타겟 비중은 최대 10% 안쪽에서 아래 요소로 축소/확대된다.

- stage1 risk regime
- stage2 market shock
- stage3 evidence 강도
- stage5 execution multiplier

즉 decision pipeline은 "살까 말까"보다 "얼마나 강하게 살까"에도 큰 영향을 준다.

## 8. 브레인 프롬프트가 보는 것

`prepare_gpt_prompt.py`는 아래를 한 번에 합친다.

- 시장 레짐
- adaptive policy
- 잔고/현금/보유종목
- 현재 동적 TP/SL 상태
- 미체결 주문
- watchlist 상위/하위
- position snapshot
- 최근 뉴스/센티먼트/클러스터/이벤트 프레임
- hidden relation signals/reasonings
- news research
- earnings calendar
- latest decision debug
- web market signals
- DART
- 데이터 신선도
- persistent memory / HEARTBEAT / SOUL
- 긴급 뉴스 트리거 컨텍스트

중요한 설계 철학:

- 브레인은 raw DB 값과 요약 테이블을 동시에 본다.
- 규칙 점수보다 종합판단을 우선하라고 프롬프트에 적혀 있다.
- 하지만 출력 JSON은 엄격한 주문 스키마를 따라야 한다.

## 9. 주문 실행기의 실제 하드 가드레일

`execute_gpt_orders.py`가 실전의 마지막 문지기다.

### 9-1. 공통 차단

- 시장 세션이 닫혀 있으면 차단
- venue와 session이 맞지 않으면 차단
- 데이터 stale이면 차단
- kill switch 상태면 차단
- 6자리 종목코드 아니면 차단
- 수량 0 이하면 차단
- confidence 부족하면 차단
- 같은 종목 같은 방향 미체결 있으면 차단
- 일일 주문 제한 초과면 차단

### 9-2. BUY 추가 차단

- Stage2 EXTREME shock면 차단
- relation score가 임계값보다 낮으면 차단 가능
- `thesis_path`, `time_horizon`, `evidence_refs|urls` 없으면 차단
- 15:10 이후 신규 매수 금지
- RSI 과열 차단 옵션 가능
- EPS 음수면 매수 금지
- 주문금액 cap 초과면 차단
- 최소 현금비중 위반이면 차단
- 종목당 포트폴리오 비중 한도 초과면 차단

### 9-3. SELL 검증

- 보유 수량보다 많이 팔 수 없다.

### 9-4. 동적 TP/SL 및 강제 보호

이 실행기는 LLM 주문만 실행하는 게 아니다.
보호 주문도 자체 생성한다.

- risk_targets를 상태파일에 반영
- BUY 체결 시 초기 TP/SL 저장
- hard emergency stop loss
  - 기본값은 손익률 -8% 수준
  - LLM override 불가
- hard take profit
  - 기본값은 +15% 도달 시 일부 익절

즉 "LLM이 안 팔라고 했는데도 시스템이 강제로 파는" 경로가 존재한다.

## 10. 포지션 매니저 로직

`manage_positions.py`는 포트폴리오 유지 관리 전용 엔진이다.

### 10-1. 액션 셋

- `HOLD`
- `REDUCE`
- `EXIT`
- `ADD`
- `TIGHTEN_STOP`
- `TAKE_PROFIT_PARTIAL`
- `NO_ACTION_REVIEW_LATER`

### 10-2. 입력 컨텍스트

보유종목별로 다음을 모은다.

- 실보유 수량/평단/현재가/PnL
- 기술지표
- 수급 스냅샷
- 최근 뉴스 요약
- relation score
- prior thesis state
- 기존 dynamic exit 상태

### 10-3. 포지션 매니저의 내부 가드

LLM 결과를 그대로 쓰지 않고 다시 정규화한다.

- confidence 낮으면 HOLD로 강등
- `allow_add=false`면 ADD 금지
- cooldown 중이면 HOLD
- 종목별 daily action limit 초과면 HOLD
- 당일 EXIT 후 ADD 재진입 금지
- max_actions 초과분은 HOLD

그리고 액션별 size를 강제한다.

- EXIT: -100%
- REDUCE: 기본 -35%
- TAKE_PROFIT_PARTIAL: 기본 -40%
- ADD: 기본 +20%, 상한 50%

### 10-4. fallback 정책

LLM이 실패해도 포지션 매니저는 멈추지 않는다.

fallback 예:

- PnL <= -7%: EXIT
- PnL <= -4% and RSI 약세: REDUCE
- PnL >= 12% and RSI hot: TAKE_PROFIT_PARTIAL

그 후 결과를 다시 `trading_response` 형식으로 만들어 `execute_gpt_orders.py`에 넘긴다.
즉 보유 포지션 관리도 동일한 하드 가드레일을 탄다.

## 11. 상태파일과 테이블

### 11-1. 상태파일

- `~/.openclaw/state/codex_brain/events.jsonl`
- `~/.openclaw/state/stock_dynamic_exits.json`
- `~/.openclaw/state/position_manager_state.json`
- `~/.openclaw/state/news_urgent_context.json`
- `~/.openclaw/state/adaptive_policy.json`
- `~/.openclaw/state/kill_switch_state.json`

### 11-2. 핵심 ClickHouse 테이블

- `trading.market_regime`
- `trading.news`
- `trading.news_event_frames`
- `trading.event_memory`
- `trading.news_research`
- `trading.hidden_relation_signals`
- `trading.interest_watchlist`
- `trading.interest_watchlist_runs`
- `trading.position_snapshot`
- `trading.decision_run`
- `trading.decision_candidate`
- `trading.position_review_run`
- `trading.position_review_action`

## 12. OpenClaw에게 강의할 때의 권장 순서

### 12-1. 5분 버전

1. 이 시스템은 LLM 단독 봇이 아니라 데이터 파이프라인 기반 하이브리드 엔진이다.
2. watchlist가 후보를 정하고 decision pipeline이 점수화한다.
3. 브레인이 주문안을 만든다.
4. execute_gpt_orders가 실제로 대부분의 현실 제약을 강제한다.
5. manage_positions는 보유종목 전용 관리 루프다.

### 12-2. 15분 버전

1. `enrich_data.sh`가 시장/뉴스/연관/watchlist/decision을 만든다.
2. `prepare_gpt_prompt.py`가 포트폴리오와 시장 컨텍스트를 한 프롬프트로 묶는다.
3. `codex_brain.sh`가 OpenClaw agent를 호출하고 실패 시 codex로 폴백한다.
4. `execute_gpt_orders.py`가 session, stale, confidence, explainability, cash, weight, liquidity를 검사한다.
5. `manage_positions.py`는 HOLD/REDUCE/EXIT/ADD/TIGHTEN_STOP/TAKE_PROFIT_PARTIAL을 보유종목별로 돌린다.
6. 속보가 보유종목에 걸리면 `monitor_news.py`가 urgent trigger를 다시 태운다.

### 12-3. 반드시 교정해야 할 오해

- 오해 1: "브레인이 종목을 직접 발굴한다"
  - 실제로는 watchlist와 decision pipeline이 이미 후보 공간을 정의한다.
- 오해 2: "LLM이 BUY를 말하면 바로 주문된다"
  - 실제로는 execute_gpt_orders에서 많이 잘린다.
- 오해 3: "포지션 관리는 브레인 본체가 한다"
  - 실제로는 별도 포지션 매니저 루프가 있다.
- 오해 4: "뉴스는 보조 정보다"
  - 실제로는 뉴스 -> event frame -> relation -> research -> watchlist로 이어지는 핵심 입력이다.

## 13. OpenClaw에게 한 문장으로 설명하면

"이 시스템은 한국 주식용 event-driven hybrid trading OS이고, LLM은 그 안에서 후보 평가와 포지션 해석을 맡지만, 최종 체결 권한은 데이터 품질·리스크·실행 가능성을 보는 Python 가드레일 계층이 쥐고 있다."
