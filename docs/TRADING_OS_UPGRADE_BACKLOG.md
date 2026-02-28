# Trading OS 업그레이드 백로그

이 문서는 현재 저장소(`scripts/trading/*`, `cron/*`) 기준으로, 시스템을 다음 단계로 올리기 위한 실행 백로그를 정리한다.

## 1) 현재 운영 흐름(기준선)
- 데이터 수집/정규화: `enrich_data.sh` -> `collect_market_data.py`, `technical_indicators.py`, `market_regime.py`, `collect_dart.py`, `sync_normalized_flow_daily.py`
- 뉴스/이벤트: `collect_news.py` -> `monitor_news.py` -> `cluster_news.py` -> `llm_relation_reasoner.py` -> `analyze_news_research.py`
- 후보 선정: `refresh_interest_watchlist.py` (rule + LLM rerank)
- 의사결정: `decision_operating_pipeline.py` (Stage0~5, `decision_run`, `decision_candidate`)
- 실행 검증/주문: `execute_gpt_orders.py` (신선도/현금/리스크/제약 검증 후 KIS 실행)
- 오케스트레이션: `cron/codex_jobs.json` + `codex_cron_router.sh` + `codex_brain.sh`

## 2) 업그레이드 목표
- 목표 A: 판단 품질을 정량적으로 개선(Replay + Outcome Loop)
- 목표 B: 종목 단위가 아닌 포트폴리오 단위 리스크 최적화
- 목표 C: 데이터 품질/관측성/운영 복구 자동화
- 목표 D: 브리핑의 점수-근거-집행 연계 일관성 강화

## 3) 우선순위 백로그

### P0 (즉시, 1~2주)

#### P0-1. Decision Replay 엔진
- 작업:
- `scripts/trading/replay_decision.py` 신규 추가
- 입력: `decision_id` 또는 `decision_time`
- 출력: Stage별 재계산 점수, 원본과 diff, 차단코드 비교
- 저장: `trading.decision_replay`
- 완료 기준:
- 동일 입력에서 재실행 시 Stage 점수 오차가 허용 범위 이내(`abs(diff)<=0.1`)
- 브리핑에서 `replay_status=PASS/FAIL` 조회 가능

#### P0-2. Outcome Join(사후 성과 연결)
- 작업:
- `scripts/trading/build_decision_outcome.py` 신규 추가
- `decision_candidate`와 이후 `N일 수익률`, `MDD`, `변동성`, `체결 품질` 조인
- 저장: `trading.decision_outcome`
- 완료 기준:
- 매일 장후 자동 집계(최근 30일 누락률 0%)
- 주간 리포트에 Stage별 성과 요약 자동 출력

#### P0-3. Stage2/Stage3/Stage5 무결성 강화
- 작업:
- Stage2: 분모/소스 불일치 탐지, shock/pass/warn/alert 정책 유지
- Stage3: 근거 0건이면 뉴스 점수 캡(`NO_EVIDENCE_CAP`) 고정
- Stage5: run-level Top3 제약 + `exec=0` 건수 고정 출력
- 완료 기준:
- 브리핑에서 점수와 근거가 충돌하지 않음
- `decision_run.stage_debug_json`에서 원인 재현 가능

#### P0-4. 테이블/잡 신선도 헬스체크
- 작업:
- `scripts/trading/healthcheck_pipeline.py` 신규 추가
- 핵심 테이블 최신시각, 누락률, 에러율 체크
- 실패 시 도메인별 경고(`DATA_STALE`, `TABLE_GAP`, `JOB_FAIL`)
- 완료 기준:
- 장중 1회 이상 자동 알림
- 경고 발생 시 즉시 재수집 트리거 가능

### P1 (단기, 2~4주)

#### P1-1. 포트폴리오 리스크 엔진
- 작업:
- `execute_gpt_orders.py`에 포트폴리오 제약 확장
- 섹터/테마 최대 비중, 상관군 동시보유 제한, 일손실/주손실 한도
- 완료 기준:
- `risk_block_code`에 포트폴리오 사유 기록
- 한 종목 시그널이 좋아도 포트폴리오 제약 시 자동 감산/차단

#### P1-2. 변동성 기반 포지션 사이징
- 작업:
- ATR/실현변동성 기반 수량 계산 함수 분리
- Stage1/2/5 상태에 따른 size multiplier 연동
- 완료 기준:
- 브리핑에 `suggested_size`와 `size_reason` 출력
- 급변동 장에서 주문 금액 분산 효과 확인

#### P1-3. 체결 품질 모델 고도화
- 작업:
- `execution_pred`와 실제 체결 로그 비교 학습
- 시간대/유동성/스프레드 기반 예상 슬리피지 모델 업데이트
- 완료 기준:
- `predicted_slippage`와 `realized_slippage` 오차 감소 추세 확인
- 주문 방식 선택(시장가/지정가/분할) 추천 가능

### P2 (중기, 1~2개월)

#### P2-1. 뉴스 출처 신뢰도 점수의 Stage3 편입
- 작업:
- `news_research.source_verdict`, `confidence`를 Stage3 가중치에 반영
- 불확실 출처는 설명용으로만 사용(결정 점수 영향 제한)
- 완료 기준:
- 출처 품질 낮은 뉴스로 인한 오판 비율 감소

#### P2-2. 레짐 적응형 가중치
- 작업:
- Stage 가중치/컷오프를 레짐별 정책으로 분리
- Replay/Outcome 결과 기반 반자동 튜닝
- 완료 기준:
- 고변동 구간 MDD 개선, 안정 구간 CAGR 훼손 최소화

#### P2-3. 공통 데이터 접근 모듈화
- 작업:
- ClickHouse 접근 중복 로직을 `scripts/trading/lib/ch_client.py`로 통합
- 재시도/타임아웃/포맷/인증 규칙 일원화
- 완료 기준:
- 쿼리 실패율 감소
- 스크립트별 중복 코드 감소

## 4) 측정 지표(KPI)
- 성과: CAGR, Sharpe, Sortino, MDD, Profit Factor
- 품질: Stage0 fail율, 데이터 stale 빈도, Replay 일치율
- 실행: 체결 성공률, 평균 슬리피지, 미체결률, 제약코드 분포
- 운영: 잡 실패율, 자동복구 성공률, 브리핑 누락률

## 5) 브리핑 고정 형식(운영자 가독성)
- 시장 요약(지수/수급/레짐)
- Stage0~5 점수 + PASS/WARN/ALERT
- 관찰 후보(점수/근거/집행 제약/원문 링크)
- LLM 해석(시장, 수급, 뉴스 연관, 매매전략, 핵심리스크)
- 실행 결론(`BUY/HOLD/REDUCE/SELL`)과 이유 코드

## 6) 즉시 실행 순서 제안
1. P0-1 Replay 엔진
2. P0-2 Outcome 조인
3. P0-4 Healthcheck 자동화
4. P1-1 포트폴리오 리스크 제약
5. P1-2 포지션 사이징

위 1~3이 먼저 완료되면, 이후 튜닝은 감이 아니라 데이터로 진행할 수 있다.
