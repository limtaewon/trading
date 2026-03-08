# Reporting Redesign Plan

이 문서는 현재 `OpenClaw + trading repo` 기준으로 보고 체계를 재설계하기 위한 기준 문서다.

## 1. 목표

- 채널마다 다른 데이터 소스를 쓰는 문제를 줄인다.
- `Dooray`, `Telegram owner`, `Telegram public`이 같은 사실과 같은 정책을 보게 한다.
- LLM은 설명만 맡고, 판단은 시스템이 하도록 유지한다.
- 첫 구현 타겟은 `scripts/trading/morning_briefing.py` 전면 개편이다.

## 2. 현재 문제

- `send_dooray_briefing.py`는 `decision_run/decision_candidate` 축에 가깝다.
- `morning_briefing.py`는 여전히 `technical_signals + recent news` 중심이다.
- `execute_gpt_orders.py`의 Telegram brief는 체결/미체결 요약은 되지만 전략 설명과 포트 변화 설명이 약하다.
- `weekly_market_report.py`는 시장/전략 보고 성격이 강하고 실제 운용 복기는 부족하다.
- 공개 채널과 private 채널의 redaction 규칙이 아직 구조화돼 있지 않다.

## 3. 아키텍처 원칙

- `data pipeline = 사실`
- `rule engine = 결정`
- `LLM = 설명`

권장 구조:

```text
system data
  -> report_payload_builder
  -> payload.json
  -> LLM report generator
  -> channel renderer
  -> Dooray / Telegram owner / Telegram public
```

## 4. First Refactor Target

대상: `scripts/trading/morning_briefing.py`

목표:

- Dooray와 같은 기준 사실을 보게 한다.
- `execution_mode`, `stress_flags`, `allowed_actions`, `blocked_actions`, `mode_change_trigger`를 메시지 중심에 둔다.
- Telegram owner 아침 브리핑을 "전략 설명"이 아니라 "정책 설명 + 상황 요약"으로 바꾼다.

## 5. Shared Payload

공용 payload는 최소 아래 top-level context를 포함한다.

```json
{
  "report_type": "dooray_internal|telegram_owner_ops|telegram_owner_execution|telegram_public|weekly_review|weekly_outlook",
  "audience": "internal|owner|public",
  "generated_at": "ISO8601",
  "as_of": "ISO8601",
  "market_context": {},
  "mode_context": {},
  "event_context": {},
  "candidate_context": {},
  "execution_context": {},
  "change_context": {},
  "guidance_context": {},
  "ops_context": {}
}
```

세부 구조는 아래 원칙을 따른다.

- `market_context`
  - 지수, 변동성, 환율, 수급, `market_phase`
- `mode_context`
  - `execution_mode`, `allowed_actions`, `blocked_actions`, `mode_change_triggers`
- `event_context`
  - `top_events`, `dominant_theme`, `theme_summary`
- `candidate_context`
  - `selection_policy`, `top_candidates`, `avoid_list`
- `execution_context`
  - 주문 시도/체결/스킵, `portfolio_delta`
- `change_context`
  - 이전 리포트 대비 변화
- `guidance_context`
  - 오늘 원칙, 관찰 포인트, `what_changes_my_mind`
- `ops_context`
  - 파이프라인 상태, alerts, freshness

## 6. Channel Rules

### 6.1 Dooray Internal

- 목적: 내부 전략 브리핑
- 성격: 분석, 정책, 후보 설명
- 표시 가능:
  - execution mode
  - stress flags
  - decision candidates
  - why not buy / why hold
  - mode change trigger
- 출력 우선순위:
  1. 오늘 모드
  2. 핵심 리스크
  3. 허용 전략
  4. 금지 전략
  5. 후보 종목과 이유

### 6.2 Telegram Owner

- 목적: 운영/실행 모니터링
- 성격: 짧고 사실 중심
- 표시 가능:
  - orders attempted/executed/skipped
  - skip reason
  - pending exit
  - watchlist health
  - news pipeline health
  - portfolio delta
- 출력 우선순위:
  1. mode
  2. execution summary
  3. exceptions
  4. risk state

### 6.3 Telegram Public

- 목적: 투자 설명 / 교육
- 성격: 쉬운 언어, 추천처럼 보이지 않게
- 표시 가능:
  - 시장 상황
  - execution mode 의미
  - 허용/금지 전략
  - 관찰 종목과 이유
  - 무엇이 바뀌면 전략이 바뀌는지
- 표시 금지:
  - 계좌 잔고
  - 정확한 수량
  - target weight
  - raw skip reason code
  - broker error

## 7. Actual Message Templates

### 7.1 Dooray Internal Template

```text
[오늘 모드]
- execution_mode: {execution_mode}
- posture: {action_posture}
- 핵심 리스크: {stress_flags_top3}
- 허용: {allowed_actions}
- 금지: {blocked_actions}

[어제 대비 바뀐 점]
- {top_change_summary}

[시장 상황]
- KOSPI {kospi_value} ({kospi_pct}%)
- KOSDAQ {kosdaq_value} ({kosdaq_pct}%)
- VIX {vix}, USD/KRW {usdkrw}
- 해석: {market_summary}

[핵심 이벤트]
- {event_1}
- {event_2}

[후보 종목]
- {name_1}: {thesis_1} / 현재 판단 {action_1} / 신규매수 제한 이유 {why_not_buy_1}
- {name_2}: {thesis_2} / 현재 판단 {action_2} / 신규매수 제한 이유 {why_not_buy_2}

[오늘 실행 원칙]
- {allowed_action_1}
- {allowed_action_2}
- 모드 전환 조건: {mode_change_trigger}
```

### 7.2 Telegram Owner Morning Template

```text
OWNER MORNING DIGEST

mode: {execution_mode}
stress: {stress_flags_short}

market:
KOSPI {kospi_pct} / KOSDAQ {kosdaq_pct}
VIX {vix} / USDKRW {usdkrw}

policy:
allow {allowed_actions_short}
block {blocked_actions_short}

watch:
{watch_name_1}, {watch_name_2}

change:
{top_change_summary}
```

### 7.3 Telegram Owner Execution Template

```text
EXECUTION REPORT

mode: {execution_mode}
attempted: {attempted}
executed: {executed}
skipped: {skipped}

top skip:
{skip_reason_top}

portfolio:
cash {cash_before}% -> {cash_after}%

risk:
{risk_state}
```

### 7.4 Telegram Public Template

```text
오늘 시장은 {market_state_one_liner}.

VIX는 {vix}, 환율은 {usdkrw} 수준이라
현재는 {execution_mode_kor}로 해석합니다.

이 환경에서 시스템은
{allowed_actions_short} 중심으로 대응하고,
{blocked_actions_short}는 제한합니다.

오늘 관찰 종목은 {watch_name_1}, {watch_name_2}입니다.
이 종목들은 {watch_reason_short} 때문에 관찰 대상이지만
지금은 추격보다 확인이 우선입니다.

전략이 바뀌려면
{mode_change_trigger_short}
같은 조건이 먼저 확인돼야 합니다.
```

## 8. File Change Plan

### Phase 1

- Add `scripts/trading/report_payload_schema.json`
- Add `scripts/trading/report_payload_builder.py`
- Refactor `scripts/trading/morning_briefing.py`

### Phase 2

- Add `scripts/trading/report_renderer_dooray.py`
- Add `scripts/trading/report_renderer_telegram_owner.py`
- Add `scripts/trading/report_renderer_telegram_public.py`
- Wire `send_dooray_briefing.py` to shared payload

### Phase 3

- Expand `execute_gpt_orders.py` execution brief using shared payload
- Split `weekly_market_report.py` into:
  - `weekly_review_report.py`
  - `weekly_outlook_report.py`

## 9. morning_briefing.py Detailed Refactor

현재:

- market_regime
- market_index
- exchange_rate
- technical_signals
- recent news

개편 후:

- latest execution mode / action posture
- latest market snapshot
- stress flags / mode reason
- top decision candidates
- allowed_actions / blocked_actions
- delta vs prev

제거 또는 축소:

- 단순 technical_signals 기반 buy list
- "매수 후보" 중심 표현

추가:

- 정책 중심 문구
- 관찰 종목 2~3개
- 모드 전환 조건

## 10. Rollout Order

1. `morning_briefing.py`만 shared payload builder를 먼저 쓰게 한다.
2. owner Telegram이 안정되면 Dooray도 같은 payload로 전환한다.
3. public Telegram renderer는 마지막에 붙인다.

이 순서로 가면 가장 큰 drift를 가장 먼저 줄일 수 있다.
