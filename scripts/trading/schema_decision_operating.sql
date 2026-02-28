-- ============================================================
-- Decision Operating Spec (P0) - Core Tables
-- 실행: clickhouse-client < schema_decision_operating.sql
-- ============================================================

-- 1) 종목 일별 정규화 수급 (수량 + KRW + 거래대금 대비 비율)
CREATE TABLE IF NOT EXISTS trading.stock_flow_daily
(
    trade_date             Date,
    ticker                 String,
    market                 LowCardinality(String),     -- KOSPI/KOSDAQ/UNKNOWN
    investor_type          LowCardinality(String),     -- FOREIGN/INST
    net_buy_shares         Float64,
    net_buy_value_krw      Float64,
    foreign_ownership_pct  Float64 DEFAULT 0,
    traded_value_krw       Float64 DEFAULT 0,
    net_buy_pct_turnover   Float64 DEFAULT 0,          -- 100 * net_buy_value / traded_value
    source_session         LowCardinality(String) DEFAULT '',
    ingested_at            DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (trade_date, ticker, investor_type)
COMMENT 'feature_snapshot 기반 종목 일별 정규화 수급';

-- 2) 시장 일별 정규화 수급 (시장/전체 집계)
CREATE TABLE IF NOT EXISTS trading.market_flow_daily
(
    trade_date               Date,
    market                   LowCardinality(String),   -- KOSPI/KOSDAQ/ALL
    investor_type            LowCardinality(String),   -- FOREIGN/INST
    net_buy_value_krw        Float64,
    market_traded_value_krw  Float64,
    net_buy_pct_turnover     Float64,                  -- 100 * net_buy / market_traded
    market_traded_value_krw_source LowCardinality(String) DEFAULT 'UNKNOWN',
    market_traded_value_krw_universe_n UInt32 DEFAULT 0,
    n_tickers                UInt32,
    ingested_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (trade_date, market, investor_type)
COMMENT '시장 일별 정규화 수급 집계';

-- 3) 의사결정 실행 로그 (stage별 pass/score)
CREATE TABLE IF NOT EXISTS trading.decision_run
(
    decision_id             UUID,
    decision_time           DateTime,
    horizon                 LowCardinality(String),    -- INTRADAY / D1_3 / W1_2
    universe                LowCardinality(String),    -- watchlist / holdings / all
    stage0_pass             UInt8,
    stage0_score            Float32,
    stage1_pass             UInt8,
    stage1_score            Float32,
    stage2_pass             UInt8,
    stage2_score            Float32,
    stage3_pass             UInt8,
    stage3_score            Float32,
    stage4_pass             UInt8,
    stage4_score            Float32,
    stage5_pass             UInt8,
    stage5_score            Float32,
    total_score             Float32,
    penalty_score           Float32 DEFAULT 0,
    absolute_block_reason   Array(String) DEFAULT [],
    data_freshness_json     String DEFAULT '{}',
    stage_debug_json        String DEFAULT '{}',
    model_version           String DEFAULT 'decision-operating-spec-p0',
    prompt_hash             String DEFAULT '',
    created_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (decision_time, decision_id)
COMMENT 'Stage 기반 트레이딩 의사결정 실행 로그';

-- 4) 의사결정 후보 로그 (티커별 점수/행동/근거)
CREATE TABLE IF NOT EXISTS trading.decision_candidate
(
    decision_id                 UUID,
    ticker                      String,
    action                      LowCardinality(String), -- BUY/HOLD/REDUCE/SELL
    target_weight               Float32 DEFAULT 0,
    stage1_score                Float32 DEFAULT 0,
    stage2_stock_flow_score     Float32 DEFAULT 0,
    stage3_event_score          Float32 DEFAULT 0,
    stage4_timing_score         Float32 DEFAULT 0,
    stage5_risk_score           Float32 DEFAULT 0,
    stage5_fail_codes           Array(String) DEFAULT [],
    stage5_exec_multiplier      Float32 DEFAULT 1,
    stage3_evidence_count       UInt16 DEFAULT 0,
    stage3_score_capped         UInt8 DEFAULT 0,
    total_score                 Float32 DEFAULT 0,
    absolute_block_reason       Array(String) DEFAULT [],
    primary_cluster_id          String DEFAULT '',
    primary_event_frame_id      String DEFAULT '',
    primary_reasoning_id        String DEFAULT '',
    explanation_codes           Array(String) DEFAULT [],
    created_at                  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (decision_id, ticker)
COMMENT '티커별 의사결정 후보/근거 로그';

-- 5) 포지션 매니저 실행 로그 (보유종목 동적 관리)
CREATE TABLE IF NOT EXISTS trading.position_review_run
(
    review_id               String,
    review_time             DateTime,
    mode                    LowCardinality(String), -- position_manager
    holdings_count          UInt16,
    llm_status              LowCardinality(String), -- ok / fallback / llm_call_failed...
    market_regime           String,
    market_summary          String,
    proposed_actions        UInt16,
    executable_orders       UInt16,
    dry_run                 UInt8,
    response_path           String,
    created_at              DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (review_time, review_id)
COMMENT '포지션 매니저 실행 단위 로그';

-- 6) 포지션 매니저 티커별 액션 로그
CREATE TABLE IF NOT EXISTS trading.position_review_action
(
    review_id               String,
    review_time             DateTime,
    ticker                  String,
    ticker_name             String,
    action                  LowCardinality(String), -- HOLD/REDUCE/EXIT/ADD/...
    size_change_pct         Float32,
    confidence              Float32,
    thesis_status           LowCardinality(String),
    reasoning               String,
    invalidation            String,
    time_horizon            LowCardinality(String),
    evidence_refs           Array(String),
    risk_flags              Array(String),
    block_codes             Array(String),
    order_action            String, -- BUY/SELL/""
    order_qty               UInt32,
    created_at              DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (review_time, review_id, ticker)
COMMENT '포지션 매니저 티커별 액션/차단 사유 로그';

-- 7) Decision Replay 로그 (원본 decision_run 재현성 점검)
CREATE TABLE IF NOT EXISTS trading.decision_replay
(
    replay_id                UUID,
    decision_id              UUID,
    source_decision_time     DateTime,
    replay_time              DateTime,
    horizon                  LowCardinality(String),
    universe                 LowCardinality(String),
    candidate_count          UInt16,
    orig_stage2_score        Float32,
    orig_stage3_score        Float32,
    orig_stage4_score        Float32,
    orig_stage5_score        Float32,
    orig_total_score         Float32,
    recalc_stage2_score      Float32,
    recalc_stage3_score      Float32,
    recalc_stage4_score      Float32,
    recalc_stage5_score      Float32,
    recalc_total_score       Float32,
    diff_stage2_score        Float32,
    diff_stage3_score        Float32,
    diff_stage4_score        Float32,
    diff_stage5_score        Float32,
    diff_total_score         Float32,
    buy_count                UInt16,
    hold_count               UInt16,
    reduce_count             UInt16,
    replay_status            LowCardinality(String), -- PASS/FAIL
    replay_reason_codes      Array(String) DEFAULT [],
    detail_json              String DEFAULT '{}',
    created_at               DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (source_decision_time, decision_id, replay_time)
COMMENT 'decision_run vs decision_candidate 재집계 검증 로그';

-- 8) Decision Outcome 로그 (사후 성과 연결)
CREATE TABLE IF NOT EXISTS trading.decision_outcome
(
    decision_id             UUID,
    decision_time           DateTime,
    ticker                  String,
    action                  LowCardinality(String),
    horizon_days            UInt16, -- 1/3/5 trading days
    entry_date              Date,
    entry_price             Float64,
    exit_date               Date,
    exit_price              Float64,
    raw_return_pct          Float32,
    action_return_pct       Float32,
    max_drawdown_pct        Float32,
    max_runup_pct           Float32,
    realized_vol_pct        Float32,
    bars                    UInt16,
    resolved                UInt8, -- 1: horizon available, 0: pending/no-exit
    quality_code            LowCardinality(String),
    candidate_total_score   Float32,
    stage2_score            Float32,
    stage3_score            Float32,
    stage4_score            Float32,
    stage5_score            Float32,
    created_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (decision_time, decision_id, ticker, horizon_days)
COMMENT '의사결정 후보의 사후 수익률/리스크 성과 로그';
