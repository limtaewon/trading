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
