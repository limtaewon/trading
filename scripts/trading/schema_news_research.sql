-- 뉴스 심층 연구 결과 저장 테이블
CREATE TABLE IF NOT EXISTS trading.news_research
(
    analyzed_at            DateTime DEFAULT now(),
    news_id                String,                  -- sha256(published_at|source_url|title)
    published_at           DateTime,
    title                  String,
    source_url             String,
    source_domain          String,
    importance             UInt8,
    sentiment              LowCardinality(String),
    impact_type            LowCardinality(String),
    tickers_raw            Array(String),

    direct_tickers         Array(String),           -- 1차 직접 수혜/피해
    secondary_tickers      Array(String),           -- 2차 공급망/경쟁사
    tertiary_tickers       Array(String),           -- 3차 우회수혜/대체재

    source_verdict         LowCardinality(String),  -- valid|uncertain|conflict
    source_notes           String,

    hidden_point           String,                  -- 놓치기 쉬운 포인트
    followup_question      String,
    followup_plan          String,

    thesis                 LowCardinality(String),  -- bullish|bearish|mixed
    confidence             Float32,
    expected_horizon_days  UInt16,
    pnl_hypothesis         String,

    model                  String,
    model_output_json      String,
    created_at             DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (published_at, news_id)
COMMENT '뉴스 심층 연구(연관종목/출처검증/수익가설)';
