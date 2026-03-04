CREATE TABLE IF NOT EXISTS trading.ticker_sector
(
    ticker       String,
    ticker_name  String,
    sector       String,
    sub_sector   String,
    theme_tags   Array(String),
    source       LowCardinality(String),
    confidence   Float32 DEFAULT 0.5,
    updated_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (ticker);

CREATE TABLE IF NOT EXISTS trading.report_prediction
(
    report_id             UUID,
    report_time           DateTime,
    decision_id           UUID,
    ticker                String,
    ticker_name           String,
    tier                  LowCardinality(String),
    predicted_direction   LowCardinality(String),
    predicted_action      LowCardinality(String),
    confidence            Float32,
    context_note          String,
    source                LowCardinality(String),
    actual_1d_return_pct  Nullable(Float32),
    actual_3d_return_pct  Nullable(Float32),
    actual_5d_return_pct  Nullable(Float32),
    hit_1d                Nullable(UInt8),
    created_at            DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (report_time, report_id, ticker);

CREATE TABLE IF NOT EXISTS trading.position_snapshot
(
    snapshot_time   DateTime,
    ticker          String,
    ticker_name     String,
    qty             Int32,
    avg_price       Float64,
    current_price   Float64,
    pnl_rate        Float32,
    eval_amount     Float64,
    take_profit_pct Float32,
    stop_loss_pct   Float32,
    pm_confidence   Float32,
    thesis_status   LowCardinality(String),
    source          LowCardinality(String),
    created_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (snapshot_time, ticker);

CREATE TABLE IF NOT EXISTS trading.earnings_calendar
(
    event_date      Date,
    ticker          String,
    ticker_name     String,
    event_name      String,
    event_source    LowCardinality(String),
    importance      UInt8,
    sentiment_hint  LowCardinality(String),
    confidence      Float32,
    raw_ref         String,
    created_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (event_date, ticker, event_source);
