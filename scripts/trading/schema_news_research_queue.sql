-- 뉴스 심층 연구 비동기 큐 테이블
CREATE TABLE IF NOT EXISTS trading.news_research_queue
(
    enqueued_at            DateTime DEFAULT now(),
    news_id                String,                  -- sha256(published_at|source_url|title)
    published_at           DateTime,
    title                  String,
    summary                String,
    source_url             String,
    importance             UInt8,
    sentiment              LowCardinality(String),
    impact_type            LowCardinality(String),
    tickers                Array(String),

    status                 LowCardinality(String) DEFAULT 'pending', -- pending|processing|retry|done|dead
    retry_count            UInt16 DEFAULT 0,
    next_retry_at          DateTime DEFAULT now(),
    last_error             String DEFAULT '',
    source                 LowCardinality(String) DEFAULT 'collect_news',

    updated_at             DateTime DEFAULT now(),
    created_at             DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (news_id)
COMMENT 'news_research 비동기 큐';

