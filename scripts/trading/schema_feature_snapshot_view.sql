CREATE VIEW IF NOT EXISTS trading.v_feature_snapshot AS
SELECT
    ts,
    symbol,
    session,
    toFloat64(price) AS price,
    toFloat64(vwap) AS vwap,
    toFloat64(atr14) AS atr14,
    toFloat64(rsi14) AS rsi14,
    toFloat64(spread_bp) AS spread_bp,
    toFloat64(liquidity_krw) AS liquidity_krw,
    toFloat64(foreign_flow) AS foreign_flow,
    toFloat64(inst_flow) AS inst_flow,
    toFloat64(news_event_score) AS news_event_score,
    toFloat64(dart_event_score) AS dart_event_score,
    regime_label,
    toFloat64(foreign_flow) AS foreign_ownership_pct,
    toFloat64(news_event_score) AS foreign_net_flow,
    toFloat64(inst_flow) AS inst_net_flow
FROM trading.feature_snapshot;
