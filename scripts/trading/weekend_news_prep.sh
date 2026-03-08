#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] weekend news prep start"

"$PYTHON_BIN" "$SCRIPT_DIR/collect_news.py" morning
"$PYTHON_BIN" "$SCRIPT_DIR/cluster_news.py" --window-hours "${WEEKEND_CLUSTER_WINDOW_HOURS:-72}" --threshold "${WEEKEND_CLUSTER_THRESHOLD:-0.48}" --limit "${WEEKEND_CLUSTER_LIMIT:-10000}" --min-size 2
"$PYTHON_BIN" "$SCRIPT_DIR/hidden_relation_scorer.py" --lookback-hours "${RELATION_LOOKBACK_HOURS:-168}" --limit "${RELATION_FRAME_LIMIT:-6000}" --max-tickers "${RELATION_MAX_TICKERS:-500}" --min-abs-score "${RELATION_MIN_ABS_SCORE:-0.0}"
"$PYTHON_BIN" "$SCRIPT_DIR/analyze_news_research.py"
"$PYTHON_BIN" "$SCRIPT_DIR/llm_relation_reasoner.py" --lookback-hours "${WEEKEND_RELATION_LOOKBACK_HOURS:-96}" --min-score "${WEEKEND_RELATION_MIN_SCORE:-0.10}" --top-tickers "${WEEKEND_RELATION_TOP_TICKERS:-40}" --events-per-ticker "${WEEKEND_RELATION_EVENTS_PER_TICKER:-6}" --states-per-ticker "${WEEKEND_RELATION_STATES_PER_TICKER:-4}" --cache-ttl-sec "${WEEKEND_RELATION_CACHE_TTL_SEC:-300}" --timeout-sec "${WEEKEND_RELATION_TIMEOUT_SEC:-180}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] weekend news prep done"
