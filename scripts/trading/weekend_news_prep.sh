#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
WAIT_FOR_NEWS_LOCK_SEC="${WEEKEND_NEWS_PREP_WAIT_FOR_NEWS_LOCK_SEC:-5400}"
WAIT_FOR_NEWS_LOCK_POLL_SEC="${WEEKEND_NEWS_PREP_LOCK_POLL_SEC:-5}"
MAX_NEWS_AGE_MIN="${WEEKEND_NEWS_PREP_MAX_NEWS_AGE_MIN:-90}"

wait_for_collect_news_idle() {
  COLLECT_NEWS_LOCK_FILE="${COLLECT_NEWS_LOCK_FILE:-~/.openclaw/state/collect_news.lock}" \
  WEEKEND_NEWS_PREP_WAIT_FOR_NEWS_LOCK_SEC="$WAIT_FOR_NEWS_LOCK_SEC" \
  WEEKEND_NEWS_PREP_LOCK_POLL_SEC="$WAIT_FOR_NEWS_LOCK_POLL_SEC" \
  "$PYTHON_BIN" - <<'PY'
import fcntl
import os
import sys
import time
from pathlib import Path

lock_file = Path(os.path.expanduser(os.environ.get("COLLECT_NEWS_LOCK_FILE", "~/.openclaw/state/collect_news.lock")))
wait_sec = max(0, int(float(os.environ.get("WEEKEND_NEWS_PREP_WAIT_FOR_NEWS_LOCK_SEC", "5400"))))
poll_sec = max(0.2, float(os.environ.get("WEEKEND_NEWS_PREP_LOCK_POLL_SEC", "5")))
lock_file.parent.mkdir(parents=True, exist_ok=True)
fd = os.open(str(lock_file), os.O_CREAT | os.O_RDWR, 0o644)
deadline = time.time() + wait_sec
while True:
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        break
    except BlockingIOError:
        if wait_sec <= 0 or time.time() >= deadline:
            os.close(fd)
            sys.exit(2)
        time.sleep(poll_sec)
fcntl.flock(fd, fcntl.LOCK_UN)
os.close(fd)
PY
}

latest_news_age_min() {
  SCRIPT_DIR="$SCRIPT_DIR" "$PYTHON_BIN" - <<'PY'
import os
import sys

script_dir = os.environ["SCRIPT_DIR"]
sys.path.insert(0, script_dir)
from prepare_gpt_prompt import ch_query

rows = ch_query(
    "SELECT if(count()=0, 999999, greatest(dateDiff('minute', max(collected_at), now()), 0)) AS age_min FROM trading.news"
)
if not rows:
    print("999999")
else:
    print(rows[0].get("age_min", 999999))
PY
}

echo "[$(date '+%Y-%m-%d %H:%M:%S')] weekend news prep start"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] waiting for collect_news lock (max ${WAIT_FOR_NEWS_LOCK_SEC}s)"

if wait_for_collect_news_idle; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] collect_news lock clear"
else
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] collect_news lock wait timeout; proceed with existing data" >&2
fi

NEWS_AGE_MIN="$(latest_news_age_min | tail -n 1 | tr -d '\r')"
if [[ "$NEWS_AGE_MIN" =~ ^[0-9]+$ ]] && (( NEWS_AGE_MIN <= MAX_NEWS_AGE_MIN )); then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] latest news age ${NEWS_AGE_MIN}m <= ${MAX_NEWS_AGE_MIN}m; skip duplicate collect_news"
else
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] latest news age ${NEWS_AGE_MIN:-unknown}m; run collect_news morning"
  COLLECT_NEWS_WAIT_FOR_LOCK_SEC="$WAIT_FOR_NEWS_LOCK_SEC" \
  COLLECT_NEWS_WAIT_FOR_LOCK_POLL_SEC="$WAIT_FOR_NEWS_LOCK_POLL_SEC" \
  "$PYTHON_BIN" "$SCRIPT_DIR/collect_news.py" morning
fi

"$PYTHON_BIN" "$SCRIPT_DIR/cluster_news.py" --window-hours "${WEEKEND_CLUSTER_WINDOW_HOURS:-72}" --threshold "${WEEKEND_CLUSTER_THRESHOLD:-0.48}" --limit "${WEEKEND_CLUSTER_LIMIT:-10000}" --min-size 2
"$PYTHON_BIN" "$SCRIPT_DIR/hidden_relation_scorer.py" --lookback-hours "${RELATION_LOOKBACK_HOURS:-168}" --limit "${RELATION_FRAME_LIMIT:-6000}" --max-tickers "${RELATION_MAX_TICKERS:-500}" --min-abs-score "${RELATION_MIN_ABS_SCORE:-0.0}"
"$PYTHON_BIN" "$SCRIPT_DIR/analyze_news_research.py"
"$PYTHON_BIN" "$SCRIPT_DIR/llm_relation_reasoner.py" --lookback-hours "${WEEKEND_RELATION_LOOKBACK_HOURS:-96}" --min-score "${WEEKEND_RELATION_MIN_SCORE:-0.10}" --top-tickers "${WEEKEND_RELATION_TOP_TICKERS:-40}" --events-per-ticker "${WEEKEND_RELATION_EVENTS_PER_TICKER:-6}" --states-per-ticker "${WEEKEND_RELATION_STATES_PER_TICKER:-4}" --cache-ttl-sec "${WEEKEND_RELATION_CACHE_TTL_SEC:-300}" --timeout-sec "${WEEKEND_RELATION_TIMEOUT_SEC:-180}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] weekend news prep done"
