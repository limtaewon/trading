#!/bin/bash
set -euo pipefail

# 임의 날짜 범위 뉴스 백필 실행기
# 사용:
#   bash ~/.openclaw/scripts/trading/run_news_backfill_range.sh 2026-03-07 2026-03-08
#   NEWS_TRIGGER_TYPE=backfill_gap MAX_PAGES=6 MAX_NEWS_TOTAL=3000 REQUEST_DELAY=5 \
#     bash ~/.openclaw/scripts/trading/run_news_backfill_range.sh 2026-03-07 2026-03-08

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 YYYY-MM-DD YYYY-MM-DD"
  exit 1
fi

START_DATE="$1"
END_DATE="$2"

if [[ ! "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Invalid start date: $START_DATE (expected YYYY-MM-DD)"
  exit 1
fi

if [[ ! "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Invalid end date: $END_DATE (expected YYYY-MM-DD)"
  exit 1
fi

if [[ "$START_DATE" > "$END_DATE" ]]; then
  echo "start date must be <= end date"
  exit 1
fi

source "$HOME/.openclaw/cron/codex_env.sh"

export NEWS_TRIGGER_TYPE="${NEWS_TRIGGER_TYPE:-backfill_range}"
export BACKFILL_START_DATE="$START_DATE"
export BACKFILL_END_DATE="$END_DATE"
export BACKFILL_DAYS="${BACKFILL_DAYS:-0}"
export MAX_PAGES="${MAX_PAGES:-6}"
export MAX_NEWS_TOTAL="${MAX_NEWS_TOTAL:-2500}"
export REQUEST_DELAY="${REQUEST_DELAY:-5}"

mkdir -p "$HOME/.openclaw/logs"
LOG_PATH="$HOME/.openclaw/logs/news-backfill-${START_DATE}_to_${END_DATE}.log"

echo "[run_news_backfill_range] window=${START_DATE}~${END_DATE} max_pages=${MAX_PAGES} max_total=${MAX_NEWS_TOTAL}" | tee -a "$LOG_PATH"
python3 "$HOME/.openclaw/scripts/trading/collect_news.py" morning >> "$LOG_PATH" 2>&1
echo "[run_news_backfill_range] done window=${START_DATE}~${END_DATE}" | tee -a "$LOG_PATH"
