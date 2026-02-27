#!/bin/bash
set -euo pipefail

# 월별 뉴스 백필 실행기
# 사용:
#   bash ~/.openclaw/scripts/trading/run_news_backfill_month.sh 2025-01
#   MAX_NEWS_TOTAL=4000 MAX_PAGES=20 REQUEST_DELAY=5 bash ~/.openclaw/scripts/trading/run_news_backfill_month.sh 2025-12

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 YYYY-MM"
  exit 1
fi

MONTH="$1"
if [[ ! "$MONTH" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
  echo "Invalid month format: $MONTH (expected YYYY-MM)"
  exit 1
fi

START_DATE="${MONTH}-01"
# GNU date(gdate) 우선, 없으면 BSD date 사용
if command -v gdate >/dev/null 2>&1; then
  END_DATE="$(gdate -d "${START_DATE} +1 month -1 day" +%F)"
else
  END_DATE="$(date -j -v+1m -f "%Y-%m-%d" "$START_DATE" +"%Y-%m-01" | xargs -I{} date -j -v-1d -f "%Y-%m-%d" {} +%F)"
fi

source "$HOME/.openclaw/cron/codex_env.sh"

export NEWS_TRIGGER_TYPE="${NEWS_TRIGGER_TYPE:-backfill_month}"
export BACKFILL_START_DATE="$START_DATE"
export BACKFILL_END_DATE="$END_DATE"
export BACKFILL_DAYS="${BACKFILL_DAYS:-0}"
export MAX_PAGES="${MAX_PAGES:-10}"
export MAX_NEWS_TOTAL="${MAX_NEWS_TOTAL:-2500}"
export REQUEST_DELAY="${REQUEST_DELAY:-10}"

mkdir -p "$HOME/.openclaw/logs"
LOG_PATH="$HOME/.openclaw/logs/news-backfill-${MONTH}.log"

echo "[run_news_backfill_month] month=$MONTH window=${START_DATE}~${END_DATE} max_pages=${MAX_PAGES} max_total=${MAX_NEWS_TOTAL}" | tee -a "$LOG_PATH"
python3 "$HOME/.openclaw/scripts/trading/collect_news.py" morning >> "$LOG_PATH" 2>&1

echo "[run_news_backfill_month] done month=$MONTH" | tee -a "$LOG_PATH"
