#!/bin/bash
set -euo pipefail

mkdir -p "$HOME/.openclaw/logs"
MASTER_LOG="$HOME/.openclaw/logs/news-backfill-2025-all-months.log"

echo "[$(date '+%F %T')] START 2025 all-month backfill" | tee -a "$MASTER_LOG"

for i in $(seq 1 12); do
  m=$(printf "%02d" "$i")
  month="2025-$m"
  echo "[$(date '+%F %T')] >>> RUN $month" | tee -a "$MASTER_LOG"
  # 필요 시 환경변수로 조정 가능: MAX_NEWS_TOTAL, MAX_PAGES, REQUEST_DELAY
  if bash "$HOME/.openclaw/scripts/trading/run_news_backfill_month.sh" "$month"; then
    echo "[$(date '+%F %T')] <<< DONE $month" | tee -a "$MASTER_LOG"
  else
    echo "[$(date '+%F %T')] !!! FAIL $month" | tee -a "$MASTER_LOG"
  fi

done

echo "[$(date '+%F %T')] END 2025 all-month backfill" | tee -a "$MASTER_LOG"
