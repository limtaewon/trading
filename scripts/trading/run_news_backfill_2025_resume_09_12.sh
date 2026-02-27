#!/bin/bash
set -euo pipefail

mkdir -p "$HOME/.openclaw/logs"
MASTER_LOG="$HOME/.openclaw/logs/news-backfill-2025-resume-09-12.log"

echo "[$(date '+%F %T')] RESUME START 2025-09..12" | tee -a "$MASTER_LOG"
for m in 09 10 11 12; do
  month="2025-$m"
  echo "[$(date '+%F %T')] >>> RUN $month" | tee -a "$MASTER_LOG"
  if bash "$HOME/.openclaw/scripts/trading/run_news_backfill_month.sh" "$month"; then
    echo "[$(date '+%F %T')] <<< DONE $month" | tee -a "$MASTER_LOG"
  else
    echo "[$(date '+%F %T')] !!! FAIL $month" | tee -a "$MASTER_LOG"
  fi
done

echo "[$(date '+%F %T')] RESUME END" | tee -a "$MASTER_LOG"
