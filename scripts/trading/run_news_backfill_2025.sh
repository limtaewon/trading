#!/bin/bash
set -euo pipefail

# 2025년 뉴스 "베스트에포트" 백필 러너.
#
# 주의:
# - Naver OpenAPI는 쿼리당 pagination이 1,000건 제한이라 "2025년 전체"를 보장할 수 없습니다.
# - 실행시간이 길고(수십분~수시간), LLM 분석(Codex CLI) 비용이 증가할 수 있습니다.
# - 기본은 안전하게 소량만 적재하도록 캡을 걸어둡니다. 필요 시 환경변수로 조정하세요.
#
# 예:
#   MAX_NEWS_TOTAL=8000 MAX_PAGES=33 REQUEST_DELAY=5 bash ~/.openclaw/scripts/trading/run_news_backfill_2025.sh

source "$HOME/.openclaw/cron/codex_env.sh"

export NEWS_TRIGGER_TYPE="${NEWS_TRIGGER_TYPE:-backfill_2025}"
export BACKFILL_DAYS="${BACKFILL_DAYS:-420}"
export MAX_PAGES="${MAX_PAGES:-10}"
export MAX_NEWS_TOTAL="${MAX_NEWS_TOTAL:-2500}"
export REQUEST_DELAY="${REQUEST_DELAY:-10}"

mkdir -p "$HOME/.openclaw/logs"
python3 "$HOME/.openclaw/scripts/trading/collect_news.py" morning >> "$HOME/.openclaw/logs/news-backfill-2025.log" 2>&1
