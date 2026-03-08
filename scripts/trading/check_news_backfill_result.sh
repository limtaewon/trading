#!/bin/bash
set -euo pipefail

# 뉴스 백필 결과 검증기
# 사용:
#   bash ~/.openclaw/scripts/trading/check_news_backfill_result.sh 2026-03-07 2026-03-08 backfill_weekend_gap

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 START_DATE END_DATE [TRIGGER_TYPE]"
  exit 1
fi

START_DATE="$1"
END_DATE="$2"
TRIGGER_TYPE="${3:-backfill_range}"

if [[ ! "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Invalid start date: $START_DATE (expected YYYY-MM-DD)"
  exit 1
fi

if [[ ! "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Invalid end date: $END_DATE (expected YYYY-MM-DD)"
  exit 1
fi

source "$HOME/.openclaw/cron/codex_env.sh"

CLICKHOUSE_URL="${CLICKHOUSE_URL:-${CLICKHOUSE_HOST:-http://localhost:8123}}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASS="${CLICKHOUSE_PASS:-${CLICKHOUSE_PASSWORD:-}}"

python3 - "$CLICKHOUSE_URL" "$CLICKHOUSE_USER" "$CLICKHOUSE_PASS" "$START_DATE" "$END_DATE" "$TRIGGER_TYPE" <<'PY'
import base64
import json
import sys
import urllib.request

url, user, password, start_date, end_date, trigger_type = sys.argv[1:]

query = f"""
SELECT
  '{start_date}' AS start_date,
  '{end_date}' AS end_date,
  '{trigger_type}' AS trigger_type,
  count() AS rows,
  countDistinct(source_url) AS distinct_urls,
  min(formatDateTime(toTimeZone(collected_at, 'Asia/Seoul'), '%F %T')) AS first_collected_kst,
  max(formatDateTime(toTimeZone(collected_at, 'Asia/Seoul'), '%F %T')) AS last_collected_kst,
  min(formatDateTime(toTimeZone(published_at, 'Asia/Seoul'), '%F %T')) AS first_published_kst,
  max(formatDateTime(toTimeZone(published_at, 'Asia/Seoul'), '%F %T')) AS last_published_kst
FROM trading.news
WHERE trigger_type = '{trigger_type}'
  AND toDate(toTimeZone(published_at, 'Asia/Seoul')) BETWEEN toDate('{start_date}') AND toDate('{end_date}')
FORMAT JSONEachRow
"""

req = urllib.request.Request(url, data=query.encode("utf-8"))
token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
req.add_header("Authorization", f"Basic {token}")

with urllib.request.urlopen(req, timeout=60) as resp:
    body = resp.read().decode("utf-8").strip()

if not body:
    print(json.dumps({"rows": 0, "distinct_urls": 0}, ensure_ascii=False))
    raise SystemExit(0)

row = json.loads(body.splitlines()[0])
print(json.dumps(row, ensure_ascii=False, indent=2))
PY
