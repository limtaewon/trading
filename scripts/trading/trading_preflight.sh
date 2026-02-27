#!/bin/bash
# OpenClaw trading preflight (read-only checks only)
set -uo pipefail

TS="$(date '+%Y%m%d_%H%M%S')"
LOG_DIR="$HOME/.openclaw/logs"
LOG_FILE="$LOG_DIR/trading_preflight_${TS}.log"
mkdir -p "$LOG_DIR"
CODEX_CANDIDATES=(
  "openclaw"
  "$HOME/.openclaw/bin/openclaw"
  "$HOME/.npm-global/bin/openclaw"
  "/opt/homebrew/bin/openclaw"
  "/usr/local/bin/openclaw"
  "$HOME/.npm-global/bin/codex-spark"
  "/opt/homebrew/bin/codex-spark"
  "/usr/local/bin/codex-spark"
  "/usr/bin/codex-spark"
  "codex-spark"
)

ok=0
fail=0

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
pass() { ok=$((ok+1)); log "PASS: $*"; }
ng() { fail=$((fail+1)); log "FAIL: $*"; }

run_check() {
  local title="$1"
  shift
  if "$@" >>"$LOG_FILE" 2>&1; then
    pass "$title"
  else
    ng "$title"
  fi
}

find_codemark_binary() {
  local candidate
  for candidate in "${CODEX_CANDIDATES[@]}"; do
    if [ -x "$candidate" ] 2>/dev/null; then
      echo "$candidate"
      return 0
    fi
    if command -v "$candidate" >/dev/null 2>&1; then
      command -v "$candidate"
      return 0
    fi
  done
  return 1
}

run_kis_check() {
  local title="$1"
  shift
  local tmp_file rt_cd msg
  local tries=3
  local ok_flag=0
  local n

  for n in $(seq 1 "$tries"); do
    tmp_file="$(mktemp)"
    if mcporter call "$@" --output json >"$tmp_file" 2>>"$LOG_FILE"; then
      cat "$tmp_file" >>"$LOG_FILE"
      rt_cd="$(jq -r 'if type=="object" and has("rt_cd") then (.rt_cd|tostring) else "" end' "$tmp_file" 2>/dev/null || true)"
      if [ -z "$rt_cd" ] || [ "$rt_cd" = "0" ]; then
        ok_flag=1
        rm -f "$tmp_file"
        break
      fi
      msg="$(jq -r '.msg1 // .msg_cd // "unknown_error"' "$tmp_file" 2>/dev/null || true)"
      log "KIS 응답 오류(${n}/${tries}): rt_cd=$rt_cd msg=$msg"
    fi
    rm -f "$tmp_file"
    sleep 2
  done

  if [ "$ok_flag" -eq 1 ]; then
    pass "$title"
  else
    ng "$title"
  fi
}

log "=== Trading Preflight Start ==="

CODEX_BIN="$(find_codemark_binary || true)"
if [ -n "$CODEX_BIN" ]; then
  if [[ "$CODEX_BIN" == *openclaw* ]]; then
    pass "openclaw binary: $CODEX_BIN"
  elif [[ "$CODEX_BIN" == *codex-spark* ]]; then
    pass "legacy codex-spark binary found: $CODEX_BIN"
  else
    run_check "LLM binary executable" test -x "$CODEX_BIN"
  fi
else
  ng "LLM binary not found"
  fail=$((fail+0))
fi
run_check "openclaw binary" command -v openclaw
run_check "mcporter binary" command -v mcporter

if [ -f "$HOME/.openclaw/config/mcporter.json" ]; then
  acct_type="$(jq -r '.mcpServers["kis-trading"].env.KIS_ACCOUNT_TYPE // ""' "$HOME/.openclaw/config/mcporter.json" 2>/dev/null || true)"
  cano="$(jq -r '.mcpServers["kis-trading"].env.KIS_CANO // ""' "$HOME/.openclaw/config/mcporter.json" 2>/dev/null || true)"
  acnt_cd="$(jq -r '.mcpServers["kis-trading"].env.KIS_ACNT_PRDT_CD // ""' "$HOME/.openclaw/config/mcporter.json" 2>/dev/null || true)"
  if [ "$acct_type" = "REAL" ] && [ -n "$cano" ] && [ -n "$acnt_cd" ]; then
    pass "KIS account profile REAL (${cano}-${acnt_cd})"
  else
    ng "KIS account profile REAL (${cano}-${acnt_cd})"
  fi
else
  ng "mcporter config missing"
fi

if mcporter list >>"$LOG_FILE" 2>&1; then
  if mcporter list 2>/dev/null | rg -q "kis-trading" && mcporter list 2>/dev/null | rg -q "mcp-clickhouse"; then
    pass "MCP servers (kis-trading, mcp-clickhouse)"
  else
    ng "MCP servers (kis-trading, mcp-clickhouse)"
  fi
else
  ng "mcporter list"
fi

if mcporter call mcp-clickhouse.run_select_query query="SELECT count() FROM trading.news_raw WHERE toDate(collected_at)=today()" --output json >>"$LOG_FILE" 2>&1; then
  pass "ClickHouse query (news_raw today)"
else
  ng "ClickHouse query (news_raw today)"
fi

run_kis_check "KIS quote (005930)" kis-trading.inquery-stock-price symbol=005930
run_kis_check "KIS balance" kis-trading.inquery-balance

crontab_text="$(crontab -l 2>/dev/null || true)"
if [ -n "$crontab_text" ]; then
  if echo "$crontab_text" | rg -q "collect_news.py" && echo "$crontab_text" | rg -q "monitor_news.py"; then
    pass "crontab core news jobs"
  else
    if [ -f "$HOME/.openclaw/cron/codex_jobs.json" ] && jq -e '.jobs[] | select(.payload.kind=="command" and (.payload.command|contains("collect_news.py")))' "$HOME/.openclaw/cron/codex_jobs.json" >/dev/null 2>&1 \
      && jq -e '.jobs[] | select(.payload.kind=="command" and (.payload.command|contains("monitor_news.py")))' "$HOME/.openclaw/cron/codex_jobs.json" >/dev/null 2>&1; then
      pass "codex jobs core news jobs"
    else
      ng "core news jobs (crontab/codex jobs)"
    fi
  fi
else
  if [ -f "$HOME/.openclaw/cron/codex_jobs.json" ] && jq -e '.jobs[] | select(.payload.kind=="command" and (.payload.command|contains("collect_news.py")))' "$HOME/.openclaw/cron/codex_jobs.json" >/dev/null 2>&1 \
    && jq -e '.jobs[] | select(.payload.kind=="command" and (.payload.command|contains("monitor_news.py")))' "$HOME/.openclaw/cron/codex_jobs.json" >/dev/null 2>&1; then
    pass "codex jobs core news jobs"
  else
    ng "crontab readable"
  fi
fi

if [ -f "$HOME/.openclaw/cron/jobs.json" ]; then
  enabled="$(jq '[.jobs[]|select(.enabled==true)]|length' "$HOME/.openclaw/cron/jobs.json" 2>/dev/null || echo 0)"
  if [ "${enabled:-0}" -gt 0 ]; then
    pass "openclaw jobs enabled=${enabled}"
  else
    ng "openclaw jobs enabled=0"
  fi
else
  ng "openclaw jobs.json missing"
fi

log "=== Trading Preflight End | pass=$ok fail=$fail ==="
echo "$LOG_FILE"

if [ "$fail" -gt 0 ]; then
  exit 1
fi
exit 0
