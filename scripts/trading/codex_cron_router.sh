#!/bin/bash
# codex_cron_router.sh
# OpenClaw cron payload를 Codex 중심 실행기로 라우팅한다.
#
# usage:
#   bash ~/.openclaw/scripts/trading/codex_cron_router.sh --job-name "pre-market"

set -euo pipefail

JOB_NAME=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --job-name)
            JOB_NAME="${2:-}"
            shift 2
            ;;
        *)
            echo "Unknown arg: $1" >&2
            exit 2
            ;;
    esac
done

if [[ -z "$JOB_NAME" ]]; then
    echo "Usage: $0 --job-name <name>" >&2
    exit 2
fi

BASE="$HOME/.openclaw"
TRADING_SCRIPTS="$BASE/scripts/trading"
JOBS_FILE="$BASE/cron/codex_jobs.json"
LEGACY_JOBS_FILE="$BASE/cron/jobs.json"
LOG_FILE="$BASE/logs/codex-trading-cron.log"
STATE_DIR="$BASE/state/codex_brain"
JOURNAL_FILE="$STATE_DIR/events.jsonl"
PROMPT_FILE="$BASE/workspace/CODEX_PERSISTENT_MEMORY.md"
ORDER_EXEC="$TRADING_SCRIPTS/execute_gpt_orders.py"
COIN_RUNNER="$BASE/scripts/coin_codex_runner.py"
LLM_BACKEND="${LLM_EXEC_BACKEND:-${OPENCLAW_LLM_BACKEND:-openclaw}}"
CODEX_BIN="${CODEX_BIN:-openclaw}"
CODEX_MODEL="${CODEX_MODEL:-openai-codex/gpt-5.3-codex-spark}"
OPENCLAW_SESSION_ID="${OPENCLAW_SESSION_ID:-openclaw-codex-router}"
CODEX_JOB_LOCK_WAIT="${CODEX_JOB_LOCK_WAIT:-45}"
JOB_LOCK_DIR="$STATE_DIR/locks"
JOB_LOCK_PATH="$JOB_LOCK_DIR/${JOB_NAME}.lock"

mkdir -p "$STATE_DIR" "$BASE/logs"

# cron 최소 PATH 환경에서 mcporter/codex/python을 안정적으로 찾도록 보강.
export PATH="/opt/homebrew/bin:/usr/local/bin:$HOME/.npm-global/bin:$HOME/.local/bin:$PATH"

ts_now() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$JOB_NAME] $*" >> "$LOG_FILE"; }
notify_telegram() {
    local msg="$1"
    python3 "$BASE/scripts/telegram_notify.py" "$msg" >> "$LOG_FILE" 2>&1 || true
}

release_job_lock() {
    if [[ -d "$JOB_LOCK_PATH" && -f "$JOB_LOCK_PATH/pid" ]]; then
        local pid
        pid="$(cat "$JOB_LOCK_PATH/pid" 2>/dev/null || true)"
        if [[ "$pid" == "$$" ]]; then
            rm -rf "$JOB_LOCK_PATH" >/dev/null 2>&1 || true
        fi
    elif [[ -d "$JOB_LOCK_PATH" ]]; then
        rm -rf "$JOB_LOCK_PATH" >/dev/null 2>&1 || true
    fi
}

acquire_job_lock() {
    local waited=0
    mkdir -p "$JOB_LOCK_DIR"
    while true; do
        if mkdir "$JOB_LOCK_PATH" 2>/dev/null; then
            echo "$$" > "$JOB_LOCK_PATH/pid"
            return 0
        fi

        local lock_pid=""
        if [[ -f "$JOB_LOCK_PATH/pid" ]]; then
            lock_pid="$(cat "$JOB_LOCK_PATH/pid" 2>/dev/null || true)"
        fi
        if [[ -n "$lock_pid" ]] && ! ps -p "$lock_pid" >/dev/null 2>&1; then
            rm -rf "$JOB_LOCK_PATH" >/dev/null 2>&1 || true
            continue
        fi

        if (( waited >= CODEX_JOB_LOCK_WAIT )); then
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
}

trap release_job_lock EXIT

if [[ ! -f "$JOBS_FILE" ]]; then
    # 통합 파일이 없으면 레거시 파일 fallback
    JOBS_FILE="$LEGACY_JOBS_FILE"
fi

if ! acquire_job_lock; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$JOB_NAME] duplicate job lock skip" >> "$LOG_FILE"
    exit 0
fi
if [[ ! -f "$JOBS_FILE" ]]; then
    log "ERROR: jobs file missing: $JOBS_FILE"
    notify_telegram "❌ codex_cron_router 실패: jobs file missing (${JOBS_FILE})"
    exit 1
fi

# codex_jobs.json / jobs.json 모두 대응
JOB_JSON="$(jq -c --arg name "$JOB_NAME" '
    if has("jobs") then
        .jobs[] | select(.name == $name)
    else
        .[] | select(.name == $name)
    end
' "$JOBS_FILE" | head -n 1)"
if [[ -z "$JOB_JSON" ]]; then
    log "ERROR: job not found in jobs.json"
    notify_telegram "❌ codex_cron_router 실패: job not found (${JOB_NAME})"
    exit 1
fi

PAYLOAD_KIND="$(printf '%s' "$JOB_JSON" | jq -r '.payload.kind // "systemEvent"')"
SCHEDULE_EXPR="$(printf '%s' "$JOB_JSON" | jq -r '.schedule.expr // ""')"
EVENT_TEXT="$(
    printf '%s' "$JOB_JSON" | jq -r '
        if .payload.kind == "agentTurn"
        then (.payload.message // "")
        else (.payload.text // "")
        end
    '
)"

START_EPOCH="$(date +%s)"
START_TS="$(ts_now)"

jq -nc \
    --arg ts "$START_TS" \
    --arg event "start" \
    --arg job_name "$JOB_NAME" \
    --arg payload_kind "$PAYLOAD_KIND" \
    --arg schedule "$SCHEDULE_EXPR" \
    --arg event_text "$EVENT_TEXT" \
    '{timestamp:$ts,event:$event,job_name:$job_name,payload_kind:$payload_kind,schedule:$schedule,event_text:$event_text}' \
    >> "$JOURNAL_FILE"

log "START kind=$PAYLOAD_KIND schedule='$SCHEDULE_EXPR'"

STATUS="ok"
ERROR_MSG=""
URGENT_CTX_FILE="$BASE/state/news_urgent_context.json"

if [[ "$PAYLOAD_KIND" == "systemEvent" ]]; then
    if [[ "$JOB_NAME" == coin-* ]]; then
    if ! python3 "$COIN_RUNNER" --job-name "$JOB_NAME" >> "$LOG_FILE" 2>&1; then
        STATUS="error"
        ERROR_MSG="coin_codex_runner failed"
    fi
    else
    # Trading brain run
    export OPENCLAW_SYSTEM_EVENT="$EVENT_TEXT"
    export OPENCLAW_EVENT_NAME="$JOB_NAME"
    if ! bash "$TRADING_SCRIPTS/codex_brain.sh" >> "$LOG_FILE" 2>&1; then
        STATUS="error"
        ERROR_MSG="codex_brain.sh failed"
    fi
    fi
    elif [[ "$PAYLOAD_KIND" == "agentTurn" ]]; then
        if [[ "${LLM_BACKEND}" != "openclaw" ]]; then
            STATUS="error"
            ERROR_MSG="agentTurn requires openclaw backend"
        else
            # Briefing/announce run
            TMP_PROMPT="/tmp/codex_agentturn_prompt_$$.txt"
            TMP_OUT="/tmp/codex_agentturn_out_$$.txt"

            {
                echo "너는 한국 주식시장 운영 에이전트다."
                echo "아래 지시사항대로 사용자에게 보낼 메시지를 작성하라."
                echo ""
                if [[ -f "$PROMPT_FILE" ]]; then
                    echo "## 영구 메모리"
                    cat "$PROMPT_FILE"
                    echo ""
                fi
                echo "## 지시사항"
                echo "$EVENT_TEXT"
                echo ""
                echo "출력 규칙:"
                echo "- 한국어 평문"
                echo "- 불필요한 서론 금지"
                echo "- 지시사항의 줄수 제한 준수"
            } > "$TMP_PROMPT"

            if python3 - "$CODEX_BIN" "$OPENCLAW_SESSION_ID" "$TMP_PROMPT" "$TMP_OUT" <<'PY' >> "$LOG_FILE" 2>&1; then
import json
import pathlib
import subprocess
import sys

agent_bin, session_id, prompt_path, out_path = sys.argv[1:5]
prompt = pathlib.Path(prompt_path).read_text(encoding="utf-8")
cmd = [agent_bin, "agent", "--json", "--session-id", session_id, "--message", prompt]
run = subprocess.run(cmd, capture_output=True, text=True, check=False)
if run.returncode != 0:
    raise SystemExit(run.stderr or run.stdout or "openclaw agent failed")
raw = (run.stdout or "").strip()
if not raw:
    raise SystemExit("openclaw empty output")
try:
    obj = json.loads(raw)
except Exception:
    pathlib.Path(out_path).write_text(raw, encoding="utf-8")
else:
    text = ""
    if isinstance(obj, dict):
        result = obj.get("result")
        if isinstance(result, dict):
            payloads = result.get("payloads")
            if isinstance(payloads, list) and payloads:
                first = payloads[0]
                if isinstance(first, dict):
                    t = first.get("text", "")
                    if isinstance(t, str):
                        text = t.strip()
            for key in ("output", "summary"):
                if not text:
                    v = result.get(key)
                    if isinstance(v, str) and v.strip():
                        text = v.strip()
    if not text:
        try:
            parsed = json.loads(raw)
            text = json.dumps(parsed, ensure_ascii=False, indent=2)
        except Exception:
            text = raw
    pathlib.Path(out_path).write_text(text, encoding="utf-8")
PY
                MSG="$(cat "$TMP_OUT")"
                if ! python3 "$BASE/scripts/telegram_notify.py" "$MSG" >> "$LOG_FILE" 2>&1; then
                    STATUS="error"
                    ERROR_MSG="telegram notify failed"
                fi
            else
                STATUS="error"
                ERROR_MSG="openclaw agent for agentTurn failed"
            fi
            rm -f "$TMP_PROMPT" "$TMP_OUT"
        fi
elif [[ "$PAYLOAD_KIND" == "command" ]]; then
    CMD="$(printf '%s' "$JOB_JSON" | jq -r '.payload.command // ""')"
    if [[ -z "$CMD" ]]; then
        STATUS="error"
        ERROR_MSG="empty command payload"
    else
        if ! /bin/bash -lc "$CMD" >> "$LOG_FILE" 2>&1; then
            STATUS="error"
            ERROR_MSG="command payload failed"
        fi
    fi
else
    STATUS="error"
    ERROR_MSG="unsupported payload kind: $PAYLOAD_KIND"
fi

# systemEvent는 Codex 응답 주문을 직접 실행한다.
if [[ "$STATUS" == "ok" && "$PAYLOAD_KIND" == "systemEvent" && "$JOB_NAME" != coin-* ]]; then
    if ! python3 "$ORDER_EXEC" --response /tmp/gpt_response.json >> "$LOG_FILE" 2>&1; then
        STATUS="error"
        ERROR_MSG="order execution stage failed"
    fi
fi

# 긴급 뉴스 트리거는 1회 처리 후 컨텍스트를 삭제해 재처리를 방지한다.
if [[ "$STATUS" == "ok" && "$JOB_NAME" == "news-urgent-trigger" ]]; then
    rm -f "$URGENT_CTX_FILE" >/dev/null 2>&1 || true
fi

END_EPOCH="$(date +%s)"
DURATION="$((END_EPOCH - START_EPOCH))"
END_TS="$(ts_now)"

jq -nc \
    --arg ts "$END_TS" \
    --arg event "finish" \
    --arg job_name "$JOB_NAME" \
    --arg payload_kind "$PAYLOAD_KIND" \
    --arg status "$STATUS" \
    --arg error "$ERROR_MSG" \
    --argjson duration_sec "$DURATION" \
    '{timestamp:$ts,event:$event,job_name:$job_name,payload_kind:$payload_kind,status:$status,error:$error,duration_sec:$duration_sec}' \
    >> "$JOURNAL_FILE"

if [[ "$STATUS" != "ok" ]]; then
    log "END status=error duration=${DURATION}s reason='$ERROR_MSG'"
    notify_telegram "⚠️ codex_cron_router 실패: ${JOB_NAME} (${PAYLOAD_KIND}) ${ERROR_MSG}"
    exit 1
fi

log "END status=ok duration=${DURATION}s"
exit 0
