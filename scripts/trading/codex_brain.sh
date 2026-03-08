#!/bin/bash
# codex_brain.sh — OpenClaw 2-Tier 두뇌 상담 (Codex CLI 버전)
#
# 흐름:
#   1. prepare_gpt_prompt.py → 프롬프트 생성
#   2. openclaw agent → GPT 두뇌에 전달 → 구조화된 JSON 응답
#      (codex exec 폴백은 기본 비활성화)
#   3. 응답 파일 저장 → codex_cron_router/execute_gpt_orders.py가 주문 실행
#
# 사용법:
#   bash ~/.openclaw/scripts/trading/codex_brain.sh
#
# 출력:
#   /tmp/gpt_prompt*.txt    — 생성된 프롬프트
#   /tmp/gpt_response*.json — GPT 응답 (JSON)
#   exit code 0 = 성공, 1 = 실패 (fallback 필요)

set -euo pipefail

# ── 경로 설정 ──────────────────────────────────────────────────────────────
SCRIPTS_DIR="$HOME/.openclaw/scripts/trading"
DEFAULT_PROMPT_FILE="/tmp/gpt_prompt.txt"
DEFAULT_RESPONSE_FILE="/tmp/gpt_response.json"
PROMPT_FILE="${OPENCLAW_PROMPT_FILE:-$DEFAULT_PROMPT_FILE}"
RESPONSE_FILE="${OPENCLAW_RESPONSE_FILE:-$DEFAULT_RESPONSE_FILE}"
SCHEMA_FILE="$SCRIPTS_DIR/trading_response_schema.json"
LLM_BACKEND="${LLM_EXEC_BACKEND:-${OPENCLAW_LLM_BACKEND:-openclaw}}"
CODEX_BIN="${CODEX_BIN:-openclaw}"
OPENCLAW_BIN="${OPENCLAW_BIN:-openclaw}"
CODEX_MODEL="${CODEX_MODEL:-${OPENCLAW_PRIMARY_MODEL:-gpt-5.4}}"
TIMEOUT_SEC="${CODEX_TIMEOUT:-180}"  # 3분 타임아웃
CODEX_CACHE_DIR="${CODEX_EXEC_CACHE_DIR:-$HOME/.openclaw/cache/codex-exec/brain}"
CODEX_BRAIN_CACHE_TTL="${CODEX_BRAIN_CACHE_TTL:-120}"
CODEX_BRAIN_LOCK_WAIT="${CODEX_BRAIN_LOCK_WAIT:-20}"
OPENCLAW_SESSION_ID="${OPENCLAW_SESSION_ID:-openclaw-codex-bridge}"
OPENCLAW_ERR_LOG="/tmp/openclaw_agent_stderr.log"
OPENCLAW_AGENT_RETRY_MAX="${OPENCLAW_AGENT_RETRY_MAX:-2}"
OPENCLAW_AGENT_RETRY_DELAY_SEC="${OPENCLAW_AGENT_RETRY_DELAY_SEC:-1}"
ENABLE_CODEX_EXEC_FALLBACK="${ENABLE_CODEX_EXEC_FALLBACK:-0}"
CODEX_FALLBACK_ON_ANY_ERROR="${CODEX_FALLBACK_ON_ANY_ERROR:-}"
CODEX_FALLBACK_BIN="${CODEX_FALLBACK_BIN:-codex}"
CODEX_FALLBACK_MODEL="${CODEX_FALLBACK_MODEL:-${OPENCLAW_FALLBACK_MODEL:-}}"
if [[ -z "$CODEX_FALLBACK_MODEL" ]]; then
    if [[ "$CODEX_MODEL" == *"codex-spark"* ]] || [[ "$CODEX_MODEL" == openai-codex/* ]]; then
        CODEX_FALLBACK_MODEL="${OPENCLAW_FALLBACK_MODEL:-gpt-5.4}"
    else
        CODEX_FALLBACK_MODEL="$CODEX_MODEL"
    fi
fi
CODEX_FALLBACK_TIMEOUT_SEC="${CODEX_FALLBACK_TIMEOUT_SEC:-240}"

if [[ -z "$CODEX_FALLBACK_ON_ANY_ERROR" ]]; then
    # Spark 계열은 quota/rate-limit 오류가 비정형 문자열로 떨어져도 폴백을 타도록 기본 ON.
    if [[ "$CODEX_MODEL" == *"codex-spark"* ]] || [[ "$CODEX_MODEL" == openai-codex/* ]]; then
        CODEX_FALLBACK_ON_ANY_ERROR="0"
    else
        CODEX_FALLBACK_ON_ANY_ERROR="0"
    fi
fi

# npm/homebrew global bin 경로 추가 (macOS 호환)
export PATH="/opt/homebrew/bin:$HOME/.npm-global/bin:$HOME/.openclaw/bin:/usr/local/bin:$PATH"

# uv/mcporter 캐시 경로를 workspace 내부로 고정 (sandbox 권한 이슈 회피)
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$HOME/.openclaw/workspace/.cache}"
export UV_CACHE_DIR="${UV_CACHE_DIR:-$XDG_CACHE_HOME/uv}"
mkdir -p "$UV_CACHE_DIR"
mkdir -p "$(dirname "$PROMPT_FILE")" "$(dirname "$RESPONSE_FILE")"

# ── 로깅 ────────────────────────────────────────────────────────────────────
log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }
die() { log "ERROR: $*"; exit 1; }

is_recoverable_openclaw_error() {
    local text="${1:-}"
    text="$(echo "$text" | tr '[:upper:]' '[:lower:]')"
    [[ "$text" == *"ctx max"* ]] \
        || [[ "$text" == *"context max"* ]] \
        || [[ "$text" == *"context limit"* ]] \
        || [[ "$text" == *"context length"* ]] \
        || [[ "$text" == *"context overflow"* ]] \
        || [[ "$text" == *"maximum context"* ]] \
        || [[ "$text" == *"prompt too large"* ]] \
        || [[ "$text" == *"too many tokens"* ]] \
        || [[ "$text" == *"token limit"* ]] \
        || [[ "$text" == *"conversation too long"* ]] \
        || [[ "$text" == *"session expired"* ]] \
        || [[ "$text" == *"session has expired"* ]] \
        || [[ "$text" == *"session not found"* ]] \
        || [[ "$text" == *"invalid session"* ]] \
        || [[ "$text" == *"stale session"* ]] \
        || [[ "$text" == *"429"* ]] \
        || [[ "$text" == *"rate limit"* ]] \
        || [[ "$text" == *"too many requests"* ]] \
        || [[ "$text" == *"quota exceeded"* ]]
}

resolve_codex_fallback_bin() {
    for cand in \
        "$CODEX_FALLBACK_BIN" \
        "/opt/homebrew/bin/codex" \
        "/usr/local/bin/codex" \
        "/usr/bin/codex" \
        "codex"; do
        if [ -n "$cand" ] && command -v "$cand" >/dev/null 2>&1; then
            command -v "$cand"
            return 0
        fi
        if [ -x "$cand" ] 2>/dev/null; then
            echo "$cand"
            return 0
        fi
    done
    return 1
}

run_codex_exec_fallback() {
    local fallback_bin
    local fallback_log="/tmp/codex_exec_fallback.log"
    if ! fallback_bin="$(resolve_codex_fallback_bin)"; then
        log "  codex exec 폴백 바이너리를 찾지 못함"
        return 1
    fi
    log "  codex exec 폴백 시도: $fallback_bin"
    : > "$fallback_log"
    if run_with_timeout "${CODEX_FALLBACK_TIMEOUT_SEC}" \
        "$fallback_bin" exec \
        --skip-git-repo-check \
        --dangerously-bypass-approvals-and-sandbox \
        --model "$CODEX_FALLBACK_MODEL" \
        --output-schema "$SCHEMA_FILE" \
        --output-last-message "$RESPONSE_FILE" \
        - < "$PROMPT_FILE" >"$fallback_log" 2>&1; then
        if [[ -s "$RESPONSE_FILE" ]]; then
            return 0
        fi
        log "  codex exec 폴백 응답이 비어 있음"
    fi

    if rg -qi "invalid_json_schema|invalid schema for response_format" "$fallback_log"; then
        log "  codex output-schema 비호환 감지 → schema 없이 재시도"
        if run_with_timeout "${CODEX_FALLBACK_TIMEOUT_SEC}" \
            "$fallback_bin" exec \
            --skip-git-repo-check \
            --dangerously-bypass-approvals-and-sandbox \
            --model "$CODEX_FALLBACK_MODEL" \
            --output-last-message "$RESPONSE_FILE" \
            - < "$PROMPT_FILE" >>"$fallback_log" 2>&1; then
            if [[ -s "$RESPONSE_FILE" ]]; then
                return 0
            fi
            log "  codex exec 재시도 응답도 비어 있음"
        fi
    fi
    tail -n 30 "$fallback_log" 2>/dev/null | while read -r line; do log "  $line"; done
    return 1
}

# ── macOS 호환 timeout 함수 ──────────────────────────────────────────────────
# macOS에는 coreutils timeout이 없으므로 perl로 대체
run_with_timeout() {
    local secs="$1"
    shift
    if command -v gtimeout &>/dev/null; then
        # brew install coreutils 설치된 경우
        gtimeout "$secs" "$@"
    elif command -v timeout &>/dev/null; then
        # Linux 또는 timeout 있는 경우
        timeout "$secs" "$@"
    else
        # macOS 기본: perl 기반 타임아웃
        perl -e '
            use POSIX ":sys_wait_h";
            my $timeout = shift @ARGV;
            my $pid = fork();
            if ($pid == 0) { exec @ARGV; exit 127; }
            eval {
                local $SIG{ALRM} = sub { kill "TERM", $pid; die "timeout\n"; };
                alarm $timeout;
                waitpid($pid, 0);
                alarm 0;
            };
            if ($@ =~ /timeout/) { waitpid($pid, WNOHANG); exit 124; }
            exit ($? >> 8);
        ' "$secs" "$@"
    fi
}

codex_cache_fresh() {
    local cache_file="$1"
    local ttl_sec="$2"
    if [[ "$ttl_sec" -le 0 ]]; then
        return 1
    fi
    python3 - "$cache_file" "$ttl_sec" <<'PY'
import os
import sys
import time

cache_file, ttl = sys.argv[1], float(sys.argv[2])
if not os.path.exists(cache_file):
    raise SystemExit(1)
age = time.time() - os.path.getmtime(cache_file)
raise SystemExit(0 if age <= ttl else 1)
PY
}

codex_prompt_hash() {
    python3 - "$1" <<'PY'
import hashlib
import sys

path = sys.argv[1]
with open(path, "rb") as f:
    data = f.read()
print(hashlib.sha256(data).hexdigest())
PY
}

# ── 1단계: 프롬프트 생성 ────────────────────────────────────────────────────
log "=== OpenClaw Codex Brain 시작 ==="
log "[1/3] 프롬프트 생성 중..."

python3 "$SCRIPTS_DIR/prepare_gpt_prompt.py" --output "$PROMPT_FILE" 2>&1 | while read -r line; do
    log "  $line"
done

if [ ! -f "$PROMPT_FILE" ] || [ ! -s "$PROMPT_FILE" ]; then
    die "프롬프트 파일 생성 실패: $PROMPT_FILE"
fi

PROMPT_LEN=$(wc -c < "$PROMPT_FILE")
log "  프롬프트 생성 완료 (${PROMPT_LEN}바이트)"
PROMPT_HASH="$(codex_prompt_hash "$PROMPT_FILE")"
CACHE_FILE="$CODEX_CACHE_DIR/$PROMPT_HASH.json"
CACHE_LOCK_DIR="$CODEX_CACHE_DIR/locks/$PROMPT_HASH"

mkdir -p "$CODEX_CACHE_DIR" "$CODEX_CACHE_DIR/locks"

# 동일 prompt의 응답은 캐시 우선 사용
if codex_cache_fresh "$CACHE_FILE" "$CODEX_BRAIN_CACHE_TTL"; then
    cp "$CACHE_FILE" "$RESPONSE_FILE"
    log "  캐시 응답 사용: ${PROMPT_HASH}.json (${CODEX_BRAIN_CACHE_TTL}초)"
    CACHE_HIT=1
fi

CACHE_HIT="${CACHE_HIT:-0}"

# ── 2단계: LLM 호출 ─────────────────────────────────────────────────────
log "[2/3] LLM 호출 중..."

if [[ "${LLM_BACKEND}" == "openclaw" ]]; then
    if ! command -v "$OPENCLAW_BIN" &>/dev/null; then
        for try_path in \
            "$HOME/.npm-global/bin/openclaw" \
            "/opt/homebrew/bin/openclaw" \
            "/usr/local/bin/openclaw" \
            "$HOME/.openclaw/bin/openclaw"; do
            if [ -x "$try_path" ] 2>/dev/null; then
                OPENCLAW_BIN="$try_path"
                break
            fi
        done
        if ! command -v "$OPENCLAW_BIN" &>/dev/null && [ ! -x "$OPENCLAW_BIN" ]; then
            die "openclaw를 찾을 수 없음. openClaw CLI/로그인 경로를 확인하세요"
        fi
    fi

    log "  OpenClaw Agent: $("$OPENCLAW_BIN" --version 2>/dev/null || echo 'version unknown')"
    log "  OpenClaw session: ${OPENCLAW_SESSION_ID}"

    run_openclaw() {
        local session_id="$1"
        python3 - "$OPENCLAW_BIN" "$session_id" "$PROMPT_FILE" "$RESPONSE_FILE" "$TIMEOUT_SEC" <<'PY'
import json
import os
import pathlib
import subprocess
import sys

agent_bin, session_id, prompt_path, response_path, timeout_sec = sys.argv[1:6]
prompt = pathlib.Path(prompt_path).read_text(encoding="utf-8")
cmd = [
    agent_bin,
    "agent",
    "--json",
    "--session-id",
    session_id,
    "--message",
    prompt,
]
agent_id = os.environ.get("OPENCLAW_AGENT_ID", "").strip()
if agent_id:
    cmd.extend(["--agent", agent_id])
thinking = os.environ.get("OPENCLAW_AGENT_THINKING", "").strip()
if thinking:
    cmd.extend(["--thinking", thinking])

run = subprocess.run(cmd, capture_output=True, text=True, timeout=int(timeout_sec) + 20, check=False)
if run.returncode != 0:
    err = (run.stderr or run.stdout or "").strip()
    raise SystemExit(f"openclaw agent failed: {err[:400]}")
raw = (run.stdout or "").strip()
if not raw:
    raise SystemExit("openclaw agent empty output")

def is_recoverable_text(text: str) -> bool:
    s = (text or "").lower()
    pats = (
        "ctx max",
        "context max",
        "context limit",
        "context length",
        "context overflow",
        "maximum context",
        "prompt too large",
        "too many tokens",
        "token limit",
        "conversation too long",
        "session expired",
        "session has expired",
        "session not found",
        "invalid session",
        "stale session",
        "429",
        "rate limit",
        "too many requests",
        "quota exceeded",
    )
    return any(p in s for p in pats)

try:
    obj = json.loads(raw)
except Exception:
    if is_recoverable_text(raw):
        raise SystemExit(f"openclaw recoverable: {raw[:400]}")
    pathlib.Path(response_path).write_text(raw, encoding="utf-8")
    sys.exit(0)

if is_recoverable_text(raw):
    raise SystemExit(f"openclaw recoverable: {raw[:400]}")
if isinstance(obj, dict):
    for ek in ("error", "message", "detail"):
        ev = obj.get(ek)
        if isinstance(ev, str) and is_recoverable_text(ev):
            raise SystemExit(f"openclaw recoverable: {ev[:400]}")

if isinstance(obj, dict):
    result = obj.get("result")
    if isinstance(result, dict):
        payloads = result.get("payloads")
        if isinstance(payloads, list) and payloads:
            first = payloads[0]
            if isinstance(first, dict):
                text = first.get("text", "").strip()
                if isinstance(text, str) and text:
                    if is_recoverable_text(text):
                        raise SystemExit(f"openclaw recoverable: {text[:400]}")
                    try:
                        payload_json = json.loads(text)
                        pathlib.Path(response_path).write_text(
                            json.dumps(payload_json, ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )
                    except Exception:
                        pathlib.Path(response_path).write_text(text, encoding="utf-8")
                    sys.exit(0)
        for key in ("output", "summary"):
            text = result.get(key)
            if isinstance(text, str) and text.strip():
                if is_recoverable_text(text):
                    raise SystemExit(f"openclaw recoverable: {text[:400]}")
                pathlib.Path(response_path).write_text(text.strip(), encoding="utf-8")
                sys.exit(0)

pathlib.Path(response_path).write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
PY
        return $?
    }

    if [[ "${CACHE_HIT}" != "1" ]]; then
        WAITED=0
        LOCK_OK=0
        while true; do
            if [[ -d "$CACHE_LOCK_DIR" ]]; then
                LOCK_PID=""
                if [[ -f "$CACHE_LOCK_DIR/pid" ]]; then
                    LOCK_PID="$(cat "$CACHE_LOCK_DIR/pid" 2>/dev/null || true)"
                fi
                if [[ -n "$LOCK_PID" ]] && ! ps -p "$LOCK_PID" >/dev/null 2>&1; then
                    rm -rf "$CACHE_LOCK_DIR" >/dev/null 2>&1 || true
                elif [[ -z "$LOCK_PID" ]]; then
                    rm -rf "$CACHE_LOCK_DIR" >/dev/null 2>&1 || true
                fi
            fi

            if mkdir "$CACHE_LOCK_DIR" 2>/dev/null; then
                LOCK_OK=1
                echo "$$" > "$CACHE_LOCK_DIR/pid"
                break
            fi
            if codex_cache_fresh "$CACHE_FILE" "$CODEX_BRAIN_CACHE_TTL"; then
                cp "$CACHE_FILE" "$RESPONSE_FILE"
                CACHE_HIT=1
                break
            fi
            if (( WAITED >= CODEX_BRAIN_LOCK_WAIT )); then
                break
            fi
            sleep 1
            WAITED=$((WAITED + 1))
        done

        if [[ "${CACHE_HIT}" != "1" ]]; then
            if [[ "$LOCK_OK" -ne 1 ]] && codex_cache_fresh "$CACHE_FILE" "$CODEX_BRAIN_CACHE_TTL"; then
                cp "$CACHE_FILE" "$RESPONSE_FILE"
                CACHE_HIT=1
            fi
        fi

        if [[ "${CACHE_HIT}" != "1" ]]; then
            OPENCLAW_OK=0
            OPENCLAW_LAST_ERR=""
            LLM_EXIT=1
            TRY=0
            while true; do
                CURRENT_SESSION_ID="$OPENCLAW_SESSION_ID"
                if (( TRY > 0 )); then
                    CURRENT_SESSION_ID="${OPENCLAW_SESSION_ID}-retry-${TRY}-$(date +%s)"
                fi
                if run_openclaw "$CURRENT_SESSION_ID" 2>"$OPENCLAW_ERR_LOG"; then
                    OPENCLAW_OK=1
                    log "  OpenClaw 응답 수신 완료 (attempt=$((TRY + 1)))"
                    break
                fi
                LLM_EXIT=$?
                OPENCLAW_LAST_ERR="$(tail -n 40 "$OPENCLAW_ERR_LOG" 2>/dev/null | tr '\n' ' ')"
                tail -n 20 "$OPENCLAW_ERR_LOG" 2>/dev/null | while read -r line; do log "  $line"; done
                if (( TRY < OPENCLAW_AGENT_RETRY_MAX )) && is_recoverable_openclaw_error "$OPENCLAW_LAST_ERR"; then
                    TRY=$((TRY + 1))
                    sleep "$OPENCLAW_AGENT_RETRY_DELAY_SEC"
                    continue
                fi
                break
            done

            if [[ "$OPENCLAW_OK" != "1" ]]; then
                USE_FALLBACK=0
                if [[ "$ENABLE_CODEX_EXEC_FALLBACK" == "1" ]]; then
                    if [[ "$CODEX_FALLBACK_ON_ANY_ERROR" == "1" ]] || is_recoverable_openclaw_error "$OPENCLAW_LAST_ERR"; then
                        USE_FALLBACK=1
                    fi
                fi
                if [[ "$USE_FALLBACK" == "1" ]] && run_codex_exec_fallback; then
                    log "  OpenClaw 실패 → codex exec 폴백 성공"
                else
                    [[ "$LOCK_OK" -eq 1 ]] && rm -rf "$CACHE_LOCK_DIR" || true
                    if [ "${LLM_EXIT:-1}" -eq 124 ]; then
                        die "OpenClaw 응답 타임아웃 (${TIMEOUT_SEC}초 초과)"
                    fi
                    die "OpenClaw agent 실행 실패 (exit code: ${LLM_EXIT:-1})"
                fi
            fi

            cp "$RESPONSE_FILE" "$CACHE_FILE"
        fi

        if [[ "$LOCK_OK" -eq 1 ]]; then
            rm -rf "$CACHE_LOCK_DIR" || true
        fi
    fi
else
    die "codex 브레인 실행기는 openclaw 우선 경로만 지원합니다. LLM_BACKEND=openclaw 로 설정하세요."
fi

# ── 3단계: 응답 검증 ───────────────────────────────────────────────────────
log "[3/3] 응답 검증 중..."

if [ ! -f "$RESPONSE_FILE" ] || [ ! -s "$RESPONSE_FILE" ]; then
    die "응답 파일이 비어있음: $RESPONSE_FILE"
fi

# JSON 유효성 검사
if ! python3 -c "import json; json.load(open('$RESPONSE_FILE'))" 2>/dev/null; then
    # 응답에서 JSON 블록 추출 시도
    log "  직접 JSON 파싱 실패 — JSON 블록 추출 시도..."
    python3 -c "
import json, re, sys

with open('$RESPONSE_FILE', 'r') as f:
    text = f.read()

# \`\`\`json ... \`\`\` 블록 추출
match = re.search(r'\`\`\`json\s*\n(.*?)\n\`\`\`', text, re.DOTALL)
if match:
    data = json.loads(match.group(1))
    with open('$RESPONSE_FILE', 'w') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print('JSON 블록 추출 성공')
    sys.exit(0)

# 최외곽 { ... } 추출
match = re.search(r'\{.*\}', text, re.DOTALL)
if match:
    data = json.loads(match.group(0))
    with open('$RESPONSE_FILE', 'w') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print('JSON 객체 추출 성공')
    sys.exit(0)

print('JSON 추출 실패')
sys.exit(1)
" 2>&1 | while read -r line; do log "  $line"; done

    if [ $? -ne 0 ]; then
        die "JSON 파싱/추출 실패"
    fi
fi

# 공통 strict validator 검사
if ! python3 - "$SCRIPTS_DIR" "$RESPONSE_FILE" <<'PY' 2>&1 | while read -r line; do log "  $line"; done
import json
import sys
from pathlib import Path

scripts_dir = Path(sys.argv[1])
response_path = Path(sys.argv[2])
sys.path.insert(0, str(scripts_dir))

from response_validator import validate_trading_response  # type: ignore

data = json.loads(response_path.read_text(encoding="utf-8"))
errors = validate_trading_response(data)
if errors:
    print("strict validation failed:")
    for err in errors[:20]:
        print(f"- {err}")
    raise SystemExit(1)
print(f'검증 통과: orders={len(data.get("orders", []))}건, regime={data.get("regime_action","?")}, watch_list={len(data.get("watch_list", []))}건')
PY
then
    die "응답 strict validation 실패"
fi

RESPONSE_LEN=$(wc -c < "$RESPONSE_FILE")
log "  응답 저장: $RESPONSE_FILE (${RESPONSE_LEN}바이트)"

# ── 완료 ──────────────────────────────────────────────────────────────────
log "=== Codex Brain 완료 ==="
log "  프롬프트: $PROMPT_FILE"
log "  응답:     $RESPONSE_FILE"
log "  후속 주문 단계에서 ${RESPONSE_FILE} 을 파싱하여 주문을 실행합니다."

exit 0
