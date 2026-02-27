#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

rsync -a --delete "$HOME/.openclaw/scripts/trading/" "$ROOT/scripts/trading/"
cp "$HOME/.openclaw/cron/codex_jobs.json" "$ROOT/cron/codex_jobs.json"
cp "$HOME/.openclaw/cron/jobs.json" "$ROOT/cron/jobs.json"
cp "$HOME/.openclaw/scripts/build_codex_jobs_manifest.py" "$ROOT/scripts/build_codex_jobs_manifest.py"

echo "Synced runtime -> repo"
