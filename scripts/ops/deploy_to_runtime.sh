#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

rsync -a --delete "$ROOT/scripts/trading/" "$HOME/.openclaw/scripts/trading/"
cp "$ROOT/cron/codex_jobs.json" "$HOME/.openclaw/cron/codex_jobs.json"
cp "$ROOT/cron/jobs.json" "$HOME/.openclaw/cron/jobs.json"
cp "$ROOT/scripts/build_codex_jobs_manifest.py" "$HOME/.openclaw/scripts/build_codex_jobs_manifest.py"

echo "Deployed repo -> runtime"
