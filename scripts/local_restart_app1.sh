#!/usr/bin/env bash
# Hard-kill (SIGKILL, simulating a crash) and restart the app1 process.
# Used by tests/selftest.py via --restart-hook.
set -euo pipefail
cd "$(dirname "$0")/.."
RUN_DIR="${RUN_DIR:-/tmp/fkdemo}"
export DB_PATH="$RUN_DIR/app.db"

if [ -f "$RUN_DIR/app1.pid" ]; then
  kill -9 "$(cat "$RUN_DIR/app1.pid")" 2>/dev/null || true
  sleep 0.5
fi
SERVER_NAME=app1 nohup "${PYTHON:-python3}" -m uvicorn app.main:app \
    --host 127.0.0.1 --port 8001 > "$RUN_DIR/app1.log" 2>&1 &
echo $! > "$RUN_DIR/app1.pid"
echo "app1 restarted, pid $(cat "$RUN_DIR/app1.pid")"
