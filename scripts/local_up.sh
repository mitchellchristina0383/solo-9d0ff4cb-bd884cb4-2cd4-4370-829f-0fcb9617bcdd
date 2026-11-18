#!/usr/bin/env bash
# Start two uvicorn processes sharing ONE SQLite file -- the same architecture
# as docker-compose.yml, for environments without Docker.
set -euo pipefail
cd "$(dirname "$0")/.."
RUN_DIR="${RUN_DIR:-/tmp/fkdemo}"
mkdir -p "$RUN_DIR"
export DB_PATH="$RUN_DIR/app.db"

start_one() {
  local name="$1" port="$2"
  SERVER_NAME="$name" nohup python3 -m uvicorn app.main:app \
      --host 127.0.0.1 --port "$port" > "$RUN_DIR/$name.log" 2>&1 &
  echo $! > "$RUN_DIR/$name.pid"
  echo "$name pid $(cat "$RUN_DIR/$name.pid") on :$port"
}

start_one app1 8001
start_one app2 8002
echo "shared db: $DB_PATH"
