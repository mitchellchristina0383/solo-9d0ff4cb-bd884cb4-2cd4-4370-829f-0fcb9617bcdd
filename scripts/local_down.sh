#!/usr/bin/env bash
set -euo pipefail
RUN_DIR="${RUN_DIR:-/tmp/fkdemo}"
for name in app1 app2; do
  if [ -f "$RUN_DIR/$name.pid" ]; then
    kill "$(cat "$RUN_DIR/$name.pid")" 2>/dev/null || true
    rm -f "$RUN_DIR/$name.pid"
  fi
done
echo "stopped"
