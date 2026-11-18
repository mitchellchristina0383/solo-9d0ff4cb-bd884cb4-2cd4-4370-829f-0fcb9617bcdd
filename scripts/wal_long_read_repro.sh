#!/usr/bin/env bash
# Local reproduction of the "long reader pins old WAL frames" situation.
#
# It drives the two already-running service processes over HTTP (default
# 8001/8002) and shows, step by step:
#   1. WAL frames accumulating while a read transaction is held open
#   2. a bounded TRUNCATE checkpoint that CANNOT truncate (snapshot retained)
#      - frames may be copied back, but on-disk WAL bytes are NOT released
#   3. once the long read ends, a re-checkpoint truncates and frees the space
#
# Requires only curl + python3. Start the services first:
#   bash scripts/local_up.sh
# (or: docker compose up --build, then this script works unchanged)
set -euo pipefail

APP1="${APP1:-http://127.0.0.1:8001}"
APP2="${APP2:-http://127.0.0.1:8002}"
HOLD="${HOLD:-20}"          # seconds the long read stays open
WAIT_MS="${WAIT_MS:-1500}"  # bounded checkpoint wait used in step 2

j() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
wal() { curl -s "$1/wal/status" | j "'wal_size_bytes=%d wal_frames=%s truncated=%s' % (d['wal_size_bytes'], d['wal_frames'], d['truncated'])"; }
post() {
  local base="$1" path="$2" body="${3-}"
  if [ -n "$body" ]; then
    curl -s -X POST "$base$path" -H 'Content-Type: application/json' --data "$body"
  else
    curl -s -X POST "$base$path"
  fi
}

echo "== reset + enable the FK (so we can also show the constraint still holds) =="
post "$APP1" /demo/reset >/dev/null
post "$APP1" /services '{"name":"wal-svc"}' >/dev/null
post "$APP1" /validation/start >/dev/null
post "$APP1" /validation/finalize >/dev/null

echo "== clean baseline checkpoint =="
post "$APP1" /wal/checkpoint '{"mode":"TRUNCATE","wait_ms":5000}' >/dev/null

echo "== held writer keeps frames in the WAL, THEN a long read pins a snapshot =="
post "$APP1" /demo/held-writer "{\"service_id\":1,\"count\":60,\"hold_seconds\":$((HOLD+10))}" >/dev/null
sleep 1
post "$APP1" /wal/long-read "{\"hold_seconds\":$HOLD}" | j "'long-read token: ' + d['token']"
sleep 1
post "$APP2" /demo/held-writer "{\"service_id\":1,\"count\":60,\"hold_seconds\":$((HOLD+5))}" >/dev/null
sleep 1

echo
echo "WAL WHILE THE SNAPSHOT IS HELD:"
echo "  app1 -> $(wal "$APP1")"
echo "  app2 -> $(wal "$APP2")"

echo
echo "== bounded TRUNCATE on app2 (wait_ms=$WAIT_MS): expect retained, 0 bytes freed =="
post "$APP2" /wal/checkpoint "{\"mode\":\"TRUNCATE\",\"wait_ms\":$WAIT_MS}" | j "\
'outcome=%s busy=%s frames_backfilled=%s snapshot_retained=%s truncated=%s space_released_bytes=%s' % \
(d['outcome'], d['busy'], d['frames_backfilled'], d['snapshot_retained'], d['truncated'], d['space_released_bytes'])"
echo "  WAL still on disk -> $(wal "$APP2")"

echo
echo "== ordinary writes keep working and stay fast during the contention =="
for i in $(seq 1 5); do
  post "$APP1" /deployments '{"service_id":1,"note":"concurrent"}' >/dev/null
done
echo "  5 concurrent writes accepted; FK still rejects orphans:"
post "$APP2" /deployments '{"service_id":424242}' | j "'  orphan insert -> HTTP detail present: ' + str(d.get('detail') is not None)"

echo
echo "== waiting for the long read ($HOLD s) to release its snapshot =="
sleep "$HOLD"
sleep 2

echo "== re-checkpoint now truncates; re-query reports the CURRENT state =="
post "$APP1" /wal/checkpoint '{"mode":"TRUNCATE","wait_ms":5000}' | j "\
'outcome=%s truncated=%s snapshot_retained=%s space_released_bytes=%s' % \
(d['outcome'], d['truncated'], d['snapshot_retained'], d['space_released_bytes'])"
echo "  app1 -> $(wal "$APP1")"
echo "  app2 -> $(wal "$APP2")"
echo
echo "Done. Swagger UI: $APP1/docs and $APP2/docs (see GET /wal/status, POST /wal/checkpoint)."
