# Online FK enablement on a legacy SQLite database

A legacy database allows `deployments.service_id` to reference services that do
not exist. This demo enables the referential constraint **online** — while two
service processes keep reading and writing the same database — using FastAPI +
SQLite, modeled on PostgreSQL's `NOT VALID` → `VALIDATE CONSTRAINT` flow.

```
state:  off  --POST /validation/start-->  validating  --POST /validation/finalize-->  enabled
```

## How it works

| Concern | Mechanism |
| --- | --- |
| "From validation start, no new orphans" | 4 enforcement **triggers** (deployments INSERT / UPDATE service_id, services DELETE / UPDATE id) are created **in the same transaction** as the `off → validating` flip. They live in the SQLite schema, so they bind *every* process sharing the file and survive restarts. |
| "Writes continue during validation" | Valid inserts/updates/deletes are untouched; only orphan-creating statements are refused (HTTP 409). Old orphans can still be fixed (`PATCH /deployments/{id}` to an existing service). |
| "Batched validation" | `POST /validation/step?batch_size=N` scans `deployments` in id order; the cursor is committed per batch (`validation_progress`), so a crash resumes where it stopped. Found orphans are logged to `validation_findings`. |
| "Interleaved corrections can't slip through" | Triggers make valid→invalid transitions impossible from the start; `finalize` re-counts **live** orphans inside the same write transaction that flips the state, so post-scan changes are rechecked atomically. |
| "Enabled only when all rows valid" | `POST /validation/finalize` drains the unscanned tail and requires `live_orphans == 0` in one `BEGIN IMMEDIATE` transaction — no writer on either process can interleave with the decision. Otherwise 409 + current orphan ids, state stays `validating`. |
| "No enabled-with-orphans after failure/restart" | `enabled` is only ever written together with a zero-orphan check; triggers persist in the schema; on startup the app re-ensures triggers exist whenever state ∈ {validating, enabled}. |

## Run with Docker Compose

```bash
docker compose up --build
# app1 → http://localhost:8001/docs
# app2 → http://localhost:8002/docs   (same database via shared volume)
```

## Demo walkthrough (via /docs)

All steps are clickable in Swagger UI (`/docs` on either port):

1. `POST /demo/reset` — clean slate (state `off`, no triggers).
2. `POST /services` `{"name": "web"}` → note the id (e.g. 1).
3. `POST /deployments` `{"service_id": 1}` (valid) and `{"service_id": 9999}` — **accepted while `off`**: the legacy defect.
4. `GET /orphans` — see the orphan.
5. `DELETE /services/1` — also allowed while `off` (creates more orphans). This is the problem being fixed.
6. `POST /validation/start` — triggers installed, state `validating`. From now on, on **both** processes: bad inserts / re-points / referenced-service deletes → 409; valid writes still work.
7. `POST /validation/step?batch_size=2` repeatedly — watch `cursor`/`remaining`; `GET /validation/findings` lists orphans found by the scan.
8. `PATCH /deployments/{id}` `{"service_id": <existing>}` — correct old orphans (still allowed).
9. `POST /validation/finalize` — 409 while any orphan remains; once all are fixed → 200, state `enabled`.
10. `GET /validation/status` — state, scan progress, live orphan count, triggers.

## Tests

```bash
# with Docker:
docker compose up -d --build
python3 tests/selftest.py --restart-hook "docker compose restart app1"

# without Docker (two local processes sharing one SQLite file):
bash scripts/local_up.sh
python3 tests/selftest.py --restart-hook "bash scripts/local_restart_app1.sh"
bash scripts/local_down.sh
```

`tests/selftest.py` (stdlib only) covers: legacy behavior, cross-process
enforcement from validation start, batched scan with interleaved corrections,
failed-then-successful finalize, SIGKILL restart mid-validation / after failed
finalize / after enable, and an 8 s concurrency storm (scanner + 3 fixers +
deleter + 2 bad writers + invariant monitor across both processes).
Last run here: **94 checks, 0 failures** (3 consecutive runs).

## Layout

```
app/main.py            FastAPI app: schema, triggers, endpoints, validation state machine
tests/selftest.py      end-to-end self test (phases A/B/C)
scripts/local_*.sh     run/restart/stop two processes without Docker
Dockerfile, docker-compose.yml, requirements.txt
```

## Known limitations (honest list)

* **Logical, not native, FK.** SQLite cannot add a foreign key to an existing
  table; enforcement is via triggers and the `enabled` flag is a logical state.
  Migrating to a native `REFERENCES` clause would require a table rebuild
  (shadow table + swap) — not implemented here.
* **Triggers can be dropped by direct DB access.** Enforcement covers all
  writers going through either service process (and any client that doesn't
  touch the schema); it does not defend against someone with shell access to
  the database file dropping triggers — same trust model as any non-native
  constraint.
* **Single-writer SQLite.** `finalize` and each scan batch take the write lock
  briefly; throughput is bounded by SQLite's single writer. Fine for a demo;
  not a horizontally scalable design.
* **Scan cursor relies on monotonic ids** (`AUTOINCREMENT`). Even if ids were
  reused, the atomic live-orphan recount in `finalize` remains authoritative.
* **Shared volume must be a local filesystem** (docker named volume on one
  host). SQLite locking does not work reliably over NFS.
* The concurrency storm exercises interleaved HTTP requests; it does not
  formally prove race-freedom — the safety argument rests on the transactional
  design (triggers + atomic finalize), which the tests probe but cannot
  exhaustively verify.
