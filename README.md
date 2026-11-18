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

## WAL checkpointing: reclaim disk while both processes keep reading/writing

The database runs in WAL mode. Frames written by the two processes accumulate in
the `-wal` file; a **checkpoint** copies them back into the main database and
(only `TRUNCATE`) returns the WAL bytes to the filesystem. The complication is a
**long-lived reader** holding an old snapshot: SQLite must keep every frame that
snapshot can still see, so the WAL cannot be truncated even after those frames
were copied back.

### Endpoints (all visible and clickable in `/docs`)

| Endpoint | Purpose |
| --- | --- |
| `GET /wal/status` | Read-only on-disk view: `wal_size_bytes`, `wal_frames` (parsed from the WAL header, no SQL/lock), `shm_size_bytes`, `truncated`. Never mutates the DB. |
| `POST /wal/checkpoint` | Body `{"mode": "PASSIVE"\|"FULL"\|"RESTART"\|"TRUNCATE", "wait_ms": 0..25000}`. Bounded checkpoint; always returns **200 with the progress made**, even when blocked or timed out. |
| `POST /wal/long-read` | Body `{"hold_seconds": n}`. Opens a read transaction and keeps its snapshot open on a background thread — the long-read repro primitive. |
| `GET /wal/long-read` | Lists this process's open long-read holders. |
| `POST /demo/held-writer` | Writes rows then keeps the writer connection open, so its frames stay in the WAL (SQLite otherwise reaps an empty WAL when the last connection closes). Needed to build a mid-WAL snapshot. |
| `POST /demo/bulk-write` | Plain batched insert on one connection/transaction. |

### What the checkpoint response tells you (and the key invariant)

`PRAGMA wal_checkpoint` on SQLite 3.40 does **not** raise on contention — it
returns a row `(busy, log, checkpointed)`. The API surfaces this as:

* `outcome`: `truncated` / `checkpointed` (done), `busy` (blocked immediately),
  `timeout` (used the whole `wait_ms` budget), or `error`.
* `frames_backfilled`: frames copied **into the main database**.
* `snapshot_retained`: the checkpoint could not finish **and frames still
  physically remain** — a long reader (or the writer) still occupies them.
* `truncated` / `space_released_bytes`: the only measures of **disk actually
  returned**.

> **Backfilled ≠ freed.** Frames can be copied back (`frames_backfilled > 0`)
> while the WAL stays fully on disk (`space_released_bytes == 0`,
> `snapshot_retained == true`). The space is only released by a successful
> `TRUNCATE` after the old snapshot ends. A re-query of `GET /wal/status` always
> reports the current on-disk truth, never a stale result.

### Bounded waiting and the two-process lock rules

* Each checkpoint runs on a **dedicated connection** whose `busy_timeout` is set
  to the request's `wait_ms` (capped at **25 s**). Ordinary write connections
  keep their own 30 s timeout, so checkpoint contention can never stretch a
  normal write to its timeout, and no request waits on a lock forever.
* Two processes calling the checkpoint at once are serialized by SQLite itself:
  one finishes, the other gets an immediate busy result (`busy=1, log=-1,
  checkpointed=-1`) or waits at most its own `wait_ms`. Nothing blocks
  indefinitely.
* The FK enforcement triggers live in the shared schema and are unaffected by
  checkpointing; referential enforcement continues on both processes.

### Reproduce the long read locally

```bash
# Docker (preferred):
docker compose up --build
bash scripts/wal_long_read_repro.sh

# Without Docker (two local processes sharing one SQLite file):
bash scripts/local_up.sh
HOLD=20 WAIT_MS=1500 bash scripts/wal_long_read_repro.sh
bash scripts/local_down.sh
```

The script prints the WAL while the snapshot is held, the blocked checkpoint
(`snapshot_retained=true`, zero bytes freed), continuing concurrent writes, and
then the successful truncation once the reader ends. Or drive it by hand in
`/docs`: `POST /demo/held-writer` → `POST /wal/long-read` → another
`/demo/held-writer` → watch `GET /wal/status` → `POST /wal/checkpoint` before
and after the hold ends.

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
finalize / after enable, an 8 s concurrency storm (scanner + 3 fixers +
deleter + 2 bad writers + invariant monitor across both processes), and a
**Phase D WAL suite**: read-only status, clean truncate, long-read pinned
snapshot (busy / bounded-timeout / frames-backfilled-but-not-freed), ordinary
writes staying fast under contention, FK enforcement intact, truncation after
the reader ends, dual-process concurrent checkpoints, and request validation.
Use `--skip-phase A|B|C|D` to run a subset.
Last full run here: **144 checks, 0 failures** (3 consecutive runs, A–D with
the restart hook).

## Layout

```
app/main.py                     FastAPI app: schema, triggers, validation state machine, WAL endpoints
tests/selftest.py               end-to-end self test (phases A/B/C/D)
scripts/local_*.sh              run/restart/stop two processes without Docker
scripts/wal_long_read_repro.sh  one-shot long-read / WAL-reclaim reproduction
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
* **WAL file can briefly disappear when idle.** SQLite deletes a fully
  checkpointed `-wal` file when the last connection closes. The app keeps an
  idle process-lifetime connection (`_keepalive`) to mimic a real connection
  pool, but an empty WAL can still be reaped; it is recreated on the next
  write. `GET /wal/status` reports whatever is on disk at call time. To keep
  frames observable for the repro, use `/demo/held-writer` (a connection that
  stays open) rather than one-request-per-row writes.
* **`snapshot_retained` does not name the blocker.** It reports that frames
  remain occupied (a long reader's snapshot or the writer), not which
  connection/process holds them; the `/wal/long-read` registry only tracks
  holders created through this API on that process, not external clients.
* **`wait_ms` bounds lock waiting, not total HTTP latency.** It caps the
  SQLite busy wait inside the pragma; request queueing or thread-pool scheduling
  under overload is outside this bound.
* The concurrency storm exercises interleaved HTTP requests; it does not
  formally prove race-freedom — the safety argument rests on the transactional
  design (triggers + atomic finalize), which the tests probe but cannot
  exhaustively verify.
