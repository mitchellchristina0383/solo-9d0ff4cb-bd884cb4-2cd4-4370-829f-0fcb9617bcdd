"""
Online enablement of a referential constraint on a legacy SQLite database.

Legacy situation
----------------
``deployments.service_id`` references ``services.id`` but the column was created
WITHOUT a foreign key, so orphan deployments (pointing at deleted / never
existing services) exist. SQLite cannot add an FK to an existing table without
rebuilding it, so this demo enables the constraint *logically* and *online*,
modeled on PostgreSQL's ``NOT VALID`` -> ``VALIDATE CONSTRAINT`` flow:

  state: off  --/validation/start-->  validating  --/validation/finalize-->  enabled

Mechanism
---------
1. /validation/start installs four enforcement triggers and flips the state to
   ``validating`` inside ONE immediate transaction. From that commit on, no
   writer in ANY process can create a new orphan (insert bad deployment,
   re-point a deployment to a missing service, delete/renumber a referenced
   service). Writes that keep the constraint are still accepted, and existing
   orphans can still be fixed. The triggers live in the SQLite schema, so they
   are shared by both service processes and survive restarts.
2. /validation/step scans ``deployments`` in id-ordered batches (cursor kept in
   ``validation_progress``, committed per batch -> crash resumable) and logs
   orphans into ``validation_findings``.
3. /validation/finalize, inside ONE immediate (write-locked) transaction:
   drains the unscanned tail, counts *live* orphans, and only if zero flips the
   state to ``enabled``. Because the write lock is held, no writer on either
   process can interleave between the check and the state flip, and rows
   scanned earlier cannot have become invalid (the triggers forbid it). If
   orphans remain, the state stays ``validating`` and the caller gets the
   current orphan ids; it can be retried after corrections.

Invariants
----------
* From validation start: no new orphans, from any process, ever.
* Corrections interleaved with the scan can only shrink the orphan set; the
  atomic finalize re-checks every row, so nothing slips through.
* ``enabled`` is only ever written in the same transaction as a zero-orphan
  check, and the triggers persist -> after a failure or a restart the database
  can never be in an "enabled but orphans exist" state.
"""

import enum
import os
import sqlite3
import struct
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

DB_PATH = os.environ.get("DB_PATH", "data/app.db")
SERVER_NAME = os.environ.get("SERVER_NAME", "app")

# Upper bound for the checkpoint request's own lock wait. It stays strictly
# below the 30 s busy_timeout of ordinary write connections (see ``connect``),
# so a stuck checkpoint can never delay a normal write up to the write timeout.
CHECKPOINT_WAIT_CAP_MS = 25_000
# A checkpoint waits this long at most; readers are re-probed with a zero
# timeout so the probe itself never blocks.
WAL_HEADER_SIZE = 32
WAL_FRAME_HEADER_SIZE = 24
# SQLite WAL magic numbers (big-endian / little-endian checksum variants).
WAL_MAGICS = {0x377F0682, 0x377F0683}

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS services (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
-- NOTE: no FOREIGN KEY on service_id. That is the legacy defect being fixed.
CREATE TABLE IF NOT EXISTS deployments (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    service_id INTEGER NOT NULL,
    note       TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS constraint_state (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    state      TEXT NOT NULL CHECK (state IN ('off', 'validating', 'enabled')),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS validation_progress (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    cursor      INTEGER NOT NULL DEFAULT 0,
    scanned     INTEGER NOT NULL DEFAULT 0,
    started_at  TEXT,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS validation_findings (
    deployment_id INTEGER PRIMARY KEY,
    service_id    INTEGER NOT NULL,
    found_at      TEXT NOT NULL
);
"""

TRIGGER_DDL = [
    """
    CREATE TRIGGER IF NOT EXISTS trg_deployments_ins_fk
    BEFORE INSERT ON deployments FOR EACH ROW
    WHEN NOT EXISTS (SELECT 1 FROM services WHERE id = NEW.service_id)
    BEGIN
        SELECT RAISE(ABORT, 'FK violation: deployments.service_id has no matching services.id');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_deployments_upd_fk
    BEFORE UPDATE OF service_id ON deployments FOR EACH ROW
    WHEN NOT EXISTS (SELECT 1 FROM services WHERE id = NEW.service_id)
    BEGIN
        SELECT RAISE(ABORT, 'FK violation: deployments.service_id has no matching services.id');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_services_del_fk
    BEFORE DELETE ON services FOR EACH ROW
    WHEN EXISTS (SELECT 1 FROM deployments WHERE service_id = OLD.id)
    BEGIN
        SELECT RAISE(ABORT, 'FK violation: services.id is still referenced by deployments');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_services_upd_fk
    BEFORE UPDATE OF id ON services FOR EACH ROW
    WHEN EXISTS (SELECT 1 FROM deployments WHERE service_id = OLD.id)
    BEGIN
        SELECT RAISE(ABORT, 'FK violation: services.id is still referenced by deployments');
    END
    """,
]

TRIGGER_NAMES = [
    "trg_deployments_ins_fk",
    "trg_deployments_upd_fk",
    "trg_services_del_fk",
    "trg_services_upd_fk",
]


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect() -> sqlite3.Connection:
    # isolation_level=None -> autocommit; multi-statement sections use
    # explicit BEGIN IMMEDIATE so check-and-act sequences are atomic
    # across both processes sharing the file.
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


@contextmanager
def immediate_txn(conn: sqlite3.Connection):
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


# --------------------------------------------------------------------------- #
# WAL observation / checkpoint helpers
# --------------------------------------------------------------------------- #

class CheckpointMode(str, enum.Enum):
    # Values are the exact SQLite pragma keywords; never interpolate raw input.
    passive = "PASSIVE"
    full = "FULL"
    restart = "RESTART"
    truncate = "TRUNCATE"


def _file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def read_wal_frames() -> dict:
    """Parse the WAL file header to count frames WITHOUT touching the database.

    A read-only filesystem inspection: it takes no lock and runs no SQL, so
    GET /wal/status cannot itself change the database or the WAL. A WAL file is
    a 32-byte header followed by (24-byte frame header + page) per frame; the
    page size lives at header offset 8 (value 1 encodes 65536).
    """
    wal_path = DB_PATH + "-wal"
    size = _file_size(wal_path)
    info = {"wal_exists": size > 0, "wal_size_bytes": size,
            "wal_frames": None, "wal_page_size": None, "wal_header_valid": None}
    if size == 0:
        info["wal_frames"] = 0
        return info
    try:
        with open(wal_path, "rb") as fh:
            header = fh.read(WAL_HEADER_SIZE)
    except OSError:
        return info
    if len(header) < WAL_HEADER_SIZE:
        return info
    magic = struct.unpack(">I", header[0:4])[0]
    if magic not in WAL_MAGICS:
        info["wal_header_valid"] = False
        return info
    page_size = struct.unpack(">I", header[8:12])[0]
    if page_size == 1:
        page_size = 65536
    info["wal_header_valid"] = True
    info["wal_page_size"] = page_size
    info["wal_frames"] = max(0, (size - WAL_HEADER_SIZE) // (WAL_FRAME_HEADER_SIZE + page_size))
    return info


def wal_snapshot() -> dict:
    """Current on-disk WAL state plus the DB journal mode. Read-only."""
    db_size = _file_size(DB_PATH)
    shm_size = _file_size(DB_PATH + "-shm")
    info = read_wal_frames()
    conn = connect()
    try:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    finally:
        conn.close()
    return {
        "journal_mode": mode,
        "db_size_bytes": db_size,
        "shm_size_bytes": shm_size,
        **info,
        "truncated": info["wal_size_bytes"] == 0,
    }


def run_checkpoint(mode: CheckpointMode, wait_ms: int) -> dict:
    """Run one bounded checkpoint and report what actually happened.

    SQLite contract (verified on 3.40.x): ``PRAGMA wal_checkpoint`` does NOT
    raise on lock contention — it returns a row ``(busy, log, checkpointed)``.
    ``busy=1`` means some lock/snapshot could not be acquired; the
    ``busy_timeout`` we install on this *dedicated* connection still bounds how
    long the pragma blocks, so a concurrent request can never wait forever nor
    inherit the 30 s timeout of ordinary write connections.

    ``checkpointed`` counts frames copied back into the main DB. It is NOT the
    same as disk freed: only a successful TRUNCATE returns WAL bytes to the
    filesystem, and a long-lived reader can keep old frames (and the bytes)
    occupied even after every frame was copied back.
    """
    conn = connect()
    try:
        conn.execute(f"PRAGMA busy_timeout = {int(wait_ms)}")
        before = wal_snapshot()
        started = time.monotonic()
        try:
            busy, log_frames, checkpointed_frames = conn.execute(
                f"PRAGMA wal_checkpoint({mode.value})"
            ).fetchone()
            error = None
        except sqlite3.OperationalError as exc:
            # Defensive: some SQLite builds surface SQLITE_BUSY as an error.
            busy, log_frames, checkpointed_frames = 1, -1, -1
            error = str(exc)
        waited_ms = round((time.monotonic() - started) * 1000, 1)
        after = wal_snapshot()
    finally:
        conn.close()

    # A zero-timeout PASSIVE on a fresh connection is a non-blocking diagnostic:
    # it reports how many frames are copyable right now without waiting. It is
    # NOT used to decide retention — a fresh connection has a newer read mark and
    # may not see the old snapshot that actually pins the frames.
    probe = connect()
    try:
        probe.execute("PRAGMA busy_timeout = 0")
        probe_busy, probe_log, probe_checkpointed = probe.execute(
            "PRAGMA wal_checkpoint(PASSIVE)"
        ).fetchone()
    finally:
        probe.close()

    frames_backfilled = checkpointed_frames if checkpointed_frames >= 0 else 0
    space_released_bytes = max(0, before["wal_size_bytes"] - after["wal_size_bytes"])
    # "Truncated" means the WAL holds no bytes afterwards (a zero-length or
    # absent WAL). An already-empty WAL before the call stays empty, so that is
    # also a truncated/clean end state rather than a failure.
    truncated = after["wal_size_bytes"] == 0
    # The authoritative "still occupied" signal: the checkpoint itself could not
    # finish (busy=1) AND frames still physically remain in the WAL. This holds
    # whether the blocker is a long-lived reader's old snapshot or the writer;
    # either way the on-disk space is NOT released. We deliberately do not treat
    # copied-back frames (frames_backfilled) as freed space.
    snapshot_retained = bool(busy == 1 and (after["wal_frames"] or 0) > 0)

    if not busy and (mode != CheckpointMode.truncate or truncated):
        outcome = "truncated" if mode == CheckpointMode.truncate else "checkpointed"
    elif error is not None:
        outcome = "error"
    elif wait_ms > 0 and waited_ms >= wait_ms * 0.9:
        # The caller asked to wait and the pragma used essentially the whole
        # budget without acquiring what it needed: a bounded timeout.
        outcome = "timeout"
    else:
        outcome = "busy"

    return {
        "mode": mode.value,
        "requested_wait_ms": wait_ms,
        "waited_ms": waited_ms,
        "outcome": outcome,
        "busy": bool(busy),
        "error": error,
        "frames_in_wal_reported": log_frames,
        "frames_checkpointed_reported": checkpointed_frames,
        "frames_backfilled": frames_backfilled,
        "snapshot_retained": snapshot_retained,
        "truncated": truncated,
        "space_released_bytes": space_released_bytes,
        "probe_busy": bool(probe_busy),
        "probe_frames_in_wal": probe_log,
        "probe_frames_checkpointed": probe_checkpointed,
        "before": before,
        "after": after,
    }


# A process-lifetime idle connection. Real services keep a connection pool open;
# without any open connection SQLite deletes the -wal file on last-close, which
# would make the on-disk WAL flicker out of existence between requests. This
# idle connection holds no transaction (so it pins no snapshot and blocks no
# checkpoint) but keeps the WAL file present for stable observation.
_keepalive: sqlite3.Connection | None = None


def init_db() -> None:
    global _keepalive
    directory = os.path.dirname(os.path.abspath(DB_PATH))
    os.makedirs(directory, exist_ok=True)
    conn = connect()
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.executescript(SCHEMA_DDL)
        conn.execute(
            "INSERT OR IGNORE INTO constraint_state (id, state, updated_at) VALUES (1, 'off', ?)",
            (utcnow(),),
        )
        conn.execute("INSERT OR IGNORE INTO validation_progress (id) VALUES (1)")
        # Restart/crash recovery: if the constraint is (being) enforced, the
        # enforcement triggers must exist. Idempotent.
        state = conn.execute("SELECT state FROM constraint_state WHERE id = 1").fetchone()["state"]
        if state in ("validating", "enabled"):
            for ddl in TRIGGER_DDL:
                conn.execute(ddl)
    finally:
        conn.close()
    _keepalive = connect()


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    try:
        yield
    finally:
        if _keepalive is not None:
            _keepalive.close()


app = FastAPI(
    title="Online FK enablement demo",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "Legacy `deployments.service_id` has no enforced foreign key. "
        "Flow: seed data (orphans allowed while state=off) -> POST /validation/start "
        "-> POST /validation/step (batched scan) -> fix orphans via PATCH /deployments/{id} "
        "-> POST /validation/finalize (atomic zero-orphan gate) -> state=enabled."
    ),
)


@app.exception_handler(sqlite3.OperationalError)
async def operational_error_handler(_, exc: sqlite3.OperationalError):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=503, content={"detail": f"database temporarily unavailable: {exc}"})


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

def get_state(conn: sqlite3.Connection) -> str:
    return conn.execute("SELECT state FROM constraint_state WHERE id = 1").fetchone()["state"]


def live_orphan_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT d.id, d.service_id, d.note
        FROM deployments d
        WHERE NOT EXISTS (SELECT 1 FROM services s WHERE s.id = d.service_id)
        ORDER BY d.id
        """
    ).fetchall()


def scan_batch(conn: sqlite3.Connection, batch_size: int) -> dict:
    """Scan up to ``batch_size`` unscanned deployments. Caller holds the txn."""
    prog = conn.execute("SELECT cursor, scanned FROM validation_progress WHERE id = 1").fetchone()
    cursor, scanned = prog["cursor"], prog["scanned"]
    rows = conn.execute(
        "SELECT id, service_id FROM deployments WHERE id > ? ORDER BY id LIMIT ?",
        (cursor, batch_size),
    ).fetchall()
    found = []
    for row in rows:
        exists = conn.execute("SELECT 1 FROM services WHERE id = ?", (row["service_id"],)).fetchone()
        if not exists:
            conn.execute(
                "INSERT OR IGNORE INTO validation_findings (deployment_id, service_id, found_at)"
                " VALUES (?, ?, ?)",
                (row["id"], row["service_id"], utcnow()),
            )
            found.append(row["id"])
    if rows:
        cursor = rows[-1]["id"]
    scanned += len(rows)
    remaining = conn.execute(
        "SELECT COUNT(*) AS c FROM deployments WHERE id > ?", (cursor,)
    ).fetchone()["c"]
    conn.execute(
        "UPDATE validation_progress SET cursor = ?, scanned = ? WHERE id = 1",
        (cursor, scanned),
    )
    return {
        "batch": len(rows),
        "orphans_in_batch": found,
        "cursor": cursor,
        "scanned_total": scanned,
        "remaining": remaining,
    }


# --------------------------------------------------------------------------- #
# services
# --------------------------------------------------------------------------- #

class ServiceIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)


@app.post("/services", status_code=201)
def create_service(body: ServiceIn):
    conn = connect()
    try:
        try:
            cur = conn.execute("INSERT INTO services (name) VALUES (?)", (body.name,))
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {"served_by": SERVER_NAME, "id": cur.lastrowid, "name": body.name}
    finally:
        conn.close()


@app.get("/services")
def list_services():
    conn = connect()
    try:
        rows = conn.execute(
            """
            SELECT s.id, s.name,
                   (SELECT COUNT(*) FROM deployments d WHERE d.service_id = s.id) AS deployment_count
            FROM services s ORDER BY s.id
            """
        ).fetchall()
        return {"served_by": SERVER_NAME, "services": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.delete("/services/{service_id}")
def delete_service(service_id: int):
    conn = connect()
    try:
        try:
            cur = conn.execute("DELETE FROM services WHERE id = ?", (service_id,))
        except sqlite3.IntegrityError as exc:
            # Enforcement trigger (state validating/enabled) refused the delete.
            raise HTTPException(status_code=409, detail=f"refused: {exc}")
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="service not found")
        return {"served_by": SERVER_NAME, "deleted_service_id": service_id}
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# deployments
# --------------------------------------------------------------------------- #

class DeploymentIn(BaseModel):
    service_id: int
    note: str = ""


class DeploymentPatch(BaseModel):
    service_id: int


@app.post("/deployments", status_code=201)
def create_deployment(body: DeploymentIn):
    conn = connect()
    try:
        try:
            cur = conn.execute(
                "INSERT INTO deployments (service_id, note) VALUES (?, ?)",
                (body.service_id, body.note),
            )
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail=f"refused: {exc}")
        return {"served_by": SERVER_NAME, "id": cur.lastrowid, "service_id": body.service_id}
    finally:
        conn.close()


@app.get("/deployments")
def list_deployments():
    conn = connect()
    try:
        rows = conn.execute(
            """
            SELECT d.id, d.service_id, d.note,
                   EXISTS (SELECT 1 FROM services s WHERE s.id = d.service_id) AS valid
            FROM deployments d ORDER BY d.id
            """
        ).fetchall()
        deployments = [
            {"id": r["id"], "service_id": r["service_id"], "note": r["note"], "valid": bool(r["valid"])}
            for r in rows
        ]
        return {"served_by": SERVER_NAME, "deployments": deployments}
    finally:
        conn.close()


@app.patch("/deployments/{deployment_id}")
def patch_deployment(deployment_id: int, body: DeploymentPatch):
    """Re-point a deployment. This is how legacy orphans are corrected; the
    triggers only forbid pointing at a *missing* service."""
    conn = connect()
    try:
        try:
            cur = conn.execute(
                "UPDATE deployments SET service_id = ? WHERE id = ?",
                (body.service_id, deployment_id),
            )
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail=f"refused: {exc}")
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="deployment not found")
        return {"served_by": SERVER_NAME, "id": deployment_id, "service_id": body.service_id}
    finally:
        conn.close()


@app.delete("/deployments/{deployment_id}")
def delete_deployment(deployment_id: int):
    conn = connect()
    try:
        cur = conn.execute("DELETE FROM deployments WHERE id = ?", (deployment_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="deployment not found")
        return {"served_by": SERVER_NAME, "deleted_deployment_id": deployment_id}
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# orphans
# --------------------------------------------------------------------------- #

@app.get("/orphans")
def get_orphans():
    """Live view of deployments whose service_id currently has no service."""
    conn = connect()
    try:
        rows = live_orphan_rows(conn)
        return {
            "served_by": SERVER_NAME,
            "count": len(rows),
            "orphans": [dict(r) for r in rows],
        }
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# validation lifecycle
# --------------------------------------------------------------------------- #

@app.post("/validation/start")
def validation_start():
    """Begin online validation: install enforcement triggers and switch
    state off -> validating, atomically. From this commit on, no process can
    create new orphans."""
    conn = connect()
    try:
        with immediate_txn(conn):
            state = get_state(conn)
            if state == "validating":
                raise HTTPException(status_code=409, detail="validation already running")
            if state == "enabled":
                raise HTTPException(status_code=409, detail="constraint already enabled")
            for ddl in TRIGGER_DDL:
                conn.execute(ddl)
            conn.execute("DELETE FROM validation_findings")
            conn.execute(
                "UPDATE validation_progress SET cursor = 0, scanned = 0,"
                " started_at = ?, finished_at = NULL WHERE id = 1",
                (utcnow(),),
            )
            conn.execute(
                "UPDATE constraint_state SET state = 'validating', updated_at = ? WHERE id = 1",
                (utcnow(),),
            )
        return {
            "served_by": SERVER_NAME,
            "state": "validating",
            "detail": "enforcement triggers installed; batched scan ready (POST /validation/step)",
        }
    finally:
        conn.close()


@app.post("/validation/step")
def validation_step(batch_size: int = Query(100, ge=1, le=10000)):
    """Scan one batch of deployments (id-ordered, resumable cursor)."""
    conn = connect()
    try:
        with immediate_txn(conn):
            state = get_state(conn)
            if state != "validating":
                raise HTTPException(
                    status_code=409,
                    detail=f"validation not running (state={state}); POST /validation/start first",
                )
            result = scan_batch(conn, batch_size)
        return {"served_by": SERVER_NAME, **result}
    finally:
        conn.close()


@app.post("/validation/finalize")
def validation_finalize():
    """Atomically: drain the unscanned tail, require zero live orphans, and
    only then mark the constraint enabled. Runs inside one write transaction,
    so no writer on either process can interleave with the decision."""
    conn = connect()
    try:
        with immediate_txn(conn):
            state = get_state(conn)
            if state == "enabled":
                raise HTTPException(status_code=409, detail="constraint already enabled")
            if state != "validating":
                raise HTTPException(
                    status_code=409,
                    detail=f"validation not running (state={state}); POST /validation/start first",
                )
            tail = scan_batch(conn, 1_000_000)
            orphans = live_orphan_rows(conn)
            if orphans:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "cannot enable constraint: orphan deployments remain",
                        "orphan_deployment_ids": [r["id"] for r in orphans],
                    },
                )
            conn.execute(
                "UPDATE constraint_state SET state = 'enabled', updated_at = ? WHERE id = 1",
                (utcnow(),),
            )
            conn.execute(
                "UPDATE validation_progress SET finished_at = ? WHERE id = 1",
                (utcnow(),),
            )
        return {
            "served_by": SERVER_NAME,
            "state": "enabled",
            "tail_scanned": tail["batch"],
            "detail": "all existing deployments valid; constraint now enforced for all writes",
        }
    finally:
        conn.close()


@app.get("/validation/status")
def validation_status():
    conn = connect()
    try:
        st = conn.execute("SELECT state, updated_at FROM constraint_state WHERE id = 1").fetchone()
        prog = conn.execute("SELECT * FROM validation_progress WHERE id = 1").fetchone()
        orphans = live_orphan_rows(conn)
        findings = conn.execute("SELECT COUNT(*) AS c FROM validation_findings").fetchone()["c"]
        present = {
            r["name"]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'")
        }
        return {
            "served_by": SERVER_NAME,
            "state": st["state"],
            "state_updated_at": st["updated_at"],
            "scan": {
                "cursor": prog["cursor"],
                "scanned": prog["scanned"],
                "started_at": prog["started_at"],
                "finished_at": prog["finished_at"],
            },
            "live_orphans": len(orphans),
            "findings_recorded": findings,
            "enforcement_triggers_present": sorted(set(TRIGGER_NAMES) & present),
        }
    finally:
        conn.close()


@app.get("/validation/findings")
def validation_findings():
    """History of orphans found by the scan, reconciled against current data."""
    conn = connect()
    try:
        rows = conn.execute(
            """
            SELECT f.deployment_id, f.service_id, f.found_at,
                   CASE
                       WHEN d.id IS NULL THEN 'deleted'
                       WHEN EXISTS (SELECT 1 FROM services s WHERE s.id = d.service_id) THEN 'fixed'
                       ELSE 'still_orphan'
                   END AS current_status
            FROM validation_findings f
            LEFT JOIN deployments d ON d.id = f.deployment_id
            ORDER BY f.deployment_id
            """
        ).fetchall()
        return {"served_by": SERVER_NAME, "findings": [dict(r) for r in rows]}
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# WAL checkpointing (disk reclamation while two processes keep reading/writing)
# --------------------------------------------------------------------------- #

# Tracks deliberately held read transactions (the long-read repro). The dict
# is process-local: it only bookkeeps THIS process's holders. A connection may
# only be used by the thread that created it (sqlite3 default), so the holder
# connection is created and destroyed inside the worker thread; this dict only
# keeps the tokens of live holders. Keeping the connection OPEN is also what
# keeps the -wal file present: SQLite deletes the WAL when the last connection
# closes.
_locks_guard = threading.Lock()
_locks_held: set[str] = set()


@app.get("/wal/status")
def wal_status():
    """Read-only view of the WAL on disk. Runs no checkpoint and takes no DB
    lock, so it never mutates the database and is safe to poll during a long
    read. Frame counts are parsed from the WAL file header."""
    return {"served_by": SERVER_NAME, **wal_snapshot()}


class CheckpointBody(BaseModel):
    mode: CheckpointMode = Field(
        default=CheckpointMode.truncate,
        description="PASSIVE copies what it can; FULL waits for readers; "
        "RESTART also restarts the WAL; TRUNCATE additionally truncates the "
        "WAL to zero bytes (the only mode that returns disk space).",
    )
    wait_ms: int = Field(
        default=5000,
        ge=0,
        le=CHECKPOINT_WAIT_CAP_MS,
        description="Upper bound on lock/snapshot waiting for THIS request. "
        "Capped below the 30 s busy_timeout used by ordinary write connections, "
        "so checkpoint contention can never stretch a normal write to its timeout.",
    )


@app.post("/wal/checkpoint")
def wal_checkpoint(body: CheckpointBody | None = None):
    """Checkpoint the WAL with a bounded wait.

    Always returns 200 with the progress made so far — including on timeout or
    contention — because SQLite may already have copied frames back even when
    it could not truncate. Read ``outcome`` / ``truncated`` /
    ``snapshot_retained`` rather than assuming success:

    * ``truncated``        — TRUNCATE completed; WAL bytes were returned.
    * ``checkpointed``     — non-truncate mode completed its copy.
    * ``busy``             — a reader/writer blocked completion immediately.
    * ``timeout``          — waited up to ``wait_ms`` and still could not finish.
    * ``error``            — unexpected database error (message in ``error``).

    ``frames_backfilled`` is progress into the main DB; it is NOT disk freed.
    When ``snapshot_retained`` is true a long-lived reader still pins old WAL
    frames, so the space stays occupied until that reader ends — re-checkpoint
    afterwards. Re-query GET /wal/status for the current on-disk truth.
    """
    body = body or CheckpointBody()
    result = run_checkpoint(body.mode, body.wait_ms)
    return {"served_by": SERVER_NAME, **result}


class LongReadBody(BaseModel):
    hold_seconds: float = Field(
        default=10.0, gt=0, le=120.0,
        description="How long the read transaction stays open, pinning the snapshot.",
    )
    token: str | None = Field(default=None, max_length=64)


@app.post("/wal/long-read", status_code=202)
def wal_long_read(body: LongReadBody):
    """Open a read transaction and keep it (and its snapshot) open in a
    background thread for ``hold_seconds``. The connection is created inside
    that thread and stays open for the whole hold, which both pins the WAL
    snapshot and keeps the -wal file present. While held, a TRUNCATE checkpoint
    can copy frames back but cannot truncate: the response reports
    ``snapshot_retained=true``. GET /wal/long-read shows this process's holders."""
    token = body.token or f"lr-{utcnow()}-{threading.get_ident()}"
    hold_seconds = body.hold_seconds

    def hold():
        # Dedicated connection created AND used in this same thread; kept open
        # until the sleep ends so its read mark pins old WAL frames.
        conn = connect()
        try:
            conn.execute("BEGIN")
            conn.execute("SELECT COUNT(*) FROM deployments").fetchone()
            time.sleep(hold_seconds)
        except sqlite3.Error:
            pass
        finally:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            conn.close()
            with _locks_guard:
                _locks_held.discard(token)

    with _locks_guard:
        _locks_held.add(token)
    threading.Thread(target=hold, daemon=True).start()
    return {
        "served_by": SERVER_NAME,
        "token": token,
        "hold_seconds": hold_seconds,
        "detail": "snapshot held; POST /wal/checkpoint now reports snapshot_retained until it ends",
    }


@app.get("/wal/long-read")
def wal_long_read_status():
    with _locks_guard:
        holders = sorted(_locks_held)
    return {"served_by": SERVER_NAME, "holders_on_this_process": holders}


# --------------------------------------------------------------------------- #
# demo / ops
# --------------------------------------------------------------------------- #

@app.post("/demo/reset")
def demo_reset():
    """Demo helper: wipe all data, drop enforcement triggers, state -> off."""
    conn = connect()
    try:
        with immediate_txn(conn):
            for name in TRIGGER_NAMES:
                conn.execute(f"DROP TRIGGER IF EXISTS {name}")
            conn.execute("DELETE FROM deployments")
            conn.execute("DELETE FROM services")
            conn.execute("DELETE FROM validation_findings")
            try:
                conn.execute("DELETE FROM sqlite_sequence")
            except sqlite3.OperationalError:
                pass
            conn.execute(
                "UPDATE validation_progress SET cursor = 0, scanned = 0,"
                " started_at = NULL, finished_at = NULL WHERE id = 1"
            )
            conn.execute(
                "UPDATE constraint_state SET state = 'off', updated_at = ? WHERE id = 1",
                (utcnow(),),
            )
        return {"served_by": SERVER_NAME, "state": "off"}
    finally:
        conn.close()


class BulkWriteBody(BaseModel):
    service_id: int
    count: int = Field(default=50, ge=1, le=5000)
    note: str = "bulk"


@app.post("/demo/bulk-write", status_code=201)
def demo_bulk_write(body: BulkWriteBody):
    """Insert ``count`` deployments in ONE transaction on ONE connection.

    SQLite auto-checkpoints and deletes an empty WAL when a connection closes,
    so ordinary one-request-per-row writes leave little to observe. For the WAL
    repro use POST /demo/held-writer instead, which keeps the writer connection
    open; this endpoint is a simple batched insert and the FK triggers apply."""
    conn = connect()
    try:
        try:
            with immediate_txn(conn):
                first_id = None
                for i in range(body.count):
                    cur = conn.execute(
                        "INSERT INTO deployments (service_id, note) VALUES (?, ?)",
                        (body.service_id, f"{body.note}-{i}"),
                    )
                    if first_id is None:
                        first_id = cur.lastrowid
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail=f"refused: {exc}")
        return {"served_by": SERVER_NAME, "inserted": body.count, "first_id": first_id}
    finally:
        conn.close()


# Held writer connections for the long-read/WAL repro. A connection is only
# usable from its creating thread, so each holder owns a worker thread that
# keeps the connection (and its un-checkpointed WAL frames) open.
_writers_guard = threading.Lock()
_writers_held: set[str] = set()


class HeldWriterBody(BaseModel):
    hold_seconds: float = Field(default=15.0, gt=0, le=120.0)
    count: int = Field(default=40, ge=1, le=2000,
                       description="rows written on the held connection before it idles")
    service_id: int = 1
    token: str | None = Field(default=None, max_length=64)


@app.post("/demo/held-writer", status_code=202)
def demo_held_writer(body: HeldWriterBody):
    """Open a writer connection, write ``count`` rows, then keep the connection
    OPEN (idle, transaction committed) for ``hold_seconds``. Because it never
    closes, the written frames remain in the WAL instead of being reaped — the
    building block for a mid-WAL snapshot. Combine with POST /wal/long-read to
    reproduce 'frames backfilled but space not released'."""
    token = body.token or f"hw-{utcnow()}-{threading.get_ident()}"
    hold_seconds, count, service_id = body.hold_seconds, body.count, body.service_id

    def hold():
        conn = connect()
        try:
            with immediate_txn(conn):
                for i in range(count):
                    conn.execute(
                        "INSERT INTO deployments (service_id, note) VALUES (?, ?)",
                        (service_id, f"held-{i}"),
                    )
            # connection stays open and idle; its WAL frames are not reaped
            time.sleep(hold_seconds)
        except sqlite3.Error:
            pass
        finally:
            conn.close()
            with _writers_guard:
                _writers_held.discard(token)

    with _writers_guard:
        _writers_held.add(token)
    threading.Thread(target=hold, daemon=True).start()
    return {
        "served_by": SERVER_NAME,
        "token": token,
        "hold_seconds": hold_seconds,
        "written": count,
        "detail": "writer connection held open; its frames stay in the WAL",
    }


@app.get("/demo/held-writer")
def demo_held_writer_status():
    with _writers_guard:
        return {"served_by": SERVER_NAME, "holders_on_this_process": sorted(_writers_held)}


@app.get("/health")
def health():
    return {"ok": True, "served_by": SERVER_NAME}


@app.get("/")
def index():
    return {
        "service": "online FK enablement demo",
        "served_by": SERVER_NAME,
        "docs": "/docs",
        "flow": [
            "POST /services, POST /deployments (orphans allowed while state=off)",
            "POST /validation/start",
            "POST /validation/step?batch_size=N (repeat)",
            "GET /orphans, PATCH /deployments/{id} to correct",
            "POST /validation/finalize",
            "GET /validation/status",
        ],
        "wal_ops": [
            "GET /wal/status (WAL file size / frames / truncated, read-only)",
            "POST /wal/checkpoint {mode, wait_ms} (bounded reclaim, reports progress even when blocked)",
            "POST /wal/long-read {hold_seconds} (repro: pin an old snapshot)",
        ],
    }
