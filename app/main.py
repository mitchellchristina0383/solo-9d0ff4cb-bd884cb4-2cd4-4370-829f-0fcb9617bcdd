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

import os
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

DB_PATH = os.environ.get("DB_PATH", "data/app.db")
SERVER_NAME = os.environ.get("SERVER_NAME", "app")

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


def init_db() -> None:
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


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


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
    }
