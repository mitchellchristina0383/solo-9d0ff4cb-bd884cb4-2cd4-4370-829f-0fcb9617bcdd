#!/usr/bin/env python3
"""
End-to-end self test for the online FK enablement demo.

Runs against two service processes (default ports 8001/8002) sharing one
SQLite database. Covers:

  Phase A  legacy writes -> start -> cross-process enforcement -> batched scan
           with interleaved corrections -> finalize fail -> fix -> finalize ok
           -> post-enable enforcement
  Phase B  restart recovery: restart mid-validation, restart after failed
           finalize, restart after enabled; invariant: never "enabled" while
           orphans exist
  Phase C  concurrency storm: parallel scanner / fixers / deleters / bad
           writers / invariant monitor across both processes

Usage:
  python3 tests/selftest.py [--app1 8001] [--app2 8002] \
      [--restart-hook "shell command to restart the app1 process"]

For Docker Compose:  --restart-hook "docker compose restart app1"
"""

import argparse
import json
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Thread

CHECKS = 0
FAILURES = []


def check(cond, msg):
    global CHECKS
    CHECKS += 1
    if cond:
        print(f"    ok   {msg}")
    else:
        FAILURES.append(msg)
        print(f"    FAIL {msg}")


def req(method, port, path, body=None):
    url = f"http://127.0.0.1:{port}{path}"
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, (json.loads(raw) if raw else {})
        except json.JSONDecodeError:
            return e.code, {"raw": raw[:200].decode(errors="replace")}


def expect(method, port, path, body, want, msg):
    try:
        st, js = req(method, port, path, body)
    except Exception as exc:  # connection refused etc.
        check(False, f"{msg} [{method} :{port}{path} raised {exc!r}]")
        return {}
    check(st == want, f"{msg} [{method} :{port}{path} -> {st}, want {want}]"
                      + ("" if st == want else f" body={js}"))
    return js


def wait_ready(port, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            st, _ = req("GET", port, "/health")
            if st == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def drain_scan(port):
    """Run /validation/step until nothing remains; return last response."""
    last = {}
    for _ in range(1000):
        st, js = req("POST", port, "/validation/step?batch_size=50")
        if st != 200:
            return None
        last = js
        if js["remaining"] == 0:
            return last
    return last


def fix_all_orphans(port, service_id):
    st, js = req("GET", port, "/orphans")
    assert st == 200, js
    for o in js["orphans"]:
        st2, js2 = req("PATCH", port, f"/deployments/{o['id']}", {"service_id": service_id})
        assert st2 == 200, (st2, js2)
    return len(js["orphans"])


# --------------------------------------------------------------------------- #
# Phase A: basic lifecycle
# --------------------------------------------------------------------------- #

def phase_a(app1, app2):
    print("== Phase A: legacy -> validating -> enabled lifecycle ==")

    js = expect("POST", app1, "/demo/reset", None, 200, "A0 reset")
    st, js = req("GET", app2, "/validation/status")
    check(js.get("state") == "off", "A0 state is off on both processes")
    check(js.get("enforcement_triggers_present") == [], "A0 no triggers while off")

    # Seed services via both processes.
    s_web = expect("POST", app1, "/services", {"name": "web"}, 201, "A1 create svc web (app1)")["id"]
    s_api = expect("POST", app1, "/services", {"name": "api"}, 201, "A1 create svc api (app1)")["id"]
    expect("POST", app2, "/services", {"name": "worker"}, 201, "A1 create svc worker (app2)")

    # Legacy mode: orphans can be created freely.
    d1 = expect("POST", app1, "/deployments", {"service_id": s_web, "note": "d1"}, 201, "A2 valid dep (app1)")["id"]
    d2 = expect("POST", app2, "/deployments", {"service_id": 9999, "note": "d2-orphan"}, 201, "A2 orphan dep allowed while off (app2)")["id"]
    d3 = expect("POST", app1, "/deployments", {"service_id": s_api, "note": "d3"}, 201, "A2 valid dep (app1)")["id"]
    d4 = expect("POST", app2, "/deployments", {"service_id": 9998, "note": "d4-orphan"}, 201, "A2 orphan dep allowed while off (app2)")["id"]
    expect("POST", app1, "/deployments", {"service_id": s_web, "note": "d5"}, 201, "A2 valid dep (app1)")

    st, js = req("GET", app2, "/orphans")
    check(st == 200 and {o["id"] for o in js["orphans"]} == {d2, d4}, "A3 /orphans lists the 2 seeded orphans")

    # Legacy mode: deleting a referenced service creates an orphan (the problem).
    s_tmp = expect("POST", app1, "/services", {"name": "tmp"}, 201, "A4 create svc tmp")["id"]
    d6 = expect("POST", app2, "/deployments", {"service_id": s_tmp, "note": "d6"}, 201, "A4 dep -> tmp")["id"]
    expect("DELETE", app2, f"/services/{s_tmp}", None, 200, "A4 legacy delete of referenced svc allowed (off)")
    st, js = req("GET", app1, "/orphans")
    check({o["id"] for o in js["orphans"]} == {d2, d4, d6}, "A4 delete created a new orphan (legacy defect)")

    # Start validation: triggers installed atomically with the state flip.
    js = expect("POST", app1, "/validation/start", None, 200, "A5 start validation (app1)")
    st, js = req("GET", app2, "/validation/status")
    check(js.get("state") == "validating", "A5 state=validating visible from app2")
    check(len(js.get("enforcement_triggers_present", [])) == 4, "A5 all 4 triggers present")
    expect("POST", app2, "/validation/start", None, 409, "A5 second start rejected")

    # From validation start: no new orphans, from EITHER process.
    expect("POST", app2, "/deployments", {"service_id": 7777}, 409, "A6 insert orphan rejected (app2)")
    expect("PATCH", app1, f"/deployments/{d1}", {"service_id": 7777}, 409, "A6 re-point to missing svc rejected (app1)")
    expect("DELETE", app2, f"/services/{s_web}", None, 409, "A6 delete referenced svc rejected (app2)")
    s_solo = expect("POST", app1, "/services", {"name": "solo"}, 201, "A6 create unreferenced svc")["id"]
    expect("DELETE", app2, f"/services/{s_solo}", None, 200, "A6 delete unreferenced svc still allowed")
    expect("POST", app2, "/deployments", {"service_id": s_api, "note": "valid"}, 201, "A6 valid insert still accepted")

    # Old orphans can still be corrected.
    expect("PATCH", app2, f"/deployments/{d2}", {"service_id": s_web}, 200, "A7 fix old orphan (app2)")
    st, js = req("GET", app1, "/orphans")
    check({o["id"] for o in js["orphans"]} == {d4, d6}, "A7 orphan set shrank")

    # Batched scan across both processes.
    scanned_ids = []
    total_scanned = 0
    for port in (app1, app2, app1, app2, app1, app2):
        st, js = req("POST", port, "/validation/step?batch_size=2")
        check(st == 200, f"A8 step on :{port} ok")
        if st != 200:
            break
        total_scanned = js["scanned_total"]
        if js["remaining"] == 0:
            break
    st, js = req("GET", app1, "/validation/status")
    check(js["scan"]["scanned"] >= 6, "A8 all deployments scanned")
    st, js = req("GET", app2, "/validation/findings")
    found_ids = {f["deployment_id"] for f in js["findings"]}
    check(found_ids == {d4, d6}, f"A8 findings == remaining orphans at scan time (got {found_ids})")

    # Rows already scanned cannot be turned invalid afterwards.
    expect("PATCH", app1, f"/deployments/{d3}", {"service_id": 8888}, 409, "A9 re-break scanned row rejected")
    expect("DELETE", app2, f"/services/{s_api}", None, 409, "A9 delete svc of scanned row rejected")

    # Finalize gate: orphans remain -> 409, state stays validating.
    js = expect("POST", app1, "/validation/finalize", None, 409, "A10 finalize with orphans rejected")
    st, js = req("GET", app2, "/validation/status")
    check(js.get("state") == "validating", "A10 state still validating after failed finalize")

    # Correct the rest, then finalize succeeds.
    n = fix_all_orphans(app2, s_api)
    check(n == 2, "A11 fixed the 2 remaining orphans")
    st, js = req("GET", app1, "/orphans")
    check(js.get("count") == 0, "A11 no orphans left")
    js = expect("POST", app2, "/validation/finalize", None, 200, "A12 finalize succeeds (app2)")
    check(js.get("state") == "enabled", "A12 state=enabled")
    st, js = req("GET", app1, "/validation/status")
    check(js.get("state") == "enabled" and js.get("live_orphans") == 0, "A12 enabled + zero orphans (app1 view)")

    # Constraint keeps holding after enablement, on both processes.
    expect("POST", app1, "/deployments", {"service_id": 4242}, 409, "A13 bad insert rejected after enable (app1)")
    expect("PATCH", app2, f"/deployments/{d1}", {"service_id": 4242}, 409, "A13 bad re-point rejected after enable (app2)")
    expect("DELETE", app1, f"/services/{s_web}", None, 409, "A13 delete referenced svc rejected after enable")
    expect("POST", app2, "/deployments", {"service_id": s_web, "note": "post-enable"}, 201, "A13 valid insert accepted after enable")
    expect("POST", app1, "/validation/start", None, 409, "A13 re-start rejected once enabled")


# --------------------------------------------------------------------------- #
# Phase B: restart / crash recovery
# --------------------------------------------------------------------------- #

def phase_b(app1, app2, restart_hook):
    print("== Phase B: restart recovery ==")
    if not restart_hook:
        print("    (skipped: no --restart-hook given)")
        return

    expect("POST", app1, "/demo/reset", None, 200, "B0 reset")
    s1 = expect("POST", app1, "/services", {"name": "s1"}, 201, "B1 svc s1")["id"]
    s2 = expect("POST", app2, "/services", {"name": "s2"}, 201, "B1 svc s2")["id"]
    e1 = expect("POST", app1, "/deployments", {"service_id": s1}, 201, "B1 dep e1")["id"]
    e2 = expect("POST", app2, "/deployments", {"service_id": 5555}, 201, "B1 orphan e2")["id"]
    e3 = expect("POST", app1, "/deployments", {"service_id": 5556}, 201, "B1 orphan e3")["id"]
    expect("POST", app2, "/deployments", {"service_id": s2}, 201, "B1 dep e4")

    expect("POST", app2, "/validation/start", None, 200, "B2 start validation (app2)")
    st, js = req("POST", app1, "/validation/step?batch_size=2")
    check(st == 200 and js["cursor"] == e2 and js["scanned_total"] == 2, "B2 first batch scanned (cursor at e2)")

    # Crash app1 mid-validation.
    subprocess.run(restart_hook, shell=True, check=True)
    check(wait_ready(app1), "B3 app1 back after restart")
    st, js = req("GET", app1, "/validation/status")
    check(js.get("state") == "validating", "B3 state still validating after restart")
    check(js["scan"]["cursor"] == e2 and js["scan"]["scanned"] == 2, "B3 scan cursor survived restart")
    check(len(js.get("enforcement_triggers_present", [])) == 4, "B3 triggers survived restart")
    expect("POST", app1, "/deployments", {"service_id": 6666}, 409, "B3 enforcement works on restarted process")

    # Failed finalize, then another restart: must NOT become enabled.
    expect("POST", app1, "/validation/finalize", None, 409, "B4 finalize fails while orphans exist")
    subprocess.run(restart_hook, shell=True, check=True)
    check(wait_ready(app1), "B4 app1 back after 2nd restart")
    st, js = req("GET", app1, "/validation/status")
    check(js.get("state") == "validating", "B4 failure+restart did not produce enabled state")
    check(js.get("live_orphans", 0) == 2, "B4 orphans still tracked")

    # Resume after restart: fix orphans, drain, finalize.
    expect("PATCH", app1, f"/deployments/{e2}", {"service_id": s1}, 200, "B5 fix e2 after restart")
    expect("PATCH", app2, f"/deployments/{e3}", {"service_id": s2}, 200, "B5 fix e3 (app2)")
    last = drain_scan(app2)
    check(last is not None and last["remaining"] == 0, "B5 scan drained after restart")
    js = expect("POST", app1, "/validation/finalize", None, 200, "B5 finalize after restart")
    check(js.get("state") == "enabled", "B5 enabled")

    # Enabled state survives restart, enforcement intact, no orphans.
    subprocess.run(restart_hook, shell=True, check=True)
    check(wait_ready(app1), "B6 app1 back after 3rd restart")
    st, js = req("GET", app1, "/validation/status")
    check(js.get("state") == "enabled" and js.get("live_orphans") == 0,
          "B6 enabled + zero orphans after restart (no enabled-with-orphans)")
    expect("POST", app1, "/deployments", {"service_id": 6666}, 409, "B6 enforcement intact after restart")
    expect("POST", app2, "/deployments", {"service_id": s1}, 201, "B6 valid insert still works")


# --------------------------------------------------------------------------- #
# Phase C: concurrency storm
# --------------------------------------------------------------------------- #

def phase_c(app1, app2):
    print("== Phase C: concurrency storm (scan/fix/delete/bad-writes interleaved) ==")
    random.seed(20261118)

    expect("POST", app1, "/demo/reset", None, 200, "C0 reset")
    svc_ids = [
        expect("POST", app1, "/services", {"name": n}, 201, f"C0 svc {n}")["id"]
        for n in ("alpha", "beta", "gamma")
    ]
    # 3 guaranteed references (one per service) + 27 random valid + 30 orphans.
    for sid in svc_ids:
        req("POST", app2, "/deployments", {"service_id": sid, "note": "anchor"})
    for i in range(27):
        port = app1 if i % 2 == 0 else app2
        req("POST", port, "/deployments", {"service_id": random.choice(svc_ids), "note": f"v{i}"})
    for i in range(30):
        port = app1 if i % 2 == 0 else app2
        req("POST", port, "/deployments", {"service_id": 9000 + i, "note": f"orphan{i}"})
    st, js = req("GET", app1, "/orphans")
    check(js.get("count") == 30, "C0 seeded 30 orphans")

    expect("POST", app1, "/validation/start", None, 200, "C0 start validation")

    stop = threading.Event()
    errors = []
    lock = threading.Lock()

    def record(msg):
        with lock:
            errors.append(msg)

    def port_of(i):
        return app1 if i % 2 == 0 else app2

    def scanner():
        i = 0
        while not stop.is_set():
            try:
                st, js = req("POST", port_of(i), "/validation/step?batch_size=7")
                if st != 200:
                    record(f"scanner: step -> {st} {js}")
            except Exception as exc:
                record(f"scanner: {exc!r}")
            i += 1
            time.sleep(0.01)

    def fixer():
        while not stop.is_set():
            try:
                st, js = req("GET", port_of(random.randrange(2)), "/orphans")
                if st != 200:
                    record(f"fixer: orphans -> {st}")
                    continue
                for o in js["orphans"][:5]:
                    st2, js2 = req("PATCH", port_of(random.randrange(2)),
                                   f"/deployments/{o['id']}",
                                   {"service_id": random.choice(svc_ids)})
                    if st2 not in (200, 404):
                        record(f"fixer: patch {o['id']} -> {st2} {js2}")
            except Exception as exc:
                record(f"fixer: {exc!r}")
            time.sleep(0.01)

    def deleter():
        while not stop.is_set():
            try:
                st, js = req("GET", port_of(random.randrange(2)), "/orphans")
                if st != 200:
                    record(f"deleter: orphans -> {st}")
                    continue
                for o in js["orphans"]:
                    if o["id"] % 7 == 0:  # delete some orphans instead of fixing
                        st2, js2 = req("DELETE", port_of(random.randrange(2)),
                                       f"/deployments/{o['id']}", None)
                        if st2 not in (200, 404):
                            record(f"deleter: delete {o['id']} -> {st2} {js2}")
            except Exception as exc:
                record(f"deleter: {exc!r}")
            time.sleep(0.02)

    def bad_writer():
        while not stop.is_set():
            try:
                port = port_of(random.randrange(2))
                st, js = req("POST", port, "/deployments", {"service_id": 8888, "note": "bad"})
                if st != 409:
                    record(f"bad_writer: orphan insert -> {st} {js}")
                st, js = req("GET", port, "/deployments")
                if st == 200 and js["deployments"]:
                    d = random.choice(js["deployments"])
                    st2, js2 = req("PATCH", port, f"/deployments/{d['id']}", {"service_id": 8888})
                    if st2 not in (409, 404):
                        record(f"bad_writer: re-point -> {st2} {js2}")
                sid = random.choice(svc_ids)
                st3, js3 = req("DELETE", port, f"/services/{sid}", None)
                if st3 != 409:
                    record(f"bad_writer: delete referenced svc -> {st3} {js3}")
                st4, js4 = req("POST", port, "/deployments",
                               {"service_id": random.choice(svc_ids), "note": "ok"})
                if st4 != 201:
                    record(f"bad_writer: valid insert -> {st4} {js4}")
                name = f"tmp-{uuid.uuid4().hex[:12]}"
                st5, js5 = req("POST", port, "/services", {"name": name})
                if st5 != 201:
                    record(f"bad_writer: create tmp svc -> {st5} {js5}")
                else:
                    st6, js6 = req("DELETE", port, f"/services/{js5['id']}", None)
                    if st6 != 200:
                        record(f"bad_writer: delete unreferenced svc -> {st6} {js6}")
            except Exception as exc:
                record(f"bad_writer: {exc!r}")
            time.sleep(0.01)

    def monitor():
        while not stop.is_set():
            try:
                for port in (app1, app2):
                    st, js = req("GET", port, "/validation/status")
                    if st != 200:
                        record(f"monitor: status -> {st}")
                    elif js["state"] != "validating":
                        record(f"monitor: unexpected state {js['state']} on :{port}")
                    elif len(js["enforcement_triggers_present"]) != 4:
                        record(f"monitor: triggers missing on :{port}")
            except Exception as exc:
                record(f"monitor: {exc!r}")
            time.sleep(0.1)

    with ThreadPoolExecutor(max_workers=9) as pool:
        futures = [pool.submit(scanner),
                   pool.submit(fixer), pool.submit(fixer), pool.submit(fixer),
                   pool.submit(deleter),
                   pool.submit(bad_writer), pool.submit(bad_writer),
                   pool.submit(monitor)]
        time.sleep(8)
        stop.set()
        for f in futures:
            f.result(timeout=60)

    check(not errors, f"C1 storm produced no unexpected responses ({len(errors)} errors)"
                      + ("" if not errors else f": {errors[:5]}"))

    # Drain and reconcile.
    last = drain_scan(app1)
    check(last is not None and last["remaining"] == 0, "C2 scan drained after storm")
    n = fix_all_orphans(app2, svc_ids[0])
    print(f"    (fixed {n} orphans remaining after storm)")
    st, js = req("GET", app1, "/orphans")
    check(js.get("count") == 0, "C2 zero orphans after final fixes")

    js = expect("POST", app2, "/validation/finalize", None, 200, "C3 finalize after storm")
    check(js.get("state") == "enabled", "C3 enabled")
    for port in (app1, app2):
        st, js = req("GET", port, "/validation/status")
        check(js.get("state") == "enabled" and js.get("live_orphans") == 0,
              f"C3 enabled + zero orphans on :{port}")
    st, js = req("GET", app1, "/validation/findings")
    still = [f for f in js["findings"] if f["current_status"] == "still_orphan"]
    check(len(js["findings"]) > 0, "C4 scan recorded findings during storm")
    check(not still, "C4 every finding reconciled (fixed or deleted), none still orphan")

    expect("POST", app1, "/deployments", {"service_id": 8888}, 409, "C5 bad insert rejected after enable")
    expect("DELETE", app2, f"/services/{svc_ids[0]}", None, 409, "C5 delete referenced svc rejected after enable")


# --------------------------------------------------------------------------- #
# Phase D: WAL checkpointing under long reads, concurrency and dual processes
# --------------------------------------------------------------------------- #

def _wal_status(port):
    st, js = req("GET", port, "/wal/status")
    assert st == 200, js
    return js


def _checkpoint(port, mode="TRUNCATE", wait_ms=5000):
    st, js = req("POST", port, "/wal/checkpoint",
                 {"mode": mode, "wait_ms": wait_ms})
    assert st == 200, js
    return js


def _seed_writes(port, other, n):
    """Spread valid writes across both processes; needs an existing service 1."""
    for i in range(n):
        target = port if i % 2 == 0 else other
        st, _ = req("POST", target, "/deployments", {"service_id": 1, "note": f"w{i}"})
        assert st == 201, st


def phase_d(app1, app2):
    print("== Phase D: WAL checkpoint (long read / bounded wait / dual process) ==")

    expect("POST", app1, "/demo/reset", None, 200, "D0 reset")
    expect("POST", app1, "/services", {"name": "wal-svc"}, 201, "D0 seed service")["id"]
    expect("POST", app2, "/validation/start", None, 200, "D0 start validation (triggers on)")
    expect("POST", app1, "/validation/finalize", None, 200, "D0 finalize (constraint enabled)")

    # D1: read-only status reflects the on-disk WAL and never errors.
    js = _wal_status(app1)
    check(js["journal_mode"] == "wal", "D1 journal_mode=wal")
    check(isinstance(js["wal_size_bytes"], int) and js["wal_size_bytes"] >= 0, "D1 wal_size reported")
    check(js["truncated"] == (js["wal_size_bytes"] == 0), "D1 truncated flag matches wal size")

    # D2: a truncate with no readers ends truncated and never reports retained.
    # Short-lived writer connections may already have auto-checkpointed the WAL,
    # so this does not assume the WAL is non-empty beforehand; it asserts the
    # end state and the space-released accounting instead.
    _seed_writes(app1, app2, 30)
    before = _wal_status(app2)
    check(before["wal_size_bytes"] >= 0, "D2 WAL size readable after writes")
    js = _checkpoint(app1, "TRUNCATE", 5000)
    check(js["outcome"] == "truncated", "D2 clean checkpoint outcome=truncated")
    check(js["truncated"] is True and js["snapshot_retained"] is False, "D2 truncated, nothing retained")
    check(js["after"]["wal_size_bytes"] == 0, "D2 WAL truncated to zero bytes")
    check(js["space_released_bytes"] == before["wal_size_bytes"], "D2 released bytes match pre-checkpoint WAL")

    # D3: long reader pins an old snapshot; checkpoint reports retained, and
    # backfilled frames are NOT confused with released space. A held writer
    # keeps its frames in the WAL; frames written before the reader are
    # backfillable, frames written after stay pinned by the snapshot.
    st, _ = req("POST", app1, "/demo/held-writer",
                {"service_id": 1, "count": 40, "hold_seconds": 30})
    check(st == 202, "D3 held writer (pre-snapshot frames kept in WAL)")
    time.sleep(0.8)  # let the writer commit and leave its frames in the WAL
    expect("POST", app1, "/wal/long-read", {"hold_seconds": 25}, 202, "D3 long read started (app1)")
    time.sleep(0.5)
    st, _ = req("POST", app2, "/demo/held-writer",
                {"service_id": 1, "count": 40, "hold_seconds": 20})
    check(st == 202, "D3 held writer on app2 (post-snapshot frames)")
    time.sleep(0.8)
    held = _wal_status(app2)
    check(held["wal_size_bytes"] > 0 and (held["wal_frames"] or 0) > 0,
          f"D3 WAL retains frames during long read ({held['wal_frames']} frames, {held['wal_size_bytes']} bytes)")

    # D3a: zero-wait truncate must return immediately, busy, not truncated.
    t0 = time.monotonic()
    js = _checkpoint(app2, "TRUNCATE", 0)
    dt = time.monotonic() - t0
    check(js["outcome"] == "busy", f"D3a zero-wait checkpoint outcome=busy (got {js['outcome']})")
    check(js["busy"] is True, "D3a busy flag set")
    check(js["truncated"] is False, "D3a WAL not truncated while snapshot held")
    check(js["snapshot_retained"] is True, "D3a snapshot_retained=true (old snapshot still occupies)")
    check(js["space_released_bytes"] == 0, "D3a no disk space released despite any backfill")
    check(js["after"]["wal_size_bytes"] == held["wal_size_bytes"], "D3a WAL bytes unchanged")
    check(dt < 2.0, f"D3a zero-wait returned promptly ({dt:.2f}s)")

    # D3b: bounded wait is honored (waits ~wait_ms) then reports timeout, still
    # without releasing space.
    wait_ms = 1200
    t0 = time.monotonic()
    js = _checkpoint(app1, "TRUNCATE", wait_ms)
    dt = time.monotonic() - t0
    check(js["outcome"] == "timeout", "D3b contended checkpoint outcome=timeout")
    check(js["snapshot_retained"] is True and js["truncated"] is False, "D3b still retained, not truncated")
    check(js["space_released_bytes"] == 0, "D3b timeout released zero bytes")
    check(dt * 1000 >= wait_ms * 0.9, f"D3b waited at least ~{wait_ms}ms ({dt*1000:.0f}ms)")
    check(dt < wait_ms / 1000 + 3.0, "D3b did not wait far beyond the cap")

    # D3c: the key distinction — frames CAN be copied back into the main DB
    # (frames_backfilled > 0) while the on-disk WAL stays fully occupied and
    # zero bytes are released. PASSIVE never waits and never truncates.
    js = _checkpoint(app2, "PASSIVE", 0)
    check(js["outcome"] == "checkpointed", "D3c PASSIVE completes without waiting")
    check(js["frames_backfilled"] > 0,
          f"D3c frames were backfilled into the main DB ({js['frames_backfilled']})")
    check(js["space_released_bytes"] == 0, "D3c backfilling released NO disk space")
    check(js["truncated"] is False, "D3c PASSIVE does not truncate")
    check((js["after"]["wal_frames"] or 0) > 0, "D3c WAL frames still physically present after backfill")

    # D4: ordinary writes during the held snapshot + checkpoint contention stay
    # fast (never stretched to the 30s write busy_timeout) and keep succeeding.
    t0 = time.monotonic()
    latencies = []
    ok = 0
    for i in range(12):
        w0 = time.monotonic()
        st, _ = req("POST", app2 if i % 2 else app1, "/deployments",
                    {"service_id": 1, "note": f"during-hold-{i}"})
        latencies.append(time.monotonic() - w0)
        if st == 201:
            ok += 1
    write_elapsed = time.monotonic() - t0
    check(ok == 12, "D4 all concurrent writes accepted during long read/checkpoint")
    check(max(latencies) < 5.0, f"D4 no write blocked near 30s timeout (max {max(latencies):.2f}s)")
    check(write_elapsed < 10.0, f"D4 write batch finished well under write timeout ({write_elapsed:.2f}s)")

    # D5: referential constraint still enforced on both processes.
    st, _ = req("POST", app1, "/deployments", {"service_id": 999999})
    check(st == 409, "D5 orphan insert rejected on app1 (FK triggers intact)")
    st, _ = req("POST", app2, "/deployments", {"service_id": 999999})
    check(st == 409, "D5 orphan insert rejected on app2 (FK triggers intact)")

    # D6: wait for the long reader to finish; re-checkpoint now truncates and a
    # re-query reports the CURRENT on-disk truth (no stale "retained").
    deadline = time.time() + 30
    while time.time() < deadline:
        st, holders = req("GET", app1, "/wal/long-read")
        if not holders["holders_on_this_process"]:
            break
        time.sleep(0.5)
    st, holders = req("GET", app1, "/wal/long-read")
    check(not holders["holders_on_this_process"], "D6 long reader released")
    js = _checkpoint(app2, "TRUNCATE", 5000)
    check(js["outcome"] == "truncated" and js["truncated"] is True, "D6 truncate succeeds after reader ends")
    check(js["snapshot_retained"] is False, "D6 no false retained report once reader gone")
    check(js["after"]["wal_size_bytes"] == 0, "D6 WAL back to zero bytes")
    again = _wal_status(app1)
    check(again["wal_size_bytes"] == 0 and again["truncated"] is True,
          "D6 status re-query reflects current truncated state")

    # D7: two processes fire checkpoints at the same time; neither hangs past
    # its cap and the database ends in a consistent (truncated) state.
    _seed_writes(app1, app2, 50)
    results = {}

    def fire(name, port):
        results[name] = _checkpoint(port, "TRUNCATE", 4000)

    threads = [threading.Thread(target=fire, args=("app1", app1)),
               Thread(target=fire, args=("app2", app2))]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)
    dual_elapsed = time.monotonic() - t0
    check(all(not t.is_alive() for t in threads), "D7 both concurrent checkpoints returned (no infinite lock wait)")
    check(dual_elapsed < 12.0, f"D7 dual checkpoints finished bounded ({dual_elapsed:.2f}s)")
    r1, r2 = results["app1"], results["app2"]
    check({r1["outcome"], r2["outcome"]} <= {"truncated", "busy", "timeout"},
          f"D7 outcomes valid ({r1['outcome']}, {r2['outcome']})")
    final = _wal_status(app1)
    check(final["wal_size_bytes"] == 0 or r1["snapshot_retained"] or r2["snapshot_retained"],
          "D7 WAL truncated or explicitly reported retained — never silently stuck")
    # A follow-up checkpoint with no reader always converges to truncated.
    js = _checkpoint(app1, "TRUNCATE", 5000)
    check(js["outcome"] == "truncated" and js["after"]["wal_size_bytes"] == 0,
          "D7 follow-up checkpoint converges to truncated")

    # D8: validation cap is enforced by the API (wait_ms above the cap -> 422).
    st, js = req("POST", app1, "/wal/checkpoint", {"mode": "TRUNCATE", "wait_ms": 999999})
    check(st == 422, f"D8 wait_ms above cap rejected ({st})")
    st, js = req("POST", app1, "/wal/checkpoint", {"mode": "NOPE", "wait_ms": 100})
    check(st == 422, f"D8 invalid mode rejected ({st})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--app1", type=int, default=8001)
    parser.add_argument("--app2", type=int, default=8002)
    parser.add_argument("--restart-hook", default=None,
                        help="shell command that restarts the app1 process")
    parser.add_argument("--skip-phase", action="append", default=[],
                        help="phase letter to skip (A/B/C/D), repeatable")
    args = parser.parse_args()

    print(f"targets: app1=:{args.app1} app2=:{args.app2}")
    check(wait_ready(args.app1), "app1 /health")
    check(wait_ready(args.app2), "app2 /health")
    if FAILURES:
        print("processes not reachable; aborting")
        sys.exit(1)

    if "A" not in args.skip_phase:
        phase_a(args.app1, args.app2)
    if "B" not in args.skip_phase:
        phase_b(args.app1, args.app2, args.restart_hook)
    if "C" not in args.skip_phase:
        phase_c(args.app1, args.app2)
    if "D" not in args.skip_phase:
        phase_d(args.app1, args.app2)

    print(f"\n{CHECKS} checks, {len(FAILURES)} failures")
    if FAILURES:
        for f in FAILURES:
            print(f"  FAILED: {f}")
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
