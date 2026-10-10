"""
Persistent job history for the Media Assets panel.

ComfyUI only keeps job history for the current session, and a serverless
remote starts every cold boot with an empty one. This module keeps the
*finished* jobs (completed / failed / cancelled) in a small SQLite database
next to proxy_config.json, so the panel can still list them after ComfyUI —
or the remote — restarts, without waking the remote just to browse.

Deliberately plain `sqlite3` from the standard library (no ORM, no extra
dependency) and its own file, rather than ComfyUI's asset database, which is
managed by ComfyUI's own migrations. One row per job:

  - the job object exactly as the remote's /api/jobs returned it (`job`),
  - the few columns the list endpoint filters/sorts by,
  - optionally the full /api/jobs/{id} detail (`detail`), saved when it was
    fetched, so an old job can still be opened.

Rows are keyed by (remote URL, job id), so switching to a different remote
doesn't mix histories. Pending / in-progress jobs are never saved: they only
exist live, on the remote.

Every public function is blocking (it takes a lock and talks to SQLite), so
async callers run it with asyncio.to_thread(). If the database can't be
opened or is damaged, the functions log a warning and return their default
(None / 0 / []), and callers carry on as if there were no saved history.
"""

import json
import logging
import os
import sqlite3
import threading
import time

from . import config as cfgmod

logger = logging.getLogger("ComfyUIProxy")

# Bump when the table layout changes. A database with a different version —
# e.g. one written by a fork that changed the structure — is moved aside
# (not modified or deleted) and a fresh one is started.
SCHEMA_VERSION = 1

TERMINAL_STATUSES = ("completed", "failed", "cancelled")
# Present in the /api/jobs/{id} detail but not in the list items.
_DETAIL_ONLY_KEYS = ("outputs", "execution_status", "workflow")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    remote      TEXT    NOT NULL,
    id          TEXT    NOT NULL,
    status      TEXT    NOT NULL,
    create_time INTEGER NOT NULL DEFAULT 0,
    duration    REAL    NOT NULL DEFAULT 0,
    workflow_id TEXT,
    job         TEXT    NOT NULL,
    detail      TEXT,
    updated_at  REAL    NOT NULL,
    PRIMARY KEY (remote, id)
);
CREATE INDEX IF NOT EXISTS jobs_by_time ON jobs (remote, create_time);
"""

_UPSERT_SUMMARY = """
INSERT INTO jobs (remote, id, status, create_time, duration, workflow_id, job, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (remote, id) DO UPDATE SET
    status = excluded.status, create_time = excluded.create_time,
    duration = excluded.duration, workflow_id = excluded.workflow_id,
    job = excluded.job, updated_at = excluded.updated_at
"""

_UPSERT_DETAIL = """
INSERT INTO jobs (remote, id, status, create_time, duration, workflow_id, job, detail, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (remote, id) DO UPDATE SET
    status = excluded.status, create_time = excluded.create_time,
    duration = excluded.duration, workflow_id = excluded.workflow_id,
    job = excluded.job, detail = excluded.detail, updated_at = excluded.updated_at
"""

_lock = threading.RLock()
_conn = None
_broken = False  # set once the database proved unusable; stays off until restart
_warned = set()


def _warn_once(key, message):
    if key not in _warned:
        _warned.add(key)
        logger.warning(message)


def _move_aside(path, tag):
    """Rename the database (and its WAL/SHM side files) out of the way, so
    nothing in it is lost, and return the new name."""
    stamp = time.strftime("%Y%m%d-%H%M%S")
    aside = f"{path}.{tag}.{stamp}.bak"
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(path + suffix):
            try:
                os.replace(path + suffix, aside + suffix)
            except OSError:
                pass
    return aside


def _open():
    path = cfgmod.JOBS_DB_PATH
    conn = sqlite3.connect(path, timeout=10, check_same_thread=False)
    try:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        has_jobs = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'jobs'"
        ).fetchone() is not None
        bad = has_jobs and version != SCHEMA_VERSION
        tag = f"v{version}"
    except sqlite3.DatabaseError:
        bad, tag = True, "unreadable"
    if bad:
        conn.close()
        aside = _move_aside(path, tag)
        logger.warning(
            f"[ComfyUI Proxy] {os.path.basename(path)} has a different layout (or can't be read); "
            f"moved it to {os.path.basename(aside)} and started a new job history."
        )
        conn = sqlite3.connect(path, timeout=10, check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
    except sqlite3.DatabaseError:
        pass  # e.g. a filesystem without shared-memory support; the default journal works too
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.executescript(_SCHEMA)
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    conn.commit()
    return conn


def _run(fn, *args, default=None):
    global _conn, _broken
    if _broken:
        return default
    with _lock:
        try:
            if _conn is None:
                _conn = _open()
            return fn(_conn, *args)
        except sqlite3.OperationalError as e:
            # Typically "database is locked" — transient; try again next time.
            _warn_once(f"op:{e}", f"[ComfyUI Proxy] Job history database busy/unavailable: {e}")
            return default
        except (sqlite3.DatabaseError, OSError) as e:
            _warn_once("broken", f"[ComfyUI Proxy] Job history database unusable ({type(e).__name__}: {e}); "
                                 "job history won't be saved until ComfyUI is restarted.")
            _broken = True
            try:
                if _conn is not None:
                    _conn.close()
            except sqlite3.Error:
                pass
            _conn = None
            return default


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------

def _number(value, cast):
    try:
        return cast(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def _summary_row(remote, job, now):
    """(remote, id, status, create_time, duration, workflow_id, job_json, now)
    for a finished job, or None for anything we don't save (live jobs, junk)."""
    if not isinstance(job, dict):
        return None
    job_id, status = job.get("id"), job.get("status")
    if not isinstance(job_id, str) or not job_id or status not in TERMINAL_STATUSES:
        return None
    start, end = job.get("execution_start_time"), job.get("execution_end_time")
    try:
        duration = float(end - start) if end and start else 0.0  # same rule as ComfyUI's duration sort
    except TypeError:
        duration = 0.0
    workflow_id = job.get("workflow_id")
    return (
        remote, job_id, status, _number(job.get("create_time"), int), duration,
        workflow_id if isinstance(workflow_id, str) else None,
        json.dumps(job, ensure_ascii=False, separators=(",", ":")), now,
    )


def _trim(conn, remote, max_entries):
    """Keep only the newest `max_entries` jobs of this remote."""
    if max_entries and max_entries > 0:
        conn.execute(
            "DELETE FROM jobs WHERE remote = ? AND id IN ("
            "  SELECT id FROM jobs WHERE remote = ? "
            "  ORDER BY create_time DESC, updated_at DESC LIMIT -1 OFFSET ?)",
            (remote, remote, max_entries),
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def available() -> bool:
    return _run(lambda conn: True, default=False) is True


def count(remote) -> int:
    return _run(lambda conn: conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE remote = ?", (remote,)).fetchone()[0], default=0)


def upsert_jobs(remote, jobs, max_entries=0) -> int:
    """Save the finished jobs among `jobs` (list items from /api/jobs).
    Existing rows are updated in place (keeping any saved detail). Returns
    how many jobs were written."""
    def work(conn):
        now = time.time()
        rows = [r for r in (_summary_row(remote, j, now) for j in jobs) if r]
        if not rows:
            return 0
        with conn:
            conn.executemany(_UPSERT_SUMMARY, rows)
            _trim(conn, remote, max_entries)
        return len(rows)
    return _run(work, default=0)


def put_detail(remote, detail, max_entries=0) -> bool:
    """Save a finished job's full /api/jobs/{id} response (its list-item
    fields are derived from it). Returns whether anything was written."""
    def work(conn):
        if not isinstance(detail, dict):
            return False
        summary = {k: v for k, v in detail.items() if k not in _DETAIL_ONLY_KEYS}
        row = _summary_row(remote, summary, time.time())
        if row is None:
            return False
        remote_, job_id, status, create_time, duration, workflow_id, job_json, now = row
        with conn:
            conn.execute(_UPSERT_DETAIL, (remote_, job_id, status, create_time, duration, workflow_id,
                                          job_json, json.dumps(detail, ensure_ascii=False, separators=(",", ":")), now))
            _trim(conn, remote, max_entries)
        return True
    return _run(work, default=False)


def get_detail(remote, job_id):
    def work(conn):
        row = conn.execute(
            "SELECT detail FROM jobs WHERE remote = ? AND id = ? AND detail IS NOT NULL", (remote, job_id)
        ).fetchone()
        return json.loads(row[0]) if row else None
    return _run(work, default=None)


def query(remote, statuses, workflow_id=None, sort_by="created_at", sort_order="desc", limit=None, offset=0):
    """Same filtering / sorting / paging as ComfyUI's GET /api/jobs, over the
    saved finished jobs. Returns (jobs, total) with total counted before
    offset/limit, or None if the database isn't usable."""
    statuses = [s for s in statuses if s in TERMINAL_STATUSES]

    def work(conn):
        if not statuses:
            return [], 0
        where = ["remote = ?", "status IN (%s)" % ",".join("?" * len(statuses))]
        args = [remote, *statuses]
        if workflow_id:
            where.append("workflow_id = ?")
            args.append(workflow_id)
        cond = " AND ".join(where)
        total = conn.execute(f"SELECT COUNT(*) FROM jobs WHERE {cond}", args).fetchone()[0]
        # Column and direction come from this fixed whitelist, never from the request.
        col = "duration" if sort_by == "execution_duration" else "create_time"
        direction = "ASC" if sort_order == "asc" else "DESC"
        sql = f"SELECT job FROM jobs WHERE {cond} ORDER BY {col} {direction}, create_time {direction}, id ASC"
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            args += [limit, max(0, offset)]
        elif offset > 0:
            sql += " LIMIT -1 OFFSET ?"
            args.append(offset)
        return [json.loads(r[0]) for r in conn.execute(sql, args)], total
    return _run(work, default=None)


def forget(remote, job_ids) -> int:
    """Remove specific jobs (the user deleted them in the panel)."""
    ids = [(remote, j) for j in job_ids if isinstance(j, str)]

    def work(conn):
        if not ids:
            return 0
        with conn:
            return conn.executemany("DELETE FROM jobs WHERE remote = ? AND id = ?", ids).rowcount
    return _run(work, default=0)


def clear(remote) -> int:
    """Remove everything saved for this remote (the user cleared the history)."""
    def work(conn):
        with conn:
            return conn.execute("DELETE FROM jobs WHERE remote = ?", (remote,)).rowcount
    return _run(work, default=0)
