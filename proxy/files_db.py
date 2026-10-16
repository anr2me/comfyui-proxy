"""
Saved copies of the remote's file lists, for the Media Assets panel — see
files_cache.py, which asks the remote for them and falls back to what is kept
here when the remote can't be asked.

A small SQLite database next to proxy_config.json (remote_files.db), plain
`sqlite3` from the standard library, in the style of jobs_db.py. Nothing is
reconstructed from partial data: each saved request is replayed exactly as the
remote answered it, but stored in pieces:

  - lists:      one row per saved request (path + query, per remote URL) with
                the answer's other fields (total, has_more, ...),
  - list_items: that request's items in order — asset ids for GET /api/assets,
                file names for GET /internal/files/input,
  - assets:     each asset's JSON once per remote, however many saved requests
                contain it (other filters, sorts, pages). Assets no saved
                request refers to any more are dropped.

So a write only touches what changed (an unchanged list costs one timestamp
update), nothing has to be loaded as a whole, and a list is rebuilt on the fly
for the one request that needs it. At most `max_lists` requests are kept per
remote; the least recently saved go first.

Every public function is blocking (it takes a lock and talks to SQLite), so
async callers run it with asyncio.to_thread(). If the database can't be opened
or is damaged, the functions log a warning and return their default
(False / None / []), and callers carry on as if nothing had been saved.
"""

import json
import logging
import os
import sqlite3
import threading
import time

from . import config as cfgmod

logger = logging.getLogger("ComfyUIProxy")

# Bump when the table layout changes. A database with a different version is
# moved aside (not modified or deleted) and a fresh one is started.
SCHEMA_VERSION = 1

FILES_ROUTE = "/internal/files/input"  # the one list of plain file names; every other key is an asset list

_DIR = os.path.dirname(cfgmod.CONFIG_PATH)
DB_PATH = os.path.join(_DIR, "remote_files.db")
LEGACY_JSON_PATH = os.path.join(_DIR, "remote_files_cache.json")  # the format before this module

_SCHEMA = """
CREATE TABLE IF NOT EXISTS lists (
    remote   TEXT NOT NULL,
    key      TEXT NOT NULL,
    kind     TEXT NOT NULL,
    meta     TEXT NOT NULL,
    saved_at REAL NOT NULL,
    PRIMARY KEY (remote, key)
);
CREATE TABLE IF NOT EXISTS list_items (
    remote TEXT    NOT NULL,
    key    TEXT    NOT NULL,
    pos    INTEGER NOT NULL,
    ref    TEXT    NOT NULL,
    PRIMARY KEY (remote, key, pos)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS list_items_by_ref ON list_items (remote, ref);
CREATE TABLE IF NOT EXISTS assets (
    remote TEXT NOT NULL,
    id     TEXT NOT NULL,
    data   TEXT NOT NULL,
    PRIMARY KEY (remote, id)
) WITHOUT ROWID;
"""

# Only rewritten when the asset actually changed.
_UPSERT_ASSET = """
INSERT INTO assets (remote, id, data) VALUES (?, ?, ?)
ON CONFLICT (remote, id) DO UPDATE SET data = excluded.data WHERE assets.data != excluded.data
"""

_UPSERT_LIST = """
INSERT INTO lists (remote, key, kind, meta, saved_at) VALUES (?, ?, ?, ?, ?)
ON CONFLICT (remote, key) DO UPDATE SET
    kind = excluded.kind, meta = excluded.meta, saved_at = excluded.saved_at
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
    path = DB_PATH
    conn = sqlite3.connect(path, timeout=10, check_same_thread=False)
    try:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        has_lists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'lists'"
        ).fetchone() is not None
        bad = has_lists and version != SCHEMA_VERSION
        tag = f"v{version}"
    except sqlite3.DatabaseError:
        has_lists, bad, tag = False, True, "unreadable"
    if bad:
        conn.close()
        aside = _move_aside(path, tag)
        logger.warning(
            f"[ComfyUI Proxy] {os.path.basename(path)} has a different layout (or can't be read); "
            f"moved it to {os.path.basename(aside)} and started with no saved file lists."
        )
        conn = sqlite3.connect(path, timeout=10, check_same_thread=False)
        has_lists = False
    try:
        conn.execute("PRAGMA journal_mode = WAL")
    except sqlite3.DatabaseError:
        pass  # e.g. a filesystem without shared-memory support; the default journal works too
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.executescript(_SCHEMA)
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    conn.commit()
    if not has_lists:
        _import_legacy(conn)
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
            _warn_once(f"op:{e}", f"[ComfyUI Proxy] Saved file lists database busy/unavailable: {e}")
            return default
        except (sqlite3.DatabaseError, OSError) as e:
            _warn_once("broken", f"[ComfyUI Proxy] Saved file lists database unusable ({type(e).__name__}: {e}); "
                                 "file lists won't be saved until ComfyUI is restarted.")
            _broken = True
            try:
                if _conn is not None:
                    _conn.close()
            except sqlite3.Error:
                pass
            _conn = None
            return default


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def _dumps(value) -> str:
    # Sorted keys: the same content always gives the same text, so "did it
    # change?" is a plain comparison.
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _prune(conn, remote, max_lists):
    """Drop the least recently saved lists beyond `max_lists`, then every asset
    no remaining asset list refers to."""
    if max_lists and max_lists > 0:
        conn.execute(
            "DELETE FROM lists WHERE remote = ? AND key IN ("
            "  SELECT key FROM lists WHERE remote = ? ORDER BY saved_at DESC LIMIT -1 OFFSET ?)",
            (remote, remote, max_lists),
        )
        conn.execute(
            "DELETE FROM list_items WHERE remote = ? AND key NOT IN (SELECT key FROM lists WHERE remote = ?)",
            (remote, remote),
        )
    conn.execute(
        "DELETE FROM assets WHERE remote = ? AND id NOT IN ("
        "  SELECT i.ref FROM list_items i JOIN lists l ON l.remote = i.remote AND l.key = i.key"
        "  WHERE i.remote = ? AND l.kind = 'assets')",
        (remote, remote),
    )


def _write_list(conn, remote, key, kind, meta_json, refs, asset_rows, max_lists, saved_at=None):
    now = time.time() if saved_at is None else saved_at
    unchanged = False
    row = conn.execute("SELECT kind, meta FROM lists WHERE remote = ? AND key = ?", (remote, key)).fetchone()
    if row is not None and row[0] == kind and row[1] == meta_json:
        old = [r[0] for r in conn.execute(
            "SELECT ref FROM list_items WHERE remote = ? AND key = ? ORDER BY pos", (remote, key))]
        unchanged = old == refs
    with conn:
        if asset_rows:
            conn.executemany(_UPSERT_ASSET, asset_rows)
        if unchanged:
            conn.execute("UPDATE lists SET saved_at = ? WHERE remote = ? AND key = ?", (now, remote, key))
            return
        conn.execute("DELETE FROM list_items WHERE remote = ? AND key = ?", (remote, key))
        conn.executemany(
            "INSERT INTO list_items (remote, key, pos, ref) VALUES (?, ?, ?, ?)",
            [(remote, key, pos, ref) for pos, ref in enumerate(refs)],
        )
        conn.execute(_UPSERT_LIST, (remote, key, kind, meta_json, now))
        _prune(conn, remote, max_lists)


def _store_in(conn, remote, key, data, max_lists, saved_at=None) -> bool:
    """Save one answer. A list of names for the files route, an object with an
    "assets" list (each asset with an "id") for anything else; False if `data`
    isn't that."""
    if key.split("?", 1)[0] == FILES_ROUTE:
        if not isinstance(data, list) or not all(isinstance(n, str) for n in data):
            return False
        _write_list(conn, remote, key, "files", "{}", list(data), [], max_lists, saved_at)
        return True
    items = data.get("assets") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return False
    refs, rows = [], []
    for asset in items:
        if not isinstance(asset, dict) or not isinstance(asset.get("id"), str) or not asset["id"]:
            return False
        refs.append(asset["id"])
        rows.append((remote, asset["id"], _dumps(asset)))
    meta = {k: v for k, v in data.items() if k != "assets"}
    _write_list(conn, remote, key, "assets", _dumps(meta), refs, rows, max_lists, saved_at)
    return True


def _import_legacy(conn):
    """One time, when the database is new: take over the lists saved by the
    earlier JSON file format, then remove that file."""
    try:
        with open(LEGACY_JSON_PATH, "r", encoding="utf-8") as f:
            old = json.load(f)
    except FileNotFoundError:
        return
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] Could not read {os.path.basename(LEGACY_JSON_PATH)} ({e}); ignoring it.")
        return
    imported = 0
    if isinstance(old, dict):
        for remote, entries in old.items():
            if not isinstance(entries, dict):
                continue
            for key, entry in entries.items():
                if not isinstance(entry, dict):
                    continue
                saved_at = entry.get("t")
                if _store_in(conn, remote, key, entry.get("data"), 0,
                             saved_at if isinstance(saved_at, (int, float)) else None):
                    imported += 1
    try:
        os.remove(LEGACY_JSON_PATH)
    except OSError:
        pass
    logger.info(f"[ComfyUI Proxy] Moved {imported} saved file list(s) from {os.path.basename(LEGACY_JSON_PATH)} "
                f"into {os.path.basename(DB_PATH)}.")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def store(remote, key, data, max_lists=0) -> bool:
    """Save the remote's answer to `key` (path + query). Only what changed is
    written. At most `max_lists` requests are kept for this remote (0: no
    limit). Returns False if `data` isn't a valid answer or can't be saved."""
    return _run(lambda conn: _store_in(conn, remote, key, data, max_lists), default=False)


def load(remote, key):
    """The saved answer to `key`, rebuilt as the remote gave it: a list of names
    or an object with its "assets" — or None if there is none."""
    def work(conn):
        row = conn.execute("SELECT kind, meta FROM lists WHERE remote = ? AND key = ?", (remote, key)).fetchone()
        if row is None:
            return None
        kind, meta_json = row
        if kind == "files":
            return [r[0] for r in conn.execute(
                "SELECT ref FROM list_items WHERE remote = ? AND key = ? ORDER BY pos", (remote, key))]
        rows = conn.execute(
            "SELECT a.data FROM list_items i JOIN assets a ON a.remote = i.remote AND a.id = i.ref "
            "WHERE i.remote = ? AND i.key = ? ORDER BY i.pos", (remote, key))
        data = json.loads(meta_json)
        data["assets"] = [json.loads(r[0]) for r in rows]
        return data
    return _run(work, default=None)


def recent_keys(remote, count) -> list:
    """The most recently saved requests of this remote, newest first."""
    return _run(lambda conn: [r[0] for r in conn.execute(
        "SELECT key FROM lists WHERE remote = ? ORDER BY saved_at DESC LIMIT ?", (remote, count))], default=[])
