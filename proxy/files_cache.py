"""
The file lists behind the Media Assets panel: GET /internal/files/input (the
"Imported" tab, when ComfyUI runs without the assets system) and GET
/api/assets (the lists of an --enable-assets ComfyUI: imported, generated, ...).

Both just list files on the machine ComfyUI runs on, so left alone they list
THIS computer's files — not the ones on the remote, which is where the workflows
actually run. With the proxy on they are asked of the remote instead, the same
way /view is: the CPU endpoint whenever one is configured, otherwise the GPU,
but only if it's already up — opening the panel is never worth waking the GPU.

When the remote can't be reached that way (asleep, no CPU endpoint, just failed,
or the assets system is off over there), the last list seen for this remote URL
and request is shown instead, and an empty one if there never was any. This
computer's own files are never shown in place of the remote's: they may not
exist there. The lists are kept in remote_files_cache.json next to the config,
per remote URL, so they survive restarts.

refresh_known() is called by the shadow relay (relay.py) right after a job
finishes, while its connection still proves the GPU is awake, to bring the lists
the panel has asked for up to date (new outputs, newly uploaded inputs). It is
only needed without a CPU endpoint: with one, the lists are read from it on demand.
"""

import asyncio
import json
import logging
import os
import threading
import time
from urllib.parse import urlencode

import aiohttp
from aiohttp import web

from . import config as cfgmod
from . import forwarder

logger = logging.getLogger("ComfyUIProxy")

FILES_PATH = "/internal/files/input"
ASSETS_PATH = "/api/assets"

CACHE_PATH = os.path.join(os.path.dirname(cfgmod.CONFIG_PATH), "remote_files_cache.json")
MAX_KEYS_PER_REMOTE = 40  # distinct requests (path + query) kept per remote URL; oldest dropped first
REFRESH_KEYS = 6          # how many of the most recent ones refresh_known() asks the remote again
_CONDITIONAL_HEADERS = {"if-none-match", "if-modified-since", "if-range", "range"}

_lock = threading.Lock()
_cache = None  # {remote_url: {request key: {"t": saved at, "data": parsed JSON}}}
_MISSING = object()
_warned_disabled = set()  # remote URLs we already said have no assets system


def is_list_route(path: str) -> bool:
    return path == FILES_PATH or path == ASSETS_PATH


def _cpu_configured() -> bool:
    """A CPU URL that is set and different from the GPU one."""
    cpu = forwarder.target_base(True)
    return bool(cpu) and cpu != forwarder.target_base()


def _remote_key() -> str:
    return (cfgmod.get("remote_url") or "").rstrip("/")


def _request_key(path: str, query) -> str:
    items = sorted(query.items())
    return path + ("?" + urlencode(items) if items else "")


def _empty_answer(path: str):
    if path == FILES_PATH:
        return []
    return {"assets": [], "total": 0, "has_more": False}


def _valid(key: str, data) -> bool:
    """Does `data` look like what that route answers with?"""
    if key.split("?", 1)[0] == FILES_PATH:
        return isinstance(data, list)
    return isinstance(data, dict) and isinstance(data.get("assets"), list)


# ---------------------------------------------------------------------------
# Saved lists (blocking file access: always called through asyncio.to_thread)
# ---------------------------------------------------------------------------

def _load_locked() -> dict:
    global _cache
    if _cache is None:
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            _cache = data if isinstance(data, dict) else {}
        except FileNotFoundError:
            _cache = {}
        except Exception as e:
            logger.warning(f"[ComfyUI Proxy] Could not read {os.path.basename(CACHE_PATH)} ({e}); starting with no saved file lists.")
            _cache = {}
    return _cache


def _save_locked():
    tmp = CACHE_PATH + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_cache, f)
        os.replace(tmp, CACHE_PATH)
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] Could not save {os.path.basename(CACHE_PATH)}: {e}")


def _store(remote: str, key: str, data):
    with _lock:
        entries = _load_locked().setdefault(remote, {})
        old = entries.get(key)
        if isinstance(old, dict) and old.get("data") == data:
            return  # unchanged: no reason to rewrite the file
        entries[key] = {"t": time.time(), "data": data}
        while len(entries) > MAX_KEYS_PER_REMOTE:
            del entries[min(entries, key=lambda k: entries[k].get("t", 0))]
        _save_locked()


def _lookup(remote: str, key: str):
    with _lock:
        entry = _load_locked().get(remote, {}).get(key)
        if isinstance(entry, dict) and "data" in entry:
            return entry["data"]
    return _MISSING


def _recent_keys(remote: str, count: int) -> list:
    with _lock:
        entries = _load_locked().get(remote, {})
        return sorted(entries, key=lambda k: entries[k].get("t", 0), reverse=True)[:count]


# ---------------------------------------------------------------------------
# Talking to the remote
# ---------------------------------------------------------------------------

async def _fetch(key: str, use_cpu: bool, request: web.Request = None):
    """GET `key` (path + query) from the target. Returns (status, body bytes,
    content type). Raises on transport / decode failure."""
    base = forwarder.target_base(use_cpu)
    if request is not None:
        headers = forwarder.copy_request_headers(request, limit_encoding=True, use_cpu=use_cpu)
        for name in [h for h in headers if h.lower() in _CONDITIONAL_HEADERS]:
            del headers[name]  # a "304 Not Modified" has no list in it to show
    else:
        headers = {"Accept-Encoding": forwarder.get_safe_accept_encoding()}
        auth_key = cfgmod.get("remote_cpu_auth_key" if use_cpu else "auth_key")
        if auth_key:
            headers["Authorization"] = f"Bearer {auth_key}"
    session = forwarder.get_tracking_session()
    async with session.get(f"{base}{key}", headers=headers, timeout=forwarder.get_timeout()) as r:
        status = r.status
        raw = await r.read()
        ctype = (r.headers.get("Content-Type") or "").split(";", 1)[0].strip()
    return status, raw, ctype or "application/json"


def _parse(raw: bytes):
    try:
        return json.loads(raw)
    except Exception:
        return _MISSING


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

async def handle_request(request: web.Request, reachable: bool, use_cpu: bool) -> web.Response:
    """GET of one of the list routes. `reachable` says the target can be asked
    without waking anything (see server_hooks.py); `use_cpu` which target it is.
    Always answers: the remote's list, else the saved one, else an empty one."""
    path = request.rel_url.path
    remote = _remote_key()
    key = _request_key(path, request.rel_url.query)
    label = "CPU" if use_cpu else "GPU"

    if reachable and not forwarder.circuit_is_open(use_cpu):
        try:
            status, raw, ctype = await _fetch(key, use_cpu, request)
        except asyncio.TimeoutError:
            logger.warning(f"[ComfyUI Proxy] Timeout fetching {path} from the remote {label}; showing the saved list.")
            forwarder.circuit_record_failure(use_cpu)
        except aiohttp.ClientError as e:
            logger.warning(f"[ComfyUI Proxy] Failed to fetch {path} from the remote {label} ({e}); showing the saved list.")
            forwarder.circuit_record_failure(use_cpu)
        except (RuntimeError, LookupError, UnicodeDecodeError) as e:
            logger.warning(f"[ComfyUI Proxy] Could not decode {path} from the remote {label} ({e}); showing the saved list.")
        else:
            forwarder.circuit_record_success(use_cpu)
            if 200 <= status < 300:
                data = _parse(raw)
                if data is not _MISSING and _valid(key, data):
                    await asyncio.to_thread(_store, remote, key, data)
                    return web.Response(body=raw, status=status, content_type=ctype)
                logger.warning(f"[ComfyUI Proxy] The remote {label} answered {path} with something unexpected; showing the saved list.")
            elif status >= 500:
                if status == 503 and path == ASSETS_PATH and remote not in _warned_disabled:
                    _warned_disabled.add(remote)
                    logger.warning(
                        f"[ComfyUI Proxy] The remote {label} answered {path} with HTTP 503: its assets system is "
                        "probably off. Start the remote ComfyUI with --enable-assets (and without --disable-assets) "
                        "to list its assets; showing the saved list meanwhile."
                    )
                else:
                    logger.warning(f"[ComfyUI Proxy] The remote {label} answered {path} with HTTP {status}; showing the saved list.")
            else:
                # A 4xx is the remote's own answer to a bad request; show it as is.
                return web.Response(body=raw, status=status, content_type=ctype)

    data = await asyncio.to_thread(_lookup, remote, key)
    if data is _MISSING:
        data = _empty_answer(path)
    resp = web.json_response(data)
    resp.headers["X-ComfyUI-Proxy-Cache"] = "saved"
    return resp


async def refresh_known(reason: str = ""):
    """Ask the GPU again for the lists the panel has asked for before (the most
    recent few), which saves them. Called while the GPU is known to be awake."""
    if not forwarder.target_base() or forwarder.circuit_is_open():
        return
    if _cpu_configured():
        return  # the lists are read from the CPU endpoint, which wakes on demand
    remote = _remote_key()
    keys = await asyncio.to_thread(_recent_keys, remote, REFRESH_KEYS)
    refreshed = 0
    for key in keys:
        try:
            status, raw, _ctype = await _fetch(key, False)
        except (asyncio.TimeoutError, aiohttp.ClientError, RuntimeError, LookupError, UnicodeDecodeError) as e:
            logger.warning(f"[ComfyUI Proxy] Could not refresh the saved file list {key} ({reason}): {e}")
            return
        if 200 <= status < 300:
            data = _parse(raw)
            if data is not _MISSING and _valid(key, data):
                await asyncio.to_thread(_store, remote, key, data)
                refreshed += 1
    if refreshed:
        logger.info(f"[ComfyUI Proxy] Saved file lists updated ({refreshed} list(s), {reason}).")
