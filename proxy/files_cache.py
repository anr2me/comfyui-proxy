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
exist there. The lists are kept in a small SQLite database next to the config
(remote_files.db, see files_db.py), per remote URL, so they survive restarts.

The Imported tab's list (/internal/files/input) only changes when something is
uploaded, so it is shown from the saved copy without asking the remote at all —
a CPU endpoint would otherwise be woken every time the tab is opened. The proxy
refreshes the saved copy itself right after an upload it forwarded
(refresh_input_soon(), asking the container that took the upload, which is awake
then). The remote is only asked when there is no saved list yet, or the saved one
is older than INPUT_LIST_MAX_AGE (files added some other way get picked up then).

Output scan: with the assets system on only in the CPU container, nothing there
learns about the files a job saved from the GPU container until it scans its
output folder, which walks the whole folder. So the shadow relay (relay.py) tells
this module when a job saved a file (note_message()), and when the last job of a
batch has finished it calls scan_output_soon(): one scan request to the CPU
container, in the background, and none at all if nothing was saved meanwhile or if
the GPU container runs the assets system itself (it registers its own outputs).

refresh_known() is called by the shadow relay (relay.py) right after a job
finishes, while its connection still proves the GPU is awake, to bring the lists
the panel has asked for up to date (new outputs, newly uploaded inputs). It is
only needed without a CPU endpoint: with one, the lists are read from it on demand.
"""

import asyncio
import json
import logging
import time
from urllib.parse import urlencode

import aiohttp
from aiohttp import web

from . import config as cfgmod
from . import files_db
from . import forwarder

logger = logging.getLogger("ComfyUIProxy")

FILES_PATH = files_db.FILES_ROUTE
ASSETS_PATH = "/api/assets"

MAX_KEYS_PER_REMOTE = 40  # distinct requests (path + query) kept per remote URL; oldest dropped first
REFRESH_KEYS = 6          # how many of the most recent ones refresh_known() asks the remote again
INPUT_LIST_MAX_AGE = 24 * 3600.0  # seconds the Imported list is shown from the saved copy before the remote is asked again
_CONDITIONAL_HEADERS = {"if-none-match", "if-modified-since", "if-range", "range"}

_MISSING = object()
_warned_disabled = set()  # remote URLs we already said have no assets system
_output_dirty = False     # a job saved a file to the output folder since the last scan request
_gpu_assets = {}          # GPU URL -> (does that container run the assets system?, when asked)
_GPU_ASSETS_TTL = 600.0
_background = set()       # keeps the background scan task from being garbage-collected
_input_refresh = {"running": False, "again": False}  # coalesces refreshes of the Imported list


def is_list_route(path: str) -> bool:
    return path == FILES_PATH or path == ASSETS_PATH


def _cpu_configured() -> bool:
    """A CPU URL that is set and different from the GPU one."""
    cpu = forwarder.target_base(True)
    return bool(cpu) and cpu != forwarder.target_base()


def _auth_headers(use_cpu: bool) -> dict:
    headers = {"Accept-Encoding": forwarder.get_safe_accept_encoding()}
    auth_key = cfgmod.get("remote_cpu_auth_key" if use_cpu else "auth_key")
    if auth_key:
        headers["Authorization"] = f"Bearer {auth_key}"
    return headers


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
        return isinstance(data, list) and all(isinstance(name, str) for name in data)
    return (
        isinstance(data, dict) and isinstance(data.get("assets"), list)
        and all(isinstance(a, dict) and isinstance(a.get("id"), str) and a["id"] for a in data["assets"])
    )


# ---------------------------------------------------------------------------
# Saved lists (blocking database access: always called through asyncio.to_thread)
# ---------------------------------------------------------------------------

def _store(remote: str, key: str, data):
    files_db.store(remote, key, data, MAX_KEYS_PER_REMOTE)


def _lookup(remote: str, key: str):
    data = files_db.load(remote, key)
    return _MISSING if data is None else data


def _recent_keys(remote: str, count: int) -> list:
    return files_db.recent_keys(remote, count)


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

    if path == FILES_PATH:
        # The Imported list only changes by uploads, which refresh it (see
        # refresh_input_soon()): no need to ask the remote — and wake a CPU
        # endpoint — every time the tab is opened.
        age = await asyncio.to_thread(files_db.age, remote, key)
        if age is not None and age < INPUT_LIST_MAX_AGE:
            data = await asyncio.to_thread(_lookup, remote, key)
            if data is not _MISSING:
                resp = web.json_response(data)
                resp.headers["X-ComfyUI-Proxy-Cache"] = "saved"
                return resp

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


# ---------------------------------------------------------------------------
# Output scan (see the module docstring)
# ---------------------------------------------------------------------------

def note_message(parsed):
    """Remember that a job saved a file to the output folder: called for every
    message the relay reads from the remote. An "executed" message carries the
    node's outputs; only {"type": "output"} items are files in that folder
    (previews are "temp")."""
    global _output_dirty
    if not isinstance(parsed, dict) or parsed.get("type") != "executed":
        return
    outputs = (parsed.get("data") or {}).get("output")
    if not isinstance(outputs, dict):
        return
    for items in outputs.values():
        if isinstance(items, list) and any(isinstance(i, dict) and i.get("type") == "output" for i in items):
            _output_dirty = True
            return


async def _gpu_runs_assets(gpu_base: str) -> bool:
    """Does the GPU container run the assets system (its /features says)? Then it
    registers its own outputs. Unknown counts as no: better one scan too many."""
    known = _gpu_assets.get(gpu_base)
    if known is not None and time.time() - known[1] < _GPU_ASSETS_TTL:
        return known[0]
    try:
        status, raw, _ctype = await _fetch("/features", False)
    except (asyncio.TimeoutError, aiohttp.ClientError, RuntimeError, LookupError, UnicodeDecodeError):
        return False
    data = _parse(raw) if 200 <= status < 300 else _MISSING
    if not isinstance(data, dict) or "assets" not in data:
        return False
    runs = bool(data["assets"])
    _gpu_assets[gpu_base] = (runs, time.time())
    return runs


async def _post_output_scan(use_cpu: bool) -> int:
    base = forwarder.target_base(use_cpu)
    session = forwarder.get_tracking_session()
    async with session.post(
        f"{base}/api/assets/seed", json={"roots": ["output"]},
        headers=_auth_headers(use_cpu), timeout=forwarder.get_timeout(),
    ) as r:
        await r.read()
        return r.status


async def _scan_output(reason: str):
    global _output_dirty
    if not _output_dirty:
        return  # another call got here first
    _output_dirty = False
    try:
        if await _gpu_runs_assets(forwarder.target_base()):
            return  # that container registers the outputs it saves
        if forwarder.circuit_is_open(True):
            _output_dirty = True  # try again after the next batch
            return
        status = await _post_output_scan(True)
    except (asyncio.TimeoutError, aiohttp.ClientError) as e:
        logger.warning(f"[ComfyUI Proxy] Could not ask the remote CPU to rescan its output folder ({reason}): {e}")
        _output_dirty = True
        return
    if status in (200, 202):
        logger.info(f"[ComfyUI Proxy] Asked the remote CPU to rescan its output folder, so the new results show up in the asset lists ({reason}).")
    elif status == 409:
        pass  # a scan is already running there; it will pick the new files up or the next one will
    elif status == 503:
        remote = _remote_key()
        if remote not in _warned_disabled:
            _warned_disabled.add(remote)
            logger.warning(
                "[ComfyUI Proxy] The remote CPU answered the output scan request with HTTP 503: its assets system is "
                "probably off. Start it with --enable-assets (and without --disable-assets) to list new results."
            )
    else:
        logger.warning(f"[ComfyUI Proxy] The remote CPU answered the output scan request with HTTP {status} ({reason}).")


def scan_output_soon(reason: str = ""):
    """Ask the remote CPU container to rescan its output folder — in the
    background, and only if a job saved a file since the last request (see the
    module docstring). The relay calls this when the last job has finished."""
    if not _output_dirty or not _cpu_configured():
        return
    task = asyncio.ensure_future(_scan_output(reason))
    _background.add(task)
    task.add_done_callback(_background.discard)


async def _refresh_input(use_cpu: bool, reason: str):
    if _input_refresh["running"]:
        _input_refresh["again"] = True  # one more round once the current one is done
        return
    _input_refresh["running"] = True
    try:
        while True:
            _input_refresh["again"] = False
            try:
                status, raw, _ctype = await _fetch(FILES_PATH, use_cpu)
            except (asyncio.TimeoutError, aiohttp.ClientError, RuntimeError, LookupError, UnicodeDecodeError) as e:
                logger.warning(f"[ComfyUI Proxy] Could not refresh the saved Imported file list ({reason}): {e}")
                return
            if 200 <= status < 300:
                data = _parse(raw)
                if data is not _MISSING and _valid(FILES_PATH, data):
                    await asyncio.to_thread(_store, _remote_key(), FILES_PATH, data)
            if not _input_refresh["again"]:
                return
    finally:
        _input_refresh["running"] = False


def refresh_input_soon(use_cpu: bool, reason: str = "upload"):
    """Bring the saved Imported list up to date, in the background, from the
    container that has just taken an upload (so it is awake). Several uploads in
    a row make at most one extra round."""
    if not forwarder.target_base(use_cpu):
        return
    task = asyncio.ensure_future(_refresh_input(use_cpu, reason))
    _background.add(task)
    task.add_done_callback(_background.discard)


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
