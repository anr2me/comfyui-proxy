"""
Local response cache for GET /api/jobs (used by the frontend's Media Assets
panel to list completed/failed job history).

Cached whenever a fetch actually reaches the remote; served from here
instead whenever the proxy deliberately avoids waking the remote just to
browse (see server_hooks.py's routing rules). refresh_known() is called by
the shadow relay (relay.py) right after a job finishes — while its
connection still proves the remote is awake — so every query variant this
cache already knows about is warm again by the time a panel gets opened
later, after that proof of liveness (the relay) is gone. Kept as its own
module, rather than living in server_hooks.py or relay.py, specifically so
both of them can import it without an import cycle between each other.
"""

import asyncio
import json
import logging
import time
from urllib.parse import parse_qs

import aiohttp
from aiohttp import web

from . import config as cfgmod
from . import forwarder

logger = logging.getLogger("ComfyUIProxy")

PATH = "/api/jobs"

_cache = {}  # query_string -> {"status": int, "data": Any, "text": str|None, "cached_at": float}
_last_key = None  # most recently cached query string — used as a fallback
                   # when the exact query the panel asks for doesn't match
                   # anything cached (e.g. its pagination/filter params
                   # differ from whatever was proactively refreshed), since
                   # showing slightly-mismatched history beats showing none.


def clear():
    global _last_key
    _cache.clear()
    _last_key = None


def _store(query: str, status: int, data, text):
    global _last_key
    max_entries = int(cfgmod.get("jobs_cache_max_entries", 64) or 64)
    if query not in _cache:
        while len(_cache) >= max_entries and _cache:
            _cache.pop(next(iter(_cache)), None)  # evict oldest
    _cache[query] = {"status": status, "data": data, "text": text, "cached_at": time.time()}
    _last_key = query


def _response(entry: dict) -> web.Response:
    if entry["data"] is not None:
        resp = web.json_response(entry["data"], status=entry["status"])
    else:
        resp = web.Response(text=entry["text"] or "", status=entry["status"])
    resp.headers["X-ComfyUI-Proxy-Cache"] = f"hit; age={time.time() - entry['cached_at']:.0f}s"
    return resp


def _status_param(query: str):
    """The 'status' query param (e.g. 'completed,failed,cancelled' vs
    'in_progress,pending'), if present — ComfyUI's /api/jobs uses this to
    request fundamentally different lists, so a fallback must match on it
    rather than just grabbing whatever was cached most recently, or a
    completed-jobs request could end up 'falling back' to an in-progress
    list (or vice versa) and look empty/wrong instead of just stale."""
    vals = parse_qs(query).get("status")
    return vals[0] if vals else None


def _find_fallback(query: str):
    """Best available cached entry for a query with no exact match: prefer
    the most recent entry sharing the same 'status' filter, if any; only
    fall back to the single most-recently-cached entry overall when nothing
    shares that filter (or neither query has one)."""
    target_status = _status_param(query)
    if target_status is not None:
        for key in reversed(list(_cache.keys())):
            if _status_param(key) == target_status:
                return key, _cache[key]
    if _last_key is not None:
        return _last_key, _cache.get(_last_key)
    return None, None


async def _live_fetch(path: str, query: str):
    """GET path?query from the remote. Caches on success (2xx). Raises on
    transport/decode failure — callers decide what to do about that."""
    base = forwarder.target_base()
    headers = {"Accept-Encoding": forwarder.get_safe_accept_encoding()}
    auth_key = cfgmod.get("auth_key")
    if auth_key:
        headers["Authorization"] = f"Bearer {auth_key}"
    session = forwarder.get_tracking_session()
    timeout = forwarder.get_timeout()
    url = f"{base}{path}"
    if query:
        url += f"?{query}"

    async with session.get(url, headers=headers, timeout=timeout) as r:
        status = r.status
        raw = await r.read()

    try:
        data = json.loads(raw) if raw else None
    except Exception:
        data = None
    text = None if data is not None else raw.decode("utf-8", errors="replace")

    if 200 <= status < 300:
        _store(query, status, data, text)
    return status, data, text


async def handle_request(request: web.Request, remote_known_active: bool):
    """GET /api/jobs(/*) entry point for the middleware. Returns None
    (meaning 'fall through to the local handler') only when there's truly
    nothing to serve — no cache, and not worth waking the remote for."""
    query = request.rel_url.query_string
    path = request.rel_url.path

    if not remote_known_active or forwarder.circuit_is_open():
        # Either genuinely idle, or the remote just failed and we're
        # deliberately not attempting another live request yet — either way,
        # serve whatever's cached rather than touching the remote again.
        entry = _cache.get(query)
        if entry:
            return _response(entry)
        fallback_key, fallback = _find_fallback(query)
        if fallback is not None and fallback_key != query:
            # Routine and frequent (the frontend polls /api/jobs often) —
            # debug-level only, so it doesn't spam the console by default.
            # The served response's X-ComfyUI-Proxy-Cache header still shows
            # this happened, for anyone who wants to check.
            logger.debug(f"[ComfyUI Proxy] No exact cached copy for {path}?{query}; serving cached {path}?{fallback_key} instead.")
            return _response(fallback)
        return None

    try:
        status, data, text = await _live_fetch(path, query)
    except asyncio.TimeoutError:
        logger.error(f"[ComfyUI Proxy] Timeout fetching {path} from remote")
        forwarder.circuit_record_failure()
        entry = _cache.get(query)
        if entry:
            logger.warning(f"[ComfyUI Proxy] {path} timed out; serving cached copy.")
            return _response(entry)
        return web.json_response({"error": "ComfyUI Proxy: remote GPU timed out"}, status=504)
    except aiohttp.ClientError as e:
        logger.error(f"[ComfyUI Proxy] Failed to fetch {path}: {e}")
        forwarder.circuit_record_failure()
        entry = _cache.get(query)
        if entry:
            logger.warning(f"[ComfyUI Proxy] {path} failed; serving cached copy.")
            return _response(entry)
        return web.json_response({"error": f"ComfyUI Proxy: failed to reach remote GPU: {e}"}, status=502)
    except (RuntimeError, LookupError, UnicodeDecodeError) as e:
        # Decode errors aren't a sign the remote is unresponsive — don't
        # trip the breaker for these.
        logger.error(f"[ComfyUI Proxy] Decode error fetching {path}: {e}")
        entry = _cache.get(query)
        if entry:
            logger.warning(f"[ComfyUI Proxy] {path} decode failed; serving cached copy.")
            return _response(entry)
        return web.json_response({"error": f"ComfyUI Proxy: failed to decode remote response: {e}"}, status=502)

    forwarder.circuit_record_success()

    if data is not None:
        return web.json_response(data, status=status)
    return web.Response(text=text or "", status=status)


async def refresh_known(reason: str = ""):
    """Re-fetch every query variant currently cached (or just the bare
    endpoint, if nothing is cached yet) so the Media Assets panel shows
    fresh data even once the connection that justified reaching the remote
    (the shadow relay) has already closed by the time it's opened."""
    if not forwarder.target_base():
        return
    if forwarder.circuit_is_open():
        # The remote just failed elsewhere and we're deliberately not
        # touching it again yet — don't let this closing-relay refresh
        # attempt (and potentially wait out the full timeout on) a live
        # fetch either; that would just delay the relay closing promptly.
        logger.info(f"[ComfyUI Proxy] Skipping job history refresh ({reason}): remote recently unresponsive.")
        return
    queries = list(_cache.keys()) or [""]
    failures = 0
    attempted = 0
    for q in queries:
        if forwarder.circuit_is_open():
            # The first failure in this batch already proved the remote's
            # down — no point waiting out the timeout again for every
            # remaining query too.
            break
        attempted += 1
        logger.info(f"[ComfyUI Proxy] Retrieving remote job history ({PATH}{'?' + q if q else ''}) to cache...")
        try:
            await _live_fetch(PATH, q)
            forwarder.circuit_record_success()
        except Exception as e:
            failures += 1
            forwarder.circuit_record_failure()
            logger.warning(f"[ComfyUI Proxy] Pre-fetch of {PATH}?{q} ({reason}) failed: {e}")
    ok = attempted - failures
    logger.info(
        f"[ComfyUI Proxy] Refreshed job history cache ({ok}/{len(queries)} "
        f"quer{'y' if len(queries) == 1 else 'ies'} ok, {reason})."
    )
