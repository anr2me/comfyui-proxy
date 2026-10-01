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

import aiohttp
from aiohttp import web

from . import config as cfgmod
from . import forwarder

logger = logging.getLogger("ComfyUIProxy")

PATH = "/api/jobs"

_cache = {}  # query_string -> {"status": int, "data": Any, "text": str|None, "cached_at": float}


def clear():
    _cache.clear()


def _store(query: str, status: int, data, text):
    max_entries = int(cfgmod.get("jobs_cache_max_entries", 64) or 64)
    if query not in _cache:
        while len(_cache) >= max_entries and _cache:
            _cache.pop(next(iter(_cache)), None)  # evict oldest
    _cache[query] = {"status": status, "data": data, "text": text, "cached_at": time.time()}


def _response(entry: dict) -> web.Response:
    if entry["data"] is not None:
        resp = web.json_response(entry["data"], status=entry["status"])
    else:
        resp = web.Response(text=entry["text"] or "", status=entry["status"])
    resp.headers["X-ComfyUI-Proxy-Cache"] = f"hit; age={time.time() - entry['cached_at']:.0f}s"
    return resp


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

    if not remote_known_active:
        entry = _cache.get(query)
        return _response(entry) if entry else None

    try:
        status, data, text = await _live_fetch(path, query)
    except asyncio.TimeoutError:
        logger.error(f"[ComfyUI Proxy] Timeout fetching {path} from remote")
        entry = _cache.get(query)
        if entry:
            logger.warning(f"[ComfyUI Proxy] {path} timed out; serving cached copy.")
            return _response(entry)
        return web.json_response({"error": "ComfyUI Proxy: remote GPU timed out"}, status=504)
    except aiohttp.ClientError as e:
        logger.error(f"[ComfyUI Proxy] Failed to fetch {path}: {e}")
        entry = _cache.get(query)
        if entry:
            logger.warning(f"[ComfyUI Proxy] {path} failed; serving cached copy.")
            return _response(entry)
        return web.json_response({"error": f"ComfyUI Proxy: failed to reach remote GPU: {e}"}, status=502)
    except (RuntimeError, LookupError, UnicodeDecodeError) as e:
        logger.error(f"[ComfyUI Proxy] Decode error fetching {path}: {e}")
        entry = _cache.get(query)
        if entry:
            logger.warning(f"[ComfyUI Proxy] {path} decode failed; serving cached copy.")
            return _response(entry)
        return web.json_response({"error": f"ComfyUI Proxy: failed to decode remote response: {e}"}, status=502)

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
    queries = list(_cache.keys()) or [""]
    failures = 0
    for q in queries:
        try:
            await _live_fetch(PATH, q)
        except Exception as e:
            failures += 1
            logger.warning(f"[ComfyUI Proxy] Pre-fetch of {PATH}?{q} ({reason}) failed: {e}")
    ok = len(queries) - failures
    logger.info(
        f"[ComfyUI Proxy] Refreshed job history cache ({ok}/{len(queries)} "
        f"quer{'y' if len(queries) == 1 else 'ies'} ok, {reason})."
    )
