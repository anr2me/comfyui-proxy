"""
Registers, on import:
  1. An aiohttp middleware on PromptServer's app that intercepts job-related
     routes and forwards them to the configured remote GPU when the proxy is
     enabled.
  2. Local-only REST endpoints (/comfyui_proxy/*) the UI panel uses to read
     and update settings — these are never proxied.

Routing rules:
  - /prompt, /interrupt, /upload/image, /upload/mask, /free, and any
    /history*, /view*, /viewvideo*, /api/jobs*, /api/crystools* path are
    always forwarded when the proxy is enabled (these are job/output related
    and meaningless without the cloud GPU that actually ran the job).
  - /queue is always forwarded when enabled (needed to know remote job state).
  - /ws and /internal/logs are ONLY forwarded while a job is known to be
    incomplete, so opening the UI alone never spins up a serverless instance.
  - /object_info is always served locally (workflow editing needs no cloud
    GPU), but its combo/dropdown model lists are patched with the cached
    remote model list when available, so users can't pick a model that only
    exists locally and would fail when the job actually runs remotely.
  - Everything else (static assets, node definitions, settings, etc.) is
    left completely alone.
  - Waking the remote is only triggered by /prompt, /queue, an already-known
    incomplete job, or the first-time model list pull.
"""

import asyncio
import json
import logging

import aiohttp
from aiohttp import web
from server import PromptServer

from . import config as cfgmod
from . import forwarder
from . import models_cache
from . import state

logger = logging.getLogger("ComfyUIProxy")

ALWAYS_PROXY_EXACT = {"/prompt", "/interrupt", "/upload/image", "/upload/mask", "/free", "/queue"}
ALWAYS_PROXY_PREFIXES = ("/history", "/view", "/viewvideo", "/api/jobs", "/api/crystools")
CONDITIONAL_ROUTES = {"/ws", "/internal/logs"}
WAKE_TRIGGER_ROUTES = {"/prompt", "/queue"}


def _is_enabled() -> bool:
    return bool(cfgmod.get("enabled", False)) and bool(cfgmod.get("remote_url"))


def _should_proxy(path: str) -> bool:
    if path in CONDITIONAL_ROUTES:
        return state.has_incomplete_job()
    if path in ALWAYS_PROXY_EXACT:
        return True
    return any(path == p or path.startswith(p + "/") for p in ALWAYS_PROXY_PREFIXES)


# ---------------------------------------------------------------------------
# /object_info patching: swap in the cached remote model list so users only
# see models that actually exist on the cloud GPU, without touching local
# editing at all.
# ---------------------------------------------------------------------------

def _merge_remote_models(resp: web.StreamResponse, cache: dict) -> web.StreamResponse:
    if not isinstance(resp, web.Response):
        logger.warning(
            f"[ComfyUI Proxy] /object_info response is {type(resp).__name__}, not a plain "
            "web.Response (likely streamed by another middleware) — cannot patch model dropdowns."
        )
        return resp
    try:
        raw = resp.body
        if not raw:
            logger.warning("[ComfyUI Proxy] /object_info response had no body to patch.")
            return resp
        data = json.loads(raw)
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] Could not parse local /object_info JSON to patch models: {e}")
        return resp

    changed = False
    matched_keys = 0
    for key, model_list in cache.items():
        node_name, _, pname = key.partition(".")
        node_info = data.get(node_name)
        if not isinstance(node_info, dict):
            continue
        inputs = node_info.get("input", {})
        for group in ("required", "optional"):
            group_inputs = inputs.get(group, {})
            pdef = group_inputs.get(pname)
            if isinstance(pdef, list) and len(pdef) > 0:
                pdef[0] = model_list
                changed = True
                matched_keys += 1

    if not changed:
        logger.warning(
            f"[ComfyUI Proxy] Model cache has {len(cache)} field(s) but none matched a local "
            "node/input name — the remote and local node sets may differ (e.g. a custom "
            "combo widget), so nothing was patched."
        )
        return resp

    logger.info(f"[ComfyUI Proxy] Patched {matched_keys} model dropdown field(s) in /object_info from the remote cache.")
    return web.Response(body=json.dumps(data).encode("utf-8"), status=resp.status, content_type="application/json")


# ---------------------------------------------------------------------------
# Buffered (non-streamed) handlers for routes we need to introspect
# ---------------------------------------------------------------------------

async def _read_tracking_response(r: aiohttp.ClientResponse, context: str):
    """Read + parse a response from the auto-decompressing tracking session.

    Decompression happens lazily on read, so a missing 'Brotli'/'zstandard'
    package (or a corrupt/unsupported encoding) surfaces here rather than
    deep inside aiohttp. Returns (data, text, error_response); error_response
    is only set when reading itself failed, so callers can bail out cleanly
    instead of risking a second failing read on the same broken stream.
    """
    try:
        raw = await r.read()
    except (aiohttp.ClientPayloadError, RuntimeError, LookupError, UnicodeDecodeError) as e:
        logger.error(f"[ComfyUI Proxy] Failed to decode remote response for {context}: {e}")
        return None, None, web.json_response(
            {
                "error": (
                    f"ComfyUI Proxy: failed to decode the remote's response for {context} ({e}). "
                    "If the remote/gateway compresses responses with brotli or zstd, install the "
                    "'Brotli' and 'zstandard' packages in ComfyUI's Python environment."
                )
            },
            status=502,
        )

    try:
        data = json.loads(raw) if raw else None
    except Exception:
        data = None
    text = None if data is not None else raw.decode("utf-8", errors="replace")
    return data, text, None


async def _handle_prompt(request: web.Request) -> web.Response:
    base = forwarder.target_base()
    body = await request.read()
    headers = forwarder.copy_request_headers(request)
    session = forwarder.get_tracking_session()
    timeout = forwarder.get_timeout()

    try:
        async with session.post(f"{base}/prompt", data=body, headers=headers, timeout=timeout) as r:
            status = r.status
            data, text, err = await _read_tracking_response(r, "/prompt")
            if err:
                return err
    except asyncio.TimeoutError:
        logger.error("[ComfyUI Proxy] Timeout submitting prompt to remote GPU")
        return web.json_response(
            {"error": "ComfyUI Proxy: remote GPU timed out while queuing the prompt (try raising the timeout)"},
            status=504,
        )
    except aiohttp.ClientError as e:
        logger.error(f"[ComfyUI Proxy] Failed to submit prompt: {e}")
        return web.json_response({"error": f"ComfyUI Proxy: failed to reach remote GPU: {e}"}, status=502)

    if data is not None:
        if 200 <= status < 300:
            prompt_id = data.get("prompt_id")
            state.mark_job_queued(prompt_id)
            logger.info(f"[ComfyUI Proxy] Job {prompt_id} queued on remote GPU.")
        elif status >= 500:
            logger.error(f"[ComfyUI Proxy] Remote GPU returned HTTP {status} queuing prompt.")
        return web.json_response(data, status=status)
    return web.Response(text=text or "", status=status)


async def _handle_queue(request: web.Request) -> web.Response:
    base = forwarder.target_base()
    headers = forwarder.copy_request_headers(request)
    session = forwarder.get_tracking_session()
    timeout = forwarder.get_timeout()
    url = f"{base}{request.rel_url.path}"
    if request.rel_url.query_string:
        url += f"?{request.rel_url.query_string}"

    try:
        async with session.get(url, headers=headers, timeout=timeout) as r:
            status = r.status
            data, _text, err = await _read_tracking_response(r, "/queue")
            if err:
                return err
    except asyncio.TimeoutError:
        logger.error("[ComfyUI Proxy] Timeout fetching remote queue state")
        return web.json_response({"error": "ComfyUI Proxy: remote GPU timed out"}, status=504)
    except aiohttp.ClientError as e:
        logger.error(f"[ComfyUI Proxy] Failed to fetch remote queue: {e}")
        return web.json_response({"error": f"ComfyUI Proxy: failed to reach remote GPU: {e}"}, status=502)

    if data is not None:
        if not data.get("queue_running") and not data.get("queue_pending"):
            state.clear_all()
        return web.json_response(data, status=status)
    return web.Response(status=status)


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

@web.middleware
async def proxy_middleware(request: web.Request, handler):
    path = request.rel_url.path

    # Local management API — never proxied.
    if path.startswith("/comfyui_proxy/"):
        return await handler(request)

    # Workflow editing stays local always; only patch the model dropdowns.
    if path == "/object_info" or path.startswith("/object_info/"):
        resp = await handler(request)
        if not _is_enabled():
            logger.info(
                f"[ComfyUI Proxy] Skipping /object_info model patch: proxy is "
                f"{'disabled' if not cfgmod.get('enabled') else 'missing a remote_url'}."
            )
            return resp
        cache = cfgmod.get("models_cache")
        if not cache:
            logger.info("[ComfyUI Proxy] Skipping /object_info model patch: no cached model list yet.")
            return resp
        return _merge_remote_models(resp, cache)

    if not _is_enabled() or not _should_proxy(path):
        return await handler(request)

    # Wake-up gating: only /prompt, /queue, or an already-known incomplete
    # job (covers /ws, /internal/logs, /history, /view while a job runs).
    if path in WAKE_TRIGGER_ROUTES:
        await forwarder.wake_remote_if_needed(f"request to {path}")
    elif state.has_incomplete_job():
        asyncio.create_task(forwarder.wake_remote_if_needed(f"request to {path}"))

    if path == "/ws":
        return await forwarder.forward_websocket(request)
    if path == "/prompt" and request.method == "POST":
        return await _handle_prompt(request)
    if path == "/queue" and request.method == "GET":
        return await _handle_queue(request)
    if path == "/interrupt":
        state.clear_all()
        return await forwarder.forward_http(request)

    # Everything else (history, view, viewvideo, api/jobs, api/crystools,
    # uploads, free, ...): stream through untouched, preserving encoding,
    # partial content, and status codes.
    return await forwarder.forward_http(request)


# ---------------------------------------------------------------------------
# Local REST management API + registration
# ---------------------------------------------------------------------------

def _public_config() -> dict:
    cfg = dict(cfgmod.load_config())
    models_cache = cfg.get("models_cache") or {}
    cfg["has_models_cache"] = bool(models_cache)
    cfg["models_cache_count"] = len(models_cache)
    cfg["auth_key_set"] = bool(cfg.get("auth_key"))
    cfg.pop("models_cache", None)
    cfg.pop("auth_key", None)
    return cfg


def setup():
    server = PromptServer.instance
    if proxy_middleware not in server.app.middlewares:
        server.app.middlewares.append(proxy_middleware)

    routes = server.routes

    @routes.get("/comfyui_proxy/config")
    async def _get_config(request):
        return web.json_response(_public_config())

    @routes.post("/comfyui_proxy/config")
    async def _set_config(request):
        try:
            patch = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON body"}, status=400)

        allowed = {"enabled", "remote_url", "timeout", "auth_key"}
        clean = {k: v for k, v in patch.items() if k in allowed}
        if "remote_url" in clean:
            clean["remote_url"] = (clean["remote_url"] or "").strip().rstrip("/")
        if "timeout" in clean:
            try:
                clean["timeout"] = max(5, float(clean["timeout"]))
            except (TypeError, ValueError):
                clean.pop("timeout", None)
        if "auth_key" in clean and not clean["auth_key"]:
            clean.pop("auth_key", None)  # blank means "leave unchanged"

        prev = cfgmod.load_config()
        url_changed = "remote_url" in clean and clean["remote_url"] != prev.get("remote_url")
        newly_enabled = clean.get("enabled") and not prev.get("enabled")

        cfg = cfgmod.update_config(clean)

        if cfg.get("enabled") and cfg.get("remote_url") and (url_changed or newly_enabled or not cfg.get("models_cache")):
            asyncio.create_task(models_cache.refresh_models_cache(force=url_changed))

        if url_changed:
            state.clear_all()  # a switched endpoint invalidates any tracked remote job

        return web.json_response(_public_config())

    @routes.post("/comfyui_proxy/refresh_models")
    async def _refresh_models(request):
        if not cfgmod.get("remote_url"):
            return web.json_response({"error": "remote URL not configured"}, status=400)
        cache = await models_cache.refresh_models_cache(force=True)
        return web.json_response({"count": len(cache) if cache else 0})

    logger.info("[ComfyUI Proxy] Ready. Remote GPU forwarding is %s.", "enabled" if _is_enabled() else "disabled")


setup()
