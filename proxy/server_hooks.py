"""
Registers, on import:
  1. An aiohttp middleware on PromptServer's app that intercepts job-related
     routes and forwards them to the configured remote GPU when the proxy is
     enabled.
  2. Local-only REST endpoints (/comfyui_proxy/*) the UI panel uses to read
     and update settings — these are never proxied.

Routing rules:
  - /prompt, /queue, /upload/image, /upload/mask are always forwarded when
    the proxy is enabled — these are the legitimate reasons to wake the
    remote (queuing/checking a job, or preparing input for one).
  - /interrupt, /free, and browsing/history routes (/history*, /view*,
    /viewvideo*, /api/crystools*, /api/crysmonitor/monitor*,
    /api/resource-monitor*) are only forwarded while a job is known to be
    incomplete or its shadow progress relay is live — they're not worth a
    cold start on their own (e.g. opening the logs console while idle).
    Older remote job history simply won't show once the relay has closed;
    this is a deliberate cost/idle-wake tradeoff, not a bug.
  - /api/jobs is handled specially (see jobs_cache.py): finished jobs are
    saved in a local database (jobs_history.db) whenever the remote is
    reached, and the job list is rebuilt from it — so the Media Assets panel
    keeps showing history across restarts, of ComfyUI and of the remote,
    without waking the remote just to browse it.
  - While the remote GPU is still waking up, each submitted /prompt shows up
    in the UI's Job Queue as a placeholder pending job ("Waiting for a
    machine"; see placeholders.py) instead of nothing happening — and it can
    be cancelled there without waking the GPU.
  - /ws is NEVER proxied as an HTTP route: the browser's websocket connection
    is opened once at page load and can't be redirected after the fact.
    Instead, submitting /prompt opens a dedicated shadow websocket from the
    proxy itself to the remote (same clientId) and relays every message onto
    the browser's already-open local socket — see relay.py. /internal/logs
    is likewise never proxied as an HTTP route; remote log lines arrive
    through that same shadow connection once relay.py subscribes to them.
  - /object_info is always served locally (workflow editing needs no cloud
    GPU), but its combo/dropdown model lists are patched with the cached
    remote model list when available, so users can't pick a model that only
    exists locally and would fail when the job actually runs remotely.
  - The file lists behind the Media Assets panel — GET /internal/files/input
    (the Imported tab) and, with --enable-assets, GET /api/assets — come from
    the remote, and the last list seen is kept (files_cache.py) to show while
    the remote is asleep. The Imported list is shown from that saved copy (it
    only changes by uploads, after which it is refreshed) instead of waking the
    remote whenever the tab is opened. The rest of the asset API (/api/assets/<id>, its
    content, ...) is forwarded too — to the CPU endpoint when there is one,
    else the GPU while it's up; only /api/assets/seed and /api/assets/prune
    stay local, since they scan this computer's folders.
  - GET /crystools/monitor/GPU (and its /api alias) that stays local — i.e.
    isn't forwarded because no job is active — has an empty "no GPU" answer
    replaced with a fake GPU (config.GPU_NAME / GPU_COUNT), so Crystools'
    GPU monitors still appear while the real GPU is remote. PATCH
    /crystools/monitor/GPU/<index> for those fake GPUs is answered with a
    stub 200 instead of Crystools' "400 Bad Request" for a missing GPU. The
    same holds for the MonitorOnly fork of Crystools: its routes are
    /crysmonitor/monitor/..., it changes a GPU's switches with POST instead of
    PATCH, and its /crysmonitor/folder_name (the name of the local extension
    folder, for its stylesheet) is never forwarded.
  - comfyui-resource-monitor gets the same fake GPU (config.GPU_NAME /
    GPU_VRAM_GB). It reports its readings in HTTP answers (GET
    /resource-monitor/snapshot, and the snapshot in the answer of POST
    /resource-monitor/configure) and as "resource-monitor.snapshot" websocket
    events its local service pushes; all of the local ones say there is no GPU,
    so each gets one added. Those local events are held back while the remote's
    own are arriving through the shadow stream.
  - Everything else (static assets, node definitions, settings, etc.) is
    left completely alone.
  - Waking the remote is only triggered by /prompt, /queue, an already-known
    incomplete job, or fetching the model list (see models_cache.py): that
    asks the GPU only when it's already up, otherwise the CPU endpoint when
    one is configured, and wakes the GPU only when there is no CPU endpoint
    and no list yet — or when the user clicks Refresh Models.
"""

import asyncio
import json
import logging
import os
import re
import time

import aiohttp
from aiohttp import web
from server import PromptServer

from . import config as cfgmod
from . import files_cache
from . import forwarder
from . import jobs_cache
from . import keepalive
from . import models_cache
from . import placeholders
from . import relay
from . import state
from . import viewcache

logger = logging.getLogger("ComfyUIProxy")

ALWAYS_PROXY_EXACT = {"/prompt", "/queue", "/upload/image", "/upload/mask"}
# /interrupt and /free are only meaningful while something is actually
# running on the remote — forwarding them while idle would just wake the
# remote for no reason, so they're gated the same as the browsing routes.
CONDITIONAL_EXACT = {"/interrupt", "/free"}
# /history is GPU-only (job/queue state lives with the live container, not
# just on the shared volume) — gated purely by GPU activity.
GPU_ONLY_CONDITIONAL_PREFIXES = ("/history",)
# /view, /viewvideo, and uploads are file-only: readable/writable from a
# CPU-only container sharing the same persistent volume, so they can also
# proxy (to that CPU target) even while the GPU is idle, when one is
# configured — see _use_cpu_target().
CPU_ELIGIBLE_EXACT = {"/upload/image", "/upload/mask"}
CPU_ELIGIBLE_PREFIXES = ("/view", "/viewvideo")
# The asset API of an --enable-assets remote: "/api/assets...", and only that.
# Reads and changes alike go to the CPU endpoint whenever one is configured (even
# while a job runs), so every asset ID comes from that one container's database,
# and the GPU container doesn't need the assets system at all; without a CPU
# endpoint they go to the GPU, while it's up. Kept apart from
# CPU_ELIGIBLE_PREFIXES, which also counts as "viewing" (keepalive.py). Its scan
# controls act on this computer's own folders, so they stay local.
# NOT "/assets/...": that is where the frontend's own static files (the .js and
# .css bundles) are served from, which must always come from this computer. So
# this is matched against the real request path, never the canonical one that
# _strip_api_prefix() makes by removing "/api" (it would make the two look alike).
ASSET_PREFIX = "/api/assets"
LOCAL_ONLY_ASSET_PREFIXES = ("/api/assets/seed", "/api/assets/prune")
# "/api/crystools" is its own real namespace (not an alias of a plain route).
# "/api/crysmonitor/monitor" is the same monitor API in the MonitorOnly fork of
# Crystools. Not "/api/crysmonitor" as a whole: its /api/crysmonitor/folder_name
# names the LOCAL extension folder (for its stylesheet) and must stay local.
# "/api/resource-monitor" is comfyui-resource-monitor's API (snapshot, configure,
# disconnect); its readings otherwise arrive as websocket events, see below.
# "/api/jobs" GET requests are handled separately below (with a local
# response cache); this prefix only still matters here for any other method.
EXTRA_CONDITIONAL_PREFIXES = (
    "/api/jobs", "/api/crystools", "/api/crysmonitor/monitor", "/api/resource-monitor",
)


def _strip_api_prefix(path: str) -> str:
    """Newer ComfyUI core versions mirror many routes under '/api/' (e.g.
    '/api/object_info' alongside '/object_info') for frontend/backend
    versioning. Route-matching decisions use this canonical form so the
    proxy behaves the same regardless of which alias the frontend calls;
    actual forwarding still uses the real incoming path untouched."""
    if path.startswith("/api/"):
        return path[4:]
    return path


def _is_enabled() -> bool:
    return bool(cfgmod.get("enabled")) and bool(cfgmod.get("remote_url"))


def _remote_known_active() -> bool:
    """True if we have a concrete reason to believe the GPU is already up —
    a tracked incomplete job, or a live shadow progress relay."""
    return state.has_incomplete_job() or relay.has_active_relay()


def _is_asset_route(path: str) -> bool:
    if path != ASSET_PREFIX and not path.startswith(ASSET_PREFIX + "/"):
        return False
    return not any(path == p or path.startswith(p + "/") for p in LOCAL_ONLY_ASSET_PREFIXES)


def _is_cpu_eligible(canonical: str, path: str) -> bool:
    return canonical in CPU_ELIGIBLE_EXACT or _is_asset_route(path) or any(
        canonical == p or canonical.startswith(p + "/") for p in CPU_ELIGIBLE_PREFIXES
    )


def _cpu_target_configured() -> bool:
    """True only if a CPU URL is set AND it's actually different from the
    GPU URL. Pointed at the same URL, 'CPU routing' isn't a separate,
    independently-stable container at all — it's just the same volatile
    serverless GPU endpoint under a second label, so it gets none of the
    intended benefit and only adds extra traffic to that endpoint during
    exactly the window (job just finished, container winding down) this
    feature exists to avoid."""
    cpu_url = (cfgmod.get("remote_cpu_url") or "").rstrip("/")
    if not cpu_url:
        return False
    gpu_url = (cfgmod.get("remote_url") or "").rstrip("/")
    return cpu_url != gpu_url


def _use_cpu_target(canonical: str, path: str) -> bool:
    """True if this request should go to the CPU-only target instead of the
    GPU one: only for file-only routes, only when a genuinely distinct CPU
    URL is configured, and only while a job isn't actually still running.
    Deliberately checks state.has_incomplete_job() rather than the broader
    _remote_known_active() (which also counts a live shadow relay): during
    the few-second grace period after a job finishes — where the relay is
    intentionally kept open only so progress/log animations can finish, not
    because new GPU work is happening — the GPU container may already be
    winding down and refusing new connections even though that websocket is
    still open, so new file-serving requests in that window are better sent
    to the CPU target (when configured) than forced onto a GPU container
    that's on its way out anyway."""
    if not _is_cpu_eligible(canonical, path):
        return False
    if not _cpu_target_configured():
        return False
    if _is_asset_route(path):
        return True  # always the CPU endpoint, job or not (see ASSET_PREFIX)
    return not state.has_incomplete_job()


def _should_proxy(path: str) -> bool:
    canonical = _strip_api_prefix(path)
    if canonical in ALWAYS_PROXY_EXACT:
        return True
    if canonical in CONDITIONAL_EXACT:
        return _remote_known_active()
    if any(canonical == p or canonical.startswith(p + "/") for p in GPU_ONLY_CONDITIONAL_PREFIXES):
        return _remote_known_active()
    if _is_cpu_eligible(canonical, path):
        # Still worth proxying even while the GPU is idle, as long as a
        # genuinely distinct CPU target is configured to actually serve it —
        # _use_cpu_target() (checked again at dispatch time) decides which
        # target that is.
        return _remote_known_active() or _cpu_target_configured()
    if any(path == p or path.startswith(p + "/") for p in EXTRA_CONDITIONAL_PREFIXES):
        return _remote_known_active()
    return False


# ---------------------------------------------------------------------------
# /api/jobs response cache lives in its own module (jobs_cache.py), rather
# than here, so relay.py can also import it (to proactively refresh it
# before closing) without creating a server_hooks<->relay import cycle.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# /object_info patching: swap in the cached remote model list so users only
# see models that actually exist on the remote GPU, without touching local
# editing at all.
# ---------------------------------------------------------------------------

def _merge_remote_models(resp: web.StreamResponse, cache: dict, context: str = "/object_info") -> web.StreamResponse:
    if not isinstance(resp, web.Response):
        logger.warning(
            f"[ComfyUI Proxy] {context} response is {type(resp).__name__}, not a plain "
            "web.Response (likely streamed by another middleware) — cannot patch model dropdowns."
        )
        return resp
    try:
        raw = resp.body
        if not raw:
            logger.warning(f"[ComfyUI Proxy] {context} response had no body to patch.")
            return resp
        data = json.loads(raw)
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] Could not parse local {context} JSON to patch models: {e}")
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

    logger.info(f"[ComfyUI Proxy] Patched {matched_keys} model dropdown field(s) in {context} from the remote cache.")
    return web.Response(body=json.dumps(data).encode("utf-8"), status=resp.status, content_type="application/json")


def _hide_local_models(resp: web.StreamResponse, context: str = "/object_info") -> web.StreamResponse:
    """No model list is cached for the current remote URL yet (the first fetch
    is pending or failed). Offering the models found on this computer would
    let users pick files that may not exist on the remote, so the model
    dropdowns are emptied instead until a list arrives."""
    if not isinstance(resp, web.Response):
        return resp
    try:
        raw = resp.body
        if not raw:
            return resp
        data = json.loads(raw)
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] Could not parse local {context} JSON to hide local models: {e}")
        return resp
    hidden = models_cache.hide_local_models(data)
    if not hidden:
        return resp
    logger.info(
        f"[ComfyUI Proxy] No model list for this remote yet: emptied {hidden} local model "
        f"dropdown field(s) in {context}."
    )
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
                    "This shouldn't normally happen since the proxy asks for gzip/deflate only on "
                    "this call, but if your gateway ignores Accept-Encoding and forces br/zstd "
                    "anyway, install 'Brotli' and/or 'backports.zstd' (Python < 3.14) in ComfyUI's "
                    "Python environment."
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
    try:
        body = await request.read()
    except ConnectionError as e:  # ConnectionResetError is a subclass
        logger.warning(
            f"[ComfyUI Proxy] Remote GPU disconnected before {request.rel_url.path} body was received "
            f"({e}); submission dropped."
        )
        return web.Response(status=499) # nobody is listening, so this is just for aiohttp
    
    client_id = None
    try:
        client_id = (json.loads(body) or {}).get("client_id")
    except Exception:
        pass

    if client_id:
        # Open (or reuse) the shadow progress relay and wait for the remote
        # to confirm its sid before the job actually starts, so the local
        # UI is guaranteed to receive progress instead of showing "running
        # in another tab".
        await relay.ensure_relay_ready(client_id, wait_timeout=forwarder.get_timeout().total)
    else:
        logger.warning("[ComfyUI Proxy] /prompt submission had no client_id — live progress relay can't be set up for it.")

    headers = forwarder.copy_request_headers(request, limit_encoding=True)
    session = forwarder.get_tracking_session()
    timeout = forwarder.get_timeout()
    url = f"{base}{request.rel_url.path}"  # preserves /prompt vs /api/prompt as actually requested

    # /prompt is an explicit user action, so it's always attempted even if
    # the circuit is currently open for automatic polling — but its outcome
    # still feeds that same shared signal, since a working /prompt is the
    # clearest possible proof the remote has recovered.
    try:
        async with session.post(url, data=body, headers=headers, timeout=timeout) as r:
            status = r.status
            data, text, err = await _read_tracking_response(r, "/prompt")
            if err:
                forwarder.circuit_record_failure()
                return err
    except asyncio.TimeoutError:
        logger.error("[ComfyUI Proxy] Timeout submitting prompt to remote GPU")
        forwarder.circuit_record_failure()
        return web.json_response(
            {"error": "ComfyUI Proxy: remote GPU timed out while queuing the prompt (try raising the timeout)"},
            status=504,
        )
    except aiohttp.ClientError as e:
        logger.error(f"[ComfyUI Proxy] Failed to submit prompt: {e}")
        forwarder.circuit_record_failure()
        return web.json_response({"error": f"ComfyUI Proxy: failed to reach remote GPU: {e}"}, status=502)

    forwarder.circuit_record_success()
    if data is not None:
        if 200 <= status < 300:
            prompt_id = data.get("prompt_id")
            state.mark_job_queued(prompt_id, client_id)
            logger.info(f"[ComfyUI Proxy] Job {prompt_id} queued on remote GPU.")
        elif status >= 500:
            logger.error(f"[ComfyUI Proxy] Remote GPU returned HTTP {status} queuing prompt.")
        return web.json_response(data, status=status)
    return web.Response(text=text or "", status=status)


async def _submit_prompt(request: web.Request, path: str) -> web.Response:
    """POST /prompt. While the remote GPU wakes up (a cold start can take a
    minute or more) the UI would otherwise show nothing at all after the user
    presses Run. So each submission that has to wait is registered as a
    placeholder pending job (placeholders.py): the Job Queue lists it right
    away, marked "Waiting for a machine", and the queue count follows. It is
    removed again as soon as the submission finishes — the real job then shows
    up from the remote — fails, or is cancelled by the user."""
    try:
        body = await request.read()  # cached, so _handle_prompt can read it again
    except ConnectionError:
        return await _handle_prompt(request)  # logs the disconnect, returns 499 — no point waking anything
    client_id = workflow_id = None
    try:
        payload = json.loads(body) or {}
        client_id = payload.get("client_id")
        workflow_id = (((payload.get("extra_data") or {}).get("extra_pnginfo") or {}).get("workflow") or {}).get("id")
    except Exception:
        pass
    if not isinstance(workflow_id, str):
        workflow_id = None

    relay.pending_add(client_id)
    ph = None
    try:
        if not relay.has_active_relay():
            ph = placeholders.add(client_id, workflow_id)
            await relay.announce_waiting(client_id, ph.id)
            await relay.announce_pending()
            await forwarder.wake_remote_if_needed(f"request to {path}")
        if ph is not None and ph.cancelled:
            # Cancelled in the Job Queue while the GPU was starting: nothing was
            # sent, and there is no job to report — an empty prompt_id is
            # answered without an error, so the UI just carries on.
            logger.info("[ComfyUI Proxy] Prompt cancelled while the remote GPU was starting; not submitting it.")
            resp = web.json_response({"prompt_id": "", "number": 0, "node_errors": {}})
        else:
            resp = await _handle_prompt(request)
    finally:
        relay.pending_done(client_id)
        if ph is not None:
            placeholders.remove(ph.id)

    if 200 <= resp.status < 300 and models_cache.needs_refresh():
        # The GPU is up now (its job is queued), so this is the free moment to
        # fetch the remote model list we deliberately didn't wake it for earlier.
        asyncio.ensure_future(models_cache.refresh_models_cache())

    if ph is not None or not (200 <= resp.status < 300) or not relay.has_relay_for(client_id):
        # The placeholder is gone (replaced by the real job on success), or
        # nothing real will follow to replace the count (the submission
        # failed, or there's no progress stream to report it): refresh the
        # UI's queue, which puts it back to exactly what is really queued.
        await relay.announce_pending()
    return resp


async def _cancel_queue_placeholders(request: web.Request, canonical: str):
    """POST /queue {"delete": [ids]} (cancelling one queued job in the Job
    Queue) or {"clear": true} ("Clear queue"), for the placeholder jobs that
    are waiting for the remote GPU. They're cancelled here: the submission
    waiting on the GPU then doesn't send its prompt. When that's all the
    request was about, it's answered locally — forwarding it would wake the GPU
    just to cancel something it never received. None means "carry on as usual"
    (nothing of ours in it, or real jobs to forward it for)."""
    if canonical != "/queue" or not placeholders.count():
        return None
    try:
        body = await request.read()  # cached for any later reader
        payload = json.loads(body) if body else {}
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    ids = payload.get("delete") if isinstance(payload.get("delete"), list) else []
    clear = payload.get("clear") is True
    cancelled = [ph for ph in (placeholders.cancel(i) for i in ids) if ph is not None]
    if clear:
        cancelled += placeholders.cancel_all()
    if not cancelled:
        return None
    logger.info(f"[ComfyUI Proxy] Cancelled {len(cancelled)} queued job(s) that were still waiting for the remote GPU.")
    await relay.announce_pending()
    done = {ph.id for ph in cancelled}
    other_ids = [i for i in ids if i not in done]
    if not other_ids and (not clear or not _remote_known_active()):
        return web.Response(status=200)
    return None


async def _handle_queue(request: web.Request) -> web.Response:
    # /queue is polled automatically and frequently by the frontend while a
    # job is incomplete — if the remote just failed, skip straight to an
    # error instead of attempting (and potentially waiting out the full
    # timeout on) another live request; see forwarder's circuit breaker.
    if forwarder.circuit_is_open():
        return web.json_response(
            {"error": "ComfyUI Proxy: remote GPU is temporarily unresponsive; pausing automatic polling"},
            status=503,
        )

    base = forwarder.target_base()
    headers = forwarder.copy_request_headers(request, limit_encoding=True)
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
                forwarder.circuit_record_failure()
                return err
    except asyncio.TimeoutError:
        logger.error("[ComfyUI Proxy] Timeout fetching remote queue state")
        forwarder.circuit_record_failure()
        return web.json_response({"error": "ComfyUI Proxy: remote GPU timed out"}, status=504)
    except aiohttp.ClientError as e:
        logger.error(f"[ComfyUI Proxy] Failed to fetch remote queue: {e}")
        forwarder.circuit_record_failure()
        return web.json_response({"error": f"ComfyUI Proxy: failed to reach remote GPU: {e}"}, status=502)

    forwarder.circuit_record_success()
    if data is not None:
        if not data.get("queue_running") and not data.get("queue_pending"):
            state.clear_all()
        return web.json_response(data, status=status)
    return web.Response(status=status)


# ---------------------------------------------------------------------------
# Crystools GPU list: when /monitor/GPU stays local and this machine has no
# GPU, report a fake one so Crystools' GPU monitors still show up in the UI
# (the real work happens on the remote GPU anyway).
# ---------------------------------------------------------------------------

_fake_gpu_logged = False
_fake_gpu_active = set()  # which monitor APIs had their last GET /monitor/GPU answered with the fake list
# Crystools and its MonitorOnly fork ("crysmonitor") have the same monitor API under
# different names; the fork changes a GPU's switches with POST where Crystools uses PATCH.
_FAKE_GPU_LIST_RE = re.compile(r"^/(crystools|crysmonitor)/monitor/GPU/?$")
_FAKE_GPU_PATCH_RE = re.compile(r"^/(crystools|crysmonitor)/monitor/GPU/(\d+)/?$")
_FAKE_GPU_PATCH_METHOD = {"crystools": "PATCH", "crysmonitor": "POST"}


def _fake_gpu_name() -> str:
    return f"NVIDIA {cfgmod.GPU_NAME}"


def _fake_gpu_list() -> list:
    try:
        count = max(1, int(cfgmod.GPU_COUNT))
    except (AttributeError, TypeError, ValueError):
        count = 1
    name = _fake_gpu_name()
    return [{"index": i, "name": name} for i in range(count)]


def _maybe_fake_crystools_gpu(resp: web.StreamResponse, api: str = "crystools") -> web.StreamResponse:
    """Replace an empty/'no GPU' local Crystools GPU list with the fake one.
    `api` is the monitor API that was asked ("crystools" or "crysmonitor").
    Anything else (a real GPU, an error status, a streamed or unparseable
    response) is returned untouched."""
    global _fake_gpu_logged
    if not isinstance(resp, web.Response) or resp.status != 200:
        return resp
    raw = resp.body
    if raw is not None and not isinstance(raw, (bytes, bytearray)):
        return resp
    try:
        data = json.loads(raw) if raw else None
    except Exception:
        return resp  # non-empty but unparseable: not ours to touch
    if data and not (isinstance(data, list) and not data[0]):
        _fake_gpu_active.discard(api)
        return resp  # a real GPU was detected locally
    fake = _fake_gpu_list()
    _fake_gpu_active.add(api)
    if not _fake_gpu_logged:
        _fake_gpu_logged = True
        logger.info(
            f"[ComfyUI Proxy] No local GPU detected; reporting {len(fake)}x {fake[0]['name']} to Crystools."
        )
    return web.json_response(fake)


def _stub_fake_gpu_patch(request: web.Request, canonical: str):
    """Crystools' PATCH /monitor/GPU/<index> (per-GPU monitor toggles) answers
    "400 Bad Request" for an index it doesn't have — which is every index
    while we're faking the GPU (the fork's POST version doesn't even catch the
    error and answers 500). Acknowledge those with a 200 instead (the toggles
    have nothing to act on anyway). None = not a request we handle."""
    m = _FAKE_GPU_PATCH_RE.match(canonical)
    if m is None or m.group(1) not in _fake_gpu_active or request.method != _FAKE_GPU_PATCH_METHOD[m.group(1)]:
        return None
    if int(m.group(2)) >= len(_fake_gpu_list()):
        return None
    return web.Response(status=200)


# ---------------------------------------------------------------------------
# comfyui-resource-monitor: the same fake GPU, for a monitor that reports in two
# ways — HTTP answers, and a websocket event its service pushes to every
# registered browser each refresh interval. The panel merges each reading over
# the previous one, so all the local ones must carry the fake GPU, or a "no GPU"
# reading would keep taking it away again.
# ---------------------------------------------------------------------------

_FAKE_RESOURCE_ROUTES = ("/resource-monitor/snapshot", "/resource-monitor/configure")
_fake_resource_logged = False


def _fake_resource_snapshot(snapshot):
    """A copy of a resource-monitor reading with the fake GPU in it, if the reading
    says there is no GPU. Anything else (a real GPU, not a reading) comes back as is."""
    global _fake_resource_logged
    if not isinstance(snapshot, dict) or snapshot.get("gpu_available") is not False:
        return snapshot
    faked = dict(snapshot)
    faked.update(
        gpu_available=True,
        gpu_name=_fake_gpu_name(),
        gpu_percent=0.0,
        vram_percent=0.0,
        vram_used_bytes=0,
        vram_total_bytes=int(cfgmod.GPU_VRAM_GB * 1024 ** 3),
        gpu_temp_celsius=0.0,
    )
    if not _fake_resource_logged:
        _fake_resource_logged = True
        logger.info(f"[ComfyUI Proxy] No local GPU detected; reporting {_fake_gpu_name()} to comfyui-resource-monitor.")
    return faked


def _maybe_fake_resource_gpu(resp: web.StreamResponse) -> web.StreamResponse:
    """The local answer of GET /resource-monitor/snapshot (the reading itself) or
    POST /resource-monitor/configure (with the reading under "snapshot"), with the
    fake GPU in it when it says there is none."""
    if not isinstance(resp, web.Response) or resp.status != 200:
        return resp
    raw = resp.body
    if not isinstance(raw, (bytes, bytearray)) or not raw:
        return resp
    try:
        data = json.loads(raw)
    except Exception:
        return resp
    if not isinstance(data, dict):
        return resp
    if isinstance(data.get("snapshot"), dict):
        faked = _fake_resource_snapshot(data["snapshot"])
        if faked is data["snapshot"]:
            return resp
        data = {**data, "snapshot": faked}
    else:
        faked = _fake_resource_snapshot(data)
        if faked is data:
            return resp
        data = faked
    return web.json_response(data)


def _hook_resource_monitor_events(server) -> None:
    """Put the fake GPU into the readings comfyui-resource-monitor pushes to the
    browser as websocket events (they never pass through the middleware), and hold
    them back for a client whose remote is sending its own through the shadow
    stream, which would otherwise alternate with them."""
    original = server.send
    if getattr(original, "_comfyui_proxy_hook", False):
        return

    async def send(event, data, sid=None):
        if event == relay.RESOURCE_MONITOR_EVENT and _is_enabled():
            if sid is not None and relay.remote_streams_resource_monitor(sid):
                return
            data = _fake_resource_snapshot(data)
        return await original(event, data, sid)

    send._comfyui_proxy_hook = True
    server.send = send


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

@web.middleware
async def proxy_middleware(request: web.Request, handler):
    path = request.rel_url.path
    canonical = _strip_api_prefix(path)

    # Local management API — never proxied.
    if path.startswith("/comfyui_proxy/"):
        return await handler(request)

    # Workflow editing stays local always; only patch the model dropdowns.
    if canonical == "/object_info" or canonical.startswith("/object_info/"):
        resp = await handler(request)
        if not _is_enabled():
            logger.info(
                f"[ComfyUI Proxy] Skipping {path} model patch: proxy is "
                f"{'disabled' if not cfgmod.get('enabled') else 'missing a remote_url'}."
            )
            return resp
        cache = models_cache.cached_models()  # only a list cached for this exact remote URL
        if not cache:
            return _hide_local_models(resp, context=path)  # no list for this remote yet: show no local models
        return _merge_remote_models(resp, cache, context=path)

    # /api/jobs GET: cache-aware handling, independent of the general
    # proxy/wake gating below, so cached history can be served even while
    # deliberately not waking the remote.
    if _is_enabled() and request.method == "GET" and (path == "/api/jobs" or path.startswith("/api/jobs/")):
        resp = await jobs_cache.handle_request(request, _remote_known_active())
        if resp is not None:
            return resp
        return await handler(request)  # nothing cached; let local take it

    # The file lists behind the Media Assets panel (the Imported tab; the asset
    # lists with --enable-assets): the remote's, when it can be asked without
    # waking the GPU, otherwise the last list seen (files_cache.py) — never this
    # computer's own files, which the remote may not have.
    if _is_enabled() and request.method == "GET" and files_cache.is_list_route(path):
        cpu = _cpu_target_configured()  # like the rest of the asset API: the CPU endpoint whenever there is one
        return await files_cache.handle_request(request, cpu or _remote_known_active(), cpu)

    # POST /history (or /api/history) with {"clear": true} or {"delete": [ids]}
    # is the frontend removing completed/failed jobs from the history — forget
    # them in our saved job history too, regardless of whether this particular
    # request ends up reaching the remote, so they don't keep showing up.
    # request.read() caches its result, so peeking here doesn't disturb
    # downstream handling (local or forwarded) reading the body again later.
    if _is_enabled() and request.method == "POST" and (canonical == "/history" or canonical.startswith("/history/")):
        try:
            body = await request.read()
            payload = json.loads(body) if body else {}
        except Exception:
            payload = {}
        if isinstance(payload, dict) and payload.get("clear") is True:
            await jobs_cache.clear()
            logger.info(f"[ComfyUI Proxy] Cleared saved job history ({path}, clear=true).")
        if isinstance(payload, dict) and isinstance(payload.get("delete"), list):
            await jobs_cache.forget(payload["delete"])

    # A queued job that is still waiting for the remote GPU (placeholders.py)
    # being cancelled — the Job Queue's cancel button, or "Clear queue". Answered
    # right here when that is all the request is about, so that cancelling
    # doesn't wake the GPU.
    if _is_enabled() and request.method == "POST":
        resp = await _cancel_queue_placeholders(request, canonical)
        if resp is not None:
            return resp

    # "Auto-download viewed input/output": a /view file that's already on disk
    # is served by ComfyUI's own /view handler (so no remote is involved at
    # all, and Range/preview handling stays identical); one that isn't gets
    # forwarded as usual below, with a sink that keeps a local copy.
    view_local_path = None
    if (
        _is_enabled()
        and cfgmod.get("auto_download_viewed")
        and canonical == "/view"
        and request.method in ("GET", "HEAD")
    ):
        view_local_path = viewcache.local_path_for(request.rel_url.query)
        if view_local_path is not None:
            if os.path.isfile(view_local_path):
                return await handler(request)
            if request.method != "GET":
                view_local_path = None

    if not _is_enabled() or not _should_proxy(path):
        if _is_enabled():
            stub = _stub_fake_gpu_patch(request, canonical)
            if stub is not None:
                return stub
        resp = await handler(request)
        if _is_enabled() and request.method == "GET":
            m = _FAKE_GPU_LIST_RE.match(canonical)
            if m is not None:
                resp = _maybe_fake_crystools_gpu(resp, m.group(1))
        if _is_enabled() and canonical in _FAKE_RESOURCE_ROUTES:
            resp = _maybe_fake_resource_gpu(resp)
        return resp

    if canonical == "/prompt" and request.method == "POST":
        return await _submit_prompt(request, path)
    if canonical == "/queue" and request.method == "GET":
        if not relay.has_active_relay():
            await forwarder.wake_remote_if_needed(f"request to {path}")
        return await _handle_queue(request)
    if canonical == "/interrupt":
        state.clear_all()
        if not relay.has_active_relay():
            asyncio.create_task(forwarder.wake_remote_if_needed(f"request to {path}"))
        return await forwarder.forward_http(request)

    # Everything else (history, view, viewvideo, api/jobs, api/crystools,
    # api/crysmonitor/monitor, uploads, free, ...): stream through untouched,
    # preserving encoding, partial content, and status codes — to the CPU
    # target when eligible and the GPU isn't already active, otherwise to the
    # GPU as usual.
    use_cpu = _use_cpu_target(canonical, path)
    is_view_route = any(canonical == p or canonical.startswith(p + "/") for p in CPU_ELIGIBLE_PREFIXES)
    if use_cpu:
        logger.info(f"[ComfyUI Proxy] Using remote CPU container for {path} (GPU not active).")
    else:
        if not relay.has_active_relay():
            asyncio.create_task(forwarder.wake_remote_if_needed(f"request to {path}"))
        if is_view_route:
            # No separate CPU target to fall back to for this request — note
            # the activity so the opt-in keep-alive (if enabled) knows to
            # keep the GPU warm for continued viewing/playback.
            keepalive.note_view_activity()
    sink = viewcache.make_sink(view_local_path, request, use_cpu) if view_local_path else None
    resp = await forwarder.forward_http(request, use_cpu=use_cpu, sink=sink)
    if request.method == "POST" and canonical in ("/upload/image", "/upload/mask") and 200 <= resp.status < 300:
        # An upload is what changes the Imported list: refresh the saved copy now,
        # from the container that took it (it is awake), so opening the tab later
        # needs no request to the remote at all (files_cache.py).
        files_cache.refresh_input_soon(use_cpu)
    return resp


# ---------------------------------------------------------------------------
# Local REST management API + registration
# ---------------------------------------------------------------------------

def _public_config() -> dict:
    cfg = dict(cfgmod.load_config())
    cached_models = models_cache.cached_models(cfg)
    cfg["has_models_cache"] = bool(cached_models)
    cfg["models_cache_count"] = len(cached_models)
    cfg["models_refreshing"] = models_cache.is_refreshing()  # a model list fetch is running or queued
    # The defaults themselves, so the panel never has to repeat them (they live in config.py only).
    cfg["defaults"] = {
        k: v for k, v in cfgmod.DEFAULT_CONFIG.items()
        if k not in ("models_cache", "models_cache_url", "auth_key", "remote_cpu_auth_key")
    }
    cfg["auth_key_set"] = bool(cfg.get("auth_key"))
    cfg["remote_cpu_auth_key_set"] = bool(cfg.get("remote_cpu_auth_key"))
    cfg.pop("models_cache", None)
    cfg.pop("auth_key", None)
    cfg.pop("remote_cpu_auth_key", None)
    return cfg


def setup():
    server = PromptServer.instance
    if proxy_middleware not in server.app.middlewares:
        server.app.middlewares.append(proxy_middleware)
    _hook_resource_monitor_events(server)

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

        allowed = {
            "enabled", "remote_url", "timeout", "auth_key",
            "remote_cpu_url", "remote_cpu_auth_key",
            "post_completion_delay", "jobs_history_max_entries",
            "gpu_keepalive_enabled", "gpu_keepalive_idle_timeout",
            "circuit_breaker_cooldown", "circuit_breaker_max_failures",
            "auto_download_viewed",
        }
        clean = {k: v for k, v in patch.items() if k in allowed}
        if "remote_url" in clean:
            clean["remote_url"] = (clean["remote_url"] or "").strip().rstrip("/")
        if "remote_cpu_url" in clean:
            clean["remote_cpu_url"] = (clean["remote_cpu_url"] or "").strip().rstrip("/")
        if "timeout" in clean:
            try:
                clean["timeout"] = max(5, float(clean["timeout"]))
            except (TypeError, ValueError):
                clean.pop("timeout", None)
        if "post_completion_delay" in clean:
            try:
                clean["post_completion_delay"] = max(0, float(clean["post_completion_delay"]))
            except (TypeError, ValueError):
                clean.pop("post_completion_delay", None)
        if "jobs_history_max_entries" in clean:
            try:
                clean["jobs_history_max_entries"] = max(1, int(clean["jobs_history_max_entries"]))
            except (TypeError, ValueError):
                clean.pop("jobs_history_max_entries", None)
        if "auto_download_viewed" in clean:
            clean["auto_download_viewed"] = bool(clean["auto_download_viewed"])
        if "gpu_keepalive_enabled" in clean:
            clean["gpu_keepalive_enabled"] = bool(clean["gpu_keepalive_enabled"])
        if "gpu_keepalive_idle_timeout" in clean:
            try:
                clean["gpu_keepalive_idle_timeout"] = max(5, float(clean["gpu_keepalive_idle_timeout"]))
            except (TypeError, ValueError):
                clean.pop("gpu_keepalive_idle_timeout", None)
        if "circuit_breaker_cooldown" in clean:
            try:
                clean["circuit_breaker_cooldown"] = max(1, float(clean["circuit_breaker_cooldown"]))
            except (TypeError, ValueError):
                clean.pop("circuit_breaker_cooldown", None)
        if "circuit_breaker_max_failures" in clean:
            try:
                clean["circuit_breaker_max_failures"] = max(0, int(clean["circuit_breaker_max_failures"]))
            except (TypeError, ValueError):
                clean.pop("circuit_breaker_max_failures", None)
        if "auth_key" in clean and not clean["auth_key"]:
            clean.pop("auth_key", None)  # blank means "leave unchanged"
        if "remote_cpu_auth_key" in clean and not clean["remote_cpu_auth_key"]:
            clean.pop("remote_cpu_auth_key", None)  # blank means "leave unchanged"

        prev = cfgmod.load_config()
        url_changed = "remote_url" in clean and clean["remote_url"] != prev.get("remote_url")
        newly_enabled = clean.get("enabled") and not prev.get("enabled")

        cfg = cfgmod.update_config(clean)
        if not cfg.get("auto_download_viewed") or url_changed:
            viewcache.cancel_all()  # stop background file downloads (turned off, or endpoint switched)

        if cfg.get("enabled") and cfg.get("remote_url") and (url_changed or newly_enabled or not models_cache.cached_models(cfg)):
            # Counted as in progress right away, so the response below already
            # reports models_refreshing=True instead of a misleading "not cached".
            models_cache.schedule_refresh(force=url_changed)

        if url_changed:
            state.clear_all()  # a switched endpoint invalidates any tracked remote job
            # (saved job history is kept per remote URL, so there's nothing to clear for it)
            forwarder.circuit_reset()  # ...and any latched "remote is down" state

        cpu_url = (cfg.get("remote_cpu_url") or "").rstrip("/")
        gpu_url = (cfg.get("remote_url") or "").rstrip("/")
        if cpu_url and cpu_url == gpu_url:
            logger.warning(
                "[ComfyUI Proxy] Remote CPU URL is identical to Remote GPU URL — this provides no "
                "benefit (it's the same volatile endpoint under a second label) and is being ignored; "
                "uploads/view/viewvideo will use the GPU target as if no CPU URL were set."
            )

        return web.json_response(_public_config())

    @routes.post("/comfyui_proxy/refresh_models")
    async def _refresh_models(request):
        if not cfgmod.get("remote_url"):
            return web.json_response({"error": "remote URL not configured"}, status=400)
        cache = await models_cache.refresh_models_cache(force=True, manual=True)  # the user asked: worth waking something
        return web.json_response({"count": len(cache) if cache else 0})

    @routes.post("/comfyui_proxy/reset_state")
    async def _reset_state(request):
        cleared_jobs = len(state.incomplete_ids())
        cancelled_relays = relay.cancel_all_relays()
        state.clear_all()
        forwarder.circuit_reset()
        viewcache.cancel_all()
        if placeholders.cancel_all():
            await relay.announce_pending()  # take the waiting jobs out of the UI's queue too
        keepalive.stop()
        logger.info(
            f"[ComfyUI Proxy] Manual state reset: cleared {cleared_jobs} tracked job(s), "
            f"cancelled {cancelled_relays} relay connection(s)."
        )
        return web.json_response({"cleared_jobs": cleared_jobs, "cancelled_relays": cancelled_relays})

    @routes.post("/comfyui_proxy/reset_config")
    async def _reset_config(request):
        relay.cancel_all_relays()
        state.clear_all()
        forwarder.circuit_reset()
        viewcache.cancel_all()
        if placeholders.cancel_all():
            await relay.announce_pending()  # take the waiting jobs out of the UI's queue too
        keepalive.stop()
        cfgmod.reset_to_defaults()
        logger.info("[ComfyUI Proxy] Config reset to defaults.")
        return web.json_response(_public_config())

    logger.info("[ComfyUI Proxy] Ready. Remote GPU forwarding is %s.", "enabled" if _is_enabled() else "disabled")


setup()
