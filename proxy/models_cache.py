"""
Pulls the remote's model list (checkpoints, loras, vaes, etc.) exactly once
per remote_url, by fetching /object_info from the remote and extracting every
combo (dropdown) input. Cached in config so later local /object_info
responses can be patched to only show what the remote actually has,
excluding models that only exist locally and would fail on the remote GPU.

A list of files doesn't need a GPU, so waking one for it is avoided where
possible. Which remote gets asked:
  - the GPU, when it is already up (a live progress connection or a running
    job) — nothing extra to wake;
  - otherwise the separate CPU endpoint, when one is configured (it shares the
    models volume with the GPU one);
  - with neither, a GPU is woken only when it has to be: there is no model
    list for this remote at all yet (a workflow can't be set up without
    picking models), or the user clicked "Refresh Models" (manual=True), e.g.
    to pick up models just added on the server. Otherwise the fetch is
    postponed until the GPU next starts for a job: needs_refresh() tells
    server_hooks.py when that moment has come.
The list is cached under the GPU URL either way.
"""

import asyncio
import logging

from . import config as cfgmod
from . import forwarder

logger = logging.getLogger("ComfyUIProxy")

_refresh_lock = asyncio.Lock()  # one fetch at a time (enable, URL change and a job starting can overlap)
_deferred_logged = False
_refreshing = 0  # refresh_models_cache() calls in progress, including ones still waiting for the lock
_background_tasks = set()  # keeps schedule_refresh() tasks from being garbage-collected mid-run


def _gpu_active() -> bool:
    """True if a GPU instance is already up — a live shadow progress relay
    (a websocket connected to it) or a tracked incomplete job — so asking it
    for something wakes nothing. Imported here, not at module level, so this
    module stays free of import cycles."""
    from . import relay, state
    return state.has_incomplete_job() or relay.has_active_relay()


def _cpu_endpoint_configured() -> bool:
    """A CPU URL that is set and genuinely different from the GPU one (the
    same URL under a second label is just the GPU endpoint again)."""
    cpu = forwarder.target_base(use_cpu=True)
    return bool(cpu) and cpu != forwarder.target_base()


def _cached_for(cfg, gpu_base) -> bool:
    """A model list is cached for exactly this remote (a list cached for a
    different URL belongs to another server, so it doesn't count)."""
    return bool(cfg.get("models_cache")) and cfg.get("models_cache_url") == gpu_base


def needs_refresh() -> bool:
    """True when there is a remote URL but no model list cached for it yet."""
    gpu_base = forwarder.target_base()
    if not gpu_base:
        return False
    return not _cached_for(cfgmod.load_config(), gpu_base)


def is_refreshing() -> bool:
    """True while a model-list fetch is running or queued. The panel shows
    "Updating model list..." for as long as this is the case."""
    return _refreshing > 0


def schedule_refresh(force: bool = False, manual: bool = False):
    """Run refresh_models_cache() in the background — unless it would only
    hand back the list that is already cached, in which case nothing starts.
    The refresh counts as in progress from this very call, before the task
    has had a chance to run, so a status read right afterwards (the config
    POST's own response) already says "updating" rather than "not cached"."""
    global _refreshing
    gpu_base = forwarder.target_base()
    if not gpu_base:
        return None
    if not force and _cached_for(cfgmod.load_config(), gpu_base):
        return None

    async def _run():
        try:
            await refresh_models_cache(force=force, manual=manual)
        except Exception as e:
            logger.warning(f"[ComfyUI Proxy] Remote model list refresh failed: {e}")

    _refreshing += 1
    try:
        task = asyncio.create_task(_run())
    except BaseException:
        _refreshing -= 1
        raise
    _background_tasks.add(task)

    def _done(t):
        global _refreshing
        _refreshing -= 1
        _background_tasks.discard(t)

    task.add_done_callback(_done)
    return task


async def refresh_models_cache(force: bool = False, manual: bool = False):
    """Fetch the remote's model list and cache it (see _refresh_models_cache);
    counted by is_refreshing() while it runs or waits for its turn."""
    global _refreshing
    _refreshing += 1
    try:
        return await _refresh_models_cache(force=force, manual=manual)
    finally:
        _refreshing -= 1


async def _refresh_models_cache(force: bool = False, manual: bool = False):
    """Fetch the remote's model list and cache it. `force` refetches even if
    one is cached; `manual` says the user asked for it (the "Refresh Models"
    button), which makes it worth waking a GPU when there is no cheaper way
    to get the list."""
    global _deferred_logged
    gpu_base = forwarder.target_base()
    if not gpu_base:
        return None

    async with _refresh_lock:
        cfg = cfgmod.load_config()
        if not force and _cached_for(cfg, gpu_base):
            return cfg["models_cache"]  # already cached for this exact URL

        wake = False
        if _gpu_active():
            use_cpu = False  # already awake: ask it, nothing to wake
        elif _cpu_endpoint_configured():
            use_cpu = True  # a file listing doesn't need a GPU: ask the cheap CPU container
            wake = True
        elif manual or not _cached_for(cfg, gpu_base):
            # No CPU endpoint to ask. Either the user explicitly wants the list
            # now, or there is none at all yet — and a workflow can't be set up
            # without picking models — so here the GPU is worth waking.
            use_cpu = False
            wake = True
            logger.info(
                "[ComfyUI Proxy] Waking the GPU to fetch the remote model list "
                f"({'you asked for a refresh' if manual else 'no model list yet and no CPU endpoint configured'})."
            )
        else:
            if not _deferred_logged:
                _deferred_logged = True
                logger.info(
                    "[ComfyUI Proxy] Not refetching the remote model list yet: the GPU isn't running and no "
                    "separate CPU endpoint is configured. It will be refreshed the next time the GPU starts for a job."
                )
            return cfg.get("models_cache")
        _deferred_logged = False

        label = "CPU" if use_cpu else "GPU"
        base = forwarder.target_base(use_cpu)
        if wake:
            await forwarder.wake_remote_if_needed("fetching remote model list", use_cpu=use_cpu)

        session = forwarder.get_tracking_session()
        timeout = forwarder.get_timeout()
        headers = {"Accept-Encoding": forwarder.get_safe_accept_encoding()}
        auth_key = cfgmod.get("remote_cpu_auth_key" if use_cpu else "auth_key")
        if auth_key:
            headers["Authorization"] = f"Bearer {auth_key}"

        try:
            async with session.get(f"{base}/object_info", headers=headers, timeout=timeout) as resp:
                if resp.status >= 400:
                    logger.warning(f"[ComfyUI Proxy] Remote {label} returned HTTP {resp.status} while fetching model list")
                    return cfg.get("models_cache")
                data = await resp.json(content_type=None)
        except Exception as e:
            logger.warning(
                f"[ComfyUI Proxy] Failed to fetch remote model list from the {label}: {e}. "
                "If this is a decode/compression error, install the 'Brotli' and 'backports.zstd' packages."
            )
            return cfg.get("models_cache")

        models = {}
        for node_name, node_info in (data or {}).items():
            if not isinstance(node_info, dict):
                continue
            inputs = node_info.get("input", {})
            for group in ("required", "optional"):
                for pname, pdef in inputs.get(group, {}).items():
                    # ComfyUI combo/dropdown inputs are encoded as [[opt1, opt2, ...], {...}]
                    if isinstance(pdef, list) and len(pdef) > 0 and isinstance(pdef[0], list):
                        models[f"{node_name}.{pname}"] = pdef[0]

        cfgmod.update_config({"models_cache": models, "models_cache_url": gpu_base})
        logger.info(f"[ComfyUI Proxy] Cached remote model list from the {label} ({base}) ({len(models)} dropdown fields).")
        return models
