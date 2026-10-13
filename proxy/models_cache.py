"""
Pulls the remote's model list (checkpoints, loras, vaes, etc.) exactly once
per remote_url, by fetching /object_info from the remote and extracting every
combo (dropdown) input. Cached in config so later local /object_info
responses can be patched to only show what the remote actually has,
excluding models that only exist locally and would fail on the remote GPU.

A list of files doesn't need a GPU, so waking one for it would be a waste.
Which remote gets asked:
  - the GPU, but only when it is already up (a live progress connection or a
    running job) — nothing extra to wake;
  - otherwise the separate CPU endpoint, when one is configured (it shares the
    models volume with the GPU one);
  - with neither, the fetch is postponed until the GPU next starts for a job:
    needs_refresh() tells server_hooks.py when that moment has come.
The list is cached under the GPU URL either way.
"""

import asyncio
import logging

from . import config as cfgmod
from . import forwarder

logger = logging.getLogger("ComfyUIProxy")

_refresh_lock = asyncio.Lock()  # one fetch at a time (enable, URL change and a job starting can overlap)
_deferred_logged = False


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


def needs_refresh() -> bool:
    """True when there is a remote URL but no model list cached for it yet."""
    gpu_base = forwarder.target_base()
    if not gpu_base:
        return False
    cfg = cfgmod.load_config()
    return not (cfg.get("models_cache") and cfg.get("models_cache_url") == gpu_base)


async def refresh_models_cache(force: bool = False):
    global _deferred_logged
    gpu_base = forwarder.target_base()
    if not gpu_base:
        return None

    async with _refresh_lock:
        cfg = cfgmod.load_config()
        if not force and cfg.get("models_cache") and cfg.get("models_cache_url") == gpu_base:
            return cfg["models_cache"]  # already cached for this exact URL

        if _gpu_active():
            use_cpu = False  # already awake: ask it, nothing to wake
        elif _cpu_endpoint_configured():
            use_cpu = True  # a file listing doesn't need a GPU: ask the cheap CPU container
        else:
            if not _deferred_logged:
                _deferred_logged = True
                logger.info(
                    "[ComfyUI Proxy] Not fetching the remote model list yet: the GPU isn't running and no "
                    "separate CPU endpoint is configured. It will be fetched the next time the GPU starts for a job."
                )
            return cfg.get("models_cache")
        _deferred_logged = False

        label = "CPU" if use_cpu else "GPU"
        base = forwarder.target_base(use_cpu)
        if use_cpu:
            await forwarder.wake_remote_if_needed("fetching remote model list", use_cpu=True)

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
