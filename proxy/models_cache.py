"""
Pulls the remote's model list (checkpoints, loras, vaes, etc.) exactly once
per remote_url, by fetching /object_info from the remote and extracting every
combo (dropdown) input. Cached in config so later local /object_info
responses can be patched to only show what the remote actually has,
excluding models that only exist locally and would fail on the cloud GPU.
"""

import logging

from . import config as cfgmod
from . import forwarder

logger = logging.getLogger("ComfyUIProxy")


async def refresh_models_cache(force: bool = False):
    base = forwarder.target_base()
    if not base:
        return None

    cfg = cfgmod.load_config()
    if not force and cfg.get("models_cache") and cfg.get("models_cache_url") == base:
        return cfg["models_cache"]  # already cached for this exact URL

    await forwarder.wake_remote_if_needed("fetching remote model list")

    session = forwarder.get_tracking_session()
    timeout = forwarder.get_timeout()
    headers = {}
    auth_key = cfgmod.get("auth_key")
    if auth_key:
        headers["Authorization"] = f"Bearer {auth_key}"

    try:
        async with session.get(f"{base}/object_info", headers=headers, timeout=timeout) as resp:
            if resp.status >= 400:
                logger.warning(f"[ComfyUI Proxy] Remote returned HTTP {resp.status} while fetching model list")
                return cfg.get("models_cache")
            data = await resp.json(content_type=None)
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] Failed to fetch remote model list: {e}")
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

    cfgmod.update_config({"models_cache": models, "models_cache_url": base})
    logger.info(f"[ComfyUI Proxy] Cached remote model list from {base} ({len(models)} dropdown fields).")
    return models
