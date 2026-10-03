"""
Optional GPU keep-alive for continued /view or /viewvideo access (e.g.
watching a long generated video, or browsing through several output images)
after a job's own completion window has passed, for people without a
separate CPU target to fall back to.

Without this, a serverless GPU container can scale to zero mid-playback:
browsers don't hold one continuous connection for an entire video, they
fetch chunks via Range requests as needed and can go quiet for stretches
(already buffered ahead) longer than the provider's own idle timeout, even
while someone is still actively watching. Once the container's gone, that
source file becomes unreachable until something wakes the GPU again.

This has a real cost (it deliberately keeps a GPU container warm), so it
only runs at all when explicitly enabled, and stops itself the moment
/view/viewvideo activity actually goes quiet for longer than the
configured idle window — it never keeps the GPU warm indefinitely.
"""

import asyncio
import logging
import time

from . import config as cfgmod
from . import forwarder

logger = logging.getLogger("ComfyUIProxy")

_last_view_at = 0.0
_task = None


def note_view_activity():
    """Call this whenever a /view or /viewvideo request is actually
    forwarded to the GPU target. Starts (or just extends) the keep-alive
    loop if the feature is enabled; a no-op otherwise."""
    global _last_view_at
    _last_view_at = time.time()
    if cfgmod.get("gpu_keepalive_enabled", False):
        _ensure_task()


def _ensure_task():
    global _task
    if _task is None or _task.done():
        _task = asyncio.ensure_future(_run())


async def _run():
    interval = max(5.0, float(cfgmod.get("gpu_keepalive_interval", 20) or 20))
    idle_timeout = max(interval, float(cfgmod.get("gpu_keepalive_idle_timeout", 60) or 60))
    logger.info(
        f"[ComfyUI Proxy] GPU keep-alive started: pinging every {interval:g}s "
        f"while /view activity is within the last {idle_timeout:g}s."
    )
    try:
        while True:
            await asyncio.sleep(interval)
            if not cfgmod.get("gpu_keepalive_enabled", False):
                logger.info("[ComfyUI Proxy] GPU keep-alive disabled; stopping.")
                return
            idle_for = time.time() - _last_view_at
            if idle_for > idle_timeout:
                logger.info(
                    f"[ComfyUI Proxy] GPU keep-alive stopping: no /view activity for {idle_for:.0f}s "
                    f"(timeout {idle_timeout:g}s)."
                )
                return
            await forwarder.wake_remote_if_needed("GPU keep-alive for ongoing video/image viewing", use_cpu=False)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] GPU keep-alive loop stopped unexpectedly: {e}")


def stop():
    """Stop the keep-alive loop immediately, e.g. on a manual state reset."""
    global _task
    if _task is not None and not _task.done():
        _task.cancel()
    _task = None
