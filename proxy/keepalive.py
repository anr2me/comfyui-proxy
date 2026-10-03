"""
Tracks /view and /viewvideo activity for the opt-in GPU keep-alive feature.

A brief periodic ping (an earlier version of this module) doesn't actually
keep a scale-to-zero serverless container warm: the moment a ping request
completes, the platform sees zero pending requests again and the container
is just as eligible for teardown as if no ping had happened at all. What
actually counts as "busy" is a connection that stays open and pending — and
the shadow relay (relay.py) already has exactly that, in the form of its
websocket to the GPU. So instead of a separate ping loop, this module only
tracks activity; relay.py itself decides whether to keep its existing
connection open longer because of it (see _extend_for_view_activity there).
"""

import time

from . import config as cfgmod

_last_view_at = 0.0


def note_view_activity():
    """Call this whenever a /view or /viewvideo request is actually
    forwarded to the GPU target. A no-op beyond recording the timestamp if
    the feature isn't currently enabled (cheap, and avoids a stale read if
    it gets enabled moments later)."""
    global _last_view_at
    _last_view_at = time.time()


def seconds_since_last_view() -> float:
    if _last_view_at == 0.0:
        return float("inf")
    return time.time() - _last_view_at


def enabled() -> bool:
    return bool(cfgmod.get("gpu_keepalive_enabled", False))


def idle_timeout() -> float:
    return max(5.0, float(cfgmod.get("gpu_keepalive_idle_timeout", 60) or 60))


def stop():
    """Force any in-progress extension (across any relay) to see itself as
    idle on its next check, ending it promptly. Used by the manual state
    reset."""
    global _last_view_at
    _last_view_at = 0.0
