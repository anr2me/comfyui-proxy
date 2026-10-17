"""
Placeholder jobs for the UI's queue while a prompt is waiting for the remote.

A cold-starting serverless GPU can take a minute or more to wake up. During
that time the frontend would show nothing at all after the user presses Run:
its Job Queue panel is built from GET /api/jobs?status=in_progress,pending,
which would still be empty. So each submission that has to wait is registered
here as a look-alike pending job:

  - jobs_cache.py merges these into the job list it answers (while the remote
    is asleep, and while the remote is awake but the prompt hasn't been handed
    to it yet), so every Run click shows up as its own queued job;
  - relay.announce_waiting() labels each one "Waiting for a machine", which the
    frontend shows as an initializing job (the state it uses on ComfyUI Cloud);
  - relay.announce_pending() sends a "status" websocket message whenever the
    set changes — the frontend responds to that by re-fetching the job list,
    which is how entries appear, and how they go away again.

A placeholder is removed as soon as its submission finishes (the real job then
shows up from the remote), fails, or is cancelled by the user (cancelling it in
the Job Queue, or "Clear queue", never wakes the GPU just to cancel something it
never received). As a safety net they also expire on their own, after the
Timeout setting plus config.QUEUE_PLACEHOLDER_MARGIN seconds — long enough to
outlast anything the proxy itself would still be waiting for.

Pure bookkeeping, no I/O: it is imported by jobs_cache.py, relay.py and
server_hooks.py, none of which should have to import each other for this.
"""

import time
import uuid

from . import config as cfgmod


class Placeholder:
    __slots__ = ("id", "client_id", "workflow_id", "create_time", "expires", "cancelled")

    def __init__(self, client_id, workflow_id, lifetime):
        self.id = f"waiting-{uuid.uuid4().hex}"
        self.client_id = client_id
        self.workflow_id = workflow_id
        self.create_time = int(time.time() * 1000)  # ms, like ComfyUI's job timestamps
        self.expires = time.monotonic() + lifetime
        self.cancelled = False  # set when the user cancelled it; the waiting submission checks this


_items = {}  # id -> Placeholder, oldest first


def lifetime() -> float:
    """Seconds a placeholder may live: the Timeout setting plus a margin."""
    timeout = cfgmod.number("timeout", 5.0)  # the same value (and floor) the requests themselves use
    try:
        margin = float(cfgmod.QUEUE_PLACEHOLDER_MARGIN)
    except (AttributeError, TypeError, ValueError):
        margin = 0.0
    return timeout + max(0.0, margin)


def _prune():
    now = time.monotonic()
    for pid in [p.id for p in _items.values() if p.expires <= now]:
        _items.pop(pid, None)


def add(client_id, workflow_id=None) -> Placeholder:
    _prune()
    ph = Placeholder(client_id, workflow_id, lifetime())
    _items[ph.id] = ph
    return ph


def remove(pid) -> None:
    """The submission is over (success or failure): drop the placeholder."""
    _items.pop(pid, None)


def cancel(pid):
    """The user cancelled this job: drop it and flag it, so the submission that
    is still waiting on the remote knows not to send the prompt after all."""
    ph = _items.pop(pid, None) if isinstance(pid, str) else None
    if ph is not None:
        ph.cancelled = True
    return ph


def cancel_all() -> list:
    cancelled = list(_items.values())
    _items.clear()
    for ph in cancelled:
        ph.cancelled = True
    return cancelled


def count() -> int:
    _prune()
    return len(_items)


def jobs(workflow_id=None) -> list:
    """The placeholders as /api/jobs list items, newest first."""
    _prune()
    out = []
    for ph in reversed(list(_items.values())):
        if workflow_id and ph.workflow_id != workflow_id:
            continue
        job = {
            "id": ph.id,
            "status": "pending",
            "create_time": ph.create_time,
            "outputs_count": 0,
            "previewable_outputs_count": 0,
        }
        if ph.workflow_id:
            job["workflow_id"] = ph.workflow_id
        out.append(job)
    return out
