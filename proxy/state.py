"""
Tracks whether any job is currently incomplete (queued/running on the
remote GPU), scoped per client_id so the progress relay knows when it's
safe to close a client's shadow websocket connection. This is what gates
conditional forwarding of /ws and /internal/logs, so we don't wake a
serverless instance just to stream logs or status when nothing is actually
running.
"""

import threading

_lock = threading.RLock()
_incomplete_prompt_ids = set()
_prompt_client = {}  # prompt_id -> client_id, so completion events (which
                      # only carry prompt_id) can be scoped back to a client.


def mark_job_queued(prompt_id, client_id=None):
    with _lock:
        if prompt_id:
            _incomplete_prompt_ids.add(prompt_id)
            if client_id:
                _prompt_client[prompt_id] = client_id


def mark_job_done(prompt_id=None):
    with _lock:
        if prompt_id:
            _incomplete_prompt_ids.discard(prompt_id)
            _prompt_client.pop(prompt_id, None)
        else:
            _incomplete_prompt_ids.clear()
            _prompt_client.clear()


def clear_client(client_id):
    """Stop tracking every incomplete job belonging to this client_id. Used
    when a client's shadow relay ends for any reason — including failure —
    so a job whose completion we can no longer observe doesn't stay
    'incomplete' forever and keep forcing every future request into
    'remote known active'."""
    with _lock:
        stale = [pid for pid, cid in _prompt_client.items() if cid == client_id]
        for pid in stale:
            _incomplete_prompt_ids.discard(pid)
            _prompt_client.pop(pid, None)


def has_incomplete_job():
    with _lock:
        return len(_incomplete_prompt_ids) > 0


def has_incomplete_job_for_client(client_id):
    with _lock:
        return any(
            cid == client_id
            for pid, cid in _prompt_client.items()
            if pid in _incomplete_prompt_ids
        )


def mark_all_done_for_client(client_id):
    """Force-clear every prompt tracked for this client, regardless of
    whether we ever saw an explicit completion message for it. Used as a
    safety net when the remote's queue_remaining count says nothing is left,
    which catches jobs that errored out mid-execution without emitting a
    clean execution_error/execution_interrupted message."""
    with _lock:
        for pid in [pid for pid, cid in _prompt_client.items() if cid == client_id]:
            _incomplete_prompt_ids.discard(pid)
            _prompt_client.pop(pid, None)


def clear_all():
    with _lock:
        _incomplete_prompt_ids.clear()
        _prompt_client.clear()


def incomplete_ids():
    with _lock:
        return set(_incomplete_prompt_ids)
