"""
Tracks whether any job is currently incomplete (queued/running on the
remote GPU). This is what gates conditional forwarding of /ws and
/internal/logs, so we don't wake a serverless instance just to stream logs
or status when nothing is actually running.
"""

import threading

_lock = threading.RLock()
_incomplete_prompt_ids = set()


def mark_job_queued(prompt_id):
    with _lock:
        if prompt_id:
            _incomplete_prompt_ids.add(prompt_id)


def mark_job_done(prompt_id=None):
    with _lock:
        if prompt_id:
            _incomplete_prompt_ids.discard(prompt_id)
        else:
            _incomplete_prompt_ids.clear()


def has_incomplete_job():
    with _lock:
        return len(_incomplete_prompt_ids) > 0


def clear_all():
    with _lock:
        _incomplete_prompt_ids.clear()


def incomplete_ids():
    with _lock:
        return set(_incomplete_prompt_ids)
