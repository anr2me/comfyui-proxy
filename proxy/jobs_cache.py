"""
Saved job history behind GET /api/jobs (used by the frontend's Media Assets
panel to list completed/failed jobs).

Every time a request really reaches the remote, the finished jobs in its
answer are saved in a local database (jobs_db.py -> jobs_history.db). A job
list for finished jobs is then built from that database instead of being
replayed from a cached response, so:

  - history survives ComfyUI restarts, and the remote's own (per-session,
    wiped on every cold boot) history;
  - any filter / sort / page the panel asks for works, not just the exact
    query that happened to be cached;
  - while the remote is idle, history is shown without waking it just to
    browse (see server_hooks.py's routing rules).

Live jobs (pending / in progress) only exist on the remote, so they're never
saved: those lists always come straight from the remote when it's active,
and fall through to ComfyUI's own (local) answer when it isn't.

refresh_known() is called by the shadow relay (relay.py) right after a job
finishes — while its connection still proves the remote is awake — so the
new job is saved before that connection closes. Kept as its own module,
rather than living in server_hooks.py or relay.py, so both of them can import
it without an import cycle between each other.
"""

import asyncio
import json
import logging
import re

import aiohttp
from aiohttp import web

from . import config as cfgmod
from . import forwarder
from . import jobs_db

logger = logging.getLogger("ComfyUIProxy")

PATH = "/api/jobs"

LIVE_STATUSES = frozenset({"pending", "in_progress"})
TERMINAL_STATUSES = frozenset(jobs_db.TERMINAL_STATUSES)
ALL_STATUSES = LIVE_STATUSES | TERMINAL_STATUSES
_DETAIL_RE = re.compile(r"^/api/jobs/[^/]+$")


def _remote_key() -> str:
    return (cfgmod.get("remote_url") or "").rstrip("/")


def _max_entries() -> int:
    try:
        return max(1, int(cfgmod.get("jobs_history_max_entries", 500) or 500))
    except (TypeError, ValueError):
        return 500


class _ListQuery:
    """The parameters of GET /api/jobs, parsed with the same rules as ComfyUI."""
    __slots__ = ("statuses", "workflow_id", "sort_by", "sort_order", "limit", "offset")


def _parse_list_query(query):
    """None if any parameter is invalid (ComfyUI would answer 400 — leave
    such requests to it rather than guessing)."""
    q = _ListQuery()
    status_param = query.get("status")
    if status_param:
        statuses = [s.strip().lower() for s in status_param.split(",") if s.strip()]
        if any(s not in ALL_STATUSES for s in statuses):
            return None
        q.statuses = set(statuses)
    else:
        q.statuses = set(ALL_STATUSES)
    q.workflow_id = query.get("workflow_id") or None
    q.sort_by = query.get("sort_by", "created_at").lower()
    q.sort_order = query.get("sort_order", "desc").lower()
    if q.sort_by not in ("created_at", "execution_duration") or q.sort_order not in ("asc", "desc"):
        return None
    q.limit = None
    if "limit" in query:
        try:
            q.limit = int(query.get("limit"))
        except (TypeError, ValueError):
            return None
        if q.limit <= 0:
            return None
    q.offset = 0
    if "offset" in query:
        try:
            q.offset = max(0, int(query.get("offset")))
        except (TypeError, ValueError):
            return None
    return q


def _saved_response(data, status=200) -> web.Response:
    resp = web.json_response(data, status=status)
    resp.headers["X-ComfyUI-Proxy-Cache"] = "saved"
    return resp


def _passthrough(status, data, text) -> web.Response:
    if data is not None:
        return web.json_response(data, status=status)
    return web.Response(text=text or "", status=status)


# ---------------------------------------------------------------------------
# Saved history
# ---------------------------------------------------------------------------

async def _remember(path: str, data):
    """Save whatever finished jobs a successful live response carries."""
    remote, limit = _remote_key(), _max_entries()
    if path == PATH and isinstance(data, dict) and isinstance(data.get("jobs"), list):
        await asyncio.to_thread(jobs_db.upsert_jobs, remote, data["jobs"], limit)
    elif _DETAIL_RE.match(path) and isinstance(data, dict):
        await asyncio.to_thread(jobs_db.put_detail, remote, data, limit)


async def _list_from_saved(q: _ListQuery):
    """The job list for `q` built from the saved history, or None when
    nothing at all is saved for this remote yet (callers then fall back to
    the live / local answer)."""
    remote = _remote_key()

    def work():
        if jobs_db.count(remote) == 0:
            return None
        return jobs_db.query(
            remote, sorted(q.statuses & TERMINAL_STATUSES), q.workflow_id,
            q.sort_by, q.sort_order, q.limit, q.offset,
        )

    result = await asyncio.to_thread(work)
    if result is None:
        return None
    jobs, total = result
    return _saved_response({
        "jobs": jobs,
        "pagination": {
            "offset": q.offset,
            "limit": q.limit,
            "total": total,
            "has_more": (q.offset + len(jobs)) < total,
        },
    })


async def _detail_from_saved(job_id: str):
    detail = await asyncio.to_thread(jobs_db.get_detail, _remote_key(), job_id)
    return _saved_response(detail) if detail is not None else None


async def clear():
    """The user cleared their job history: forget everything saved for the current remote."""
    await asyncio.to_thread(jobs_db.clear, _remote_key())


async def forget(job_ids):
    """The user deleted these jobs from the history."""
    await asyncio.to_thread(jobs_db.forget, _remote_key(), job_ids)


# ---------------------------------------------------------------------------
# Talking to the remote
# ---------------------------------------------------------------------------

async def _live_fetch(path: str, query: str):
    """GET path?query from the remote and save any finished jobs in the
    answer. Raises on transport/decode failure — callers decide what to do."""
    base = forwarder.target_base()
    headers = {"Accept-Encoding": forwarder.get_safe_accept_encoding()}
    auth_key = cfgmod.get("auth_key")
    if auth_key:
        headers["Authorization"] = f"Bearer {auth_key}"
    session = forwarder.get_tracking_session()
    timeout = forwarder.get_timeout()
    url = f"{base}{path}"
    if query:
        url += f"?{query}"

    async with session.get(url, headers=headers, timeout=timeout) as r:
        status = r.status
        raw = await r.read()

    try:
        data = json.loads(raw) if raw else None
    except Exception:
        data = None
    text = None if data is not None else raw.decode("utf-8", errors="replace")

    if 200 <= status < 300 and data is not None:
        await _remember(path, data)
    return status, data, text


async def _fetch(path: str, query: str):
    """(result, None) on success — result being (status, data, text) — or
    (None, error_response) when the remote couldn't be reached."""
    try:
        result = await _live_fetch(path, query)
    except asyncio.TimeoutError:
        logger.error(f"[ComfyUI Proxy] Timeout fetching {path} from remote")
        forwarder.circuit_record_failure()
        return None, web.json_response({"error": "ComfyUI Proxy: remote GPU timed out"}, status=504)
    except aiohttp.ClientError as e:
        logger.error(f"[ComfyUI Proxy] Failed to fetch {path}: {e}")
        forwarder.circuit_record_failure()
        return None, web.json_response({"error": f"ComfyUI Proxy: failed to reach remote GPU: {e}"}, status=502)
    except (RuntimeError, LookupError, UnicodeDecodeError) as e:
        # Decode errors aren't a sign the remote is unresponsive — don't
        # trip the breaker for these.
        logger.error(f"[ComfyUI Proxy] Decode error fetching {path}: {e}")
        return None, web.json_response({"error": f"ComfyUI Proxy: failed to decode remote response: {e}"}, status=502)
    forwarder.circuit_record_success()
    return result, None


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

async def handle_request(request: web.Request, remote_known_active: bool):
    """GET /api/jobs(/*) entry point for the middleware. Returns None
    (meaning 'fall through to the local handler') only when there's truly
    nothing to serve — nothing saved, and not worth waking the remote for."""
    path = request.rel_url.path
    if path == PATH:
        return await _handle_list(request, remote_known_active)
    return await _handle_detail(request, path, remote_known_active)


async def _handle_list(request: web.Request, remote_known_active: bool):
    path, qs = request.rel_url.path, request.rel_url.query_string
    q = _parse_list_query(request.rel_url.query)
    # Finished jobs can be answered from the saved history; anything that
    # also asks for pending / in-progress jobs needs the remote's live view.
    from_saved = q is not None and bool(q.statuses & TERMINAL_STATUSES)
    saved_only = from_saved and not (q.statuses & LIVE_STATUSES)

    if not remote_known_active or forwarder.circuit_is_open():
        # Either genuinely idle, or the remote just failed and we're
        # deliberately not attempting another live request yet.
        if from_saved:
            resp = await _list_from_saved(q)
            if resp is not None:
                return resp
        return None

    result, err = await _fetch(path, qs)
    if err is not None:
        if saved_only:
            resp = await _list_from_saved(q)
            if resp is not None:
                logger.warning(f"[ComfyUI Proxy] {path} failed; serving the saved job history instead.")
                return resp
        return err
    status, data, text = result
    if saved_only and 200 <= status < 300:
        # The remote's own history only goes back to its last restart; the
        # saved one (which this fetch just topped up) goes back further.
        resp = await _list_from_saved(q)
        if resp is not None:
            return resp
    return _passthrough(status, data, text)


async def _handle_detail(request: web.Request, path: str, remote_known_active: bool):
    """GET /api/jobs/{id}: a finished job's full detail (outputs, workflow).
    Saved when fetched, so an older job can still be opened after the remote
    restarted and forgot it."""
    job_id = path.rsplit("/", 1)[1] if _DETAIL_RE.match(path) else None

    if not remote_known_active or forwarder.circuit_is_open():
        return await _detail_from_saved(job_id) if job_id else None

    result, err = await _fetch(path, request.rel_url.query_string)
    if err is not None:
        resp = await _detail_from_saved(job_id) if job_id else None
        if resp is not None:
            logger.warning(f"[ComfyUI Proxy] {path} failed; serving the saved job detail instead.")
            return resp
        return err
    status, data, text = result
    if status == 404 and job_id:  # the remote no longer knows this job
        resp = await _detail_from_saved(job_id)
        if resp is not None:
            return resp
    return _passthrough(status, data, text)


async def refresh_known(reason: str = ""):
    """Fetch the remote's recent finished jobs (which saves them) so the
    Media Assets panel shows the job that just ended even once the connection
    that justified reaching the remote (the shadow relay) has already closed
    by the time it's opened."""
    if not forwarder.target_base():
        return
    if forwarder.circuit_is_open():
        # The remote just failed elsewhere and we're deliberately not
        # touching it again yet — don't let this closing-relay refresh
        # attempt (and potentially wait out the full timeout on) a live
        # fetch either; that would just delay the relay closing promptly.
        logger.info(f"[ComfyUI Proxy] Skipping job history refresh ({reason}): remote recently unresponsive.")
        return
    query = f"status=completed,failed,cancelled&limit={min(_max_entries(), 200)}"
    logger.info(f"[ComfyUI Proxy] Retrieving remote job history ({PATH}?{query}) to save...")
    result, err = await _fetch(PATH, query)
    if err is not None:
        logger.warning(f"[ComfyUI Proxy] Pre-fetch of {PATH} ({reason}) failed.")
        return
    saved = await asyncio.to_thread(jobs_db.count, _remote_key())
    logger.info(f"[ComfyUI Proxy] Saved job history updated ({saved} job(s) saved for this remote, {reason}).")
