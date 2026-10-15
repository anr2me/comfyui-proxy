"""
Drives live execution progress for jobs running on the remote GPU.

The browser's /ws connection is opened once when the page loads and never
re-created, so once a job starts there is no "future /ws request" to
redirect to the remote — literally proxying the route doesn't work. Instead,
this module opens a dedicated *shadow* websocket from the proxy process
itself to the remote, using the exact same clientId the browser used for
its local connection (and that ComfyUI's /prompt payload carries as
client_id), then re-emits every message the remote sends onto the browser's
already-open local socket by looking it up in PromptServer's own connection
table (keyed by that same id). From the browser's point of view these are
indistinguishable from locally generated progress/log messages.

Flow, per the intended sequence:
  1. Before /prompt is forwarded, ensure_relay_ready() opens (or reuses) the
     shadow connection and waits for the remote's first "status" message,
     which carries the sid it assigned — this is required before the actual
     job starts or the remote's own frontend-facing logic would consider
     the connection mismatched ("running in another tab").
  2. Once ready, subscribes to the remote's internal log stream for this
     clientId (PATCH /internal/logs/subscribe) so remote execution logs
     start arriving as messages on the same shadow socket and get relayed
     into the local console too.
  3. Every remote message (text or binary — progress, previews, logs) is
     immediately re-emitted onto the local socket for that clientId.
  4. On detecting completion for every prompt_id tracked against this
     client, keeps reading and relaying whatever the remote still sends for
     a configurable delay (default 5s) before unsubscribing logs and closing
     the shadow connection, so in-flight progress/log animations (and the
     final "Prompt executed in ..." log line) still reach the UI.

Before step 1 can even happen (the remote may still be cold-booting), each
waiting /prompt submission is shown in the UI's Job Queue as a placeholder job
(placeholders.py): announce_waiting() marks it "Waiting for a machine", and
announce_pending() sends the browser a locally generated "status" message,
which makes the frontend re-fetch its job list. The remote's real status
messages and jobs take over once the prompt is queued.
"""

import asyncio
import json
import logging
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit

import aiohttp
from server import PromptServer

from . import config as cfgmod
from . import files_cache
from . import forwarder
from . import jobs_cache
from . import keepalive
from . import placeholders
from . import state
from . import viewcache

logger = logging.getLogger("ComfyUIProxy")

_relay_tasks = {}   # client_id -> asyncio.Task
_ready_events = {}  # client_id -> asyncio.Event, set once shadow ws is up (or failed)
_pending_prompts = {}  # client_id -> /prompt submissions still waiting on the remote
_closing_tasks = set()  # relay tasks whose socket is already closed and that are only running final cleanup

_COMPLETION_MSG_TYPES = {"execution_success", "execution_error", "execution_interrupted"}

MAX_DOWNLOAD_KEEP_OPEN = 1800.0  # never hold the connection open longer than this just for file downloads

WS_CLOSE_TIMEOUT = 10.0  # how long closing the shadow websocket may wait for the remote's close frame


def _ws_target_url(base: str, path: str, query: str) -> str:
    parts = urlsplit(base)
    scheme = "wss" if parts.scheme == "https" else "ws"
    return urlunsplit((scheme, parts.netloc, path, query, ""))


def _ws_timeout():
    """Timeout object for ws_connect(). Only the close handshake is bounded
    here: ws_connect()'s `timeout` is NOT a connect/total timeout, and the old
    value (the whole request timeout, 300s by default) let closing a socket
    to an already-gone remote block for that long before cleanup could run.
    aiohttp >= 3.11 takes a ClientWSTimeout; older versions take a float that
    means the same thing (ws_close)."""
    cls = getattr(aiohttp, "ClientWSTimeout", None)
    return cls(ws_close=WS_CLOSE_TIMEOUT) if cls is not None else WS_CLOSE_TIMEOUT


def _auth_headers() -> dict:
    headers = {}
    auth_key = cfgmod.get("auth_key")
    if auth_key:
        headers["Authorization"] = f"Bearer {auth_key}"
    return headers


async def _inject_local(client_id: str, msg: aiohttp.WSMessage):
    """Push a message from the remote directly onto the browser's already-
    open local socket for this client_id, if it's currently connected."""
    try:
        ws = PromptServer.instance.sockets.get(client_id)
    except AttributeError:
        logger.error(
            "[ComfyUI Proxy] PromptServer.instance.sockets is unavailable — "
            "this ComfyUI version's websocket internals may differ from what this proxy expects."
        )
        return
    if ws is None:
        return  # browser not (yet/currently) connected locally; drop silently
    try:
        if msg.type == aiohttp.WSMsgType.TEXT:
            await ws.send_str(msg.data)
        elif msg.type == aiohttp.WSMsgType.BINARY:
            await ws.send_bytes(msg.data)
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] Failed relaying remote ws message to local client {client_id}: {e}")


def has_active_relay() -> bool:
    """True if any client currently has a live shadow connection to the
    remote — i.e. we already know for a fact the remote is awake, so there's
    no need to separately ping it to check readiness or log about it."""
    return any(not t.done() for t in _relay_tasks.values())


# --- "Run" feedback while the remote is still waking up -------------------
# A cold start can take a minute or more, during which the UI would show
# nothing at all after the user presses Run. These track how many /prompt
# submissions are waiting on the remote per client, and let us tell the
# browser about them through a locally generated "status" message — the same
# message type ComfyUI uses to report its queue size — until the remote's own
# (real) status messages take over once the job is actually queued.

def pending_count(client_id) -> int:
    return _pending_prompts.get(client_id, 0) if client_id else 0


def pending_add(client_id):
    if client_id:
        _pending_prompts[client_id] = _pending_prompts.get(client_id, 0) + 1


def pending_done(client_id):
    if not client_id:
        return
    left = _pending_prompts.get(client_id, 0) - 1
    if left > 0:
        _pending_prompts[client_id] = left
    else:
        _pending_prompts.pop(client_id, None)


def has_relay_for(client_id) -> bool:
    task = _relay_tasks.get(client_id)
    return task is not None and not task.done()


def _local_clients() -> list:
    try:
        return list(PromptServer.instance.sockets)
    except AttributeError:
        return []


async def _send_to_browsers(payload: dict, client_id=None):
    """Push a locally generated websocket message to one browser (client_id)
    or, without one, to every browser currently connected."""
    msg = SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(payload))
    for cid in ([client_id] if client_id else _local_clients()):
        await _inject_local(cid, msg)


async def announce_pending(client_id=None):
    """Tell the browser(s) the queue size we expect: jobs waiting on the
    remote (placeholders.py) plus jobs already tracked as running there. It is
    the "status" message ComfyUI itself uses, and the frontend answers it by
    re-fetching its job list — so this is also what makes placeholder jobs
    appear in the Job Queue, and disappear again once they're removed."""
    queue_remaining = placeholders.count() + len(state.incomplete_ids())
    await _send_to_browsers(
        {"type": "status", "data": {"status": {"exec_info": {"queue_remaining": queue_remaining}}}},
        client_id,
    )


WAITING_TEXT = "Waiting for a machine — the remote GPU is starting up"


async def announce_waiting(client_id, job_id):
    """Mark a placeholder job as waiting for its machine. The frontend reads a
    "notification" message containing "Waiting for a machine" for a job id as
    that job being initialized (the state it shows on ComfyUI Cloud), and
    clears the mark by itself once the job is no longer in its job list."""
    await _send_to_browsers({"type": "notification", "data": {"value": WAITING_TEXT, "id": job_id}}, client_id)


def _keep_pending_count(client_id, parsed, msg):
    """The remote's very first "status" message (sent as soon as the shadow
    socket connects, before the prompt has been submitted) reports an empty
    queue. Relayed as-is it would flip the badge shown by announce_pending()
    back to 0 until the prompt is really queued, so raise it to the number
    of submissions still waiting. Any other message is returned untouched."""
    pending = pending_count(client_id)
    if pending <= 0 or not isinstance(parsed, dict) or parsed.get("type") != "status":
        return msg
    try:
        exec_info = parsed["data"].setdefault("status", {}).setdefault("exec_info", {})
        exec_info["queue_remaining"] = max(pending, int(exec_info.get("queue_remaining") or 0))
    except Exception:
        return msg
    return SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(parsed))


def cancel_all_relays() -> int:
    """Forcibly stop tracking every active shadow relay immediately, and ask
    the underlying tasks to cancel in the background. Used by the manual
    'Clear stuck state' reset, so a relay wedged talking to a dead/
    unreachable remote doesn't need a full ComfyUI restart to clear.
    Returns immediately rather than waiting for the cancelled tasks' own
    cleanup (unsubscribing logs, closing the socket), since that cleanup
    could itself be slow — or hang — against an unreachable remote, which
    would defeat the point of a manual reset."""
    tasks = list(_relay_tasks.values())
    for t in tasks:
        if not t.done():
            t.cancel()
    _relay_tasks.clear()
    _ready_events.clear()
    return len(tasks)


async def _subscribe_logs(base: str, headers: dict, timeout, client_id: str, enabled: bool):
    session = forwarder.get_tracking_session()
    try:
        async with session.request(
            "PATCH", f"{base}/internal/logs/subscribe",
            json={"enabled": enabled, "clientId": client_id},
            headers=headers, timeout=timeout,
        ) as r:
            if r.status >= 400:
                logger.warning(
                    f"[ComfyUI Proxy] Remote log subscription "
                    f"({'enable' if enabled else 'disable'}) returned HTTP {r.status} for client {client_id}"
                )
    except Exception as e:
        logger.warning(
            f"[ComfyUI Proxy] Failed to {'subscribe' if enabled else 'unsubscribe'} "
            f"remote logs for client {client_id}: {e}"
        )


async def ensure_relay_ready(client_id: str, wait_timeout: float):
    """Ensure a shadow relay to the remote is running for this client_id and
    wait (up to wait_timeout) for it to report ready — connected and the
    remote's sid confirmed — before returning, so the local socket is
    guaranteed to start receiving remote progress before the job actually
    starts executing on the remote."""
    if not client_id:
        return

    task = _relay_tasks.get(client_id)
    # A task that is already in its final cleanup (socket closed, but still
    # awaiting a refresh / log unsubscribe) is not "ready": reusing it would
    # leave the new job without a live progress stream.
    if task is None or task.done() or task in _closing_tasks:
        _ready_events[client_id] = asyncio.Event()
        task = asyncio.ensure_future(_run_relay(client_id))
        _relay_tasks[client_id] = task

    ev = _ready_events.get(client_id)
    if ev is not None:
        try:
            await asyncio.wait_for(ev.wait(), timeout=wait_timeout)
        except asyncio.TimeoutError:
            logger.warning(
                f"[ComfyUI Proxy] Timed out waiting for the remote progress stream to be ready "
                f"for client {client_id}; submitting the prompt anyway."
            )


def _signal_ready(client_id: str):
    ev = _ready_events.get(client_id)
    if ev is not None and not ev.is_set():
        ev.set()


def _owns_slot(client_id: str, task) -> bool:
    """True if `task` is still the relay registered for client_id, or if
    nobody is (e.g. cancel_all_relays() already cleared the registry). False
    once a newer relay has taken the slot: a finishing old task must then
    leave that entry, its ready event, its log subscription and its job
    tracking alone — they belong to the newer relay's live connection."""
    current = _relay_tasks.get(client_id)
    return current is None or current is task


def _status_queue_remaining(parsed, default: int) -> int:
    """queue_remaining from a remote "status" message, or `default` if
    `parsed` isn't one (or doesn't carry a usable number)."""
    if not isinstance(parsed, dict) or parsed.get("type") != "status":
        return default
    try:
        exec_info = ((parsed.get("data") or {}).get("status") or {}).get("exec_info") or {}
        value = exec_info.get("queue_remaining")
        return int(value) if value is not None else default
    except (TypeError, ValueError, AttributeError):
        return default


async def _relay_for(ws_remote, client_id: str, seconds: float) -> bool:
    """Keep reading the remote shadow socket and relaying what it sends onto
    the local one for up to `seconds`, instead of just sleeping. A plain
    sleep leaves everything the remote sends in that window sitting unread
    in the socket's buffer — and when the connection is then closed those
    messages are lost, including the final "Prompt executed in ..." log line
    the remote emits right after its completion message. Returns True if the
    remote closed the socket meanwhile."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return False
        try:
            msg = await ws_remote.receive(timeout=remaining)
        except asyncio.TimeoutError:
            return False
        if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
            await _inject_local(client_id, msg)
        elif msg.type in (
            aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING,
            aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR,
        ):
            return True


async def _extend_for_view_activity(client_id: str, ws_remote):
    """Keep this relay's already-open websocket connection alive after the
    last job, instead of pinging separately: an open, pending connection is
    what actually counts as "busy" to a scale-to-zero serverless platform,
    not a brief ping that completes and is forgotten the instant it returns.
    Held open for as long as either
      - a viewed file is still being downloaded from the GPU (Auto-download,
        see viewcache.py): /view keeps being forwarded, and the GPU isn't
        shut down underneath the download, until the file is saved locally
        (capped at MAX_DOWNLOAD_KEEP_OPEN so a stuck download can't pin the
        GPU forever), or
      - the opt-in "Keep GPU warm" option is on and /view or /viewvideo
        requests keep arriving (until the configured idle timeout).
    Returns once neither applies, or when the remote closes the socket."""
    loop = asyncio.get_running_loop()
    started = loop.time()
    timeout = keepalive.idle_timeout()
    capped = False
    reason = None
    while True:
        downloading = not capped and viewcache.has_active_downloads()
        if downloading and loop.time() - started > MAX_DOWNLOAD_KEEP_OPEN:
            capped = True
            downloading = False
            logger.warning(
                f"[ComfyUI Proxy] File downloads are still running after {MAX_DOWNLOAD_KEEP_OPEN:g}s; "
                f"not holding the progress stream open for client {client_id} any longer."
            )
        idle_left = 0.0
        if keepalive.enabled():
            idle_left = timeout - keepalive.seconds_since_last_view()
        if not downloading and idle_left <= 0:
            logger.info(
                f"[ComfyUI Proxy] No file downloads in progress and no recent /view activity "
                f"for client {client_id}; ending extension."
            )
            return
        why = "a file download is in progress" if downloading else f"/view activity continues (idle timeout {timeout:g}s)"
        if why != reason:
            reason = why
            logger.info(f"[ComfyUI Proxy] Keeping progress stream open for client {client_id}: {why}...")
        step = 2.0 if downloading else min(10.0, idle_left)
        if await _relay_for(ws_remote, client_id, step):
            logger.info(f"[ComfyUI Proxy] Remote closed the progress stream for client {client_id}; ending extension.")
            return


async def _run_relay(client_id: str):
    base = forwarder.target_base()
    if not base:
        _signal_ready(client_id)
        return

    headers = _auth_headers()
    timeout = forwarder.get_timeout()
    target = _ws_target_url(base, "/ws", f"clientId={client_id}")
    session = forwarder.get_tracking_session()

    me = asyncio.current_task()
    rid = f"{id(me) & 0xFFFF:04x}"  # short per-task id, to tell overlapping relays apart in the logs
    last_queue_remaining = 0   # most recent queue_remaining the remote reported to us
    finished_clean = False     # saw "no jobs left" for this client (and nothing was queued since)
    cancelled = False          # ended by cancel_all_relays() (manual reset), not by the remote

    logger.info(f"[ComfyUI Proxy] Connecting remote GPU progress stream for client {client_id}... [relay {rid}]")
    try:
        async with session.ws_connect(target, headers=headers, timeout=_ws_timeout(), heartbeat=30) as ws_remote:
            got_sid = False

            async for msg in ws_remote:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        parsed = json.loads(msg.data)
                    except Exception:
                        parsed = None

                    last_queue_remaining = _status_queue_remaining(parsed, last_queue_remaining)
                    files_cache.note_message(parsed)  # remember that a job saved a file to the output folder

                    if not got_sid:
                        # This very first message arrives right after connecting,
                        # before /prompt has even been sent — its queue_remaining
                        # is trivially 0 and must NOT be treated as "job done".
                        got_sid = True
                        sid = None
                        if isinstance(parsed, dict) and parsed.get("type") == "status":
                            sid = (parsed.get("data") or {}).get("sid")
                        if sid and sid != client_id:
                            logger.warning(
                                f"[ComfyUI Proxy] Remote assigned sid {sid} but expected {client_id} — "
                                "the UI may show 'running in another tab' instead of live progress."
                            )
                        else:
                            logger.info(f"[ComfyUI Proxy] Remote GPU progress stream ready for client {client_id}.")
                        asyncio.ensure_future(_subscribe_logs(base, headers, timeout, client_id, True))
                        _signal_ready(client_id)
                        await _inject_local(client_id, _keep_pending_count(client_id, parsed, msg))
                        continue

                    await _inject_local(client_id, msg)

                    finished = False
                    finished_prompt_id = None
                    if isinstance(parsed, dict):
                        mtype = parsed.get("type")
                        data = parsed.get("data") or {}
                        if mtype == "executing" and data.get("node") is None:
                            finished_prompt_id = data.get("prompt_id")
                            finished = True
                        elif mtype in _COMPLETION_MSG_TYPES:
                            finished_prompt_id = data.get("prompt_id")
                            finished = True
                        elif mtype == "status":
                            queue_remaining = ((data.get("status") or {}).get("exec_info") or {}).get("queue_remaining")
                            if queue_remaining == 0:
                                # Authoritative fallback: nothing left running or
                                # queued on the remote at all, regardless of
                                # whether a clean execution_success/error/
                                # executing:null message was ever seen for this
                                # specific job (e.g. it errored out server-side
                                # mid-execution without one).
                                finished = True

                    if finished:
                        if finished_prompt_id:
                            state.mark_job_done(finished_prompt_id)
                        else:
                            state.clear_all()  # queue_remaining==0 is a global signal, not scoped to one prompt_id
                        if not state.has_incomplete_job_for_client(client_id):
                            finished_clean = True
                            # No separate refresh here: while a job is known
                            # incomplete, _remote_known_active() is already
                            # True, so the frontend's own ordinary /api/jobs
                            # polling keeps the cache warm via the normal
                            # on-demand path — no need to proactively fetch
                            # again right at this exact moment too. The one
                            # refresh that actually matters (after the delay
                            # below, right before this connection closes and
                            # that proof-of-liveness goes away) happens once,
                            # unconditionally, in the finally block.
                            delay = float(cfgmod.get("post_completion_delay", 5) or 0)
                            if delay > 0:
                                logger.info(
                                    f"[ComfyUI Proxy] No jobs left on remote GPU for client {client_id}; "
                                    f"keeping progress stream open {delay:g}s more for final UI updates."
                                )
                                if await _relay_for(ws_remote, client_id, delay):
                                    break  # the remote closed the socket on its own
                            # Re-check: ensure_relay_ready() treats this task as
                            # "already ready" for as long as it's alive, so a
                            # prompt re-submitted during the delay above reuses
                            # this same connection rather than opening a new
                            # one. If that happened, a new job is now tracked
                            # for this client — close the connection instead of
                            # abandoning that job's progress tracking with it.
                            if state.has_incomplete_job_for_client(client_id):
                                finished_clean = False
                                logger.info(
                                    f"[ComfyUI Proxy] New job queued for client {client_id} during the grace "
                                    "period; keeping the progress stream open instead of closing it."
                                )
                            else:
                                if keepalive.enabled() or viewcache.has_active_downloads():
                                    await _extend_for_view_activity(client_id, ws_remote)
                                # The extension above can run for a while —
                                # a prompt could have been resubmitted during
                                # it too, so check once more before closing.
                                if state.has_incomplete_job_for_client(client_id):
                                    finished_clean = False
                                    logger.info(
                                        f"[ComfyUI Proxy] New job queued for client {client_id} during the "
                                        "keep-alive extension; keeping the progress stream open instead of closing it."
                                    )
                                else:
                                    break

                elif msg.type == aiohttp.WSMsgType.BINARY:
                    await _inject_local(client_id, msg)
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                    break

    except asyncio.CancelledError:
        cancelled = True
        raise
    except asyncio.TimeoutError:
        logger.warning(f"[ComfyUI Proxy] Timed out connecting remote progress stream for client {client_id}")
    except aiohttp.ClientError as e:
        logger.warning(f"[ComfyUI Proxy] Remote progress stream error for client {client_id}: {e}")
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] Unexpected error in remote progress stream for client {client_id}: {e}")
    finally:
        # From here on this task's own socket is closed; it only runs cleanup.
        # Mark it so ensure_relay_ready() won't reuse it as a live relay while
        # the awaits below are in flight, and snapshot the job state now,
        # before anything else can change it.
        _closing_tasks.add(me)
        tracked_jobs = state.incomplete_count_for_client(client_id)
        try:
            # The stream ended while jobs were still queued/running: we never
            # saw the "No jobs left on remote GPU" point, so the remote most
            # likely went away mid-job (interrupted / preempted / scaled to
            # zero). A manual reset (cancelled) is not an interruption.
            if not cancelled and (tracked_jobs > 0 or (last_queue_remaining > 0 and not finished_clean)):
                logger.warning(
                    f"[ComfyUI Proxy] Remote GPU got interrupted while having "
                    f"{max(tracked_jobs, last_queue_remaining)} queued job(s) for client {client_id} "
                    f"(last reported queue_remaining={last_queue_remaining}, tracked jobs={tracked_jobs}); "
                    "the remote may have been preempted or scaled down mid-job."
                )
            if _owns_slot(client_id, me):
                _signal_ready(client_id)  # never leave a waiting /prompt hanging on failure
            # Guaranteed final refresh attempt, regardless of how the loop above
            # exited (clean "finished" detection, the remote closing the socket
            # on its own before ever sending a tidy completion message, a
            # timeout, ...). The "finished" branch already does this on the
            # clean path; this covers every other exit path too, so a dropped
            # connection doesn't silently skip caching the last job's result.
            # Best-effort: the remote may genuinely be gone by now, in which
            # case this just fails quietly like any other refresh attempt would.
            await jobs_cache.refresh_known(reason=f"client {client_id} relay closing")
            await files_cache.refresh_known(reason=f"client {client_id} relay closing")
            if not cancelled and finished_clean and not state.has_incomplete_job():
                # The last job of the batch is done: one output scan, not one per job.
                files_cache.scan_output_soon(reason=f"after the last job, client {client_id}")
            # A newer relay for the same client may have subscribed to logs
            # already; don't switch that subscription off underneath it.
            if _owns_slot(client_id, me):
                await _subscribe_logs(base, headers, timeout, client_id, False)
        except Exception as e:
            logger.warning(f"[ComfyUI Proxy] Error while closing remote progress stream for client {client_id}: {e}")
        finally:
            # No awaits from here on, so ownership can't change mid-cleanup.
            # Only release the slot (and the job tracking) if a newer relay
            # for this client hasn't taken it: otherwise this would drop that
            # relay's registration while its websocket stays open, orphaning it.
            if _owns_slot(client_id, me):
                _relay_tasks.pop(client_id, None)
                _ready_events.pop(client_id, None)
                # Whatever jobs were tracked against this client stop being tracked
                # the moment we can no longer observe them, regardless of why the
                # relay ended (clean completion, connect timeout, ws error, ...).
                # Without this, a relay that fails before ever seeing a completion
                # message leaves its prompt_id "incomplete" forever, with nothing
                # left alive to ever clear it — permanently forcing every future
                # request into "remote known active" and silently re-waking the
                # remote for routes that should only proxy while something's
                # actually running.
                state.clear_client(client_id)
            _closing_tasks.discard(me)
            logger.info(f"[ComfyUI Proxy] Remote GPU progress stream closed for client {client_id}. [relay {rid}]")
