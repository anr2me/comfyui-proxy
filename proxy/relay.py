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
     client, waits a configurable delay (default 5s) before unsubscribing
     logs and closing the shadow connection, so in-flight progress/log
     animations have time to finish.
"""

import asyncio
import json
import logging
from urllib.parse import urlsplit, urlunsplit

import aiohttp
from server import PromptServer

from . import config as cfgmod
from . import forwarder
from . import jobs_cache
from . import keepalive
from . import state

logger = logging.getLogger("ComfyUIProxy")

_relay_tasks = {}   # client_id -> asyncio.Task
_ready_events = {}  # client_id -> asyncio.Event, set once shadow ws is up (or failed)

_COMPLETION_MSG_TYPES = {"execution_success", "execution_error", "execution_interrupted"}


def _ws_target_url(base: str, path: str, query: str) -> str:
    parts = urlsplit(base)
    scheme = "wss" if parts.scheme == "https" else "ws"
    return urlunsplit((scheme, parts.netloc, path, query, ""))


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
    if task is None or task.done():
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


async def _extend_for_view_activity(client_id: str):
    """Keep this relay's already-open websocket connection alive for as
    long as /view or /viewvideo requests keep arriving for it, instead of
    pinging separately: an open, pending connection is what actually counts
    as "busy" to a scale-to-zero serverless platform, not a brief ping that
    completes and is forgotten the instant it returns. Returns once there's
    been no view activity for the configured idle timeout."""
    timeout = keepalive.idle_timeout()
    poll_interval = min(10.0, timeout)
    logger.info(
        f"[ComfyUI Proxy] Keeping progress stream open for client {client_id} while /view activity "
        f"continues (idle timeout {timeout:g}s)..."
    )
    while True:
        idle_for = keepalive.seconds_since_last_view()
        remaining = timeout - idle_for
        if remaining <= 0:
            logger.info(
                f"[ComfyUI Proxy] No /view activity for client {client_id} in the last {timeout:g}s; "
                "ending keep-alive extension."
            )
            return
        if not keepalive.enabled():
            logger.info(f"[ComfyUI Proxy] GPU keep-alive disabled; ending extension for client {client_id}.")
            return
        await asyncio.sleep(min(poll_interval, remaining))


async def _run_relay(client_id: str):
    base = forwarder.target_base()
    if not base:
        _signal_ready(client_id)
        return

    headers = _auth_headers()
    timeout = forwarder.get_timeout()
    target = _ws_target_url(base, "/ws", f"clientId={client_id}")
    session = forwarder.get_tracking_session()

    logger.info(f"[ComfyUI Proxy] Connecting remote GPU progress stream for client {client_id}...")
    try:
        async with session.ws_connect(target, headers=headers, timeout=timeout.total, heartbeat=30) as ws_remote:
            got_sid = False

            async for msg in ws_remote:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        parsed = json.loads(msg.data)
                    except Exception:
                        parsed = None

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
                        await _inject_local(client_id, msg)
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
                                await asyncio.sleep(delay)
                            # Re-check: ensure_relay_ready() treats this task as
                            # "already ready" for as long as it's alive, so a
                            # prompt re-submitted during the delay above reuses
                            # this same connection rather than opening a new
                            # one. If that happened, a new job is now tracked
                            # for this client — close the connection instead of
                            # abandoning that job's progress tracking with it.
                            if state.has_incomplete_job_for_client(client_id):
                                logger.info(
                                    f"[ComfyUI Proxy] New job queued for client {client_id} during the grace "
                                    "period; keeping the progress stream open instead of closing it."
                                )
                            else:
                                if keepalive.enabled():
                                    await _extend_for_view_activity(client_id)
                                # The extension above can run for a while —
                                # a prompt could have been resubmitted during
                                # it too, so check once more before closing.
                                if state.has_incomplete_job_for_client(client_id):
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

    except asyncio.TimeoutError:
        logger.warning(f"[ComfyUI Proxy] Timed out connecting remote progress stream for client {client_id}")
    except aiohttp.ClientError as e:
        logger.warning(f"[ComfyUI Proxy] Remote progress stream error for client {client_id}: {e}")
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] Unexpected error in remote progress stream for client {client_id}: {e}")
    finally:
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
        await _subscribe_logs(base, headers, timeout, client_id, False)
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
        logger.info(f"[ComfyUI Proxy] Remote GPU progress stream closed for client {client_id}.")
