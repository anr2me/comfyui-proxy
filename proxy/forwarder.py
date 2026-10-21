"""
Low-level forwarding primitives: stream an HTTP request/response pair to the
remote GPU verbatim (preserving Content-Encoding, Range/partial content,
chunked bodies), and bridge a browser WebSocket to the remote's WebSocket.

Two aiohttp ClientSessions are kept alive for the process lifetime:
  - the "raw" session (auto_decompress=False) used for streamed HTTP
    forwarding, so whatever encoding (gzip/br/zstd) the remote used is
    relayed to the browser untouched instead of being re-encoded.
  - the "tracking" session (auto_decompress=True) used for small JSON calls
    we need to actually read (wake pings, /prompt, /queue, /object_info),
    where introspecting the body matters more than preserving encoding.
"""

import asyncio
import importlib
import logging
import time
from urllib.parse import urlsplit

import aiohttp
from aiohttp import web

from . import config as cfgmod
from . import logsafe

logger = logging.getLogger("ComfyUIProxy")

_raw_session = None
_tracking_session = None

# Headers that must never be blindly copied between hops.
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
}

# Pause before retrying a transient connection-pool race (see forward_http).
# Brief on purpose — just enough for the connector to discard the broken
# connection and the gateway to settle, not a noticeable delay for the
# person waiting on an image/video to load.
RETRY_DELAY = 0.3

# Errors meaning "the pooled connection was already closing when we wrote to
# it". On aiohttp >= 3.10 "Cannot write to closing transport" is raised as
# ClientConnectionResetError, which is NOT a ClientOSError subclass, so it
# must be listed explicitly or it skips the retry and lands in the generic
# ClientError handler.
_TRANSIENT_CONN_ERRORS = tuple(
    c for c in (aiohttp.ClientOSError, getattr(aiohttp, "ClientConnectionResetError", None))
    if c is not None
)


# --- Circuit breaker for automatically-polled routes (/queue, /api/jobs) ---
# These get re-requested by the frontend on its own, frequently, without
# any awareness that a previous attempt just failed. If the remote is
# genuinely unresponsive (not just slow), every single poll would otherwise
# independently wait out the full configured timeout before giving up —
# and since there's then always a new one in flight, that looks like
# continuous legitimate activity to the serverless platform, preventing it
# from ever scaling down on its own even though nothing is actually
# working. After a failure, further live attempts on the affected target
# are skipped (falling straight back to cache) for a cooldown window,
# rather than attempted and left to time out one by one.
_circuit_state = {False: {"open_until": 0.0, "failures": 0}, True: {"open_until": 0.0, "failures": 0}}


def _circuit_breaker_cooldown() -> float:
    # Configurable: different serverless providers scale down after very
    # different idle windows, so a one-size-fits-all cooldown may not give
    # a given provider's own idle-timeout enough of a quiet period to
    # actually kick in.
    return cfgmod.number("circuit_breaker_cooldown", 1.0)


def _circuit_max_failures() -> int:
    return int(cfgmod.number("circuit_breaker_max_failures", 0, zero_is_valid=True))  # 0 = never latch


def circuit_is_open(use_cpu: bool = False) -> bool:
    st = _circuit_state[use_cpu]
    max_f = _circuit_max_failures()
    if max_f and st["failures"] >= max_f:
        return True  # latched: stays open until a success or manual reset
    return time.time() < st["open_until"]


def circuit_record_failure(use_cpu: bool = False):
    st = _circuit_state[use_cpu]
    st["failures"] += 1
    cooldown = _circuit_breaker_cooldown()
    st["open_until"] = time.time() + cooldown
    label = "CPU" if use_cpu else "GPU"
    max_f = _circuit_max_failures()
    if max_f and st["failures"] >= max_f:
        logger.warning(
            f"[ComfyUI Proxy] Remote {label} unresponsive (failure #{st['failures']}); giving up "
            "automatic polling until the next successful /prompt or a manual state reset."
        )
        return
    logger.warning(
        f"[ComfyUI Proxy] Remote {label} seems unresponsive (failure #{st['failures']}); pausing "
        f"automatic polling of it for {cooldown:g}s rather than keep re-attempting."
    )


def circuit_record_success(use_cpu: bool = False):
    st = _circuit_state[use_cpu]
    if st["failures"] > 0:
        logger.info(f"[ComfyUI Proxy] Remote {'CPU' if use_cpu else 'GPU'} responded again; resuming automatic polling.")
    st["failures"] = 0
    st["open_until"] = 0.0


def circuit_reset():
    """Clear the breaker for both targets (manual reset / endpoint change)."""
    for st in _circuit_state.values():
        st["failures"] = 0
        st["open_until"] = 0.0
    _reset_file_backoff()


# ---------------------------------------------------------------------------
# Files the remote could not provide
# ---------------------------------------------------------------------------
# A page of thumbnails is dozens of requests, and scrolling asks for the same ones
# again and again. When the remote can't provide them (no credits left to start a
# container, a container that won't come up, a file that is gone), sending every one
# of those requests anew only wakes or waits on it for nothing. So after a failure the
# same request is answered from that failure for a while: the cooldown above, doubling
# each time it fails again, up to FILE_BACKOFF_MAX. And when the remote itself is not
# answering (UNAVAILABLE_STATUSES; a connection error or a timeout arrives as one of
# them) no file is asked for in that time. Unlike the breaker above, this recovers by
# itself: the first request after the delay goes through, and a success clears it.
FILE_BACKOFF_MAX = 600.0
FILE_BACKOFF_KEEP = 500             # failed requests remembered at most
FILE_FAILURE_LOG_INTERVAL = 30.0
UNAVAILABLE_STATUSES = (429, 502, 503, 504)  # the remote isn't answering (a missing file is a 404, not this)
_NOT_A_VERDICT = (416, 499)         # a Range the file can't satisfy; the browser gave up: nothing learned

_file_failures = {}        # (use_cpu, route, query string) -> {"until", "count", "status", "held"}
_target_down = {}          # use_cpu -> the same, for the whole target
_file_failure_logged = {}  # (use_cpu, status) -> [monotonic time of its last line, failures since]


def _reset_file_backoff() -> None:
    _file_failures.clear()
    _target_down.clear()
    _file_failure_logged.clear()


def _file_backoff_delay(count: int) -> float:
    base = _circuit_breaker_cooldown()
    return min(base * 2 ** (count - 1), max(base, FILE_BACKOFF_MAX))


def file_backoff_response(use_cpu: bool, route: str, query_string: str):
    """The answer to give instead of asking the remote again, if this very request,
    or the whole target, failed recently. None: let it go ahead."""
    now = time.monotonic()
    for entry in (_target_down.get(use_cpu), _file_failures.get((use_cpu, route, query_string))):
        if entry is not None and entry["until"] > now:
            entry["held"] += 1
            remaining = entry["until"] - now
            return web.json_response(
                {"error": (
                    f"ComfyUI Proxy: the remote {'CPU' if use_cpu else 'GPU'} could not provide this a moment ago; "
                    f"not asking again for {remaining:.0f}s"
                )},
                status=entry["status"],
                headers={"Retry-After": str(int(remaining) + 1)},
            )
    return None


def _log_file_failure(use_cpu: bool, status: int, text: str) -> None:
    """One warning per target and status per FILE_FAILURE_LOG_INTERVAL; the others are counted."""
    now = time.monotonic()
    entry = _file_failure_logged.get((use_cpu, status))
    if entry is not None and now - entry[0] < FILE_FAILURE_LOG_INTERVAL:
        entry[1] += 1
        return
    skipped = entry[1] if entry else 0
    _file_failure_logged[(use_cpu, status)] = [now, 0]
    logger.warning(
        f"[ComfyUI Proxy] {text}" + (f" ({skipped} more not logged since the last line)" if skipped else "") + "."
    )


def note_file_result(use_cpu: bool, route: str, query_string: str, status: int, what: str) -> None:
    """Record how asking the remote for a file went. `what` names the file in the log."""
    if status in _NOT_A_VERDICT:
        return
    label = "CPU" if use_cpu else "GPU"
    key = (use_cpu, route, query_string)
    now = time.monotonic()
    if status < 400:
        _file_failures.pop(key, None)
        if _target_down.pop(use_cpu, None) is not None:
            logger.info(f"[ComfyUI Proxy] Remote {label} provided a file again; asking it for files as usual.")
        return
    if status in UNAVAILABLE_STATUSES:
        down = _target_down.get(use_cpu)
        if down is None or down["until"] <= now:  # a failure that was already being waited out is not a new one
            count = (down["count"] if down else 0) + 1
            delay = _file_backoff_delay(count)
            _target_down[use_cpu] = {"until": now + delay, "count": count, "status": status, "held": 0}
            _log_file_failure(
                use_cpu, status,
                f"Remote {label} is not answering file requests (HTTP {status}, for {what}); "
                f"not asking it for files for {delay:g}s",
            )
        return
    entry = _file_failures.get(key)
    if entry is None or entry["until"] <= now:
        count = (entry["count"] if entry else 0) + 1
        held = entry["held"] if entry else 0
        delay = _file_backoff_delay(count)
        _file_failures[key] = {"until": now + delay, "count": count, "status": status, "held": 0}
        _log_file_failure(
            use_cpu, status,
            f"The remote {label} could not provide {what} (HTTP {status}); not asking again for it for {delay:g}s"
            + (f" ({held} repeated request(s) were answered without asking)" if held else ""),
        )
        if len(_file_failures) > FILE_BACKOFF_KEEP:
            for k in [k for k, e in _file_failures.items() if e["until"] <= now]:
                del _file_failures[k]
            while len(_file_failures) > FILE_BACKOFF_KEEP:
                del _file_failures[min(_file_failures, key=lambda k: _file_failures[k]["until"])]


_safe_accept_encoding = None


def get_safe_accept_encoding() -> str:
    """Accept-Encoding value to send on calls where we actually parse the
    response body (/prompt, /queue, the model-list pull). Includes br/zstd
    only if this Python environment can actually decode them, mirroring
    aiohttp's own module checks, so we never ask the remote for something we
    can't read back — but take advantage of those codecs when available
    instead of always forcing the lowest common denominator. Computed once
    and cached; installing a codec package after ComfyUI has started is
    picked up on the next call as long as it lands on the same interpreter's
    site-packages (no restart needed in the common case)."""
    global _safe_accept_encoding
    if _safe_accept_encoding is not None:
        return _safe_accept_encoding

    encodings = ["gzip", "deflate"]

    has_brotli = False
    for mod in ("brotli", "brotlicffi"):
        try:
            importlib.import_module(mod)
            has_brotli = True
            break
        except ImportError:
            continue
    if has_brotli:
        encodings.append("br")

    has_zstd = False
    for mod in ("compression.zstd", "backports.zstd"):
        try:
            importlib.import_module(mod)
            has_zstd = True
            break
        except ImportError:
            continue
    if has_zstd:
        encodings.append("zstd")

    encodings.append("identity")
    _safe_accept_encoding = ", ".join(encodings)
    logger.info(f"[ComfyUI Proxy] Encodings this environment can decode: {_safe_accept_encoding}")
    return _safe_accept_encoding


def get_session():
    global _raw_session
    if _raw_session is None or _raw_session.closed:
        _raw_session = aiohttp.ClientSession(auto_decompress=False)
    return _raw_session


def get_tracking_session():
    global _tracking_session
    if _tracking_session is None or _tracking_session.closed:
        _tracking_session = aiohttp.ClientSession(auto_decompress=True)
    return _tracking_session


async def close_sessions():
    for s in (_raw_session, _tracking_session):
        if s and not s.closed:
            await s.close()


def target_base(use_cpu: bool = False) -> str:
    """The GPU target by default; the optional CPU-only target (sharing the
    same persistent volume, for file-only routes that don't need a GPU)
    when use_cpu is True. Returns '' if that target isn't configured."""
    key = "remote_cpu_url" if use_cpu else "remote_url"
    return (cfgmod.get(key) or "").rstrip("/")


def _auth_key_for(use_cpu: bool = False):
    return cfgmod.get("remote_cpu_auth_key" if use_cpu else "auth_key")


def copy_request_headers(request: web.Request, limit_encoding: bool = False, use_cpu: bool = False) -> dict:
    headers = {}
    for k, v in request.headers.items():
        if k.lower() in HOP_BY_HOP:
            continue
        headers[k] = v
    if limit_encoding:
        # Calls that need to actually parse the response body (/prompt,
        # /queue, the model-list pull) shouldn't ask the remote for an
        # encoding we might not be able to decode. Uses br/zstd when the
        # optional codec packages are installed, otherwise sticks to
        # gzip/deflate. Streamed pass-through routes are unaffected and
        # keep negotiating the browser's real Accept-Encoding, since those
        # bytes are relayed untouched regardless of codec.
        headers["Accept-Encoding"] = get_safe_accept_encoding()
    auth_key = _auth_key_for(use_cpu)
    if auth_key:
        headers["Authorization"] = f"Bearer {auth_key}"
    return headers


def get_timeout() -> aiohttp.ClientTimeout:
    t = cfgmod.number("timeout", 5.0)
    # sock_connect capped so a totally dead endpoint fails fast even with a
    # long total timeout meant to cover cold-boot time.
    return aiohttp.ClientTimeout(total=t, sock_connect=min(t, 30))


_wake_lock = asyncio.Lock()
_in_flight_wake = {False: None, True: None}  # keyed by use_cpu -> shared asyncio.Task


async def wake_remote_if_needed(reason: str = "", use_cpu: bool = False):
    """Ping the target once to trigger a cold boot, logging progress locally
    via the standard logging module. Concurrent callers (e.g. two
    near-simultaneous requests both hitting a conditional route right as a
    panel opens) share a single underlying ping instead of each firing their
    own — otherwise the same near-instant wake gets logged and pinged twice
    for what is really one event. GPU and CPU targets are coalesced
    independently, since they're separate containers."""
    base = target_base(use_cpu)
    if not base:
        return

    async with _wake_lock:
        existing = _in_flight_wake[use_cpu]
        if existing is None or existing.done():
            existing = asyncio.ensure_future(_do_wake_ping(base, reason, use_cpu))
            _in_flight_wake[use_cpu] = existing
        task = existing

    # Hard ceiling, defense in depth: _do_wake_ping() already bounds itself
    # with get_timeout(), but since every caller of this shared slot would
    # otherwise block on it together, a single edge case where that internal
    # bound doesn't trigger as expected would hang every future request that
    # needs this target — including, critically, the next /prompt
    # submission. asyncio.shield keeps the underlying ping running (and its
    # own timeout/cleanup intact) even if THIS caller gives up waiting.
    ceiling = get_timeout().total + 30
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=ceiling)
    except asyncio.TimeoutError:
        logger.warning(f"[ComfyUI Proxy] Wake check for remote {'CPU' if use_cpu else 'GPU'} exceeded {ceiling:g}s; continuing without waiting further.")


async def _do_wake_ping(base: str, reason: str, use_cpu: bool = False):
    label = "CPU" if use_cpu else "GPU"
    logger.info(f"[ComfyUI Proxy] Initializing remote {label}... ({reason})")
    try:
        session = get_tracking_session()
        timeout = get_timeout()
        headers = {"Accept-Encoding": get_safe_accept_encoding()}
        auth_key = _auth_key_for(use_cpu)
        if auth_key:
            headers["Authorization"] = f"Bearer {auth_key}"
        async with session.get(f"{base}/system_stats", headers=headers, timeout=timeout) as resp:
            if resp.status < 500:
                logger.info(f"[ComfyUI Proxy] Remote {label} is ready.")
            else:
                logger.warning(f"[ComfyUI Proxy] Remote {label} returned HTTP {resp.status} while waking up.")
    except asyncio.TimeoutError:
        logger.warning(
            f"[ComfyUI Proxy] Timed out waking up remote {label} "
            "(cold boot may still be in progress; consider raising the timeout)."
        )
    except aiohttp.ClientError as e:
        logger.warning(f"[ComfyUI Proxy] Error waking up remote {label}: {e}")
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] Unexpected error waking up remote {label}: {e}")


async def forward_http(request: web.Request, use_cpu: bool = False, sink=None) -> web.StreamResponse:
    """Stream a single HTTP request/response through to the target (GPU by
    default, or the optional CPU-only target when use_cpu is True),
    preserving status, headers (incl. Content-Encoding / Content-Range), and
    body as-is.

    `sink` (optional, see viewcache.py) is shown the response as it streams
    past: begin(status, headers), write(chunk) for every chunk, then
    end(complete) — used to keep a local copy of viewed files."""
    label = "CPU" if use_cpu else "GPU"
    base = target_base(use_cpu)
    if not base:
        return web.json_response({"error": f"ComfyUI Proxy: remote {label} URL not configured"}, status=502)

    parts = urlsplit(base)
    target_url = f"{parts.scheme}://{parts.netloc}{request.rel_url.path}"
    if request.rel_url.query_string:
        target_url += f"?{request.rel_url.query_string}"

    headers = copy_request_headers(request, use_cpu=use_cpu)
    timeout = get_timeout()
    # body_exists, not can_read_body: the latter turns False as soon as anything
    # upstream (e.g. the middleware peeking at a history clear/delete or a queue
    # cancel) has already read the body, which made this forward an EMPTY body
    # and the remote answer 500. read() hands back the cached bytes in that case.
    body = await request.read() if request.body_exists else None

    req_desc = f"{request.method} {request.rel_url.path_qs}"  # for error logs
    session = get_session()
    max_attempts = 2  # one retry for a transient connection-pool race — see below
    for attempt in range(1, max_attempts + 1):
        headers_sent = False
        stream_resp = None
        try:
            async with session.request(
                request.method, target_url, headers=headers, data=body,
                timeout=timeout, allow_redirects=False,
            ) as remote_resp:
                # Copy headers verbatim (this is what preserves gzip/br/zstd
                # Content-Encoding and Content-Range for partial/streamed content).
                resp_headers = {k: v for k, v in remote_resp.headers.items() if k.lower() not in HOP_BY_HOP}

                if remote_resp.status >= 500:
                    logger.error(f"[ComfyUI Proxy] Remote {label} returned {remote_resp.status} for {request.method} {request.rel_url.path}")

                stream_resp = web.StreamResponse(status=remote_resp.status, headers=resp_headers)
                if sink is not None:
                    sink.begin(remote_resp.status, remote_resp.headers)
                stream_complete = False
                try:
                    await stream_resp.prepare(request)
                    headers_sent = True  # past this point a retry is no longer safe: the
                                          # browser has already received a response's headers
                    async for chunk in remote_resp.content.iter_any():
                        if sink is not None:
                            await sink.write(chunk)
                        await stream_resp.write(chunk)
                    await stream_resp.write_eof()
                    stream_complete = True
                    return stream_resp
                finally:
                    if sink is not None:
                        sink.end(stream_complete)

        except asyncio.TimeoutError:
            logger.error(f"[ComfyUI Proxy] Timeout forwarding {request.method} {request.rel_url.path} to remote {label}")
            return web.json_response(
                {"error": f"ComfyUI Proxy: remote {label} timed out (increase the timeout in the proxy panel if it's cold-booting)"},
                status=504,
            )
        except aiohttp.ClientConnectorError as e:
            # Connection-establishment failures (DNS, refused, SSL handshake,
            # ...) — not retried here; these are checked before ClientOSError
            # below since ClientConnectorError is itself a subclass of it,
            # and a brand new connection attempt failing outright is not the
            # same transient, worth-retrying situation handled there.
            logger.error(f"[ComfyUI Proxy] Connection error reaching remote {label} for {req_desc}: {e}")
            return web.json_response({"error": f"ComfyUI Proxy: cannot reach remote {label}: {logsafe.sanitize_logs(e)}"}, status=502)
        except _TRANSIENT_CONN_ERRORS as e:
            # Covers things like "Cannot write to closing transport" — a
            # transient aiohttp connection-pool race where the pool hands
            # out an existing, pooled connection the remote/gateway is
            # already closing (distinct from ClientConnectorError above,
            # which is a brand new connection attempt failing outright).
            # Common during a burst of concurrent requests (e.g. many
            # thumbnails loading at once) hitting a target that's still
            # cold-starting. The connector discards the broken connection
            # once this is raised, so a retry almost always gets a fresh,
            # working one — but only while nothing has been sent to the
            # browser yet.
            #
            # aiohttp raises this same message when the *browser's* socket is
            # the one that's closing (it navigated away / cancelled the
            # request). Retrying then would only hit — and possibly wake —
            # the remote for nobody, so bail out quietly instead.
            transport = request.transport
            if transport is None or transport.is_closing():
                logger.warning(
                    f"[ComfyUI Proxy] Browser closed the connection while forwarding {req_desc} "
                    f"to remote {label} ({type(e).__name__}); abandoning request."
                )
                if headers_sent and stream_resp is not None:
                    return stream_resp
                return web.Response(status=499)
            if not headers_sent and attempt < max_attempts:
                logger.warning(
                    f"[ComfyUI Proxy] Transient connection error reaching remote {label} for {req_desc} "
                    f"({type(e).__name__}: {e}); "
                    f"retrying in {RETRY_DELAY:g}s (attempt {attempt + 1}/{max_attempts})..."
                )
                # A brief pause, not an instant retry: hammering the exact
                # same pool/gateway state immediately is likely to hit the
                # same race again. This gives the connector a moment to
                # actually discard the broken connection and the gateway a
                # moment to settle, before trying again.
                await asyncio.sleep(RETRY_DELAY)
                continue
            if headers_sent:
                logger.error(
                    f"[ComfyUI Proxy] Connection to remote {label} dropped mid-stream for "
                    f"{req_desc} ({type(e).__name__}: {e}) — too late to retry, the browser already "
                    "started receiving this response."
                )
            else:
                logger.error(
                    f"[ComfyUI Proxy] Client error forwarding {req_desc} to remote {label} "
                    f"after {max_attempts} attempt(s) ({type(e).__name__}): {e}"
                )
            return web.json_response({"error": f"ComfyUI Proxy: error forwarding request: {logsafe.sanitize_logs(e)}"}, status=502)
        except aiohttp.ClientResponseError as e:
            logger.error(f"[ComfyUI Proxy] Remote {label} responded with error for {req_desc}: {e}")
            return web.json_response({"error": f"ComfyUI Proxy: remote {label} error: {e.message}"}, status=e.status or 502)
        except aiohttp.ClientError as e:
            logger.error(
                f"[ComfyUI Proxy] Client error forwarding {req_desc} to remote {label} "
                f"({type(e).__name__}): {e}"
            )
            return web.json_response({"error": f"ComfyUI Proxy: error forwarding request: {logsafe.sanitize_logs(e)}"}, status=502)
        except (UnicodeDecodeError, LookupError) as e:
            logger.error(f"[ComfyUI Proxy] Encoding error while forwarding to remote {label}: {e}")
            return web.json_response({"error": "ComfyUI Proxy: encoding error while forwarding response"}, status=502)

    # Unreachable in practice (every branch above returns), but keeps this
    # function's contract honest if max_attempts is ever changed.
    return web.json_response({"error": f"ComfyUI Proxy: failed to forward request to remote {label}"}, status=502)
