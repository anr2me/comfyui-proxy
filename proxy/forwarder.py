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
    return max(1.0, float(cfgmod.get("circuit_breaker_cooldown", 30) or 30))


def circuit_is_open(use_cpu: bool = False) -> bool:
    return time.time() < _circuit_state[use_cpu]["open_until"]


def circuit_record_failure(use_cpu: bool = False):
    st = _circuit_state[use_cpu]
    st["failures"] += 1
    cooldown = _circuit_breaker_cooldown()
    st["open_until"] = time.time() + cooldown
    label = "CPU" if use_cpu else "GPU"
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
    try:
        t = float(cfgmod.get("timeout") or 120)
    except (TypeError, ValueError):
        t = 120.0
    t = max(5.0, t)
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


async def forward_http(request: web.Request, use_cpu: bool = False) -> web.StreamResponse:
    """Stream a single HTTP request/response through to the target (GPU by
    default, or the optional CPU-only target when use_cpu is True),
    preserving status, headers (incl. Content-Encoding / Content-Range), and
    body as-is."""
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
    body = await request.read() if request.can_read_body else None

    session = get_session()
    max_attempts = 2  # one retry for a transient connection-pool race — see below
    for attempt in range(1, max_attempts + 1):
        headers_sent = False
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
                await stream_resp.prepare(request)
                headers_sent = True  # past this point a retry is no longer safe: the
                                      # browser has already received a response's headers
                async for chunk in remote_resp.content.iter_any():
                    await stream_resp.write(chunk)
                await stream_resp.write_eof()
                return stream_resp

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
            logger.error(f"[ComfyUI Proxy] Connection error reaching remote {label}: {e}")
            return web.json_response({"error": f"ComfyUI Proxy: cannot reach remote {label}: {e}"}, status=502)
        except aiohttp.ClientOSError as e:
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
            if not headers_sent and attempt < max_attempts:
                logger.warning(
                    f"[ComfyUI Proxy] Transient connection error reaching remote {label} ({e}); "
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
                    f"{request.rel_url.path} ({e}) — too late to retry, the browser already "
                    "started receiving this response."
                )
            else:
                logger.error(
                    f"[ComfyUI Proxy] Client error forwarding request to remote {label} "
                    f"after {max_attempts} attempt(s): {e}"
                )
            return web.json_response({"error": f"ComfyUI Proxy: error forwarding request: {e}"}, status=502)
        except aiohttp.ClientResponseError as e:
            logger.error(f"[ComfyUI Proxy] Remote {label} responded with error: {e}")
            return web.json_response({"error": f"ComfyUI Proxy: remote {label} error: {e.message}"}, status=e.status or 502)
        except aiohttp.ClientError as e:
            logger.error(f"[ComfyUI Proxy] Client error forwarding request to remote {label}: {e}")
            return web.json_response({"error": f"ComfyUI Proxy: error forwarding request: {e}"}, status=502)
        except (UnicodeDecodeError, LookupError) as e:
            logger.error(f"[ComfyUI Proxy] Encoding error while forwarding to remote {label}: {e}")
            return web.json_response({"error": "ComfyUI Proxy: encoding error while forwarding response"}, status=502)

    # Unreachable in practice (every branch above returns), but keeps this
    # function's contract honest if max_attempts is ever changed.
    return web.json_response({"error": f"ComfyUI Proxy: failed to forward request to remote {label}"}, status=502)
