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


def target_base() -> str:
    return (cfgmod.get("remote_url") or "").rstrip("/")


def copy_request_headers(request: web.Request, limit_encoding: bool = False) -> dict:
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
    auth_key = cfgmod.get("auth_key")
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
_in_flight_wake = None  # shared asyncio.Task for any currently-in-progress wake ping


async def wake_remote_if_needed(reason: str = ""):
    """Ping the remote once to trigger a cold boot, logging progress locally
    via the standard logging module. Concurrent callers (e.g. two
    near-simultaneous requests both hitting a conditional route right as a
    panel opens) share a single underlying ping instead of each firing their
    own — otherwise the same near-instant wake gets logged and pinged twice
    for what is really one event."""
    global _in_flight_wake
    base = target_base()
    if not base:
        return

    async with _wake_lock:
        if _in_flight_wake is None or _in_flight_wake.done():
            _in_flight_wake = asyncio.ensure_future(_do_wake_ping(base, reason))
        task = _in_flight_wake

    await task


async def _do_wake_ping(base: str, reason: str):
    logger.info(f"[ComfyUI Proxy] Initializing remote GPU... ({reason})")
    try:
        session = get_tracking_session()
        timeout = get_timeout()
        headers = {"Accept-Encoding": get_safe_accept_encoding()}
        auth_key = cfgmod.get("auth_key")
        if auth_key:
            headers["Authorization"] = f"Bearer {auth_key}"
        async with session.get(f"{base}/system_stats", headers=headers, timeout=timeout) as resp:
            if resp.status < 500:
                logger.info("[ComfyUI Proxy] Remote GPU is ready.")
            else:
                logger.warning(f"[ComfyUI Proxy] Remote GPU returned HTTP {resp.status} while waking up.")
    except asyncio.TimeoutError:
        logger.warning(
            "[ComfyUI Proxy] Timed out waking up remote GPU "
            "(cold boot may still be in progress; consider raising the timeout)."
        )
    except aiohttp.ClientError as e:
        logger.warning(f"[ComfyUI Proxy] Error waking up remote GPU: {e}")
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] Unexpected error waking up remote GPU: {e}")


async def forward_http(request: web.Request) -> web.StreamResponse:
    """Stream a single HTTP request/response through to the remote, preserving
    status, headers (incl. Content-Encoding / Content-Range), and body as-is."""
    base = target_base()
    if not base:
        return web.json_response({"error": "ComfyUI Proxy: remote URL not configured"}, status=502)

    parts = urlsplit(base)
    target_url = f"{parts.scheme}://{parts.netloc}{request.rel_url.path}"
    if request.rel_url.query_string:
        target_url += f"?{request.rel_url.query_string}"

    headers = copy_request_headers(request)
    timeout = get_timeout()
    body = await request.read() if request.can_read_body else None

    session = get_session()
    try:
        async with session.request(
            request.method, target_url, headers=headers, data=body,
            timeout=timeout, allow_redirects=False,
        ) as remote_resp:
            # Copy headers verbatim (this is what preserves gzip/br/zstd
            # Content-Encoding and Content-Range for partial/streamed content).
            resp_headers = {k: v for k, v in remote_resp.headers.items() if k.lower() not in HOP_BY_HOP}

            if remote_resp.status >= 500:
                logger.error(f"[ComfyUI Proxy] Remote returned {remote_resp.status} for {request.method} {request.rel_url.path}")

            stream_resp = web.StreamResponse(status=remote_resp.status, headers=resp_headers)
            await stream_resp.prepare(request)
            async for chunk in remote_resp.content.iter_any():
                await stream_resp.write(chunk)
            await stream_resp.write_eof()
            return stream_resp

    except asyncio.TimeoutError:
        logger.error(f"[ComfyUI Proxy] Timeout forwarding {request.method} {request.rel_url.path}")
        return web.json_response(
            {"error": "ComfyUI Proxy: remote GPU timed out (increase the timeout in the proxy panel if it's cold-booting)"},
            status=504,
        )
    except aiohttp.ClientConnectorError as e:
        logger.error(f"[ComfyUI Proxy] Connection error reaching remote GPU: {e}")
        return web.json_response({"error": f"ComfyUI Proxy: cannot reach remote GPU: {e}"}, status=502)
    except aiohttp.ClientResponseError as e:
        logger.error(f"[ComfyUI Proxy] Remote responded with error: {e}")
        return web.json_response({"error": f"ComfyUI Proxy: remote GPU error: {e.message}"}, status=e.status or 502)
    except aiohttp.ClientError as e:
        logger.error(f"[ComfyUI Proxy] Client error forwarding request: {e}")
        return web.json_response({"error": f"ComfyUI Proxy: error forwarding request: {e}"}, status=502)
    except (UnicodeDecodeError, LookupError) as e:
        logger.error(f"[ComfyUI Proxy] Encoding error while forwarding: {e}")
        return web.json_response({"error": "ComfyUI Proxy: encoding error while forwarding response"}, status=502)
