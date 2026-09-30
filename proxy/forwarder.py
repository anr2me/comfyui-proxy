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
import json
import logging
from urllib.parse import urlsplit, urlunsplit

import aiohttp
from aiohttp import web

from . import config as cfgmod
from . import state

logger = logging.getLogger("ComfyUIProxy")

_raw_session = None
_tracking_session = None

# Headers that must never be blindly copied between hops.
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
}

_COMPLETION_MSG_TYPES = {
    "execution_success", "execution_error", "execution_interrupted",
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


async def wake_remote_if_needed(reason: str = ""):
    """Ping the remote once to trigger a cold boot, logging progress locally
    via the standard logging module (surfaced through /internal/logs)."""
    base = target_base()
    if not base:
        return
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


def _inspect_ws_message_for_completion(raw_text: str):
    """Best-effort peek at relayed ws JSON to detect job completion, so we can
    stop treating the job as 'incomplete' (and eventually let /ws and
    /internal/logs fall back to local) without polling anything extra."""
    try:
        msg = json.loads(raw_text)
    except Exception:
        return
    mtype = msg.get("type")
    data = msg.get("data") or {}
    prompt_id = data.get("prompt_id")
    if mtype == "executing" and data.get("node") is None:
        state.mark_job_done(prompt_id)
    elif mtype in _COMPLETION_MSG_TYPES:
        state.mark_job_done(prompt_id)


async def forward_websocket(request: web.Request) -> web.WebSocketResponse:
    """Bridge the browser's /ws connection to the remote's /ws connection for
    as long as both sides stay open."""
    base = target_base()
    ws_local = web.WebSocketResponse(heartbeat=30)
    await ws_local.prepare(request)

    if not base:
        await ws_local.close(code=1011, message=b"ComfyUI Proxy: remote URL not configured")
        return ws_local

    parts = urlsplit(base)
    ws_scheme = "wss" if parts.scheme == "https" else "ws"
    target_ws_url = urlunsplit((ws_scheme, parts.netloc, request.rel_url.path, request.rel_url.query_string, ""))

    headers = copy_request_headers(request)
    session = get_tracking_session()
    timeout = get_timeout()

    try:
        async with session.ws_connect(
            target_ws_url, headers=headers, timeout=timeout.total, heartbeat=30
        ) as ws_remote:

            async def local_to_remote():
                async for msg in ws_local:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await ws_remote.send_str(msg.data)
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        await ws_remote.send_bytes(msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        break

            async def remote_to_local():
                async for msg in ws_remote:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        _inspect_ws_message_for_completion(msg.data)
                        await ws_local.send_str(msg.data)
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        await ws_local.send_bytes(msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        break

            t1 = asyncio.ensure_future(local_to_remote())
            t2 = asyncio.ensure_future(remote_to_local())
            _, pending = await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)
            for t in pending:
                t.cancel()

    except asyncio.TimeoutError:
        logger.error("[ComfyUI Proxy] Timeout connecting websocket to remote GPU")
        await ws_local.close(code=1011, message=b"ComfyUI Proxy: remote websocket connection timed out")
    except aiohttp.ClientError as e:
        logger.error(f"[ComfyUI Proxy] Websocket connection error: {e}")
        await ws_local.close(code=1011, message=str(e).encode("utf-8", "ignore"))
    except Exception as e:
        logger.error(f"[ComfyUI Proxy] Unexpected websocket proxy error: {e}")
        await ws_local.close(code=1011, message=str(e).encode("utf-8", "ignore"))

    return ws_local
