# ComfyUI Proxy

Forwards job-related ComfyUI API/WebSocket traffic to a configurable cloud or
serverless GPU endpoint, while node-graph editing (`/object_info`, static
assets, settings, etc.) always stays local. A movable toggle pill in the
corner of the UI lets you flip between local and cloud GPU, and expands into
a small form for the remote URL, timeout, and an optional auth key.

## Install

Clone this repository into `ComfyUI/custom_nodes/` directory and restart ComfyUI. 
```bash
cd ComfyUI/custom_nodes
git clone https://github.com/anr2me/comfyui-proxy.git
```
Only `aiohttp` is a hard dependency, which ComfyUI already ships with.

Optionally, install (Brotli + zstandard) if your remote endpoint or its gateway might compress responses with `br` or `zstd`. 
```bash
cd comfyui-proxy
pip install -r requirements.txt
```
These are only used on the handful of routes the proxy actually reads and
parses (`/prompt`, `/queue`, the wake-up ping, the one-time `/object_info`
model pull) — if they're missing, those specific calls fail with a clear
"install Brotli/zstandard" error instead of crashing, everything else
(`/view`, `/viewvideo`, `/history`, etc.) is streamed byte-for-byte and never
needs them regardless of encoding.

## What gets proxied

| Route(s) | When |
|---|---|
| `/prompt`, `/queue` | Always, when the toggle is on. These also **wake** the remote (cold boot). |
| `/interrupt`, `/upload/image`, `/upload/mask`, `/free` | Always, when the toggle is on. |
| `/history*`, `/view*`, `/viewvideo*`, `/api/jobs*`, `/api/crystools*` | Always, when the toggle is on (partial/range content and streaming are preserved). |
| `/ws`, `/internal/logs` | **Only** while a job is known to be incomplete (queued/running remotely) — so simply having the toggle on never spins up a serverless instance by itself. |
| `/object_info` | Never proxied — served locally so the graph editor keeps working offline, but combo/dropdown model fields are patched in-place with the cached remote model list (see below) so you can't pick a checkpoint that only exists on your machine. |
| Everything else | Untouched, served locally as normal. |

## Waking the remote

The remote is only pinged (`GET /system_stats`) to trigger a cold boot in
these cases:
- a `/prompt` or `/queue` request comes in while the proxy is enabled,
- a request to a conditional route (`/ws`, `/internal/logs`, etc.) arrives
  while a job is already known to be incomplete,
- the model list needs its first-time pull after enabling or changing the URL.

Each wake attempt logs `Initializing remote GPU...` through Python's
`logging` module, which ComfyUI surfaces on `/internal/logs` (and console),
so you can watch cold-boot progress without extra polling.

## Model list caching

The first time the proxy is enabled, or whenever the remote URL changes, the
plugin fetches `/object_info` from the remote **once**, extracts every
dropdown/combo field (checkpoints, LoRAs, VAEs, samplers with custom nodes,
etc.) and caches it. Local `/object_info` responses then get those specific
fields swapped in-place with the cached remote list — local-only models are
simply not in that list, since it comes straight from the remote. Use the
"Refresh Models" button in the panel to force a re-pull at any time.

## Job tracking

A prompt is considered "incomplete" from the moment `/prompt` returns a
`prompt_id` until either:
- the remote's `/ws` stream reports `executing` with `node: null` or an
  `execution_success` / `execution_error` / `execution_interrupted` message
  for that id, or
- `/queue` is polled and both `queue_running` and `queue_pending` come back
  empty, or
- `/interrupt` is called.

This state is what gates `/ws` and `/internal/logs` forwarding.

## Streaming, encoding, and errors

- HTTP responses are streamed chunk-by-chunk (`web.StreamResponse`), so large
  `/view`/`/viewvideo` payloads and `Range`/`206 Partial Content` responses
  work correctly.
- `Content-Encoding` (gzip/br/zstd) is passed through untouched end-to-end —
  the proxy never decodes/re-encodes bodies for streamed routes, it just
  relays the remote's headers and bytes as-is, and forwards your browser's
  `Accept-Encoding` to the remote so negotiation still happens correctly.
- Timeouts, connection errors, and remote `5xx` responses are caught and
  turned into clear `502`/`504` JSON errors instead of hanging or crashing
  the local server. The `504` message explicitly suggests raising the
  timeout for slow cold boots.
- The request timeout (in seconds) is configurable from the panel, since
  different serverless providers cold-boot at very different speeds.

## Endpoints added for the UI panel (never proxied)

- `GET /comfyui_proxy/config` — current settings (auth key is redacted, only
  a boolean `auth_key_set` is returned).
- `POST /comfyui_proxy/config` — update `enabled`, `remote_url`, `timeout`,
  `auth_key` (send an empty string to leave the stored key unchanged).
- `POST /comfyui_proxy/refresh_models` — force a fresh pull of the remote
  model list.

## Notes / caveats

- `/api/jobs` and `/api/crystools` are proxied as path prefixes as requested;
  they're only meaningful if your remote actually exposes them (crystools is
  a separate resource-monitor custom node — install it on both sides if you
  want it working through the proxy).
- The toggle defaults to **off**. Nothing is forwarded and no remote instance
  is contacted until you turn it on and set a URL.
- Position of the floating panel is remembered per-browser via
  `localStorage`.
