# ComfyUI Proxy

Forwards job-related ComfyUI API/WebSocket traffic to a configurable cloud or
serverless GPU endpoint, while node-graph editing (`/object_info`, static
assets, settings, etc.) always stays local. A movable toggle pill in the
corner of the UI lets you flip between local and remote GPU, and expands into
a small form for the remote URL, timeout, post-completion delay, and an
optional auth key.

## Install

Clone this repository into `ComfyUI/custom_nodes/` directory and restart ComfyUI. 
```bash
cd ComfyUI/custom_nodes
git clone https://github.com/anr2me/comfyui-proxy.git
```
Only `aiohttp` is a hard dependency, which ComfyUI already ships with.

Optionally, install (Brotli + backports.zstd)
```bash
cd comfyui-proxy
pip install -r requirements.txt
```
To let proxy negotiate those encodings on the calls it actually reads and
parses (`/prompt`, `/queue`, the one-time `/object_info` model pull). It
detects what's importable in ComfyUI's Python environment and only offers
`br`/`zstd` on those calls when the matching package is present — otherwise
it sticks to `gzip`/`deflate`, which is always safe and usually all you
need. Everything streamed (`/view`, `/viewvideo`, `/history`, etc.) is
relayed byte-for-byte and never needs to decode anything regardless of
encoding.

## What gets proxied

| Route(s) | When |
|---|---|
| `/prompt`, `/queue` | Always, when the toggle is on. These also **wake** the remote (cold boot). |
| `/upload/image`, `/upload/mask` | Always, when the toggle is on — preparing input for a job is a legitimate reason to wake the remote. |
| `/interrupt`, `/free` | Only while a job is known incomplete or its shadow relay is live. Meaningless (and not worth a cold start) if nothing's running remotely. |
| `/history*`, `/view*`, `/viewvideo*`, `/api/jobs*`, `/api/crystools*` | Only while a job is known incomplete or its shadow relay is live. **By design**, this means older remote job history/outputs won't show once the relay has closed — browsing alone is never allowed to cold-start the serverless instance. Partial/range content and streaming are preserved when it does proxy. |
| `/ws`, `/internal/logs` | **Never** proxied as HTTP routes — see [Live progress](#live-progress-ws) below for how remote progress/logs actually reach the browser instead. |
| `/object_info` | Never proxied — served locally so the graph editor keeps working offline, but combo/dropdown model fields are patched in-place with the cached remote model list (see below) so you can't pick a checkpoint that only exists on your machine. |
| Everything else | Untouched, served locally as normal. |

## Waking the remote

The remote is only pinged (`GET /system_stats`) to trigger a cold boot in
these cases:
- a `/prompt` or `/queue` request comes in while the proxy is enabled,
- an upload, `/interrupt`, `/free`, or browsing route above is actually
  being forwarded (per the table, only while a job is incomplete or the
  relay is live),
- the shadow progress relay opens a connection ahead of a `/prompt` submission,
- the model list needs its first-time pull after enabling or changing the URL.

Every one of these gets an `Initializing remote GPU...` log line **unless**
a shadow relay is already known to be live (in which case the remote is
provably awake already and logging would just be noise). This is
unconditional on any actual outbound call, so a silent cold start should
never happen without at least one log line explaining why.

## Model list caching

The first time the proxy is enabled, or whenever the remote URL changes, the
plugin fetches `/object_info` from the remote **once**, extracts every
dropdown/combo field (checkpoints, LoRAs, VAEs, samplers with custom nodes,
etc.) and caches it. Local `/object_info` responses then get those specific
fields swapped in-place with the cached remote list — local-only models are
simply not in that list, since it comes straight from the remote. Use the
"Refresh Models" button in the panel to force a re-pull at any time.

## Live progress (`/ws`)

The browser opens its `/ws` connection once, at page load — by the time a
job starts there's no "future `/ws` request" left to redirect to the
remote, so literally proxying that route doesn't work. Instead:

1. When `/prompt` is submitted, the proxy reads the `client_id` from the
   payload (the same id the browser used for its own local `/ws`
   connection) and opens a dedicated **shadow websocket** from the proxy
   process itself to the remote's `/ws?clientId=<id>`, waiting for the
   remote's first `status` message (which carries the `sid` it assigned)
   before the prompt is actually sent — this avoids the remote treating the
   connection as mismatched ("running in another tab").
2. Once ready, the proxy sends `PATCH /internal/logs/subscribe` with
   `{"enabled": true, "clientId": <id>}` so remote execution logs start
   arriving as messages on that same shadow socket too.
3. Every message the remote sends (progress, previews, logs — text or
   binary) is immediately re-emitted onto the browser's **already-open
   local** `/ws` socket, found via `PromptServer.instance.sockets[client_id]`.
   From the browser's perspective these are indistinguishable from
   locally-generated messages.
4. Once every prompt tracked for that client has completed, the proxy waits
   a configurable delay (**Post-completion delay**, default 5s) before
   unsubscribing logs and closing the shadow connection, so in-flight
   progress bar / log animations have time to finish.

`/internal/logs` is likewise never proxied as an HTTP route for this same
reason — remote log lines arrive through the shadow connection instead.

## Job tracking

A prompt is considered "incomplete" from the moment `/prompt` returns a
`prompt_id` until either:
- the shadow relay's `/ws` stream reports `executing` with `node: null`, or
  an `execution_success` / `execution_error` / `execution_interrupted`
  message, for that id, or
- `/queue` is polled and both `queue_running` and `queue_pending` come back
  empty, or
- `/interrupt` is called.

This state (scoped per `client_id`) is what the shadow relay uses to decide
when it's safe to close.

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
  `post_completion_delay`, `auth_key` (send an empty string for `auth_key`
  to leave the stored key unchanged).
- `POST /comfyui_proxy/refresh_models` — force a fresh pull of the remote
  model list.
- `POST /comfyui_proxy/reset_state` — forcibly cancels any tracked shadow
  relay connections and clears all tracked "incomplete job" state. Exposed
  as the **Clear Stuck State** button in the panel; use it if the proxy
  seems to think something's still running (e.g. after the remote died
  mid-job in a way that never sent a clean completion message) without
  having to restart ComfyUI. Returns immediately rather than waiting on the
  cancelled connections' own cleanup, since that could itself hang against
  an unreachable remote.

## Notes / caveats

- `/api/jobs` and `/api/crystools` are proxied as their own path prefixes
  (not treated as `/api`-aliases of some other route). `/api/jobs` is used
  by the frontend's Media Assets panel to pull completed job/output info;
  `/api/crystools` is a separate resource-monitor custom node — install it
  on both sides if you want it working through the proxy.
- The toggle defaults to **off**. Nothing is forwarded and no remote instance
  is contacted until you turn it on and set a URL.
- Position of the floating panel is remembered per-browser via
  `localStorage`.
