# ComfyUI Proxy

Forwards job-related ComfyUI API/WebSocket traffic to a configurable cloud or
serverless GPU endpoint, while node-graph editing (`/object_info`, static
assets, settings, etc.) always stays local. A movable toggle pill in the
corner of the UI lets you flip between local and remote GPU, and expands into
a small form for the remote GPU URL (+ its own optional auth key), timeout
(default 300s — serverless cold boots can take a while when GPU capacity is
scarce), post-completion delay, job history cache size, an optional second
**remote CPU URL** (+ its own optional auth key — see
[Remote CPU target](#remote-cpu-target-uploadsdownloadspreviews) below), and
an opt-in **GPU keep-alive** for long video playback without one — see
[GPU keep-alive](#gpu-keep-alive-for-long-videoimage-viewing-opt-in) below.

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
To let the proxy negotiate those encodings on the calls it actually reads and
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
| `/prompt`, `/queue` | Always, when the toggle is on. These also **wake the GPU** (cold boot) and always go to the GPU target, never the CPU one. |
| `/upload/image`, `/upload/mask`, `/view*`, `/viewvideo*` | Always proxied (file-only, needs no GPU). Goes to the **remote CPU target** when one is configured and the GPU isn't already known active; otherwise goes to the GPU target as usual — see [Remote CPU target](#remote-cpu-target-uploadsdownloadspreviews). |
| `/interrupt`, `/free` | Only while a job is known incomplete or its shadow relay is live. Meaningless (and not worth a cold start) if nothing's running remotely. Always the GPU target. |
| `/api/jobs*` | Fetched fresh (and **cached locally**) whenever a job is known incomplete or its relay is live; served from that cache otherwise, so browsing job history never wakes the remote. Falls through to the local handler only if there's no cached copy yet at all. Always the GPU target (job/queue metadata isn't just files on the shared volume). |
| `/history*`, `/api/crystools*` | Only while a job is known incomplete or its shadow relay is live. **By design**, this means older remote history won't show once the relay has closed — browsing alone is never allowed to cold-start the serverless instance. Always the GPU target. |
| `/ws`, `/internal/logs` | **Never** proxied as HTTP routes — see [Live progress](#live-progress-ws) below for how remote progress/logs actually reach the browser instead. |
| `/object_info` | Never proxied — served locally so the graph editor keeps working offline, but combo/dropdown model fields are patched in-place with the cached remote model list (see below) so you can't pick a checkpoint that only exists on your machine. |
| Everything else | Untouched, served locally as normal. |

## Waking the remote GPU

The GPU target is only pinged (`GET /system_stats`) to trigger a cold boot
in these cases:
- a `/prompt` or `/queue` request comes in while the proxy is enabled,
- `/interrupt`, `/free`, `/history`, or `/api/crystools` is actually being
  forwarded (per the table, only while a job is incomplete or the relay is live),
- an upload/`/view`/`/viewvideo` request is being forwarded to the **GPU**
  target specifically — i.e. no CPU target is configured, or the GPU is
  already active anyway,
- the shadow progress relay opens a connection ahead of a `/prompt` submission,
- the model list needs its first-time pull after enabling or changing the URL.

Every one of these gets an `Initializing remote GPU...` log line **unless**
a shadow relay is already known to be live (in which case the remote is
provably awake already and logging would just be noise). This is
unconditional on any actual outbound call to the GPU target, so a silent
cold start should never happen without at least one log line explaining why.

## Remote CPU target (uploads/downloads/previews)

An optional second **Remote CPU URL** (+ its own optional auth key) can
point at a cheaper CPU-only container that shares the same persistent
volume as the GPU one. It's used, instead of the GPU target, for exactly
the file-only routes that don't need a GPU at all: `/upload/image`,
`/upload/mask`, `/view*`, `/viewvideo*` — nothing else. `/prompt`, `/queue`,
`/interrupt`, `/free`, `/history`, and `/api/jobs` always go to the GPU
target regardless, since they need the live GPU container's own state, not
just files on the shared volume.

The CPU target is only ever used while a job **isn't actually still
running** — if one is, these same routes go to the GPU target instead, so a
request is never split across both containers and a single job never ends
up waking both. This is checked against whether a job is genuinely
incomplete, not merely whether the shadow relay happens to still be open:
during the few-second grace period after a job finishes (kept open only so
progress/log animations can finish, not because new GPU work is happening),
the GPU container may already be winding down and start refusing new
connections before that websocket actually closes — so new file-serving
requests in that window correctly go to the CPU target (when configured)
rather than risk hitting a GPU container on its way out. There's no
separate "wake" ping for the CPU target the way there is for the GPU one
(see above): the actual upload/view/viewvideo request *is* what reaches it,
cold-starting it transparently as part of that one request, and a line
like `Using remote CPU container for /view... (GPU not active)` is logged
each time it's used — deliberately scoped to just those four route
patterns, so routine polling of other routes never touches (or wakes) it.

If no CPU URL is configured, this entire feature is inactive and every
route behaves exactly as if it didn't exist.

**The Remote CPU URL must be a genuinely separate endpoint from the Remote
GPU URL.** Pointed at the same URL, "CPU routing" isn't actually a separate,
independently-stable container — it's the same volatile serverless GPU
endpoint under a second label, so it gets none of the intended benefit and
can make things *worse* (more traffic hitting that one endpoint during
exactly the window — job just finished, container winding down — this
feature exists to avoid). The proxy detects this and ignores the CPU URL
(logging a warning on save) rather than actually using it when they match.

## GPU keep-alive for long video/image viewing (opt-in)

Without a separate CPU target, there's no way to serve `/view`/`/viewvideo`
once the GPU container scales to zero after a job finishes. A browser
`<video>` element doesn't hold one continuous connection for an entire
video — it fetches chunks via Range requests as needed, and can go quiet
for stretches (already buffered ahead) longer than the provider's own idle
timeout, even while someone is still actively watching. Once the container
is gone, playback cuts off and that output stays unreachable until
something wakes the GPU again.

The **"Keep GPU warm for video/image viewing"** toggle (off by default —
it has a real cost) addresses this directly — but *not* by pinging the GPU
on a timer. A brief ping that completes and returns doesn't actually keep a
scale-to-zero container warm: the instant it responds, the platform sees
zero pending requests again and the container is just as eligible for
teardown as if nothing had happened. What actually counts as "busy" is a
connection that stays open and pending — and the shadow progress relay
already has exactly that, in the form of its own websocket to the GPU. So
instead, while this is enabled, every `/view`/`/viewvideo` request that's
actually forwarded to the GPU target (i.e. no separate CPU target is in
play) notes the activity, and the relay simply keeps that existing
connection open for as long as such activity is within the last **view-
activity idle timeout** seconds — then lets it close on its own. It only
ever extends while there's been recent `/view`/`/viewvideo` traffic; it's
not a way to keep the GPU permanently warm.

This is the fallback for people without a separate CPU container who are
willing to pay GPU cost to keep playback working — if you *do* have a
genuinely separate CPU endpoint configured above, that's the better
solution (cheaper, and this toggle is unnecessary alongside it).

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

## Job history caching (`/api/jobs`)

`/api/jobs` (used by the Media Assets panel) is cached locally, keyed by its
exact path + query string: whenever a job is known incomplete or the shadow
relay is live, each request is fetched fresh from the remote and the result
cached (configurable **Job history cache size**, default 64 distinct
queries, oldest evicted first — including shrinking live if you lower it);
whenever neither is true, the cached copy is served directly with no remote
call at all — so browsing recent history while idle never wakes the remote. A cached
response carries an `X-ComfyUI-Proxy-Cache: hit; age=<seconds>` header. If a
live fetch fails (timeout, connection error, 5xx), the cache is used as a
fallback before giving up. The cache is also cleared whenever the frontend
clears job history (`POST /history` or `/api/history` with `"clear": true`
in the body — detected regardless of whether that particular request ends
up reaching the remote), or whenever the remote URL changes, and is
in-memory only (cleared on a ComfyUI restart, same as the job-tracking
state).

While a job is known incomplete, the frontend's own ordinary `/api/jobs`
polling already keeps the cache warm via the normal on-demand path (the
remote is known active, so each of those requests is a live fetch, cached
as a side effect) — no separate proactive refresh is needed during that
window. The one proactive refresh that actually matters happens exactly
once, unconditionally, in the shadow relay's cleanup: right after a job
finishes (or the relay closes for any other reason — the remote dropping
the connection before a clean completion message, a timeout, ...) but
*before* actually closing the connection, while it still proves the remote
is awake — so the cache is warm for every query variant it already knows
about by the time the Media Assets panel gets opened later, after the relay
(and that proof of liveness) is gone. Only this refresh logs `Retrieving
remote job history (...) to cache...`, appearing between the `No jobs
left...` and `...progress stream closed` log lines — ordinary on-demand
fetches don't log anything extra, since the frontend can call `/api/jobs`
often enough that logging every one of those would be noisy.

If a panel request's exact query string isn't in the cache (e.g. its
pagination params differ from what was proactively refreshed), the proxy
looks for another cached entry with the same `status` filter (ComfyUI uses
this to distinguish completed/failed job lists from in-progress/pending
ones) before falling back to whatever was cached most recently overall —
so a completed-jobs request can't end up silently "falling back" to an
in-progress list (or vice versa) and look wrong instead of just stale. This
fallback is also common enough with routine polling that it's logged at
debug level only, rather than the default; the response's
`X-ComfyUI-Proxy-Cache` header shows it happened either way.

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
when it's safe to close. If the relay's `finally` block ever runs without
a `No jobs left on remote GPU for client ...` line having appeared first,
that means the websocket ended some other way (a timeout, a connection
error, or the remote closing it outright) rather than through a clean
completion message — `state.clear_client()` still runs regardless, so
tracking doesn't get stuck either way, but it's a sign the remote dropped
the connection rather than the job cleanly finishing.

Every wake check (`wake_remote_if_needed`) is also bounded by a hard
ceiling (the configured timeout plus a margin) on top of its own internal
timeout, specifically so that if a single wake attempt ever hung for any
reason, it couldn't block every other request sharing that same
coalescing slot — including, critically, the next `/prompt` submission —
indefinitely.

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
- A transient aiohttp connection-pool race (`ClientOSError`, e.g. "Cannot
  write to closing transport" — common when a burst of concurrent requests,
  like many thumbnails loading at once, hits a target that's still
  cold-starting) is retried once automatically after a brief pause (not an
  instant retry — hammering the same broken pool/gateway state immediately
  has a high chance of hitting the same race again), logged as a `WARNING`.
  This only works as long as nothing has been sent to the browser yet; if
  the same error happens *after* the response has already started
  streaming back, retrying isn't possible (can't send a second response to
  an already-started one) and it's logged as an `ERROR` instead, explicitly
  noting it was too late to retry. A brand-new connection attempt failing
  outright (`ClientConnectorError` — DNS failure, refused, ...) is also an
  `ERROR` and is never retried, since that's a different, non-transient
  situation.

## Endpoints added for the UI panel (never proxied)

- `GET /comfyui_proxy/config` — current settings (both auth keys are
  redacted, only booleans `auth_key_set` / `remote_cpu_auth_key_set` are
  returned).
- `POST /comfyui_proxy/config` — update `enabled`, `remote_url`, `timeout`,
  `post_completion_delay`, `jobs_cache_max_entries`, `auth_key`,
  `remote_cpu_url`, `remote_cpu_auth_key`, `gpu_keepalive_enabled`,
  `gpu_keepalive_idle_timeout` (send an empty string for either auth key to
  leave the stored one unchanged).
- `POST /comfyui_proxy/refresh_models` — force a fresh pull of the remote
  model list.
- `POST /comfyui_proxy/reset_state` — forcibly cancels any tracked shadow
  relay connections, clears all tracked "incomplete job" state, and ends
  any in-progress GPU keep-alive extension. Exposed as the **Clear Stuck
  State** button in the panel; use it if the proxy seems to think
  something's still running (e.g. after the remote died mid-job in a way
  that never sent a clean completion message) without having to restart
  ComfyUI. Returns immediately rather than waiting on the cancelled
  connections' own cleanup, since that could itself hang against an
  unreachable remote.
- `POST /comfyui_proxy/reset_config` — resets `remote_url`, `remote_cpu_url`,
  `timeout`, `post_completion_delay`, `jobs_cache_max_entries`, `auth_key`,
  `remote_cpu_auth_key`, and the GPU keep-alive settings back to their
  defaults (and disables the proxy), additionally clearing the job history
  cache and any tracked state/relays. Exposed as the **Reset to Defaults**
  button in the panel, with a confirmation prompt since it wipes both saved
  URLs and both auth keys.

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
