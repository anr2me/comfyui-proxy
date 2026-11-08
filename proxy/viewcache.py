"""
Local on-disk cache for files the frontend views through ComfyUI's /view.

Enabled by the "Auto-download viewed input/output" setting. It only ever
touches /view requests for type=output or type=input (the types the Media
Assets panel uses); everything else behaves exactly as before.

  - If the file already exists under ComfyUI's local output/input directory
    (<dir>/<subfolder>/<filename>), the request is NOT forwarded at all: the
    proxy hands it to ComfyUI's own /view handler, so thumbnails and videos
    show up without waking the remote.
  - If only part of it is on disk (see below), a Range request for bytes that
    are all there is answered from that part, and so is a HEAD request (the
    size is known), without asking the remote (serve_partial()).
  - Otherwise the request is forwarded as usual, and the bytes streaming
    back are also written into "<filename>.tmp" at the right offset (most
    /view requests are Range requests, so each response only covers part of
    the file). A "<filename>.map" JSON file records which byte ranges are
    already on disk, so it survives a ComfyUI restart.
  - When a request ends (finished, or the browser went away), a background
    task downloads any ranges that are still missing (8 MiB slices, at most
    two files at once) from the same remote target.
  - Once every byte is present the .tmp file is renamed to its final name
    and the .map file is removed.

Responses that aren't a raw slice of the original file (the frontend's
?preview= / ?channel= thumbnails, Content-Encoding'd bodies, errors) are
never written into the cache; a thumbnail request just triggers the
background download of the real file instead.
"""

import asyncio
import json
import logging
import mimetypes
import os
import re
import threading
import time
from email.utils import parsedate_to_datetime
from urllib.parse import urlencode, urlsplit

import aiohttp
import folder_paths
from aiohttp import web

from . import config as cfgmod
from . import forwarder

logger = logging.getLogger("ComfyUIProxy")

_DIR_GETTERS = {
    "output": folder_paths.get_output_directory,
    "input": folder_paths.get_input_directory,
}
_FILL_QUERY_KEYS = ("filename", "type", "subfolder")  # what a background download needs
_TRANSFORM_KEYS = ("preview", "channel")  # /view params that change the response body

FILL_SLICE = 8 * 1024 * 1024          # bytes requested per background Range request
MAP_FLUSH_BYTES = 16 * 1024 * 1024    # persist the .map at least this often while writing
MAX_CONCURRENT_FILLS = 2
FILL_DELAY = 2.0                      # quiet period (s) before a background download starts, so a
                                      # video player's follow-up Range requests aren't fetched twice
SCAN_DELAY = 3.0                      # quiet period (s) after the last finished file before the local
                                      # asset scan is requested, so a burst becomes one scan
SERVE_CHUNK = 256 * 1024              # bytes read from the .tmp file per write to the browser

_CONTENT_RANGE_RE = re.compile(r"^\s*bytes\s+(\d+)-(\d+)/(\d+)\s*$", re.I)

_entries = {}        # final path -> _Entry (only while a download is in progress)
_fill_tasks = set()
_fill_sem = None     # created lazily, inside the running event loop
_scan_roots = set()  # "input"/"output" folders with new files waiting for a local asset scan
_scan_timer = None   # the pending call_later() that will request it


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

def local_path_for(query):
    """Final on-disk path for a /view query, or None when this request isn't
    one the cache handles (other type, no filename, or a path that would
    escape ComfyUI's output/input directory)."""
    getter = _DIR_GETTERS.get(query.get("type", "output"))  # /view defaults to "output"
    filename = query.get("filename")
    if getter is None or not filename or "\0" in filename or "\0" in (query.get("subfolder") or ""):
        return None
    try:
        filename = os.path.basename(filename)  # same as ComfyUI's own /view
        base = os.path.abspath(getter())
        subfolder = query.get("subfolder", "") or ""
        path = os.path.abspath(os.path.join(base, subfolder, filename))
    except (ValueError, OSError):  # e.g. NUL bytes
        return None
    # Accept only a path strictly inside `base`: normalized above (so any ".."
    # or absolute `subfolder` is already resolved), then compared with a plain
    # startswith() against `base` plus a trailing separator. That also rejects
    # `base` itself, and a sibling like "<base>-other". This is the form code
    # scanning (CodeQL's py/path-injection) recognizes as a safe check; on
    # Windows an escape that comes back with different letter case is refused
    # too, which is the safe direction.
    if not path.startswith(os.path.join(base, "")):
        return None
    return path


# ---------------------------------------------------------------------------
# Byte-range bookkeeping
# ---------------------------------------------------------------------------

def _add_range(ranges, start, end):
    """Merge [start, end) into a sorted list of disjoint, non-adjacent ranges."""
    if end <= start:
        return ranges
    out = []
    placed = False
    for a, b in ranges:
        if b < start:
            out.append((a, b))
        elif a > end:
            if not placed:
                out.append((start, end))
                placed = True
            out.append((a, b))
        else:  # overlaps or touches
            start = min(start, a)
            end = max(end, b)
    if not placed:
        out.append((start, end))
    return out


def _parse_response(status, headers):
    """(offset, total_or_None, etag, last_modified) when the response body is
    a raw, unencoded slice of the original file; None otherwise."""
    encoding = (headers.get("Content-Encoding") or "").strip().lower()
    if encoding and encoding != "identity":
        return None
    etag = headers.get("ETag")
    last_modified = headers.get("Last-Modified")
    if status == 206:
        m = _CONTENT_RANGE_RE.match(headers.get("Content-Range") or "")
        if not m:
            return None
        start, end, total = (int(x) for x in m.groups())
        if end < start or end >= total:
            return None
        return start, total, etag, last_modified
    if status == 200:
        length = headers.get("Content-Length")
        total = int(length) if length and length.isdigit() else None
        return 0, total, etag, last_modified
    return None


class _Entry:
    """Download state for one file: <path>.tmp + <path>.map on disk."""

    def __init__(self, path):
        self.path = path
        self.tmp = path + ".tmp"
        self.map_path = path + ".map"
        self.total = None
        self.etag = None
        self.last_modified = None
        self.ranges = []
        self.failed = False
        self.done = False
        self.fill_task = None
        self.fill_ctx = None  # (request path, [(k, v), ...], use_cpu)
        self.root = "output"  # which ComfyUI folder the file lives in: "input" or "output"
        self.streams = 0      # browser responses for this file being streamed right now
        self.served_logged = False  # said (once) that /view is answered from the partial file
        self.tlock = threading.Lock()  # serializes all access to the .tmp file
        self._unsaved = 0
        self.last_activity = time.monotonic()  # last time a browser request touched this file
        self._load()

    # -- persistence --------------------------------------------------------

    def _load(self):
        try:
            if not (os.path.isfile(self.tmp) and os.path.isfile(self.map_path)):
                return
            with open(self.map_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            total = int(data["total"])
            ranges = []
            for s, e in data.get("ranges", []):
                ranges = _add_range(ranges, int(s), int(e))
            if not ranges or ranges[-1][1] > total or os.path.getsize(self.tmp) < ranges[-1][1]:
                return
            self.total, self.ranges = total, ranges
            self.etag, self.last_modified = data.get("etag"), data.get("last_modified")
            logger.info(
                f"[ComfyUI Proxy] Resuming partial download of {self.path} "
                f"({self.covered()}/{total} bytes already on disk)."
            )
        except (OSError, ValueError, TypeError, KeyError):
            self.total, self.ranges = None, []

    def persist(self):
        if self.failed or self.done or not self.ranges or self.total is None:
            return
        try:
            with open(self.map_path, "w", encoding="utf-8") as f:
                json.dump(
                    {"version": 1, "total": self.total, "etag": self.etag,
                     "last_modified": self.last_modified, "ranges": [list(r) for r in self.ranges]},
                    f,
                )
            self._unsaved = 0
        except OSError as e:
            logger.warning(f"[ComfyUI Proxy] Could not write {self.map_path}: {e}")

    def reset(self):
        """Throw away whatever is on disk (the remote file changed)."""
        with self.tlock:
            for p in (self.tmp, self.map_path):
                try:
                    os.remove(p)
                except OSError:
                    pass
        self.total = self.etag = self.last_modified = None
        self.ranges = []
        self._unsaved = 0

    def adopt(self, total, etag, last_modified):
        """Take a response's identity (size/ETag/Last-Modified); restart the
        download if it doesn't match what's already on disk."""
        changed = (
            (self.total is not None and total is not None and total != self.total)
            or (self.etag and etag and etag != self.etag)
            or (self.last_modified and last_modified and last_modified != self.last_modified)
        )
        if changed and self.ranges:
            logger.info(f"[ComfyUI Proxy] Remote copy of {self.path} changed; restarting its download.")
            self.reset()
        if total is not None:
            self.total = total
        if etag:
            self.etag = etag
        if last_modified:
            self.last_modified = last_modified

    # -- writing --------------------------------------------------------------

    def _write_blocking(self, offset, data):
        with self.tlock:
            os.makedirs(os.path.dirname(self.tmp), exist_ok=True)
            with open(self.tmp, "r+b" if os.path.exists(self.tmp) else "w+b") as f:
                f.seek(offset)
                f.write(data)

    async def write_at(self, offset, data):
        if self.failed or self.done or not data:
            return
        try:
            await asyncio.to_thread(self._write_blocking, offset, data)
        except (OSError, ValueError) as e:
            self.failed = True
            logger.warning(f"[ComfyUI Proxy] Cannot write {self.tmp}: {e}; not caching this file.")
            return
        self.ranges = _add_range(self.ranges, offset, offset + len(data))
        self._unsaved += len(data)
        if self._unsaved >= MAP_FLUSH_BYTES:
            self.persist()

    def covered(self):
        return sum(e - s for s, e in self.ranges)

    def read_at(self, offset, size):
        """Bytes [offset, offset + size) if they are on disk, else None. Reads the
        .tmp file, or the final file if the download finished meanwhile. The lock
        keeps finalize() and reset() from moving the file during a read, and no
        handle stays open between reads (an open file blocks the rename on Windows)."""
        with self.tlock:
            if not any(s <= offset and offset + size <= e for s, e in self.ranges):
                return None
            for p in (self.tmp, self.path):
                try:
                    with open(p, "rb") as f:
                        f.seek(offset)
                        data = f.read(size)
                except OSError:
                    continue
                return data if len(data) == size else None
        return None

    def complete(self):
        return self.total is not None and self.ranges == [(0, self.total)]

    def next_gap(self):
        """Next missing [start, end) slice (at most FILL_SLICE long), or None."""
        if self.total is None:
            return (0, FILL_SLICE)
        prev = 0
        for s, e in self.ranges:
            if s > prev:
                return (prev, min(s, prev + FILL_SLICE))
            prev = max(prev, e)
        if prev < self.total:
            return (prev, min(self.total, prev + FILL_SLICE))
        return None

    def finalize(self):
        if self.done:
            return True
        with self.tlock:
            try:
                with open(self.tmp, "r+b") as f:
                    f.truncate(self.total)
                if os.path.exists(self.path):  # someone else created it meanwhile; keep theirs
                    os.remove(self.tmp)
                else:
                    os.replace(self.tmp, self.path)
            except OSError as e:
                self.failed = True
                logger.warning(f"[ComfyUI Proxy] Could not finish caching {self.path}: {e}")
                return False
        try:
            os.remove(self.map_path)
        except OSError:
            pass
        self.done = True
        _entries.pop(self.path, None)
        logger.info(f"[ComfyUI Proxy] Saved {self.path} locally ({self.total} bytes).")
        _queue_asset_scan(self.root)
        return True


# ---------------------------------------------------------------------------
# Local asset catalogue: ask ComfyUI's own seeder to index the new file
# ---------------------------------------------------------------------------

def _queue_asset_scan(root):
    """A file just landed in the local input/output folder. ComfyUI's asset
    system (the catalogue the Media Assets panel reads) only learns about
    files by scanning, and a scan normally follows a *local* job — ours run
    remotely, so nothing would ever trigger one. Queue a scan of that folder,
    batched: a burst of finished downloads (a panel full of thumbnails) ends
    up as a single scan."""
    global _scan_timer
    if root not in ("input", "output"):
        return
    _scan_roots.add(root)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        _run_asset_scan()
        return
    if _scan_timer is not None:
        _scan_timer.cancel()
    _scan_timer = loop.call_later(SCAN_DELAY, _run_asset_scan)


def _run_asset_scan():
    global _scan_timer
    _scan_timer = None
    roots = tuple(r for r in ("input", "output") if r in _scan_roots)
    _scan_roots.clear()
    if not roots:
        return
    try:
        from app.assets.seeder import ScanPhase, asset_seeder
        from comfy.cli_args import args
    except Exception:
        return  # this ComfyUI has no asset system
    try:
        if asset_seeder.is_disabled():  # --disable-assets
            return
        # Queued, not forced: if a scan is already running the roots are merged into it.
        asset_seeder.enqueue_scan(
            roots=roots,
            phase=ScanPhase.FULL,
            compute_hashes=bool(getattr(args, "enable_asset_hashing", False)),
        )
        logger.info(f"[ComfyUI Proxy] Asked ComfyUI to index the new files in your local {' and '.join(roots)} folder.")
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] Could not queue a local asset scan: {type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# Tee hook used by forwarder.forward_http
# ---------------------------------------------------------------------------

class _Sink:
    """Passed to forward_http: sees the response as it streams to the browser."""

    def __init__(self, entry, transformed):
        self.entry = entry
        self.transformed = transformed
        self.status = 0
        self.wanted = False  # a successful response: worth finishing in the background
        self.tee = False     # this response's bytes can be written into the cache directly
        self.pos = 0
        self.counted = False  # holds a count on entry.streams until end()

    def begin(self, status, headers):
        e = self.entry
        self.status = status
        self.wanted = status in (200, 206) and not e.failed and not e.done
        if self.wanted:
            e.streams += 1
            self.counted = True
        if not self.wanted or self.transformed:
            return False
        parsed = _parse_response(status, headers)
        if parsed is None:
            return False
        offset, total, etag, last_modified = parsed
        e.adopt(total, etag, last_modified)
        self.pos = offset
        self.tee = True
        e.last_activity = time.monotonic()
        return True

    async def write(self, chunk):
        if self.tee:
            self.entry.last_activity = time.monotonic()
            await self.entry.write_at(self.pos, chunk)
            self.pos += len(chunk)

    def end(self, complete):
        """Synchronous on purpose: runs in a `finally`, possibly while the
        request is being cancelled."""
        e = self.entry
        if self.counted:
            self.counted = False
            e.streams = max(0, e.streams - 1)
        if e.failed or e.done:
            return
        e.last_activity = time.monotonic()
        if self.tee and complete and self.status == 200 and e.total is None:
            e.total = self.pos  # chunked body with no Content-Length: clean EOF = whole file
        if e.complete():
            e.finalize()
            return
        e.persist()
        if self.wanted:
            _start_fill(e)


def _note_request(entry, request, use_cpu):
    """Remember which /view request (and which target) a background download repeats."""
    query = request.rel_url.query
    entry.root = query.get("type", "output")
    entry.fill_ctx = (
        request.rel_url.path,
        [(k, query[k]) for k in _FILL_QUERY_KEYS if k in query],
        use_cpu,
    )


def make_sink(path, request, use_cpu):
    entry = _entries.get(path)
    if entry is None:
        entry = _entries[path] = _Entry(path)
    _note_request(entry, request, use_cpu)
    return _Sink(entry, transformed=any(k in request.rel_url.query for k in _TRANSFORM_KEYS))


# ---------------------------------------------------------------------------
# Answering from the part of a file that is already on disk
# ---------------------------------------------------------------------------

def _split_range(header):
    """(first, last) digit strings from a single "bytes=a-b" Range header, else
    None. Plain string methods instead of a regex: the header comes from the
    browser, and a pattern with adjacent optional-whitespace and digit groups
    backtracks quadratically on it (CodeQL py/polynomial-redos)."""
    if not header or len(header) > 200:
        return None
    unit, eq, spec = header.partition("=")
    if not eq or unit.strip().lower() != "bytes":
        return None
    first, dash, last = spec.partition("-")
    if not dash:
        return None
    first, last = first.strip(), last.strip()
    for part in (first, last):
        if part and not (part.isascii() and part.isdigit()):
            return None  # also rejects several ranges ("0-1,3-4")
    return first, last


def _parse_range(header, total):
    """(start, end, open_ended), as [start, end), for a Range header that asks for
    one byte range of a file of `total` bytes. None for anything else (several
    ranges, a start past the end, a bad header): the remote decides those."""
    parts = _split_range(header)
    if not parts or not (parts[0] or parts[1]):
        return None
    first, last = parts
    if not first:  # "bytes=-N": the last N bytes
        n = int(last)
        return (max(0, total - n), total, False) if n > 0 else None
    start = int(first)
    if start >= total:
        return None
    if not last:  # "bytes=N-": everything from N on
        return start, total, True
    if int(last) < start:
        return None
    return start, min(int(last) + 1, total), False


def _etag_matches(header, etag):
    """If-None-Match: is our ETag in the browser's list? (weak comparison)"""
    if not etag:
        return False
    if header.strip() == "*":
        return True

    def bare(tag):
        tag = tag.strip()
        return tag[2:] if tag.startswith("W/") else tag

    return any(bare(t) == bare(etag) for t in header.split(","))


def _not_modified_since(since, last_modified):
    """If-Modified-Since: did the file last change no later than the browser's date?"""
    if not since or not last_modified:
        return False
    try:
        return parsedate_to_datetime(last_modified) <= parsedate_to_datetime(since)
    except (TypeError, ValueError):
        return False


def _if_range_matches(value, etag, last_modified):
    """If-Range: the browser wants the range only if the file is still the one it
    has. Needs a strong ETag, or exactly the Last-Modified date."""
    value = value.strip()
    if value.startswith("W/"):
        return False
    if value.startswith('"'):
        return bool(etag) and not etag.startswith("W/") and value == etag
    return bool(last_modified) and value == last_modified


def _guess_type(path):
    return mimetypes.guess_type(path)[0] or "application/octet-stream"


async def serve_partial(request, path, use_cpu=False, remote_awake=False):
    """Answer a /view request from the part of the file that is already on disk,
    so the remote is not woken for bytes this computer has. Returns the response,
    or None when the request must go to the remote as before.

    A HEAD request gets the headers of the whole file (200, with the size saved in
    the .map file) and no body. No byte has to be on disk for that, and Range is
    ignored, because it is defined for GET only (RFC 9110).

    A GET qualifies only with a Range header (one without it wants the whole file,
    which is not here yet), and only when every byte it asks for is on disk.
    "bytes=N-", which is what video players send, gets the stretch from N to the
    end of the part that is there, as a shorter 206; the player then asks for the
    rest, and only that request reaches the remote.

    The ETag / Last-Modified the remote gave when the bytes were saved (kept in
    the .map file) answer If-None-Match, If-Modified-Since and If-Range, so a
    browser that has a copy gets its 304 here too.

    `remote_awake`: a job is running, so the remote is up anyway; let the
    background download go on with the missing parts."""
    if request.method not in ("GET", "HEAD"):
        return None
    if any(k in request.rel_url.query for k in _TRANSFORM_KEYS):
        return None  # the remote makes a thumbnail from the whole file
    entry = _entries.get(path)
    if entry is None:
        if not (os.path.isfile(path + ".tmp") and os.path.isfile(path + ".map")):
            return None
        entry = _entries[path] = _Entry(path)  # reads the .map file
    if entry.failed or entry.done or entry.total is None or not entry.ranges:
        return None

    h = request.headers
    validators = {}
    if entry.etag:
        validators["ETag"] = entry.etag
    if entry.last_modified:
        validators["Last-Modified"] = entry.last_modified
    if "If-None-Match" in h:
        not_modified = _etag_matches(h["If-None-Match"], entry.etag)
    else:
        not_modified = _not_modified_since(h.get("If-Modified-Since"), entry.last_modified)
    if not_modified:
        return web.Response(status=304, headers=validators)

    if request.method == "HEAD":  # headers only: nothing is read, and nothing is downloaded
        resp = web.StreamResponse(status=200, headers={
            **validators,
            "Content-Type": _guess_type(path),
            "Accept-Ranges": "bytes",
            "Content-Length": str(entry.total),
        })
        try:
            await resp.prepare(request)
            await resp.write_eof()
        except ConnectionError:  # the browser went away
            pass
        return resp

    wanted = _parse_range(h.get("Range"), entry.total)
    if wanted is None:
        return None
    if "If-Range" in h and not _if_range_matches(h["If-Range"], entry.etag, entry.last_modified):
        return None
    start, end, open_ended = wanted
    run_end = next((e for s, e in entry.ranges if s <= start < e), None)  # end of the stretch that has `start`
    if run_end is None or (run_end < end and not open_ended):
        return None
    end = min(end, run_end)

    if not entry.served_logged:
        entry.served_logged = True
        logger.info(
            f"[ComfyUI Proxy] Answering /view of {entry.path} from its partly downloaded copy "
            f"(bytes {start}-{end - 1} of {entry.total}); the remote is not asked for them."
        )
    entry.last_activity = time.monotonic()
    if remote_awake:
        _note_request(entry, request, use_cpu)
        _start_fill(entry)

    headers = {
        **validators,
        "Content-Type": _guess_type(path),
        "Accept-Ranges": "bytes",
        "Content-Range": f"bytes {start}-{end - 1}/{entry.total}",
        "Content-Length": str(end - start),
    }
    resp = web.StreamResponse(status=206, headers=headers)
    try:
        await resp.prepare(request)
        pos = start
        while pos < end:
            chunk = await asyncio.to_thread(entry.read_at, pos, min(SERVE_CHUNK, end - pos))
            if chunk is None:  # the file changed under us
                break
            await resp.write(chunk)
            pos += len(chunk)
            entry.last_activity = time.monotonic()
        if pos >= end:
            await resp.write_eof()
        elif request.transport is not None:
            request.transport.close()  # cut short; the browser asks again, and that goes to the remote
    except ConnectionError:  # the browser went away (a player dropping a Range request)
        logger.debug(f"[ComfyUI Proxy] Browser closed the connection while serving {entry.path} locally.")
    return resp


# ---------------------------------------------------------------------------
# Background download of whatever the browser's requests didn't cover
# ---------------------------------------------------------------------------

def _start_fill(entry):
    if entry.fill_task is not None and not entry.fill_task.done():
        return
    task = asyncio.ensure_future(_fill(entry))
    entry.fill_task = task
    _fill_tasks.add(task)
    task.add_done_callback(_fill_tasks.discard)


def cancel_all():
    """Stop every background download (settings turned off / endpoint changed
    / state reset). Partial files and their .map stay for a later resume."""
    for task in list(_fill_tasks):
        task.cancel()


def has_active_downloads(use_cpu=False):
    """True while a viewed file that isn't fully local yet is still coming
    from the given target (the GPU by default): a browser response is being
    streamed into it, or its background download is queued or running.
    relay.py keeps its connection to the GPU open for as long as this is
    true, so /view keeps being forwarded and the GPU stays up until the file
    is safely on disk. A failed or finished file never counts, so this can't
    hold the connection open forever."""
    for e in _entries.values():
        if e.failed or e.done or e.fill_ctx is None or e.fill_ctx[2] != use_cpu:
            continue
        if e.streams > 0 or (e.fill_task is not None and not e.fill_task.done()):
            return True
    return False


# ---------------------------------------------------------------------------
# "Auto-download jobs output": files a job wrote, fetched as soon as it finishes
# ---------------------------------------------------------------------------
# relay.py passes every remote websocket message to note_job_message(), which
# remembers the output files that the "executed" messages of each job report.
# When the job is done, download_job_outputs() hands every one of them that is
# not on this computer yet to the same background download the viewed files
# use. Only jobs seen finishing here are downloaded, so old jobs are left alone.

MAX_TRACKED_JOBS = 200  # a job that never finishes can't make this grow forever
_job_outputs = {}       # prompt_id -> {(subfolder, filename), ...}


def downloads_enabled():
    """True while the proxy is on and at least one auto-download setting is."""
    return bool(cfgmod.get("enabled")) and bool(
        cfgmod.get("auto_download_viewed") or cfgmod.get("auto_download_job_outputs")
    )


def _cpu_target_configured():
    """Same rule as server_hooks: a CPU URL that differs from the GPU URL."""
    cpu_url = (cfgmod.get("remote_cpu_url") or "").rstrip("/")
    return bool(cpu_url) and cpu_url != (cfgmod.get("remote_url") or "").rstrip("/")


def note_job_message(parsed):
    """Remember the files an "executed" message says a job saved to the output folder."""
    if not isinstance(parsed, dict) or parsed.get("type") != "executed":
        return
    if not cfgmod.get("auto_download_job_outputs"):
        return
    data = parsed.get("data")
    if not isinstance(data, dict):
        return
    prompt_id, output = data.get("prompt_id"), data.get("output")
    if not prompt_id or not isinstance(output, dict):
        return
    for items in output.values():  # "images", "gifs", ... (each a list of file descriptions)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict) or item.get("type") != "output":
                continue  # not a saved file (a temp preview, or not a file at all)
            name, subfolder = item.get("filename"), item.get("subfolder") or ""
            if isinstance(name, str) and name and isinstance(subfolder, str):
                _job_outputs.setdefault(prompt_id, set()).add((subfolder, name))
    while len(_job_outputs) > MAX_TRACKED_JOBS:
        _job_outputs.pop(next(iter(_job_outputs)))


def download_job_outputs(prompt_id=None):
    """A job finished: start downloading the output files it reported that are
    not on this computer yet. Without a prompt_id, do it for every job noted so
    far. Returns how many downloads were started."""
    ids = [prompt_id] if prompt_id else list(_job_outputs)
    files = set()
    for pid in ids:
        files |= _job_outputs.pop(pid, set())
    if not files:
        return 0
    if not (cfgmod.get("enabled") and cfgmod.get("remote_url") and cfgmod.get("auto_download_job_outputs")):
        return 0
    use_cpu = _cpu_target_configured()  # the same file-only target /view uses
    started = 0
    for subfolder, filename in sorted(files):
        query = {"filename": filename, "type": "output", "subfolder": subfolder}
        path = local_path_for(query)
        if path is None or os.path.exists(path):  # unsafe path, or already here: keep it as is
            continue
        entry = _entries.get(path)
        if entry is None:
            entry = _entries[path] = _Entry(path)
        entry.root = "output"
        entry.fill_ctx = ("/view", [(k, query[k]) for k in _FILL_QUERY_KEYS], use_cpu)
        entry.last_activity = time.monotonic()
        _start_fill(entry)
        started += 1
    if started:
        logger.info(f"[ComfyUI Proxy] Downloading {started} output file(s) of the finished job to your local output folder.")
    return started


async def _fill(entry):
    global _fill_sem
    if _fill_sem is None:
        _fill_sem = asyncio.Semaphore(MAX_CONCURRENT_FILLS)
    try:
        # Wait for the browser to go quiet first: a video player typically
        # follows one Range request with others, which the tee already
        # captures for free.
        while not (entry.failed or entry.done):
            wait = FILL_DELAY - (time.monotonic() - entry.last_activity)
            if wait <= 0:
                break
            await asyncio.sleep(wait)
        async with _fill_sem:
            await _fill_inner(entry)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"[ComfyUI Proxy] Background download of {entry.path} failed: {type(e).__name__}: {e}")
    finally:
        entry.persist()


async def _fill_inner(entry):
    if entry.fill_ctx is None:
        return
    req_path, query, use_cpu = entry.fill_ctx
    base = forwarder.target_base(use_cpu)
    if not base:
        return
    parts = urlsplit(base)
    url = f"{parts.scheme}://{parts.netloc}{req_path}?{urlencode(query)}"
    headers = {"Accept-Encoding": "identity"}  # we need the raw file bytes
    auth_key = cfgmod.get("remote_cpu_auth_key" if use_cpu else "auth_key")
    if auth_key:
        headers["Authorization"] = f"Bearer {auth_key}"
    t = forwarder.get_timeout()
    # Slices are small, so bound each read instead of the whole request.
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=t.sock_connect, sock_read=min(t.total, 120))
    session = forwarder.get_session()

    stalls = 0
    while not (entry.failed or entry.done):
        if not downloads_enabled():
            return
        if forwarder.circuit_is_open(use_cpu):
            return  # remote is known to be unresponsive; try again on the next view
        gap = entry.next_gap()
        if gap is None:
            break
        start, end = gap
        before = entry.covered()
        try:
            async with session.get(
                url, headers={**headers, "Range": f"bytes={start}-{end - 1}"},
                timeout=timeout, allow_redirects=False,
            ) as r:
                if r.status == 416 and entry.ranges:  # remote file shrank/changed
                    entry.reset()
                    return
                parsed = _parse_response(r.status, r.headers)
                if parsed is None:
                    logger.warning(
                        f"[ComfyUI Proxy] Background download of {entry.path} stopped: "
                        f"unexpected response (HTTP {r.status})."
                    )
                    return
                offset, total, etag, last_modified = parsed
                entry.adopt(total, etag, last_modified)
                if r.status == 206 and offset != start:
                    return
                pos = offset
                async for chunk in r.content.iter_chunked(256 * 1024):
                    if entry.failed or entry.done:
                        return
                    await entry.write_at(pos, chunk)
                    pos += len(chunk)
                if r.status == 200 and total is None:
                    entry.total = pos
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(
                f"[ComfyUI Proxy] Background download of {entry.path} interrupted "
                f"({type(e).__name__}: {e}); will resume the next time it is viewed."
            )
            return
        if entry.complete():
            break
        if entry.covered() <= before:
            stalls += 1
            if stalls >= 2:
                logger.warning(f"[ComfyUI Proxy] Background download of {entry.path} is not making progress; giving up.")
                return
        else:
            stalls = 0
    if entry.complete():
        entry.finalize()
