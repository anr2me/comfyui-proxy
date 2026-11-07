"""
Persisted configuration for ComfyUI Proxy.

Stored as JSON next to the node package so it survives restarts. All reads/
writes go through this module so the rest of the plugin never touches the
file directly.
"""

import json
import logging
import math
import os
import threading
from urllib.parse import urlsplit

_PACKAGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(_PACKAGE_DIR, "proxy_config.json")
# Saved job history (see jobs_db.py). Kept in the node folder, like the config,
# so a fork that changes the database layout keeps its own separate file.
JOBS_DB_PATH = os.path.join(_PACKAGE_DIR, "jobs_history.db")

# Fake GPU reported to the Crystools monitor (or its MonitorOnly fork), to
# comfyui-resource-monitor, ComfyUI-KikoStats, ComfyUI Advanced Monitor,
# Feixue Universal Monitor and ComfyUI Vitals when their GPU requests stay
# local and this machine has no GPU (see server_hooks.py).
# Edit here; deliberately not a saved setting and not shown in the UI panel.
GPU_NAME = "L4"   # reported as "NVIDIA L4"
GPU_COUNT = 1     # Crystools, Advanced Monitor and Vitals; the others show a single GPU
GPU_VRAM_GB = 24  # the total VRAM comfyui-resource-monitor, KikoStats, Advanced Monitor, Feixue and Vitals show for it (an L4 has 24 GB)

# Placeholder jobs shown in the UI's queue while a prompt waits for the remote GPU
# to wake up (see placeholders.py) normally disappear as soon as that submission
# finishes, fails or is cancelled. As a safety net they also expire on their own:
# after the Timeout setting plus this many seconds. Edit here; not shown in the UI.
QUEUE_PLACEHOLDER_MARGIN = 60

# The live progress stream to the remote (see relay.py) is closed when this client has
# had no job on the remote for this many seconds, so a stream that nothing will ever end
# (a refused or lost prompt, for example) cannot keep a serverless GPU awake. 0 turns
# this off. Edit here; not shown in the UI.
IDLE_STREAM_TIMEOUT = 60

DEFAULT_CONFIG = {
    "enabled": False,          # forwarder is OFF by default
    "remote_url": "",          # the GPU endpoint, e.g. https://my-endpoint.runpod.net
    "timeout": 300,            # seconds; configurable in UI — serverless cold
                                # boots can take a long time when GPU capacity
                                # is scarce, so this defaults high rather than low
    "auth_key": "",            # optional bearer token sent to the GPU endpoint
    "remote_cpu_url": "",      # optional: a CPU-only container sharing the same
                                # persistent volume as the GPU one, used instead
                                # of it for file-only routes (uploads, /view,
                                # /viewvideo) whenever the GPU isn't already
                                # active, so those don't needlessly wake it
    "remote_cpu_auth_key": "",  # optional bearer token sent to the CPU endpoint
    "post_completion_delay": 5,  # seconds to keep the progress relay open after
                                  # a job finishes, so the UI's progress bar/log
                                  # animations have time to reach 100% before we
                                  # tear the shadow connection down
    "jobs_history_max_entries": 500,  # finished jobs to remember in jobs_history.db
                                       # per remote URL (oldest dropped first)
    "auto_download_job_outputs": True,  # right after a job finishes, save the files
                                        # it wrote (type=output) into the local output
                                        # folder (see viewcache.py). Only jobs that finish
                                        # while the proxy watches: old jobs are never
                                        # downloaded, unlike auto_download_viewed. On by
                                        # default; the panel can turn it off.
    "auto_download_viewed": True,   # save files viewed via /view (type=output/
                                    # input) into the local output/input folder
                                    # and serve them from there afterwards (see
                                    # viewcache.py), so re-rendering thumbnails
                                    # doesn't wake the remote again. On by
                                    # default; the panel can turn it off. It also
                                    # downloads old outputs that get viewed.
    "gpu_keepalive_enabled": False,   # opt-in: extend the shadow relay's own
                                       # open websocket connection to the GPU
                                       # while /view or /viewvideo activity
                                       # continues, so long video playback
                                       # survives past the normal post-job
                                       # grace window without a separate CPU
                                       # target. Has a real cost — off by
                                       # default. (A separate periodic ping
                                       # doesn't work for this: the instant a
                                       # ping request completes, the platform
                                       # sees the container idle again — an
                                       # open, pending connection is what
                                       # actually counts as "busy".)
    "gpu_keepalive_idle_timeout": 20,  # stop extending after this many
                                        # seconds with no new /view or
                                        # /viewvideo request
    "circuit_breaker_cooldown": 70,  # seconds to pause automatic /queue and
                                      # /api/jobs polling after the remote
                                      # fails to respond, so repeated polling
                                      # doesn't keep it looking "active" to
                                      # the serverless platform's own idle-
                                      # timeout. Different providers scale
                                      # down after very different idle
                                      # windows, so this is tunable per setup.
    "circuit_breaker_max_failures": 2,  # after this many consecutive failures,
                                         # stop automatic /queue and /api/jobs
                                         # polling entirely (instead of retrying
                                         # again after every cooldown) until a
                                         # /prompt succeeds or state is reset.
                                         # 0 = never latch; cooldown only.
    "models_cache": None,      # dict of {"NodeName.param": [model, ...]} pulled once from remote
    "models_cache_url": None,  # remote_url the cache was pulled from (detects URL changes)
}

_lock = threading.RLock()
_config = None


def load_config():
    global _config
    with _lock:
        if _config is not None:
            return _config
        if os.path.exists(CONFIG_PATH):
            try:
                with open(CONFIG_PATH, "r") as f:
                    data = json.load(f)
                _config = {**DEFAULT_CONFIG, **data}
            except Exception as e:
                print(f"[ComfyUI Proxy] Failed to read config, using defaults: {e}")
                _config = dict(DEFAULT_CONFIG)
        else:
            _config = dict(DEFAULT_CONFIG)
        return _config


def save_config(cfg):
    global _config
    with _lock:
        _config = cfg
        try:
            # The file holds the auth keys, so only its owner may read it (0600).
            # The mode applies when the file is created: chmod() fixes an older one.
            fd = os.open(CONFIG_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                os.chmod(CONFIG_PATH, 0o600)
            except OSError:
                pass  # e.g. a file system without permissions: still save the config
            with os.fdopen(fd, "w") as f:
                json.dump(cfg, f, indent=2)
        except Exception as e:
            print(f"[ComfyUI Proxy] Failed to persist config: {e}")


# Each auth key belongs to one URL setting: URL setting -> (its auth key, label for the log).
_KEY_FOR_URL = {
    "remote_url": ("auth_key", "GPU"),
    "remote_cpu_url": ("remote_cpu_auth_key", "CPU"),
}


def _host_of(url) -> str:
    """The host and port of a URL, in lower case and without any user:password.
    A URL typed without its scheme is understood too. "" if there is no URL."""
    url = url.strip() if isinstance(url, str) else ""
    if not url:
        return ""
    try:
        parts = urlsplit(url if "//" in url else "//" + url)
        return f"{(parts.hostname or '').lower()}:{parts.port or ''}"
    except ValueError:
        return url.lower()  # not a valid URL: it counts as a different host


def _orphaned_keys(cfg: dict, patch: dict) -> dict:
    """The auth keys to clear because `patch` points their URL at another host
    without bringing a new key. A key is meant for one server: without this, a
    changed URL would make the proxy send the saved key to a host that never
    received it."""
    cleared = {}
    for url_name, (key_name, _label) in _KEY_FOR_URL.items():
        if url_name not in patch or key_name in patch or not cfg.get(key_name):
            continue
        if _host_of(patch[url_name]) != _host_of(cfg.get(url_name)):
            cleared[key_name] = ""
    return cleared


def update_config(patch: dict):
    with _lock:
        cfg = load_config()
        cleared = _orphaned_keys(cfg, patch)
        cfg = {**cfg, **patch, **cleared}
        save_config(cfg)
        for url_name, (key_name, label) in _KEY_FOR_URL.items():
            if key_name in cleared:
                logging.getLogger("ComfyUIProxy").info(
                    f"[ComfyUI Proxy] The Remote {label} URL now points to a different host, so the saved "
                    f"Remote {label} Auth Key was removed. Enter the key again if the new endpoint needs one."
                )
        return cfg


def get(key, default=None):
    return load_config().get(key, default)


def number(key: str, minimum=None, zero_is_valid: bool = False) -> float:
    """A numeric setting as a float: the saved value, or its default from
    DEFAULT_CONFIG when that is missing, empty, not a usable number — or 0,
    unless `zero_is_valid` (for settings where 0 means something) — and never
    below `minimum`. Everything that reads such a setting goes through here, so a
    default is written down once, in DEFAULT_CONFIG, and nowhere else."""
    default = float(DEFAULT_CONFIG[key])
    try:
        value = float(get(key))
    except (TypeError, ValueError):
        value = default
    else:
        if not math.isfinite(value) or (value == 0 and not zero_is_valid):
            value = default
    if minimum is not None:
        value = max(float(minimum), value)
    return value


def reset_to_defaults():
    save_config(dict(DEFAULT_CONFIG))
    return load_config()
