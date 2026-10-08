"""
Persisted configuration for ComfyUI Proxy.

Stored as JSON next to the node package so it survives restarts. All reads/
writes go through this module so the rest of the plugin never touches the
file directly.
"""

import json
import os
import threading

_PACKAGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(_PACKAGE_DIR, "proxy_config.json")

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
    "jobs_cache_max_entries": 64,  # max distinct /api/jobs path+query variants
                                    # to keep cached locally (oldest evicted first)
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
    "circuit_breaker_cooldown": 30,  # seconds to pause automatic /queue and
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
            with open(CONFIG_PATH, "w") as f:
                json.dump(cfg, f, indent=2)
        except Exception as e:
            print(f"[ComfyUI Proxy] Failed to persist config: {e}")


def update_config(patch: dict):
    with _lock:
        cfg = load_config()
        cfg = {**cfg, **patch}
        save_config(cfg)
        return cfg


def get(key, default=None):
    return load_config().get(key, default)


def reset_to_defaults():
    save_config(dict(DEFAULT_CONFIG))
    return load_config()
