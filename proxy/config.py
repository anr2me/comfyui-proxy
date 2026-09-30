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
    "remote_url": "",          # e.g. https://my-endpoint.runpod.net
    "timeout": 120,            # seconds; configurable in UI for slow cold boots
    "auth_key": "",            # optional bearer token sent to the remote
    "post_completion_delay": 5,  # seconds to keep the progress relay open after
                                  # a job finishes, so the UI's progress bar/log
                                  # animations have time to reach 100% before we
                                  # tear the shadow connection down
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
