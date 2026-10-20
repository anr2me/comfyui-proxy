"""
Keeps the remote's address and the auth keys out of what this proxy logs, and out
of the error messages it shows in the UI: logs and screenshots are often pasted
into public bug reports.

Everything logged through the proxy's logger passes a logging filter that rewrites
the finished message, and any traceback, so that

    wss://abc-xyz.modal.run/ws?clientId=1   ->  wss://<RemoteGPU_Host>/ws?clientId=1
    Cannot connect to host abc-xyz.modal.run:443 ssl:default
                                            ->  Cannot connect to host <RemoteGPU_Host>:443 ssl:default
    Authorization: Bearer s3cr3t-token      ->  Authorization: Bearer <RemoteGPU_AuthKey>

Covered: the host (with its port) of the Remote GPU URL and of the Remote CPU URL
(<RemoteGPU_Host>, <RemoteCPU_Host>), both auth keys (<RemoteGPU_AuthKey>,
<RemoteCPU_AuthKey>, also percent-encoded) and any user:password written into a URL
(<RemoteGPU_Credentials>, <RemoteCPU_Credentials>). Values configured earlier in the
session stay covered after they are changed, since an error can still mention them.

The same filter sits on aiohttp's server logger and asyncio's, which is where an
error that nothing caught would be logged, with its message. It cannot cover lines
logged by ComfyUI or other custom nodes. sanitize_logs() is also used on the error
messages the proxy sends to the UI.

Values shorter than a few characters are left alone: replacing a 2-letter "key"
would mangle ordinary text more than it protects anything.
"""

import logging
import re
import threading
from urllib.parse import quote, urlsplit

from . import config as cfgmod

GPU_HOST = "<RemoteGPU_Host>"
CPU_HOST = "<RemoteCPU_Host>"
GPU_KEY = "<RemoteGPU_AuthKey>"
CPU_KEY = "<RemoteCPU_AuthKey>"
GPU_CREDENTIALS = "<RemoteGPU_Credentials>"
CPU_CREDENTIALS = "<RemoteCPU_Credentials>"

MIN_HOST_LEN = 3
MIN_SECRET_LEN = 4

# (setting holding a URL, placeholder for its host, placeholder for user:password in it)
_URL_SETTINGS = (
    ("remote_url", GPU_HOST, GPU_CREDENTIALS),
    ("remote_cpu_url", CPU_HOST, CPU_CREDENTIALS),
)
_KEY_SETTINGS = (("auth_key", GPU_KEY), ("remote_cpu_auth_key", CPU_KEY))

# Loggers that get the filter: ours, and the two where an unhandled error is logged.
LOGGERS = ("ComfyUIProxy", "aiohttp.server", "asyncio")

_lock = threading.Lock()
_hosts = {}    # host / host:port, lowercased -> placeholder
_secrets = {}  # key / password (and percent-encoded form) -> placeholder
_regex = {"size": -1, "compiled": None}


def _url_needles(url):
    """(host strings, credential strings) found in a URL. A URL typed without its
    scheme is understood too."""
    url = (url or "").strip() if isinstance(url, str) else ""
    if not url:
        return [], []
    try:
        parts = urlsplit(url if "//" in url else "//" + url)
        hosts = []
        if parts.netloc:
            hosts.append(parts.netloc.rpartition("@")[2])  # host[:port], without any user:password@
        if parts.hostname:
            hosts.append(parts.hostname)
            try:
                if parts.port:
                    hosts.append(f"{parts.hostname}:{parts.port}")
            except ValueError:
                pass
        return hosts, [c for c in (parts.username, parts.password) if c]
    except ValueError:
        return [], []


def _add_secret(value, placeholder):
    value = value.strip()
    if len(value) < MIN_SECRET_LEN:
        return
    for variant in {value, quote(value, safe="")}:
        _secrets.setdefault(variant, placeholder)


def remember():
    """Note the hosts and keys configured right now. Called by everything that is
    about to change them, and by every sanitize_logs(), so that a value the user
    has since replaced is still hidden in a late error message."""
    with _lock:
        for setting, host_placeholder, credentials_placeholder in _URL_SETTINGS:
            hosts, credentials = _url_needles(cfgmod.get(setting))
            for host in hosts:
                if len(host) >= MIN_HOST_LEN:
                    _hosts.setdefault(host.lower(), host_placeholder)
            for credential in credentials:
                _add_secret(credential, credentials_placeholder)
        for setting, placeholder in _KEY_SETTINGS:
            key = cfgmod.get(setting)
            if isinstance(key, str):
                _add_secret(key, placeholder)


def _compiled():
    """One pattern for all of it, longest first, so that "host:443" wins over
    "host" — and one pass, so a placeholder is never itself rewritten."""
    size = len(_hosts) + len(_secrets)
    if _regex["size"] != size:
        with _lock:
            alternatives = [(len(k), re.escape(k)) for k in _secrets]
            alternatives += [(len(h), "(?i:" + re.escape(h) + ")") for h in _hosts]
            alternatives.sort(key=lambda a: -a[0])
            _regex["compiled"] = re.compile("|".join(a for _, a in alternatives)) if alternatives else None
            _regex["size"] = size
    return _regex["compiled"]


def sanitize_logs(value) -> str:
    """`value` (an exception, a message, ...) as text, with the remotes' hosts and
    the auth keys replaced by placeholders."""
    text = value if isinstance(value, str) else str(value)
    try:
        remember()
    except Exception:
        pass  # the config couldn't be read: go on with what is already known
    pattern = _compiled()
    if pattern is None:
        return text
    return pattern.sub(lambda m: _secrets.get(m.group(0)) or _hosts.get(m.group(0).lower()) or m.group(0), text)


class SanitizeFilter(logging.Filter):
    """Rewrites each record's message and traceback with sanitize_logs(). If that
    fails, the line is withheld rather than let through unchecked."""

    def filter(self, record):
        try:
            try:
                message = record.getMessage()
            except Exception:
                message = f"{record.msg} {record.args}"
            record.msg = sanitize_logs(message)
            record.args = None
            if record.exc_info:
                formatted = record.exc_text or logging.Formatter().formatException(record.exc_info)
                record.exc_text = sanitize_logs(formatted)
                record.exc_info = None
            elif record.exc_text:
                record.exc_text = sanitize_logs(record.exc_text)
            if record.stack_info:
                record.stack_info = sanitize_logs(record.stack_info)
        except Exception:
            record.msg = "[ComfyUI Proxy] (a log line was withheld: it could not be checked for the remote's address or key)"
            record.args = None
            record.exc_info = None
            record.exc_text = None
            record.stack_info = None
        return True


def install():
    """Put the filter on the loggers above, once."""
    for name in LOGGERS:
        logger = logging.getLogger(name)
        if not any(isinstance(f, SanitizeFilter) for f in logger.filters):
            logger.addFilter(SanitizeFilter())


install()
