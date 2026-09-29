"""
ComfyUI Proxy
=============
Forwards job-related ComfyUI API/WebSocket traffic to a configurable cloud/
serverless GPU endpoint, while workflow editing (node graph, /object_info,
static assets, etc.) keeps running against the local server.

Importing `proxy.server_hooks` registers:
  - an aiohttp middleware that intercepts and forwards the relevant routes
  - local-only REST endpoints under /comfyui_proxy/* used by the UI panel
"""

from .proxy import server_hooks  # noqa: F401  (import triggers registration)

WEB_DIRECTORY = "./web"
NODE_CLASS_MAPPINGS = {}
NODE_DISPLAY_NAME_MAPPINGS = {}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS", "WEB_DIRECTORY"]
