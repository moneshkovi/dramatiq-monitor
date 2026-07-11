from __future__ import annotations

from starlette.responses import HTMLResponse, JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

_SAFE_METHODS = ("GET", "HEAD", "OPTIONS")


class ReadOnlyMiddleware:
    """When `read_only` is set, blocks any mutating request with 403.

    GET/HEAD/OPTIONS always pass through. Templates get `config.read_only`
    (already a global via the request's app.state.config) so they can skip
    rendering forms/buttons entirely; this middleware is the server-side
    backstop for direct POSTs.
    """

    def __init__(self, app: ASGIApp, read_only: bool) -> None:
        self.app = app
        self.read_only = read_only

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope["type"] != "http"
            or not self.read_only
            or scope["method"] in _SAFE_METHODS
        ):
            await self.app(scope, receive, send)
            return

        if scope["path"].startswith("/api/"):
            response = JSONResponse(
                {"ok": False, "error": "read-only mode: mutations are disabled"},
                status_code=403,
            )
        else:
            response = HTMLResponse(
                "<p class=\"flash\">Read-only mode: mutations are disabled.</p>",
                status_code=403,
            )
        await response(scope, receive, send)
