from __future__ import annotations

import base64
import secrets

from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Receive, Scope, Send

_EXEMPT_PATHS = ("/healthz",)


class BasicAuthMiddleware:
    """HTTP Basic Auth gate. Active only when `config.auth_user` is set.

    Uses secrets.compare_digest for both the username and password to avoid
    timing side-channels. Exempts /healthz so uptime checks don't need
    credentials.
    """

    def __init__(self, app: ASGIApp, auth_user: str, auth_password: str) -> None:
        self.app = app
        self.auth_user = auth_user
        self.auth_password = auth_password or ""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] in _EXEMPT_PATHS:
            await self.app(scope, receive, send)
            return

        if self._check_authorized(scope):
            await self.app(scope, receive, send)
            return

        response = PlainTextResponse(
            "Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="dramatiq-monitor"'},
        )
        await response(scope, receive, send)

    def _check_authorized(self, scope: Scope) -> bool:
        headers = dict(scope.get("headers") or [])
        header = headers.get(b"authorization")
        if not header:
            return False

        try:
            scheme, _, credentials = header.decode().partition(" ")
            if scheme.lower() != "basic":
                return False
            decoded = base64.b64decode(credentials).decode()
            user, _, password = decoded.partition(":")
        except (ValueError, UnicodeDecodeError):
            return False

        user_ok = secrets.compare_digest(user, self.auth_user)
        password_ok = secrets.compare_digest(password, self.auth_password)
        return user_ok and password_ok
