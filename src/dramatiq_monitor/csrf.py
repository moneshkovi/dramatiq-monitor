from __future__ import annotations

import hashlib
import hmac
import secrets
from urllib.parse import parse_qsl

from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

COOKIE_NAME = "dm_csrf"
_FIELD_NAME = "csrf_token"
_HEADER_NAME = "x-csrf-token"
_SAFE_METHODS = ("GET", "HEAD", "OPTIONS")


def make_token(secret: str) -> str:
    """`hmac_sha256(secret, nonce) + nonce` — stateless, no server storage."""
    nonce = secrets.token_hex(16)
    digest = hmac.new(secret.encode(), nonce.encode(), hashlib.sha256).hexdigest()
    return f"{digest}{nonce}"


def _verify_token(secret: str, token: str) -> bool:
    if not token or len(token) <= 64:
        return False
    digest, nonce = token[:64], token[64:]
    expected = hmac.new(secret.encode(), nonce.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(digest, expected)


class CSRFMiddleware:
    """Stateless double-submit-cookie CSRF protection.

    HTML GET responses get a `dm_csrf` cookie (token embedded server-side in
    a <meta name="csrf-token"> tag by base.html; forms carry it as a hidden
    field, htmx reads it via hx-headers). Non-GET page routes must present
    the same token via form field `csrf_token` or header `X-CSRF-Token`;
    mismatch/absent -> 403.

    `/api/*` is exempt from the token requirement, but non-GET `/api/*`
    requests must send `Content-Type: application/json` (403 otherwise).
    """

    def __init__(self, app: ASGIApp, secret: str) -> None:
        self.app = app
        self.secret = secret

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive=receive)
        method = request.method
        path = request.url.path
        is_api = path.startswith("/api/")

        if method in _SAFE_METHODS:
            await self._call_and_set_cookie(scope, receive, send, request, is_api)
            return

        if is_api:
            content_type = request.headers.get("content-type", "")
            if not content_type.startswith("application/json"):
                response = JSONResponse(
                    {"ok": False, "error": "Content-Type: application/json required"},
                    status_code=403,
                )
                await response(scope, receive, send)
                return
            await self.app(scope, receive, send)
            return

        # Non-GET, non-API "page" routes: require the CSRF token via the
        # cookie plus either the form field or the header.
        cookie_token = request.cookies.get(COOKIE_NAME, "")
        header_token = request.headers.get(_HEADER_NAME, "")

        body_bytes = await request.body()
        form_token = ""
        content_type = request.headers.get("content-type", "")
        if content_type.startswith("application/x-www-form-urlencoded"):
            # Parsed by hand (no python-multipart dep) — our forms are
            # simple key=value pairs, never file uploads.
            form_fields = dict(parse_qsl(body_bytes.decode(), keep_blank_values=True))
            form_token = form_fields.get(_FIELD_NAME, "")

        presented_token = form_token or header_token

        if not (
            cookie_token
            and presented_token
            and hmac.compare_digest(cookie_token, presented_token)
            and _verify_token(self.secret, cookie_token)
        ):
            response = PlainTextResponse("CSRF token missing or invalid", status_code=403)
            await response(scope, receive, send)
            return

        async def _receive_again() -> Message:
            return {"type": "http.request", "body": body_bytes, "more_body": False}

        await self.app(scope, _receive_again, send)

    async def _call_and_set_cookie(
        self, scope: Scope, receive: Receive, send: Send, request: Request, is_api: bool
    ) -> None:
        # Reuse an existing valid token rather than minting one on every GET.
        # Re-issuing per request rotated the cookie out from under any already-
        # rendered form: every htmx fragment poll is a GET, so with a polling tab
        # open the cookie changed every few seconds and a form that took longer than
        # one poll interval to submit (delete/purge, which require typing the queue
        # name to confirm) failed the double-submit check with a spurious 403. A
        # stable cookie keeps the token valid across polls and tabs; it is still
        # HMAC-verified and must still match the form/header token.
        existing = request.cookies.get(COOKIE_NAME, "")
        if existing and _verify_token(self.secret, existing):
            token = existing
            set_cookie = False
        else:
            token = make_token(self.secret)
            set_cookie = True
        scope.setdefault("state", {})
        scope["state"]["csrf_token"] = token

        async def _send(message: Message) -> None:
            if message["type"] == "http.response.start" and not is_api and set_cookie:
                response_headers = Response(headers={})
                response_headers.raw_headers = list(message.get("headers", []))
                response_headers.set_cookie(
                    COOKIE_NAME,
                    token,
                    httponly=False,
                    samesite="lax",
                )
                message["headers"] = response_headers.raw_headers
            await send(message)

        await self.app(scope, receive, _send)
