from __future__ import annotations

import dataclasses
import re
import secrets as _secrets
from datetime import datetime, timezone
from importlib import resources
from typing import Dict, Optional
from urllib.parse import parse_qsl

import jinja2
import redis
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

from . import keys as k
from .auth import BasicAuthMiddleware
from .config import Config
from .csrf import CSRFMiddleware
from .guards import ReadOnlyMiddleware
from .redis_ops import actions as actions_mod
from .redis_ops.connect import get_client
from .redis_ops.discovery import discover_namespaces
from .redis_ops.messages import get_message, list_messages
from .redis_ops.stats import queue_stats, worker_stats

_NS_RE = re.compile(r"^\S+$")
_STATES = ("waiting", "delayed", "dead", "inflight")


def fmt_age(ms: Optional[int]) -> str:
    """Humanize a millisecond duration: 42s, 3m 10s, 2h 5m, 4d."""
    if ms is None:
        return "-"
    seconds = int(ms // 1000)
    if seconds < 0:
        seconds = 0
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)

    if days:
        return f"{days}d"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def fmt_age_since(ms: Optional[int], now_ms: Optional[int]) -> str:
    """Humanize the age of an absolute epoch-ms timestamp relative to
    `now_ms` (the redis server clock, never the client's)."""
    if ms is None or now_ms is None:
        return "-"
    return fmt_age(now_ms - ms)


def fmt_ts(ms: Optional[int]) -> str:
    """UTC ISO timestamp, minute precision."""
    if ms is None:
        return "-"
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%MZ")


def _templates_env() -> jinja2.Environment:
    templates_dir = resources.files("dramatiq_monitor").joinpath("templates")
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(templates_dir)),
        autoescape=jinja2.select_autoescape(),
    )
    env.filters["fmt_age"] = fmt_age
    env.filters["fmt_ts"] = fmt_ts
    env.filters["fmt_age_since"] = fmt_age_since
    return env


def _server_now_ms(r: "redis.Redis") -> int:
    seconds, micros = r.time()
    return int(seconds) * 1000 + int(micros) // 1000


async def _form_fields(request: Request) -> Dict[str, str]:
    """Parse an `application/x-www-form-urlencoded` body without depending
    on python-multipart (kept out of core deps)."""
    body = await request.body()
    return dict(parse_qsl(body.decode(), keep_blank_values=True))


def _stale_worker_ids(config: Config, r: "redis.Redis", ns: str) -> set:
    now_ms = _server_now_ms(r)
    heartbeats = r.zrange(k.heartbeats_key(ns), 0, -1, withscores=True)
    stale = set()
    for worker_id_raw, score in heartbeats:
        worker_id = worker_id_raw.decode() if isinstance(worker_id_raw, bytes) else worker_id_raw
        if (now_ms - int(score)) > config.stale_worker_s * 1000:
            stale.add(worker_id)
    return stale


def _validate_ns(ns: str) -> None:
    if not ns or not _NS_RE.match(ns):
        raise HTTPException(status_code=404, detail="invalid namespace")


def _validate_state(state: str) -> None:
    if state not in _STATES:
        raise HTTPException(status_code=404, detail="unknown state")


def create_app(config: Config, clients: Optional[Dict[int, "redis.Redis"]] = None) -> Starlette:
    if clients is None:
        clients = {db: get_client(config, db) for db in config.dbs}

    templates = Jinja2Templates(env=_templates_env())

    def _client_for(db: int) -> "redis.Redis":
        client = clients.get(db)
        if client is None:
            raise HTTPException(status_code=404, detail="unknown db")
        return client

    def _all_namespaces() -> list:
        return discover_namespaces(config, clients)

    def _base_ctx(db: Optional[int] = None, ns: Optional[str] = None) -> Dict:
        """Uniform template context shared by every page render: config +
        namespaces (sidebar needs both on every page) plus the active db/ns
        when the page has one."""
        ctx: Dict = {"config": config, "namespaces": _all_namespaces()}
        if db is not None and ns is not None:
            ctx["db"] = db
            ctx["ns"] = ns
        return ctx

    async def index(request: Request) -> Response:
        namespaces = _all_namespaces()
        if len(namespaces) == 1:
            ref = namespaces[0]
            return RedirectResponse(
                request.url_for("overview", db=ref.db, ns=ref.ns)
            )
        return RedirectResponse(request.url_for("namespaces"))

    async def namespaces_page(request: Request) -> Response:
        ctx = _base_ctx()
        rows = []
        for ref in ctx["namespaces"]:
            client = _client_for(ref.db)
            stats = queue_stats(config, client, ref.db, ref.ns)
            waiting_total = sum(s.waiting for s in stats)
            failed_total = sum(s.failed for s in stats)
            rows.append(
                {
                    "ref": ref,
                    "waiting_total": waiting_total,
                    "failed_total": failed_total,
                }
            )
        return templates.TemplateResponse(
            request,
            "namespaces.html",
            {
                **ctx,
                "rows": rows,
            },
        )

    async def overview(request: Request) -> Response:
        db = request.path_params["db"]
        ns = request.path_params["ns"]
        _validate_ns(ns)
        client = _client_for(db)
        stats = queue_stats(config, client, db, ns)
        workers = worker_stats(config, client, db, ns)
        return templates.TemplateResponse(
            request,
            "overview.html",
            {
                **_base_ctx(db, ns),
                "stats": stats,
                "workers": workers,
            },
        )

    async def fragment_queues(request: Request) -> Response:
        db = request.path_params["db"]
        ns = request.path_params["ns"]
        _validate_ns(ns)
        client = _client_for(db)
        stats = queue_stats(config, client, db, ns)
        return templates.TemplateResponse(
            request,
            "_queues_table.html",
            {"config": config, "db": db, "ns": ns, "stats": stats},
        )

    async def fragment_workers(request: Request) -> Response:
        db = request.path_params["db"]
        ns = request.path_params["ns"]
        _validate_ns(ns)
        client = _client_for(db)
        workers = worker_stats(config, client, db, ns)
        return templates.TemplateResponse(
            request,
            "_workers_table.html",
            {"config": config, "db": db, "ns": ns, "workers": workers},
        )

    def _queue_state_params(request: Request) -> tuple:
        db = request.path_params["db"]
        ns = request.path_params["ns"]
        q = request.path_params["q"]
        state = request.path_params["state"]
        _validate_ns(ns)
        _validate_state(state)
        client = _client_for(db)
        return db, ns, q, state, client

    async def messages_page(request: Request) -> Response:
        db, ns, q, state, client = _queue_state_params(request)
        cursor = request.query_params.get("cursor")
        page = list_messages(config, client, ns, q, state, cursor=cursor)
        stats = queue_stats(config, client, db, ns)
        queue_stat = next((s for s in stats if s.name == q), None)
        return templates.TemplateResponse(
            request,
            "messages.html",
            {
                **_base_ctx(db, ns),
                "q": q,
                "state": state,
                "states": _STATES,
                "page": page,
                "queue_stat": queue_stat,
                "flash": request.query_params.get("flash"),
                "now_ms": _server_now_ms(client),
            },
        )

    async def fragment_message_rows(request: Request) -> Response:
        db, ns, q, state, client = _queue_state_params(request)
        cursor = request.query_params.get("cursor")
        page = list_messages(config, client, ns, q, state, cursor=cursor)
        return templates.TemplateResponse(
            request,
            "_message_rows.html",
            {
                "config": config,
                "db": db,
                "ns": ns,
                "q": q,
                "state": state,
                "page": page,
                "now_ms": _server_now_ms(client),
            },
        )

    async def message_detail_page(request: Request) -> Response:
        db, ns, q, state, client = _queue_state_params(request)
        rid = request.path_params["rid"]
        detail = get_message(config, client, ns, q, state, rid)
        if detail is None:
            return RedirectResponse(
                request.url_for("messages_page", db=db, ns=ns, q=q, state=state).include_query_params(
                    flash="gone"
                )
            )
        return templates.TemplateResponse(
            request,
            "message_detail.html",
            {
                **_base_ctx(db, ns),
                "q": q,
                "state": state,
                "detail": detail,
                "now_ms": _server_now_ms(client),
            },
        )

    def _redirect_to_messages(
        request: Request, db: int, ns: str, q: str, state: str, flash: str
    ) -> Response:
        url = request.url_for("messages_page", db=db, ns=ns, q=q, state=state)
        return RedirectResponse(url.include_query_params(flash=flash), status_code=303)

    def _queue_only_params(request: Request) -> tuple:
        db = request.path_params["db"]
        ns = request.path_params["ns"]
        q = request.path_params["q"]
        _validate_ns(ns)
        client = _client_for(db)
        return db, ns, q, client

    async def action_requeue_dead(request: Request) -> Response:
        db, ns, q, client = _queue_only_params(request)
        rid = request.path_params["rid"]
        new_rid = actions_mod.requeue_dead(client, ns, q, rid)
        flash = f"requeued as {new_rid[:8]}" if new_rid else "gone"
        return _redirect_to_messages(request, db, ns, q, "dead", flash)

    async def action_delete_message(request: Request) -> Response:
        db, ns, q, state, client = _queue_state_params(request)
        rid = request.path_params["rid"]
        try:
            deleted = actions_mod.delete_message(
                client,
                ns,
                q,
                state,
                rid,
                stale_worker_ids=_stale_worker_ids(config, client, ns) if state == "inflight" else None,
            )
        except actions_mod.ActionConflict:
            return JSONResponse(
                {"ok": False, "error": "worker is still live"}, status_code=409
            )
        flash = "deleted" if deleted else "gone"
        return _redirect_to_messages(request, db, ns, q, state, flash)

    async def action_requeue_all_dead(request: Request) -> Response:
        db = request.path_params["db"]
        ns = request.path_params["ns"]
        q = request.path_params["q"]
        _validate_ns(ns)
        client = _client_for(db)
        fields = await _form_fields(request)
        if fields.get("confirm") != q:
            raise HTTPException(status_code=400, detail="confirm must match queue name")
        count = actions_mod.requeue_all_dead(client, ns, q)
        return _redirect_to_messages(request, db, ns, q, "dead", f"requeued {count}")

    async def action_delete_all_dead(request: Request) -> Response:
        db = request.path_params["db"]
        ns = request.path_params["ns"]
        q = request.path_params["q"]
        _validate_ns(ns)
        client = _client_for(db)
        fields = await _form_fields(request)
        if fields.get("confirm") != q:
            raise HTTPException(status_code=400, detail="confirm must match queue name")
        count = actions_mod.delete_all_dead(client, ns, q)
        return _redirect_to_messages(request, db, ns, q, "dead", f"deleted {count}")

    async def action_purge_queue(request: Request) -> Response:
        db = request.path_params["db"]
        ns = request.path_params["ns"]
        q = request.path_params["q"]
        _validate_ns(ns)
        client = _client_for(db)
        fields = await _form_fields(request)
        if fields.get("confirm") != q:
            raise HTTPException(status_code=400, detail="confirm must match queue name")
        count = actions_mod.purge_queue(client, ns, q)
        return _redirect_to_messages(request, db, ns, q, "waiting", f"purged {count}")

    async def api_action_requeue_dead(request: Request) -> Response:
        db, ns, q, client = _queue_only_params(request)
        rid = request.path_params["rid"]
        new_rid = actions_mod.requeue_dead(client, ns, q, rid)
        if new_rid is None:
            return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
        return JSONResponse({"ok": True, "new_rid": new_rid})

    async def api_action_delete_message(request: Request) -> Response:
        db, ns, q, state, client = _queue_state_params(request)
        rid = request.path_params["rid"]
        try:
            deleted = actions_mod.delete_message(
                client,
                ns,
                q,
                state,
                rid,
                stale_worker_ids=_stale_worker_ids(config, client, ns) if state == "inflight" else None,
            )
        except actions_mod.ActionConflict:
            return JSONResponse(
                {"ok": False, "error": "worker is still live"}, status_code=409
            )
        if not deleted:
            return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
        return JSONResponse({"ok": True})

    async def api_action_requeue_all_dead(request: Request) -> Response:
        db = request.path_params["db"]
        ns = request.path_params["ns"]
        q = request.path_params["q"]
        _validate_ns(ns)
        client = _client_for(db)
        body = await request.json()
        if body.get("confirm") != q:
            return JSONResponse(
                {"ok": False, "error": "confirm must match queue name"}, status_code=400
            )
        count = actions_mod.requeue_all_dead(client, ns, q)
        return JSONResponse({"ok": True, "count": count})

    async def api_action_delete_all_dead(request: Request) -> Response:
        db = request.path_params["db"]
        ns = request.path_params["ns"]
        q = request.path_params["q"]
        _validate_ns(ns)
        client = _client_for(db)
        body = await request.json()
        if body.get("confirm") != q:
            return JSONResponse(
                {"ok": False, "error": "confirm must match queue name"}, status_code=400
            )
        count = actions_mod.delete_all_dead(client, ns, q)
        return JSONResponse({"ok": True, "count": count})

    async def api_action_purge_queue(request: Request) -> Response:
        db = request.path_params["db"]
        ns = request.path_params["ns"]
        q = request.path_params["q"]
        _validate_ns(ns)
        client = _client_for(db)
        body = await request.json()
        if body.get("confirm") != q:
            return JSONResponse(
                {"ok": False, "error": "confirm must match queue name"}, status_code=400
            )
        count = actions_mod.purge_queue(client, ns, q)
        return JSONResponse({"ok": True, "count": count})

    async def healthz(request: Request) -> Response:
        try:
            any_client = next(iter(clients.values()))
            any_client.ping()
        except Exception:
            return JSONResponse({"status": "error"}, status_code=503)
        return JSONResponse({"status": "ok"})

    async def api_namespaces(request: Request) -> Response:
        namespaces = _all_namespaces()
        return JSONResponse([dataclasses.asdict(ref) for ref in namespaces])

    async def api_queues(request: Request) -> Response:
        db = request.path_params["db"]
        ns = request.path_params["ns"]
        _validate_ns(ns)
        client = _client_for(db)
        stats = queue_stats(config, client, db, ns)
        return JSONResponse([dataclasses.asdict(s) for s in stats])

    async def api_workers(request: Request) -> Response:
        db = request.path_params["db"]
        ns = request.path_params["ns"]
        _validate_ns(ns)
        client = _client_for(db)
        workers = worker_stats(config, client, db, ns)
        return JSONResponse([dataclasses.asdict(w) for w in workers])

    async def api_messages(request: Request) -> Response:
        db, ns, q, state, client = _queue_state_params(request)
        cursor = request.query_params.get("cursor")
        n = int(request.query_params.get("n", 50))
        page = list_messages(config, client, ns, q, state, cursor=cursor, n=n)
        return JSONResponse(dataclasses.asdict(page))

    async def api_message_detail(request: Request) -> Response:
        db, ns, q, state, client = _queue_state_params(request)
        rid = request.path_params["rid"]
        detail = get_message(config, client, ns, q, state, rid)
        if detail is None:
            return JSONResponse({"detail": "not found"}, status_code=404)
        return JSONResponse(dataclasses.asdict(detail))

    static_dir = resources.files("dramatiq_monitor").joinpath("static")

    routes = [
        Route("/", index, name="index"),
        Route("/namespaces", namespaces_page, name="namespaces"),
        Route("/ns/{db:int}/{ns}/", overview, name="overview"),
        Route("/fragments/ns/{db:int}/{ns}/queues", fragment_queues, name="fragment_queues"),
        Route("/fragments/ns/{db:int}/{ns}/workers", fragment_workers, name="fragment_workers"),
        Route(
            "/ns/{db:int}/{ns}/queues/{q}/{state}",
            messages_page,
            name="messages_page",
        ),
        Route(
            "/fragments/ns/{db:int}/{ns}/queues/{q}/{state}/rows",
            fragment_message_rows,
            name="fragment_message_rows",
        ),
        Route(
            "/ns/{db:int}/{ns}/queues/{q}/{state}/msg/{rid}",
            message_detail_page,
            name="message_detail_page",
        ),
        Route(
            "/ns/{db:int}/{ns}/queues/{q}/dead/{rid}/requeue",
            action_requeue_dead,
            methods=["POST"],
            name="action_requeue_dead",
        ),
        Route(
            "/ns/{db:int}/{ns}/queues/{q}/{state}/{rid}/delete",
            action_delete_message,
            methods=["POST"],
            name="action_delete_message",
        ),
        Route(
            "/ns/{db:int}/{ns}/queues/{q}/dead/requeue-all",
            action_requeue_all_dead,
            methods=["POST"],
            name="action_requeue_all_dead",
        ),
        Route(
            "/ns/{db:int}/{ns}/queues/{q}/dead/delete-all",
            action_delete_all_dead,
            methods=["POST"],
            name="action_delete_all_dead",
        ),
        Route(
            "/ns/{db:int}/{ns}/queues/{q}/purge",
            action_purge_queue,
            methods=["POST"],
            name="action_purge_queue",
        ),
        Route("/healthz", healthz, name="healthz"),
        Route("/api/namespaces", api_namespaces, name="api_namespaces"),
        Route("/api/ns/{db:int}/{ns}/queues", api_queues, name="api_queues"),
        Route("/api/ns/{db:int}/{ns}/workers", api_workers, name="api_workers"),
        Route(
            "/api/ns/{db:int}/{ns}/queues/{q}/{state}",
            api_messages,
            name="api_messages",
        ),
        Route(
            "/api/ns/{db:int}/{ns}/queues/{q}/{state}/msg/{rid}",
            api_message_detail,
            name="api_message_detail",
        ),
        Route(
            "/api/ns/{db:int}/{ns}/queues/{q}/dead/{rid}/requeue",
            api_action_requeue_dead,
            methods=["POST"],
            name="api_action_requeue_dead",
        ),
        Route(
            "/api/ns/{db:int}/{ns}/queues/{q}/{state}/{rid}/delete",
            api_action_delete_message,
            methods=["POST"],
            name="api_action_delete_message",
        ),
        Route(
            "/api/ns/{db:int}/{ns}/queues/{q}/dead/requeue-all",
            api_action_requeue_all_dead,
            methods=["POST"],
            name="api_action_requeue_all_dead",
        ),
        Route(
            "/api/ns/{db:int}/{ns}/queues/{q}/dead/delete-all",
            api_action_delete_all_dead,
            methods=["POST"],
            name="api_action_delete_all_dead",
        ),
        Route(
            "/api/ns/{db:int}/{ns}/queues/{q}/purge",
            api_action_purge_queue,
            methods=["POST"],
            name="api_action_purge_queue",
        ),
        Mount("/static", app=StaticFiles(directory=str(static_dir)), name="static"),
    ]

    app = Starlette(routes=routes)
    app.state.config = config
    app.state.clients = clients
    app.state.templates = templates

    secret = config.secret or _secrets.token_hex(32)
    app.add_middleware(CSRFMiddleware, secret=secret)
    app.add_middleware(ReadOnlyMiddleware, read_only=config.read_only)
    if config.auth_user:
        app.add_middleware(
            BasicAuthMiddleware, auth_user=config.auth_user, auth_password=config.auth_password or ""
        )

    return app
