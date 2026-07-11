# dramatiq-monitor

A lightweight, server-rendered web dashboard for inspecting and managing
[Dramatiq](https://dramatiq.io/) task queues backed by Redis. Dramatiq ships
without an official dashboard, and the one community project that came
closest (`Bogdanp/dramatiq_dashboard`) has been unmaintained since 2022,
pins an old `redis` client, and has no TTL view, bulk operations, or fleet
awareness. dramatiq-monitor reads Dramatiq's Redis key scheme directly (no
`dramatiq` import in the core) and adds namespace/fleet discovery, a
message browser with pagination, dead-letter requeue/delete (single and
bulk), queue purge, a JSON API, and optional auth / read-only / CSRF
protections — all with only `starlette`, `jinja2`, `redis`, and `uvicorn`
as dependencies.

## Quickstart

```bash
pip install -e .
python scripts/seed_demo.py          # seeds demo data into redis://127.0.0.1:6379
dramatiq-monitor --redis-url redis://127.0.0.1:6379
```

Then open http://127.0.0.1:8321.

Or, with Docker and no manual Redis setup:

```bash
make dev     # docker Redis + seed demo data + serve, actions enabled
make stop    # tear it down
```

Pinned dependency versions (if you'd rather not go through `pyproject.toml`
extras) are in [`requirements.txt`](requirements.txt) /
[`requirements-dev.txt`](requirements-dev.txt). Development is done against a
conda environment named `dramatiq`; see
[`docs/how-to/run-locally.md`](docs/how-to/run-locally.md) for both that and
the plain pip/venv path.

### CLI flags

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--redis-url` | `DM_REDIS_URL` | - | Full redis URL; overrides host/port/password/ssl |
| `--redis-host` | `DM_REDIS_HOST` | `127.0.0.1` | Redis host (used when no URL) |
| `--redis-port` | `DM_REDIS_PORT` | `6379` | Redis port |
| `--redis-password` | `DM_REDIS_PASSWORD` | - | Redis password |
| `--ssl` | `DM_REDIS_SSL` | `false` | Enable TLS |
| `--ssl-no-verify` | `DM_REDIS_SSL_NO_VERIFY` | `false` | Skip cert verification (e.g. through an SSH tunnel) |
| `--dbs` | `DM_DBS` | `0` | Comma-separated Redis db list to scan, e.g. `0,1` |
| `--namespaces` | `DM_NAMESPACES` | - | Allowlist of namespaces with no live worker (no `__heartbeats__` key), unioned onto every configured db |
| `--base-path` | `DM_BASE_PATH` | - | Mount prefix for reverse-proxy deployments |
| `--auth-user` | `DM_AUTH_USER` | - | HTTP Basic Auth username; auth is only enforced when set |
| `--auth-password` | `DM_AUTH_PASSWORD` | - | HTTP Basic Auth password |
| `--read-only` | `DM_READ_ONLY` | `false` | Disable all mutating routes and hide action buttons |
| `--poll-queues` | - | `3` | Queue table auto-refresh interval (seconds) |
| `--poll-workers` | - | `5` | Worker table auto-refresh interval (seconds) |
| `--dead-message-ttl-ms` | `DM_DEAD_MESSAGE_TTL_MS` | - | Display hint for estimated dead-letter TTL remaining (not stored in Redis) |
| `--host` | - | `127.0.0.1` | Bind host |
| `--port` | - | `8321` | Bind port |

`DM_SECRET` sets a stable CSRF signing secret (recommended behind a load
balancer with multiple processes); if unset, a random secret is generated
per process at startup.

## The key scheme it reads

dramatiq-monitor never imports `dramatiq`; it reads Dramatiq's own Redis
layout directly (verified against dramatiq 1.17):

```
{ns}:{queue}                      LIST   waiting message ids (rids)
{ns}:{queue}.msgs                 HASH   rid -> JSON payload
{ns}:{queue}.DQ (+ .DQ.msgs)      delayed queue (list or zset; TYPE-checked at runtime)
{ns}:{queue}.XQ                   ZSET   dead rids scored by death-ts-ms
{ns}:{queue}.XQ.msgs              HASH   dead payloads
{ns}:__acks__.{worker_uuid}.{q}   SET    in-flight rids per worker
{ns}:__heartbeats__               ZSET   worker_uuid -> last-heartbeat-ms
```

List/hash/zset members are the broker-level `options.redis_message_id`
(`rid`), not the logical `message_id` — the dashboard keys everything on
`rid` and displays both.

## Requeue semantics

Requeuing a dead-lettered message is done without importing `dramatiq`, but
stays byte-compatible with what the broker itself would write:

1. Read the payload from `{ns}:{queue}.XQ.msgs`; a miss means it was already
   swept (dead-letter TTL race) and the action is a no-op (404 / flash).
2. Generate a fresh `redis_message_id` (uuid4) — the old id may still be
   referenced elsewhere (e.g. a stale ack set) and must not be reused.
3. Mutate only `options.redis_message_id`, reset `options.retries` to `0`,
   and drop `options.traceback`. Everything else — including `message_id`
   and `message_timestamp` — is preserved byte-for-byte, so external
   correlation keyed on `message_id` survives the requeue.
4. Re-encode with `json.dumps(payload, separators=(",", ":"))`, matching
   Dramatiq's own compact encoding.
5. Apply atomically: `HSET .msgs new_rid body` before `LPUSH queue new_rid`
   (a consumer can never pop an id with no payload yet), then remove the
   old entry from `.XQ` and `.XQ.msgs`.

Bulk "requeue all" / "delete all" walk the dead-letter zset in batches and
also sweep `.XQ.msgs` for drift leftovers (payloads whose zset entry was
already TTL-swept). `purge` only ever pops the waiting list and the
matching `.msgs` hash entries — it never touches acks, the delayed queue,
or the dead-letter queue, so in-flight messages sharing the same `.msgs`
hash are left intact.

## Read-only, auth, and CSRF

- **`--read-only`**: any non-GET/HEAD/OPTIONS request is rejected with 403
  (JSON for `/api/*`, a small HTML message otherwise), and action
  buttons/forms are not rendered in the templates at all.
- **`--auth-user` / `--auth-password`**: HTTP Basic Auth, only active when
  `auth_user` is set; uses `secrets.compare_digest` for both fields and
  exempts `/healthz` so uptime checks don't need credentials.
- **CSRF**: stateless double-submit-cookie protection. HTML pages set a
  `dm_csrf` cookie and embed the same token in a `<meta name="csrf-token">`
  tag (and via htmx's global `hx-headers`); non-GET page routes must
  present the token back via the `csrf_token` form field or the
  `X-CSRF-Token` header. `/api/*` is exempt from the token but requires
  `Content-Type: application/json` on every mutating request instead.
- Middleware order (outermost first): **BasicAuth → ReadOnly → CSRF →
  router**.

## ASGI mounting

`create_app` returns a plain Starlette app, so it can be mounted anywhere:

```python
from dramatiq_monitor import create_app
from dramatiq_monitor.config import Config

app = create_app(Config(redis_url="redis://localhost:6379", read_only=True))
```

```python
# mounted under a prefix in a larger ASGI app
from starlette.routing import Mount
routes = [Mount("/dramatiq", app=create_app(Config.from_env()))]
```

## Worker metadata middleware (optional)

Dramatiq's `__heartbeats__` zset only stores a worker's uuid4. Add
`WorkerMetaMiddleware` to your broker to get host/pid/pool/queues rendered
in the dashboard instead of a bare uuid. This is the only place in the
project that touches `dramatiq` — nothing in the core dashboard imports it.

```python
from dramatiq_monitor.contrib.worker_meta import WorkerMetaMiddleware

broker.add_middleware(WorkerMetaMiddleware(pool="default"))
```

## Screenshots

_TODO: add screenshots of the overview, message browser, and detail pages._

## Documentation

This README covers day-to-day usage. For architecture, the full Redis key
schema, the HTTP/JSON API reference, and the design decisions behind them,
see [`docs/`](docs/README.md).

## License

MIT — see [LICENSE](LICENSE).
