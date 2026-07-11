# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment

Always use conda env `dramatiq` for any Python/pytest/pip command in this repo — never bare `python`/`pip` from base env. Prefer `conda run -n dramatiq <cmd>` for one-offs; if your shell profile pre-activates a different conda env, `conda run -n dramatiq` can silently run the wrong env's python (see note under Commands), so `conda activate dramatiq` first or use the absolute path if that's a risk.

## Commands

```bash
make install   # editable install + dev deps into conda env `dramatiq`
make dev       # docker Redis + seed demo data + serve on :8321, actions enabled
make stop      # tear down `make dev`
make test      # pytest -q — fakeredis, no real Redis needed, ~123 tests in under a second
make seed      # seed demo data into a Redis you already have running
make build     # build the wheel/sdist (sanity-checks templates/static/fonts get packaged)
make clean     # remove build artifacts and __pycache__
```

Single file / single test:

```bash
conda run -n dramatiq pytest tests/test_actions.py -q
conda run -n dramatiq pytest tests/test_actions.py::test_requeue_dead_byte_exact_semantics -q
```

Development happens against a conda env named `dramatiq` (not a project-local `.venv`). The `Makefile` and `scripts/*.sh` resolve that env's binaries by **absolute path** (`$(conda info --base)/envs/dramatiq/bin/...`) rather than `conda run -n dramatiq`, because `conda run` resolves commands via `PATH` — if your shell profile pre-activates a different conda env, `conda run -n dramatiq` silently executes the wrong env's python instead of failing loudly. Do the same (`conda activate dramatiq` first, or use the absolute path) if invoking tools manually outside `make`.

## Architecture

One Python process: uvicorn → Starlette ASGI app → Redis, and nothing else. The dashboard has **no database of its own** (ADR 0002) — every value shown is computed fresh from Dramatiq's own Redis key scheme on each request, and write actions (requeue/delete/purge) write back into that same scheme, byte-compatible with what the broker itself would write. This single constraint shapes almost everything else below.

### Module map

- `keys.py` — single source of truth for every Redis key name (`{ns}:{queue}`, `.msgs`, `.DQ`(`.msgs`), `.XQ`(`.msgs`), `__acks__.{worker}.{queue}`, `__heartbeats__`, `__worker_meta__`). All other modules build keys through here, never by string-formatting inline.
- `redis_ops/discovery.py` — namespace/queue discovery via `SCAN`, short in-process cache (30s for namespace lists, 10s for per-namespace queue lists).
- `redis_ops/stats.py` — `queue_stats()` / `worker_stats()`, each two pipelined Redis round-trips regardless of queue count.
- `redis_ops/messages.py` — `list_messages()` / `get_message()`, one code path per state (`waiting`/`delayed`/`dead`/`inflight`). `dead` has an `HSCAN` drift-fallback for when the `.XQ` zset and `.XQ.msgs` hash have desynced (a dead-letter TTL sweep race).
- `redis_ops/actions.py` — the **only** module that writes to Redis; every write is a `MULTI` pipeline. Requeue is byte-exact: fresh `redis_message_id` (uuid4), `retries` reset to 0, `traceback` dropped — but `message_id`/`message_timestamp` are preserved so external correlation survives a manual requeue.
- `app.py` — Starlette routes. Each handler resolves `(db, ns, [q, state, rid])` from path params, pulls a cached `redis.Redis` client, calls into `redis_ops/`, then hands the resulting dataclass to Jinja2 (page route) or `dataclasses.asdict()` (`/api/...` route). Every page route has an exact JSON mirror driven by the same call — the page and the API are two renderings of one code path, not two implementations.
- `models.py` — frozen dataclasses (`QueueStats`, `WorkerInfo`, `MessageSummary`, `MessageDetail`, `Page`, `NamespaceRef`) that are the shared contract between the HTML and JSON output.
- `auth.py` / `guards.py` / `csrf.py` — ASGI middleware, applied in this order (outermost first): **BasicAuth → ReadOnly → CSRF → router**.
- `contrib/worker_meta.py` — the *only* file allowed to `import dramatiq`. Opt-in `WorkerMetaMiddleware` a deployment adds to its own broker so the dashboard can show host/pid/pool instead of a bare worker uuid.

### Hard constraint: zero `dramatiq` dependency in the core

The dashboard reads Dramatiq's Redis layout directly and never imports the `dramatiq` package outside `contrib/worker_meta.py` (ADR 0001/0002 — see `docs/adr/`). This isn't just convention: `tests/test_boundary.py` AST-walks every module under `src/dramatiq_monitor/` and fails if any top-level import falls outside `{stdlib, starlette, jinja2, redis, uvicorn, dramatiq_monitor}`.

### rid vs. message_id

`options.redis_message_id` (the "rid") is what's actually stored as the list/hash/zset member — a fresh uuid4 generated on every enqueue *and* every requeue. `message_id` is the logical, stable identity that survives retries and manual requeues. Everything in this codebase keys its lookups on rid; don't conflate the two when touching `redis_ops/` or `keys.py`.

### Discovery caching

In-process only, never Redis-backed: namespace list cached 30s per db, per-namespace queue list cached 10s per `(db, ns)`. Safe to lose at any time — a restart or TTL expiry just triggers a re-scan; it exists purely to avoid re-`SCAN`ning a potentially large, shared keyspace on every page load. `tests/conftest.py`'s autouse `_clear_caches` fixture resets both caches between tests.

### Frontend

Server-rendered Jinja2 + htmx (vendored in `static/`) + hand-rolled CSS — no JS framework, no bundler, no build step. Fragment routes under `/fragments/...` return a bare `<tbody>`/`<tr>` set for htmx polling (`hx-trigger="every Ns"`) and must never be linked to as full pages. Visual design tokens and the animated ascii-blob effect were independently reimplemented from Investi's rendered output, not copied (ADR 0003 — see `docs/explanation/design-system.md`).

### Docs

`docs/` (Diataxis-organized: `explanation/`, `reference/`, `how-to/`, `developer/`, `adr/`) is the deep reference for anything not obvious from the code — read `docs/reference/redis-key-schema.md` before changing `redis_ops/` or `keys.py`, and check `docs/adr/` before pushing back on a constraint (no DB, no `dramatiq` import, etc.) that looks like it could be relaxed. Note: `docs/` and `SESSION_NOTES.md` are currently listed in `.gitignore` — nothing under `docs/` has actually landed in the repo's git history yet, even though the README and several ADRs cross-reference it as if already published; don't assume it's part of what ships.

## Testing

`fakeredis`-backed, no real Redis required; the whole suite runs in under a second. `tests/test_boundary.py` isn't testing behavior — it's the AST import-purity check described above, and should stay passing without modification for any ordinary feature change. `tests/conftest.py`'s `seed_queue(...)` helper seeds a full realistic queue (waiting / delayed-as-list-or-zset / dead / inflight / heartbeats) in one call — prefer it over constructing raw Redis commands by hand in new tests.
