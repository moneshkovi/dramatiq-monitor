# Changelog

## 0.1.0

Initial release.

- Namespace/fleet auto-discovery with an always-visible host/db/namespace
  header and namespace switcher.
- Queue overview: waiting/delayed/live/orphaned/failed counts, oldest
  waiting age, auto-refreshing via htmx.
- Workers panel with optional worker metadata (host/pid/pool) via the
  companion `WorkerMetaMiddleware`.
- Message browser per state (waiting/delayed/dead/inflight) with
  pagination, and a message detail view (payload, TTL, traceback, death
  time).
- Actions: single requeue/delete, bulk dead-letter requeue-all/delete-all,
  queue purge — all CSRF-protected, typed-confirmation-gated for bulk
  operations, and disabled under `--read-only`.
- JSON API mirror of every GET and action route under `/api/`.
- Optional HTTP Basic Auth and `--read-only` flag.
- CLI (`dramatiq-monitor`), ASGI app factory (`create_app`), and a demo
  data seed script (`scripts/seed_demo.py`).
