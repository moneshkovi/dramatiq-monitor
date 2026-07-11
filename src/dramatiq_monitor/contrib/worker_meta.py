"""Optional Dramatiq middleware that publishes worker identity metadata.

Dramatiq's own `__heartbeats__` zset only stores a worker's uuid4, which
isn't very useful in a dashboard. Add this middleware to your Dramatiq
broker's middleware list and dramatiq-monitor will render the worker's
host/pid/pool/queues instead of a bare uuid whenever it's present.

Usage (in your worker process, wherever the broker is constructed)::

    from dramatiq_monitor.contrib.worker_meta import WorkerMetaMiddleware

    broker.add_middleware(WorkerMetaMiddleware(pool="default"))

This is the only module in dramatiq-monitor that imports `dramatiq` — the
rest of the package never does, so it works purely off the Redis key
scheme with no broker dependency.
"""
from __future__ import annotations

import json
import os
import socket
import time
from typing import Optional

import dramatiq

_DEFAULT_TTL_MS = 15 * 60 * 1000


def _worker_meta_key(namespace: str) -> str:
    return f"{namespace}:__worker_meta__"


class WorkerMetaMiddleware(dramatiq.Middleware):
    """Publishes `{host, pid, pool, queues, started_at_ms}` to
    `{namespace}:__worker_meta__` on boot, keyed by the broker's own
    `broker_id` (the same uuid Dramatiq writes into `__heartbeats__` and
    `__acks__.*`, so the dashboard's join is exact). Removed on shutdown;
    a crash leaving stale metadata behind is harmless since it's only ever
    rendered for uuids still present in `__heartbeats__`.
    """

    def __init__(self, pool: Optional[str] = None, ttl_ms: int = _DEFAULT_TTL_MS) -> None:
        self.pool = pool
        self.ttl_ms = ttl_ms

    def after_worker_boot(self, broker, worker) -> None:
        namespace = getattr(broker, "namespace", None)
        if not namespace:
            return

        meta = {
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "pool": self.pool,
            "queues": sorted(broker.queues.keys()) if getattr(broker, "queues", None) else [],
            "started_at_ms": int(time.time() * 1000),
        }
        broker.client.hset(_worker_meta_key(namespace), broker.broker_id, json.dumps(meta))

    def before_worker_shutdown(self, broker, worker) -> None:
        namespace = getattr(broker, "namespace", None)
        if not namespace:
            return
        broker.client.hdel(_worker_meta_key(namespace), broker.broker_id)
