#!/usr/bin/env python
"""Idempotently seed demo Dramatiq data into a local Redis for manual testing.

Usage:
    python scripts/seed_demo.py

Connects to `redis://127.0.0.1:6379` by default; override with env var
DM_SEED_URL. Seeds two namespaces (dramatiq-alpha, dramatiq-beta), both on
db 0, each with four queues (emails, thumbnails, billing, reports) covering
waiting/delayed/dead/inflight states, a live + a stale worker, and one
__worker_meta__ entry.

Only depends on `redis` and `dramatiq_monitor.keys` — safe to run without the
rest of the package's web/data-access layers.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from typing import Optional

import redis

from dramatiq_monitor import keys as k

SEED_URL = os.environ.get("DM_SEED_URL", "redis://127.0.0.1:6379")

NAMESPACES = ["dramatiq-alpha", "dramatiq-beta"]
QUEUES = ["emails", "thumbnails", "billing", "reports"]

# waiting message counts per queue, varied 0-25
WAITING_COUNTS = {"emails": 25, "thumbnails": 7, "billing": 0, "reports": 12}
DEAD_COUNTS = {"emails": 3, "thumbnails": 0, "billing": 5, "reports": 1}

SAMPLE_TRACEBACKS = [
    "Traceback (most recent call last):\n"
    '  File "worker.py", line 42, in process\n'
    "    send_email(msg)\n"
    "ConnectionError: could not reach SMTP relay",
    "Traceback (most recent call last):\n"
    '  File "worker.py", line 88, in process\n'
    "    render_thumbnail(path)\n"
    "OSError: [Errno 28] No space left on device",
    "Traceback (most recent call last):\n"
    '  File "worker.py", line 15, in process\n'
    "    charge_card(account_id)\n"
    "ValueError: invalid card token",
]


def _now_ms(r: "redis.Redis") -> int:
    seconds, micros = r.time()
    return int(seconds) * 1000 + int(micros) // 1000


def _make_message(
    actor: str,
    queue: str,
    message_timestamp: int,
    rid: Optional[str] = None,
    traceback: Optional[str] = None,
) -> tuple:
    rid = rid or str(uuid.uuid4())
    message_id = str(uuid.uuid4())
    options = {"redis_message_id": rid, "retries": 0}
    if traceback is not None:
        options["traceback"] = traceback

    payload = {
        "queue_name": queue,
        "actor_name": actor,
        "args": [],
        "kwargs": {},
        "options": options,
        "message_id": message_id,
        "message_timestamp": message_timestamp,
    }
    encoded = json.dumps(payload, separators=(",", ":")).encode()
    return rid, encoded


def seed_namespace(r: "redis.Redis", ns: str, use_zset_delayed: bool) -> dict:
    now_ms = _now_ms(r)
    hour_ms = 60 * 60 * 1000
    summary = {"ns": ns, "queues": {}}

    for q in QUEUES:
        waiting_n = WAITING_COUNTS[q]
        dead_n = DEAD_COUNTS[q]
        q_key = k.queue_key(ns, q)
        msgs_key = k.msgs_key(ns, q)
        dq_key = k.dq_key(ns, q)
        dq_msgs_key = k.dq_msgs_key(ns, q)
        xq_key = k.xq_key(ns, q)
        xq_msgs_key = k.xq_msgs_key(ns, q)

        # Idempotent: wipe this queue's keys before reseeding.
        r.delete(q_key, msgs_key, dq_key, dq_msgs_key, xq_key, xq_msgs_key)

        # i=0 gets the oldest timestamp and is LPUSHed first, so it ends up
        # at the list tail (dramatiq LPUSHes; tail = oldest = next to pop).
        for i in range(waiting_n):
            ts = now_ms - hour_ms + int((i / max(waiting_n - 1, 1)) * hour_ms)
            rid, encoded = _make_message("process_" + q[:-1], q, ts)
            r.hset(msgs_key, rid, encoded)
            r.lpush(q_key, rid)

        # one delayed message, list form in one ns, zset form in the other
        delayed_rid, delayed_encoded = _make_message(
            "process_" + q[:-1], q, now_ms, traceback=None
        )
        if use_zset_delayed:
            r.hset(dq_msgs_key, delayed_rid, delayed_encoded)
            r.zadd(dq_key, {delayed_rid: now_ms + hour_ms})
        else:
            r.hset(dq_msgs_key, delayed_rid, delayed_encoded)
            r.lpush(dq_key, delayed_rid)

        for i in range(dead_n):
            died_ts = now_ms - i * 60_000
            tb = SAMPLE_TRACEBACKS[i % len(SAMPLE_TRACEBACKS)]
            rid, encoded = _make_message("process_" + q[:-1], q, died_ts, traceback=tb)
            r.hset(xq_msgs_key, rid, encoded)
            r.zadd(xq_key, {rid: died_ts})

        summary["queues"][q] = {
            "waiting": waiting_n,
            "delayed": 1,
            "delayed_shape": "zset" if use_zset_delayed else "list",
            "dead": dead_n,
        }

    # Two workers: one live (fresh heartbeat), one stale (5min old).
    live_worker = str(uuid.uuid4())
    stale_worker = str(uuid.uuid4())
    heartbeats_key = k.heartbeats_key(ns)
    r.delete(heartbeats_key)
    r.zadd(heartbeats_key, {live_worker: now_ms, stale_worker: now_ms - 5 * 60_000})

    # In-flight acks for both workers against the busiest queue.
    inflight_queue = "emails"
    for worker_id, count in ((live_worker, 2), (stale_worker, 1)):
        ack_key = f"{ns}:__acks__.{worker_id}.{inflight_queue}"
        r.delete(ack_key)
        for _ in range(count):
            rid, encoded = _make_message("process_email", inflight_queue, now_ms)
            r.hset(k.msgs_key(ns, inflight_queue), rid, encoded)
            r.sadd(ack_key, rid)

    # worker_meta for the live worker only.
    meta_key = k.worker_meta_key(ns)
    r.delete(meta_key)
    r.hset(
        meta_key,
        live_worker,
        json.dumps(
            {
                "host": f"worker-{ns.split('-')[-1]}-01",
                "pool": "default",
                "pid": 4242,
            }
        ),
    )

    summary["live_worker"] = live_worker
    summary["stale_worker"] = stale_worker
    summary["inflight_queue"] = inflight_queue
    return summary


def main() -> None:
    r = redis.Redis.from_url(SEED_URL, decode_responses=False)
    r.ping()

    summaries = []
    for i, ns in enumerate(NAMESPACES):
        summaries.append(seed_namespace(r, ns, use_zset_delayed=bool(i)))

    print(f"Seeded demo data into {SEED_URL}\n")
    for summary in summaries:
        print(f"namespace={summary['ns']} (db 0)")
        for q, stats in summary["queues"].items():
            print(
                f"  {q:<10} waiting={stats['waiting']:<3} "
                f"delayed={stats['delayed']} ({stats['delayed_shape']})  "
                f"dead={stats['dead']}"
            )
        print(f"  live worker:  {summary['live_worker']} (heartbeat now)")
        print(f"  stale worker: {summary['stale_worker']} (heartbeat 5m ago)")
        print(
            f"  inflight: 2 on {summary['live_worker'][:8]}, "
            f"1 on {summary['stale_worker'][:8]} (queue={summary['inflight_queue']})"
        )
        print()


if __name__ == "__main__":
    main()
