from __future__ import annotations

import json
import uuid
from typing import Dict, List, Optional, Tuple

import fakeredis
import pytest

from dramatiq_monitor import keys as k
from dramatiq_monitor.redis_ops import connect as connect_mod
from dramatiq_monitor.redis_ops import discovery as discovery_mod


@pytest.fixture(autouse=True)
def _clear_caches():
    discovery_mod.clear_caches()
    connect_mod.clear_clients()
    yield
    discovery_mod.clear_caches()
    connect_mod.clear_clients()


@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=False)


def make_message(actor: str, queue: str, **overrides) -> Tuple[str, dict, bytes]:
    """Build a dramatiq-shaped message: (rid, payload_dict, encoded_bytes).

    rid == options.redis_message_id, distinct from the logical message_id.
    """
    rid = overrides.pop("rid", None) or str(uuid.uuid4())
    message_id = overrides.pop("message_id", None) or str(uuid.uuid4())
    message_timestamp = overrides.pop("message_timestamp", 0)
    args = overrides.pop("args", [])
    kwargs = overrides.pop("kwargs", {})
    options = overrides.pop("options", {})

    payload = {
        "queue_name": queue,
        "actor_name": actor,
        "args": args,
        "kwargs": kwargs,
        "options": {"redis_message_id": rid, "retries": 0, **options},
        "message_id": message_id,
        "message_timestamp": message_timestamp,
    }
    payload.update(overrides)

    encoded = json.dumps(payload, separators=(",", ":")).encode()
    return rid, payload, encoded


def seed_queue(
    r: "fakeredis.FakeRedis",
    ns: str,
    q: str,
    waiting: int = 0,
    delayed_list: int = 0,
    delayed_zset: int = 0,
    dead: int = 0,
    inflight: Optional[List[Tuple[str, int]]] = None,
    heartbeats: Optional[Dict[str, int]] = None,
) -> dict:
    """Seed one queue's worth of Dramatiq Redis state.

    - waiting: number of messages LPUSHed onto {ns}:{q} (+ .msgs hash)
    - delayed_list / delayed_zset: mutually exclusive DQ shapes
    - dead: number of dead messages in .XQ / .XQ.msgs
    - inflight: [(worker_id, count), ...] populates {ns}:__acks__.{worker}.{q}
    - heartbeats: {worker_id: age_ms_ago} -> ZADD __heartbeats__ using server TIME

    Returns a dict of the rids created per state for assertions.
    """
    now_s, now_us = r.time()
    now_ms = int(now_s) * 1000 + int(now_us) // 1000

    created: dict = {"waiting": [], "delayed": [], "dead": [], "inflight": []}

    for i in range(waiting):
        rid, _payload, encoded = make_message(
            "some_actor", q, message_timestamp=now_ms - (waiting - i) * 1000
        )
        r.hset(k.msgs_key(ns, q), rid, encoded)
        r.lpush(k.queue_key(ns, q), rid)
        created["waiting"].append(rid)

    if delayed_list and delayed_zset:
        raise ValueError("seed_queue: delayed_list and delayed_zset are mutually exclusive")

    for i in range(delayed_list):
        rid, _payload, encoded = make_message("some_actor", q, message_timestamp=now_ms)
        r.hset(k.dq_msgs_key(ns, q), rid, encoded)
        r.lpush(k.dq_key(ns, q), rid)
        created["delayed"].append(rid)

    for i in range(delayed_zset):
        rid, _payload, encoded = make_message("some_actor", q, message_timestamp=now_ms)
        r.hset(k.dq_msgs_key(ns, q), rid, encoded)
        r.zadd(k.dq_key(ns, q), {rid: now_ms + i})
        created["delayed"].append(rid)

    for i in range(dead):
        rid, _payload, encoded = make_message("some_actor", q, message_timestamp=now_ms)
        r.hset(k.xq_msgs_key(ns, q), rid, encoded)
        r.zadd(k.xq_key(ns, q), {rid: now_ms + i})
        created["dead"].append(rid)

    for worker_id, count in (inflight or []):
        ack_key = f"{ns}:__acks__.{worker_id}.{q}"
        for _ in range(count):
            rid, _payload, encoded = make_message("some_actor", q, message_timestamp=now_ms)
            r.hset(k.msgs_key(ns, q), rid, encoded)
            r.sadd(ack_key, rid)
            created["inflight"].append(rid)

    for worker_id, age_ms in (heartbeats or {}).items():
        r.zadd(k.heartbeats_key(ns), {worker_id: now_ms - age_ms})

    return created
