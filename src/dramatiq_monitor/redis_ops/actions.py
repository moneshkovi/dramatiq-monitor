from __future__ import annotations

import json
import uuid
from typing import Iterable, Optional, Set

import redis

from .. import keys as k
from .discovery import discover_queues
from ..config import Config

_DEAD_BATCH = 200
_PURGE_BATCH = 500


class ActionConflict(Exception):
    """Raised when an action can't safely proceed (e.g. deleting an inflight
    message owned by a live worker). Routes translate this to HTTP 409."""


def _decode(value):
    if isinstance(value, bytes):
        return value.decode()
    return value


def requeue_dead(r: "redis.Redis", ns: str, q: str, rid: str) -> Optional[str]:
    """Requeue one dead message back onto the waiting list.

    Returns the new redis_message_id, or None if the message is already gone
    (raced dead-letter TTL sweep). Byte-compatible with the broker: only
    `options.redis_message_id`, `options.retries`, and `options.traceback`
    are touched; `message_id`/`message_timestamp` are preserved so external
    correlation (e.g. WorkTracking rows) survives the requeue.
    """
    xq_msgs_key = k.xq_msgs_key(ns, q)
    raw = r.hget(xq_msgs_key, rid)
    if raw is None:
        return None

    payload = json.loads(raw)

    new_rid = str(uuid.uuid4())
    options = payload.get("options") or {}
    options["redis_message_id"] = new_rid
    options["retries"] = 0
    options.pop("traceback", None)
    payload["options"] = options

    body = json.dumps(payload, separators=(",", ":"))

    queue_key = k.queue_key(ns, q)
    msgs_key = k.msgs_key(ns, q)
    xq_key = k.xq_key(ns, q)

    pipe = r.pipeline(transaction=True)
    pipe.hset(msgs_key, new_rid, body)
    pipe.lpush(queue_key, new_rid)
    pipe.zrem(xq_key, rid)
    pipe.hdel(xq_msgs_key, rid)
    pipe.execute()

    return new_rid


def delete_message(
    r: "redis.Redis",
    ns: str,
    q: str,
    state: str,
    rid: str,
    *,
    stale_worker_ids: Optional[Set[str]] = None,
) -> bool:
    """Delete one message from the given state. Returns True if it existed.

    inflight deletes are only allowed when the owning worker is stale
    (caller passes the stale worker id set); otherwise raises
    ActionConflict so the route can return 409.
    """
    if state == "waiting":
        queue_key = k.queue_key(ns, q)
        msgs_key = k.msgs_key(ns, q)
        pipe = r.pipeline(transaction=True)
        pipe.lrem(queue_key, 0, rid)
        pipe.hdel(msgs_key, rid)
        removed, deleted = pipe.execute()
        return bool(removed) or bool(deleted)

    if state == "delayed":
        dq_key = k.dq_key(ns, q)
        dq_msgs_key = k.dq_msgs_key(ns, q)
        dq_type = _decode(r.type(dq_key))
        pipe = r.pipeline(transaction=True)
        if dq_type == "zset":
            pipe.zrem(dq_key, rid)
        else:
            pipe.lrem(dq_key, 0, rid)
        pipe.hdel(dq_msgs_key, rid)
        removed, deleted = pipe.execute()
        return bool(removed) or bool(deleted)

    if state == "dead":
        xq_key = k.xq_key(ns, q)
        xq_msgs_key = k.xq_msgs_key(ns, q)
        pipe = r.pipeline(transaction=True)
        pipe.zrem(xq_key, rid)
        pipe.hdel(xq_msgs_key, rid)
        removed, deleted = pipe.execute()
        return bool(removed) or bool(deleted)

    if state == "inflight":
        config = Config()
        _queues, ack_keys = discover_queues(config, r, id(r), ns)
        owning_ack_key = None
        for _worker_id, queue, ack_key in ack_keys:
            if queue != q:
                continue
            if r.sismember(ack_key, rid):
                owning_ack_key = (_worker_id, ack_key)
                break

        if owning_ack_key is None:
            return False

        worker_id, ack_key = owning_ack_key
        stale = stale_worker_ids or set()
        if worker_id not in stale:
            raise ActionConflict(f"worker {worker_id} is not stale")

        msgs_key = k.msgs_key(ns, q)
        pipe = r.pipeline(transaction=True)
        pipe.srem(ack_key, rid)
        pipe.hdel(msgs_key, rid)
        removed, deleted = pipe.execute()
        return bool(removed) or bool(deleted)

    raise ValueError(f"unknown state: {state}")


def _requeue_batch(r: "redis.Redis", ns: str, q: str, rids: Iterable[str]) -> int:
    xq_msgs_key = k.xq_msgs_key(ns, q)
    xq_key = k.xq_key(ns, q)
    queue_key = k.queue_key(ns, q)
    msgs_key = k.msgs_key(ns, q)

    raws = r.hmget(xq_msgs_key, list(rids))
    count = 0
    pipe = r.pipeline(transaction=True)
    for rid, raw in zip(rids, raws):
        if raw is None:
            continue
        payload = json.loads(raw)
        new_rid = str(uuid.uuid4())
        options = payload.get("options") or {}
        options["redis_message_id"] = new_rid
        options["retries"] = 0
        options.pop("traceback", None)
        payload["options"] = options
        body = json.dumps(payload, separators=(",", ":"))

        pipe.hset(msgs_key, new_rid, body)
        pipe.lpush(queue_key, new_rid)
        pipe.zrem(xq_key, rid)
        pipe.hdel(xq_msgs_key, rid)
        count += 1
    if count:
        pipe.execute()
    return count


def _delete_batch(r: "redis.Redis", ns: str, q: str, rids: Iterable[str]) -> int:
    xq_msgs_key = k.xq_msgs_key(ns, q)
    xq_key = k.xq_key(ns, q)
    rids = list(rids)
    if not rids:
        return 0
    pipe = r.pipeline(transaction=True)
    for rid in rids:
        pipe.zrem(xq_key, rid)
    pipe.hdel(xq_msgs_key, *rids)
    pipe.execute()
    return len(rids)


def requeue_all_dead(r: "redis.Redis", ns: str, q: str) -> int:
    """Requeue every dead message for a queue. Returns the total requeued.

    Chunked ZRANGE batches of the XQ index, each done in its own MULTI, then
    a terminal HSCAN sweep of any XQ.msgs leftovers (drift from the
    dead-letter TTL sweep racing the index) — those still have payloads, so
    they get requeued too.
    """
    xq_key = k.xq_key(ns, q)
    xq_msgs_key = k.xq_msgs_key(ns, q)
    total = 0

    while True:
        rids_raw = r.zrange(xq_key, 0, _DEAD_BATCH - 1)
        rids = [_decode(rid) for rid in rids_raw]
        if not rids:
            break
        total += _requeue_batch(r, ns, q, rids)

    cursor = 0
    while True:
        cursor, rows = r.hscan(xq_msgs_key, cursor=cursor, count=_DEAD_BATCH)
        leftover_rids = [_decode(rid) for rid in rows.keys()]
        if leftover_rids:
            total += _requeue_batch(r, ns, q, leftover_rids)
        if cursor == 0:
            break

    return total


def delete_all_dead(r: "redis.Redis", ns: str, q: str) -> int:
    """Delete every dead message for a queue. Returns the total deleted."""
    xq_key = k.xq_key(ns, q)
    xq_msgs_key = k.xq_msgs_key(ns, q)
    total = 0

    while True:
        rids_raw = r.zrange(xq_key, 0, _DEAD_BATCH - 1)
        rids = [_decode(rid) for rid in rids_raw]
        if not rids:
            break
        total += _delete_batch(r, ns, q, rids)

    cursor = 0
    while True:
        cursor, rows = r.hscan(xq_msgs_key, cursor=cursor, count=_DEAD_BATCH)
        leftover_rids = [_decode(rid) for rid in rows.keys()]
        if leftover_rids:
            r.hdel(xq_msgs_key, *leftover_rids)
            total += len(leftover_rids)
        if cursor == 0:
            break

    return total


def purge_queue(r: "redis.Redis", ns: str, q: str) -> int:
    """Drop every waiting message for a queue. Never touches ack sets, the
    delayed queue, the dead-letter queue, or deletes whole keys — only pops
    the waiting list and drops the matching `.msgs` hash entries, so
    in-flight payloads sharing that hash are left intact.
    """
    queue_key = k.queue_key(ns, q)
    msgs_key = k.msgs_key(ns, q)
    total = 0

    while True:
        try:
            rids_raw = r.lpop(queue_key, _PURGE_BATCH)
        except redis.exceptions.ResponseError:
            rids_raw = None
            popped = r.lpop(queue_key)
            if popped is not None:
                rids_raw = [popped]

        if not rids_raw:
            break

        rids = [_decode(rid) for rid in rids_raw]
        r.hdel(msgs_key, *rids)
        total += len(rids)

    return total
