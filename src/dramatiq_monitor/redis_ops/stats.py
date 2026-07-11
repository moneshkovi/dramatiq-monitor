from __future__ import annotations

import json
from typing import Dict, List, Optional, Tuple

import redis

from .. import keys as k
from ..config import Config
from ..models import QueueStats, WorkerInfo
from .discovery import discover_queues


def _decode(value):
    if isinstance(value, bytes):
        return value.decode()
    return value


def _server_now_ms(r: "redis.Redis") -> int:
    seconds, micros = r.time()
    return int(seconds) * 1000 + int(micros) // 1000


def _parse_json(payload) -> Optional[dict]:
    if payload is None:
        return None
    try:
        return json.loads(payload)
    except (ValueError, TypeError):
        return None


def queue_stats(config: Config, r: "redis.Redis", db: int, ns: str) -> List[QueueStats]:
    """Compute per-queue stats for a namespace.

    WAITING = LLEN q; DELAYED = DQ count (list or zset, TYPE-checked);
    LIVE/ORPHANED = SCARD of ack sets split by heartbeat freshness against
    config.stale_worker_s; FAILED = max(ZCARD XQ, HLEN XQ.msgs), with
    failed_drift=True when those two disagree (dead-letter TTL sweep race).
    """
    queues, ack_keys = discover_queues(config, r, db, ns)
    if not queues:
        return []

    now_ms = _server_now_ms(r)
    heartbeats_key = k.heartbeats_key(ns)

    # Pipeline A: heartbeats snapshot + per-queue base counts + per-ack SCARD.
    pipe_a = r.pipeline(transaction=False)
    pipe_a.zrange(heartbeats_key, 0, -1, withscores=True)
    for q in queues:
        pipe_a.llen(k.queue_key(ns, q))
        pipe_a.type(k.dq_key(ns, q))
        pipe_a.zcard(k.xq_key(ns, q))
        pipe_a.hlen(k.xq_msgs_key(ns, q))
        pipe_a.lindex(k.queue_key(ns, q), -1)
    for _worker_id, _queue, ack_key in ack_keys:
        pipe_a.scard(ack_key)

    results_a = pipe_a.execute()

    idx = 0
    heartbeat_rows = results_a[idx]
    idx += 1

    per_queue_a: Dict[str, dict] = {}
    for q in queues:
        waiting = results_a[idx]
        dq_type = _decode(results_a[idx + 1])
        xq_zcard = results_a[idx + 2]
        xq_hlen = results_a[idx + 3]
        tail_rid = results_a[idx + 4]
        idx += 5
        per_queue_a[q] = {
            "waiting": waiting or 0,
            "dq_type": dq_type,
            "xq_zcard": xq_zcard or 0,
            "xq_hlen": xq_hlen or 0,
            "tail_rid": tail_rid,
        }

    ack_scards: List[int] = []
    for _ in ack_keys:
        ack_scards.append(results_a[idx] or 0)
        idx += 1

    heartbeats: Dict[str, int] = {}
    for worker_id_raw, score in heartbeat_rows:
        heartbeats[_decode(worker_id_raw)] = int(score)

    # Pipeline B: DQ count (LLEN or ZCARD depending on TYPE) + oldest waiting
    # payload lookup (HGET q.msgs <tail_rid>).
    pipe_b = r.pipeline(transaction=False)
    for q in queues:
        dq_type = per_queue_a[q]["dq_type"]
        if dq_type == "zset":
            pipe_b.zcard(k.dq_key(ns, q))
        else:
            pipe_b.llen(k.dq_key(ns, q))
        tail_rid = per_queue_a[q]["tail_rid"]
        if tail_rid is not None:
            pipe_b.hget(k.msgs_key(ns, q), tail_rid)

    results_b = pipe_b.execute()

    idx = 0
    per_queue_b: Dict[str, dict] = {}
    for q in queues:
        delayed = results_b[idx] or 0
        idx += 1
        tail_rid = per_queue_a[q]["tail_rid"]
        oldest_payload = None
        if tail_rid is not None:
            oldest_payload = results_b[idx]
            idx += 1
        per_queue_b[q] = {"delayed": delayed, "oldest_payload": oldest_payload}

    # Per-queue live/orphaned tallies from ack keys.
    live_by_queue: Dict[str, int] = {q: 0 for q in queues}
    orphaned_by_queue: Dict[str, int] = {q: 0 for q in queues}
    for (worker_id, queue, _ack_key), cnt in zip(ack_keys, ack_scards):
        if queue not in live_by_queue:
            continue
        score = heartbeats.get(worker_id)
        if score is not None and (now_ms - score) <= config.stale_worker_s * 1000:
            live_by_queue[queue] += cnt
        else:
            orphaned_by_queue[queue] += cnt

    stats: List[QueueStats] = []
    for q in queues:
        a = per_queue_a[q]
        b = per_queue_b[q]

        xq_zcard = a["xq_zcard"]
        xq_hlen = a["xq_hlen"]
        failed = max(xq_zcard, xq_hlen)
        failed_drift = xq_zcard != xq_hlen

        oldest_age_ms: Optional[int] = None
        payload = _parse_json(b["oldest_payload"])
        if payload is not None:
            ts = payload.get("message_timestamp")
            if isinstance(ts, (int, float)):
                oldest_age_ms = now_ms - int(ts)

        stats.append(
            QueueStats(
                name=q,
                waiting=a["waiting"],
                delayed=b["delayed"],
                live=live_by_queue[q],
                orphaned=orphaned_by_queue[q],
                failed=failed,
                failed_drift=failed_drift,
                oldest_waiting_age_ms=oldest_age_ms,
            )
        )

    return stats


def worker_stats(config: Config, r: "redis.Redis", db: int, ns: str) -> List[WorkerInfo]:
    """Compute per-worker info for a namespace.

    Workers present in `__heartbeats__` are scored by heartbeat age.
    Workers absent from heartbeats but present via an ack key are still
    reported, as orphaned, with heartbeat_age_ms=None. inflight is the sum of
    SCARD across all of a worker's ack keys (across queues).
    """
    _queues, ack_keys = discover_queues(config, r, db, ns)

    now_ms = _server_now_ms(r)
    heartbeats_key = k.heartbeats_key(ns)

    pipe = r.pipeline(transaction=False)
    pipe.zrange(heartbeats_key, 0, -1, withscores=True)
    for _worker_id, _queue, ack_key in ack_keys:
        pipe.scard(ack_key)
    pipe.hgetall(k.worker_meta_key(ns))
    results = pipe.execute()

    heartbeat_rows = results[0]
    ack_scards = results[1:-1]
    meta_raw = results[-1] or {}

    heartbeats: Dict[str, int] = {}
    worker_order: List[str] = []
    for worker_id_raw, score in heartbeat_rows:
        worker_id = _decode(worker_id_raw)
        heartbeats[worker_id] = int(score)
        worker_order.append(worker_id)

    inflight_by_worker: Dict[str, int] = {}
    for (worker_id, _queue, _ack_key), cnt in zip(ack_keys, ack_scards):
        inflight_by_worker[worker_id] = inflight_by_worker.get(worker_id, 0) + (cnt or 0)
        if worker_id not in heartbeats and worker_id not in worker_order:
            worker_order.append(worker_id)

    meta_by_worker: Dict[str, Optional[dict]] = {}
    for worker_id_raw, meta_payload in meta_raw.items():
        worker_id = _decode(worker_id_raw)
        meta_by_worker[worker_id] = _parse_json(meta_payload)

    workers: List[WorkerInfo] = []
    for worker_id in worker_order:
        score = heartbeats.get(worker_id)
        heartbeat_age_ms = (now_ms - score) if score is not None else None
        workers.append(
            WorkerInfo(
                worker_id=worker_id,
                heartbeat_age_ms=heartbeat_age_ms,
                inflight=inflight_by_worker.get(worker_id, 0),
                meta=meta_by_worker.get(worker_id),
            )
        )

    return workers
