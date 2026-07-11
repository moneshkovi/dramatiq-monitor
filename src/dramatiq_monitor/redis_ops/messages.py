from __future__ import annotations

import json
from typing import Dict, List, Optional

import redis

from .. import keys as k
from ..config import Config
from ..models import MessageDetail, MessageSummary, Page
from .discovery import discover_queues

_ARGS_PREVIEW_MAX = 120


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


def _args_preview(args, kwargs) -> str:
    text = repr((args, kwargs))
    if len(text) > _ARGS_PREVIEW_MAX:
        return text[: _ARGS_PREVIEW_MAX - 1] + "…"
    return text


def _summary_from_payload(rid: str, raw, extra: Optional[dict] = None) -> MessageSummary:
    payload = _parse_json(raw)
    row_extra = dict(extra or {})
    if payload is None:
        row_extra["missing_payload"] = True
        return MessageSummary(
            rid=rid,
            message_id="",
            actor_name=None,
            args_preview="-",
            enqueued_ms=None,
            eta_ms=None,
            retries=0,
            extra=row_extra,
        )

    options = payload.get("options") or {}
    return MessageSummary(
        rid=rid,
        message_id=payload.get("message_id", ""),
        actor_name=payload.get("actor_name"),
        args_preview=_args_preview(payload.get("args", []), payload.get("kwargs", {})),
        enqueued_ms=payload.get("message_timestamp"),
        eta_ms=options.get("eta"),
        retries=options.get("retries", 0),
        extra=row_extra,
    )


def _int_cursor(cursor: Optional[str]) -> int:
    if cursor is None:
        return 0
    try:
        return max(0, int(cursor))
    except (TypeError, ValueError):
        return 0


def _list_waiting(
    config: Config, r: "redis.Redis", ns: str, queue: str, cursor: Optional[str], n: int
) -> Page:
    queue_key = k.queue_key(ns, queue)
    msgs_key = k.msgs_key(ns, queue)
    off = _int_cursor(cursor)

    pipe = r.pipeline(transaction=False)
    pipe.llen(queue_key)
    pipe.lrange(queue_key, off, off + n - 1)
    total, rids_raw = pipe.execute()

    rids = [_decode(rid) for rid in rids_raw]
    raws = r.hmget(msgs_key, rids) if rids else []

    items = [_summary_from_payload(rid, raw) for rid, raw in zip(rids, raws)]

    next_off = off + len(rids)
    next_cursor = str(next_off) if next_off < (total or 0) else None
    return Page(items=items, next_cursor=next_cursor, total=total or 0)


def _list_delayed(
    config: Config, r: "redis.Redis", ns: str, queue: str, cursor: Optional[str], n: int
) -> Page:
    dq_key = k.dq_key(ns, queue)
    dq_msgs_key = k.dq_msgs_key(ns, queue)
    msgs_key = k.msgs_key(ns, queue)
    off = _int_cursor(cursor)

    dq_type = _decode(r.type(dq_key))

    rids: List[str] = []
    scores: Dict[str, int] = {}
    total = 0

    if dq_type == "zset":
        total = r.zcard(dq_key)
        rows = r.zrange(dq_key, off, off + n - 1, withscores=True)
        for rid_raw, score in rows:
            rid = _decode(rid_raw)
            rids.append(rid)
            scores[rid] = int(score)
    else:
        total = r.llen(dq_key)
        rows = r.lrange(dq_key, off, off + n - 1)
        rids = [_decode(rid) for rid in rows]

    pipe = r.pipeline(transaction=False)
    if rids:
        pipe.hmget(dq_msgs_key, rids)
    raws = pipe.execute()[0] if rids else []

    missing_idx = [i for i, raw in enumerate(raws) if raw is None]
    fallback_raws = {}
    if missing_idx:
        missing_rids = [rids[i] for i in missing_idx]
        fallback = r.hmget(msgs_key, missing_rids)
        fallback_raws = dict(zip(missing_rids, fallback))

    items = []
    for rid, raw in zip(rids, raws):
        if raw is None:
            raw = fallback_raws.get(rid)
        extra = {}
        if rid in scores:
            extra["dq_score"] = scores[rid]
        items.append(_summary_from_payload(rid, raw, extra))

    next_off = off + len(rids)
    next_cursor = str(next_off) if next_off < total else None
    return Page(items=items, next_cursor=next_cursor, total=total)


def _list_dead(
    config: Config, r: "redis.Redis", ns: str, queue: str, cursor: Optional[str], n: int
) -> Page:
    xq_key = k.xq_key(ns, queue)
    xq_msgs_key = k.xq_msgs_key(ns, queue)

    zcard = r.zcard(xq_key)
    hlen = r.hlen(xq_msgs_key)

    if zcard == 0 and hlen > 0:
        # Drift: XQ index emptied (or never populated) but payload hash
        # still has entries. Fall back to an HSCAN sweep, using the raw
        # HSCAN cursor (opaque string) as our page cursor.
        hscan_cursor = _int_cursor(cursor)
        next_hscan_cursor, rows = r.hscan(xq_msgs_key, cursor=hscan_cursor, count=n)
        items = [
            _summary_from_payload(_decode(rid), raw) for rid, raw in list(rows.items())[:n]
        ]
        next_cursor = str(next_hscan_cursor) if next_hscan_cursor != 0 else None
        return Page(items=items, next_cursor=next_cursor, total=hlen)

    off = _int_cursor(cursor)
    total = zcard
    rows = r.zrevrange(xq_key, off, off + n - 1, withscores=True)
    rids = [_decode(rid) for rid, _score in rows]
    died_at = {_decode(rid): int(score) for rid, score in rows}

    raws = r.hmget(xq_msgs_key, rids) if rids else []

    items = []
    for rid, raw in zip(rids, raws):
        extra = {"died_at_ms": died_at.get(rid)}
        items.append(_summary_from_payload(rid, raw, extra))

    next_off = off + len(rids)
    next_cursor = str(next_off) if next_off < total else None
    return Page(items=items, next_cursor=next_cursor, total=total)


def _list_inflight(
    config: Config, r: "redis.Redis", ns: str, queue: str, cursor: Optional[str], n: int
) -> Page:
    # list_messages doesn't receive `db` (per the messages-page routes, which
    # key on ns+queue+state only); discover_queues' cache key only needs to
    # be unique per (client, ns), so key on id(r) instead of a real db int.
    _queues, ack_keys = discover_queues(config, r, id(r), ns)
    relevant = [(worker_id, ack_key) for worker_id, q, ack_key in ack_keys if q == queue]

    now_ms = _server_now_ms(r)
    heartbeats_key = k.heartbeats_key(ns)
    msgs_key = k.msgs_key(ns, queue)

    pipe = r.pipeline(transaction=False)
    pipe.zrange(heartbeats_key, 0, -1, withscores=True)
    for _worker_id, ack_key in relevant:
        pipe.smembers(ack_key)
    results = pipe.execute()

    heartbeat_rows = results[0]
    heartbeats = {_decode(w): int(s) for w, s in heartbeat_rows}

    rows = []  # (worker_id, rid)
    for (worker_id, _ack_key), members in zip(relevant, results[1:]):
        for rid_raw in members:
            rows.append((worker_id, _decode(rid_raw)))

    rows = rows[:n]
    rids = [rid for _worker_id, rid in rows]
    raws = r.hmget(msgs_key, rids) if rids else []

    items = []
    for (worker_id, rid), raw in zip(rows, raws):
        score = heartbeats.get(worker_id)
        stale = score is None or (now_ms - score) > config.stale_worker_s * 1000
        extra = {"worker_id": worker_id, "stale": stale}
        items.append(_summary_from_payload(rid, raw, extra))

    return Page(items=items, next_cursor=None, total=len(rows))


def list_messages(
    config: Config,
    r: "redis.Redis",
    ns: str,
    queue: str,
    state: str,
    cursor: Optional[str] = None,
    n: int = 50,
) -> Page:
    """List a page of messages for `queue` in `state`.

    state in {waiting, delayed, dead, inflight}. Cursor is an opaque string
    (int offset for waiting/delayed/dead's zset path, HSCAN cursor for dead's
    drift-fallback path); None when the caller should stop paginating.
    """
    if state == "waiting":
        return _list_waiting(config, r, ns, queue, cursor, n)
    if state == "delayed":
        return _list_delayed(config, r, ns, queue, cursor, n)
    if state == "dead":
        return _list_dead(config, r, ns, queue, cursor, n)
    if state == "inflight":
        return _list_inflight(config, r, ns, queue, cursor, n)
    raise ValueError(f"unknown state: {state}")


def get_message(
    config: Config, r: "redis.Redis", ns: str, queue: str, state: str, rid: str
) -> Optional[MessageDetail]:
    """Fetch full detail for one message, or None if not found."""
    if state in ("waiting", "inflight"):
        hash_key = k.msgs_key(ns, queue)
    elif state == "delayed":
        hash_key = k.dq_msgs_key(ns, queue)
    elif state == "dead":
        hash_key = k.xq_msgs_key(ns, queue)
    else:
        raise ValueError(f"unknown state: {state}")

    pipe = r.pipeline(transaction=False)
    pipe.hget(hash_key, rid)
    pipe.ttl(hash_key)
    if state == "dead":
        pipe.zscore(k.xq_key(ns, queue), rid)
    results = pipe.execute()

    raw = results[0]
    ttl_s = results[1]
    died_score = results[2] if state == "dead" else None

    if raw is None:
        return None

    payload = _parse_json(raw)
    if payload is None:
        return None

    options = payload.get("options") or {}
    died_at_ms = int(died_score) if died_score is not None else None

    now_ms = _server_now_ms(r)
    remaining_ttl_hint_ms = None
    if state == "dead" and config.dead_message_ttl_ms is not None and died_at_ms is not None:
        remaining_ttl_hint_ms = config.dead_message_ttl_ms - (now_ms - died_at_ms)

    raw_bytes = raw if isinstance(raw, (bytes, bytearray)) else raw.encode()

    return MessageDetail(
        rid=rid,
        message_id=payload.get("message_id", ""),
        actor_name=payload.get("actor_name"),
        queue_name=payload.get("queue_name", queue),
        state=state,
        args=payload.get("args", []),
        kwargs=payload.get("kwargs", {}),
        options=options,
        enqueued_ms=payload.get("message_timestamp"),
        eta_ms=options.get("eta"),
        retries=options.get("retries", 0),
        traceback=options.get("traceback"),
        died_at_ms=died_at_ms,
        msgs_key_ttl_s=ttl_s if ttl_s is not None and ttl_s >= 0 else None,
        remaining_ttl_hint_ms=remaining_ttl_hint_ms,
        raw_size_bytes=len(raw_bytes),
        payload_pretty=json.dumps(payload, indent=2),
    )
