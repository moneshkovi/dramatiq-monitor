from __future__ import annotations

from typing import Tuple

# Single source of truth for Dramatiq's Redis key scheme (v1.17).
#
#   {ns}:{queue}                      LIST   waiting message ids (rids)
#   {ns}:{queue}.msgs                 HASH   rid -> JSON payload
#   {ns}:{queue}.DQ (+ .DQ.msgs)      delayed queue (list or zset; TYPE-check)
#   {ns}:{queue}.XQ                   ZSET   dead rids scored by death-ts-ms
#   {ns}:{queue}.XQ.msgs              HASH   dead payloads
#   {ns}:__acks__.{worker_uuid}.{q}   SET    in-flight rids per worker
#   {ns}:__heartbeats__               ZSET   worker_uuid -> last-heartbeat-ms

_ACKS_INFIX = "__acks__."
_UUID4_LEN = 36  # e.g. "3fa85f64-5717-4562-b3fc-2c963f66afa6"


def queue_key(ns: str, q: str) -> str:
    return f"{ns}:{q}"


def msgs_key(ns: str, q: str) -> str:
    return f"{ns}:{q}.msgs"


def dq_key(ns: str, q: str) -> str:
    return f"{ns}:{q}.DQ"


def dq_msgs_key(ns: str, q: str) -> str:
    return f"{ns}:{q}.DQ.msgs"


def xq_key(ns: str, q: str) -> str:
    return f"{ns}:{q}.XQ"


def xq_msgs_key(ns: str, q: str) -> str:
    return f"{ns}:{q}.XQ.msgs"


def acks_pattern(ns: str) -> str:
    return f"{ns}:{_ACKS_INFIX}*"


def heartbeats_key(ns: str) -> str:
    return f"{ns}:__heartbeats__"


def worker_meta_key(ns: str) -> str:
    return f"{ns}:__worker_meta__"


def ns_from_heartbeats_key(key: str) -> str:
    """`{ns}:__heartbeats__` -> ns"""
    suffix = ":__heartbeats__"
    if key.endswith(suffix):
        return key[: -len(suffix)]
    # Defensive fallback: split on the last ':' before the marker.
    return key.rsplit(":", 1)[0]


def queue_from_msgs_key(ns: str, key: str) -> Tuple[str, str]:
    """`{ns}:{queue}.msgs` / `.DQ.msgs` / `.XQ.msgs` -> (queue, kind).

    kind in {"waiting", "delayed", "dead"}.
    """
    prefix = f"{ns}:"
    body = key[len(prefix):] if key.startswith(prefix) else key

    if body.endswith(".DQ.msgs"):
        return body[: -len(".DQ.msgs")], "delayed"
    if body.endswith(".XQ.msgs"):
        return body[: -len(".XQ.msgs")], "dead"
    if body.endswith(".msgs"):
        return body[: -len(".msgs")], "waiting"
    return body, "waiting"


def parse_ack_key(ns: str, key: str) -> Tuple[str, str]:
    """`{ns}:__acks__.{worker_uuid}.{queue}` -> (worker_id, queue).

    A uuid4 never contains dots and is always 36 chars, so the worker id is
    the first 36 chars after the `__acks__.` prefix and the queue is
    everything after the following separator dot. Falls back to a single
    split on the first "." if the fixed-width slice looks wrong (e.g. a
    non-uuid4 worker id in test/legacy data).
    """
    prefix = f"{ns}:{_ACKS_INFIX}"
    s = key[len(prefix):] if key.startswith(prefix) else key

    if len(s) > _UUID4_LEN and s[_UUID4_LEN] == ".":
        worker_id = s[:_UUID4_LEN]
        queue = s[_UUID4_LEN + 1:]
        return worker_id, queue

    worker_id, _, queue = s.partition(".")
    return worker_id, queue
