from __future__ import annotations

import time
from typing import Dict, List, Tuple

import redis

from .. import keys as k
from ..config import Config
from ..models import NamespaceRef

_NS_CACHE_TTL_S = 30
_QUEUE_CACHE_TTL_S = 10

# {db: (expires_at_monotonic, [NamespaceRef, ...])}
_ns_cache: Dict[int, Tuple[float, List[NamespaceRef]]] = {}

# {(db, ns): (expires_at_monotonic, (queues, ack_keys))}
_queue_cache: Dict[Tuple[int, str], Tuple[float, Tuple[list, list]]] = {}


def clear_caches() -> None:
    """Test helper: reset both in-process discovery caches."""
    _ns_cache.clear()
    _queue_cache.clear()


def _scan_all(r: "redis.Redis", match: str, count: int) -> List[bytes]:
    found: List[bytes] = []
    cursor = 0
    while True:
        cursor, chunk = r.scan(cursor=cursor, match=match, count=count)
        found.extend(chunk)
        if cursor == 0:
            break
    return found


def discover_namespaces(config: Config, clients: Dict[int, "redis.Redis"]) -> List[NamespaceRef]:
    """Discover namespaces per configured db via SCAN of `*:__heartbeats__`.

    Results are cached in-process for 30s per db. `config.namespaces` is an
    allowlist that is unioned onto every configured db (a namespace with
    queues but no worker ever has no __heartbeats__ key, so it would
    otherwise never be discovered).
    """
    now = time.monotonic()
    refs: List[NamespaceRef] = []

    for db, client in clients.items():
        cached = _ns_cache.get(db)
        if cached is not None and cached[0] > now:
            refs.extend(cached[1])
            continue

        found_keys = _scan_all(client, "*:__heartbeats__", config.scan_count)
        db_refs: List[NamespaceRef] = []
        seen = set()
        for key in found_keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            ns = k.ns_from_heartbeats_key(key_str)
            if ns not in seen:
                seen.add(ns)
                db_refs.append(NamespaceRef(db=db, ns=ns))

        for ns in config.namespaces:
            if ns not in seen:
                seen.add(ns)
                db_refs.append(NamespaceRef(db=db, ns=ns))

        _ns_cache[db] = (now + _NS_CACHE_TTL_S, db_refs)
        refs.extend(db_refs)

    return refs


def discover_queues(
    config: Config, r: "redis.Redis", db: int, ns: str
) -> Tuple[List[str], List[Tuple[str, str, str]]]:
    """Discover queue names and ack keys for a namespace via SCAN.

    Returns (queues, ack_keys) where ack_keys is a list of
    (worker_id, queue, key) tuples, one per `{ns}:__acks__.*` key found.
    Cached in-process for 10s, keyed by (db, ns).
    """
    now = time.monotonic()
    cache_key = (db, ns)
    cached = _queue_cache.get(cache_key)
    if cached is not None and cached[0] > now:
        queues, ack_keys = cached[1]
        return list(queues), list(ack_keys)

    msgs_keys = _scan_all(r, f"{ns}:*.msgs", config.scan_count)
    ack_raw_keys = _scan_all(r, k.acks_pattern(ns), config.scan_count)

    queues: List[str] = []
    seen_queues = set()
    for key in msgs_keys:
        key_str = key.decode() if isinstance(key, bytes) else key
        queue, _kind = k.queue_from_msgs_key(ns, key_str)
        if queue not in seen_queues:
            seen_queues.add(queue)
            queues.append(queue)

    ack_keys: List[Tuple[str, str, str]] = []
    for key in ack_raw_keys:
        key_str = key.decode() if isinstance(key, bytes) else key
        worker_id, queue = k.parse_ack_key(ns, key_str)
        ack_keys.append((worker_id, queue, key_str))
        if queue not in seen_queues:
            seen_queues.add(queue)
            queues.append(queue)

    result = (queues, ack_keys)
    _queue_cache[cache_key] = (now + _QUEUE_CACHE_TTL_S, result)
    return list(queues), list(ack_keys)
