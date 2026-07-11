from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class NamespaceRef:
    db: int
    ns: str


@dataclass(frozen=True)
class QueueStats:
    name: str
    waiting: int
    delayed: int
    live: int
    orphaned: int
    failed: int
    failed_drift: bool
    oldest_waiting_age_ms: Optional[int]


@dataclass(frozen=True)
class WorkerInfo:
    worker_id: str
    heartbeat_age_ms: Optional[int]
    inflight: int
    meta: Optional[dict]


@dataclass(frozen=True)
class MessageSummary:
    rid: str
    message_id: str
    actor_name: Optional[str]
    args_preview: str
    enqueued_ms: Optional[int]
    eta_ms: Optional[int]
    retries: int
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MessageDetail:
    rid: str
    message_id: str
    actor_name: Optional[str]
    queue_name: str
    state: str
    args: list
    kwargs: dict
    options: Dict[str, Any]
    enqueued_ms: Optional[int]
    eta_ms: Optional[int]
    retries: int
    traceback: Optional[str]
    died_at_ms: Optional[int]
    msgs_key_ttl_s: Optional[int]
    remaining_ttl_hint_ms: Optional[int]
    raw_size_bytes: Optional[int]
    payload_pretty: Optional[str]
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Page:
    items: list
    next_cursor: Optional[str]
    total: Optional[int]
