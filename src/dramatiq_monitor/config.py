from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional, Tuple


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    if val is None or val == "":
        return default
    return int(val)


def _env_optional_int(name: str, default: Optional[int]) -> Optional[int]:
    val = os.environ.get(name)
    if val is None or val == "":
        return default
    return int(val)


def _env_tuple(name: str, default: Tuple[str, ...]) -> Tuple[str, ...]:
    val = os.environ.get(name)
    if val is None or val.strip() == "":
        return default
    return tuple(part.strip() for part in val.split(",") if part.strip())


def _env_dbs(name: str, default: Tuple[int, ...]) -> Tuple[int, ...]:
    val = os.environ.get(name)
    if val is None or val.strip() == "":
        return default
    return tuple(int(part.strip()) for part in val.split(",") if part.strip())


@dataclass
class Config:
    redis_url: Optional[str] = None
    host: str = "127.0.0.1"
    port: int = 6379
    password: Optional[str] = None
    ssl: bool = False
    ssl_no_verify: bool = False
    dbs: tuple = (0,)
    namespaces: tuple = ()
    base_path: str = ""
    auth_user: Optional[str] = None
    auth_password: Optional[str] = None
    read_only: bool = False
    poll_queues_s: int = 3
    poll_workers_s: int = 5
    dead_message_ttl_ms: Optional[int] = None
    secret: Optional[str] = None
    stale_worker_s: int = 60
    scan_count: int = 10000

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            redis_url=os.environ.get("DM_REDIS_URL") or None,
            host=os.environ.get("DM_REDIS_HOST", "127.0.0.1"),
            port=_env_int("DM_REDIS_PORT", 6379),
            password=os.environ.get("DM_REDIS_PASSWORD") or None,
            ssl=_env_bool("DM_REDIS_SSL", False),
            ssl_no_verify=_env_bool("DM_REDIS_SSL_NO_VERIFY", False),
            dbs=_env_dbs("DM_DBS", (0,)),
            namespaces=_env_tuple("DM_NAMESPACES", ()),
            base_path=os.environ.get("DM_BASE_PATH", ""),
            auth_user=os.environ.get("DM_AUTH_USER") or None,
            auth_password=os.environ.get("DM_AUTH_PASSWORD") or None,
            read_only=_env_bool("DM_READ_ONLY", False),
            dead_message_ttl_ms=_env_optional_int("DM_DEAD_MESSAGE_TTL_MS", None),
            secret=os.environ.get("DM_SECRET") or None,
        )
