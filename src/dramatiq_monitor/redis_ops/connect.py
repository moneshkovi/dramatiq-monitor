from __future__ import annotations

from typing import Dict

import redis

from ..config import Config

_CONNECT_TIMEOUT_S = 5
_OPS_TIMEOUT_S = 10

_clients: Dict[int, "redis.Redis"] = {}


def get_client(config: Config, db: int) -> "redis.Redis":
    """Return a cached redis.Redis client for `db`, creating it if needed.

    decode_responses is always False: callers decode bytes explicitly so the
    key-parsing helpers in keys.py stay str-only and testable in isolation.
    """
    cached = _clients.get(db)
    if cached is not None:
        return cached

    if config.redis_url:
        client = redis.Redis.from_url(
            config.redis_url,
            decode_responses=False,
            socket_connect_timeout=_CONNECT_TIMEOUT_S,
            socket_timeout=_OPS_TIMEOUT_S,
        )
        # `db` kwarg to from_url only applies when the URL has no path/db
        # querystring of its own; force the override explicitly so callers
        # can always pin a specific db regardless of the configured URL.
        client.connection_pool.connection_kwargs["db"] = db
    else:
        kwargs = dict(
            host=config.host,
            port=config.port,
            db=db,
            password=config.password,
            decode_responses=False,
            socket_connect_timeout=_CONNECT_TIMEOUT_S,
            socket_timeout=_OPS_TIMEOUT_S,
        )
        if config.ssl:
            kwargs["ssl"] = True
            if config.ssl_no_verify:
                kwargs["ssl_cert_reqs"] = None
                kwargs["ssl_check_hostname"] = False
        client = redis.Redis(**kwargs)

    _clients[db] = client
    return client


def clear_clients() -> None:
    """Test helper: drop the client cache between tests."""
    _clients.clear()
