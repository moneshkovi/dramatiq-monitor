from __future__ import annotations

import argparse

import uvicorn

from . import __version__
from .app import create_app
from .config import Config


def _parse_dbs(value: str) -> tuple:
    return tuple(int(part.strip()) for part in value.split(",") if part.strip())


def _parse_namespaces(value: str) -> tuple:
    return tuple(part.strip() for part in value.split(",") if part.strip())


def build_config(args: argparse.Namespace) -> Config:
    config = Config.from_env()

    if args.redis_url is not None:
        config.redis_url = args.redis_url
    if args.redis_host is not None:
        config.host = args.redis_host
    if args.redis_port is not None:
        config.port = args.redis_port
    if args.redis_password is not None:
        config.password = args.redis_password
    if args.ssl:
        config.ssl = True
    if args.ssl_no_verify:
        config.ssl_no_verify = True
    if args.dbs is not None:
        config.dbs = _parse_dbs(args.dbs)
    if args.namespaces is not None:
        config.namespaces = _parse_namespaces(args.namespaces)
    if args.base_path is not None:
        config.base_path = args.base_path
    if args.auth_user is not None:
        config.auth_user = args.auth_user
    if args.auth_password is not None:
        config.auth_password = args.auth_password
    if args.read_only:
        config.read_only = True
    if args.poll_queues is not None:
        config.poll_queues_s = args.poll_queues
    if args.poll_workers is not None:
        config.poll_workers_s = args.poll_workers
    if args.dead_message_ttl_ms is not None:
        config.dead_message_ttl_ms = args.dead_message_ttl_ms

    return config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dramatiq-monitor")
    parser.add_argument("--redis-url", default=None)
    parser.add_argument("--redis-host", default=None)
    parser.add_argument("--redis-port", type=int, default=None)
    parser.add_argument("--redis-password", default=None)
    parser.add_argument("--ssl", action="store_true", default=False)
    parser.add_argument("--ssl-no-verify", action="store_true", default=False)
    parser.add_argument("--dbs", default=None, help="comma-separated db list, e.g. 0,1")
    parser.add_argument("--namespaces", default=None, help="comma-separated namespace allowlist")
    parser.add_argument("--base-path", default=None)
    parser.add_argument("--auth-user", default=None)
    parser.add_argument("--auth-password", default=None)
    parser.add_argument("--read-only", action="store_true", default=False)
    parser.add_argument("--poll-queues", type=int, default=None, help="queue fragment poll interval (s)")
    parser.add_argument("--poll-workers", type=int, default=None, help="worker fragment poll interval (s)")
    parser.add_argument("--dead-message-ttl-ms", type=int, default=None)
    parser.add_argument("--host", default="127.0.0.1", help="bind host")
    parser.add_argument("--port", type=int, default=8321, help="bind port")
    parser.add_argument("--version", action="version", version=f"dramatiq-monitor {__version__}")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = build_config(args)
    app = create_app(config)
    uvicorn.run(app, host=args.host, port=args.port, root_path=config.base_path)


if __name__ == "__main__":
    main()
