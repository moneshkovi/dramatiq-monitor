from __future__ import annotations

from dramatiq_monitor.config import Config


def test_from_env_defaults(monkeypatch):
    for name in (
        "DM_REDIS_URL",
        "DM_REDIS_HOST",
        "DM_REDIS_PORT",
        "DM_REDIS_PASSWORD",
        "DM_REDIS_SSL",
        "DM_REDIS_SSL_NO_VERIFY",
        "DM_DBS",
        "DM_NAMESPACES",
        "DM_BASE_PATH",
        "DM_AUTH_USER",
        "DM_AUTH_PASSWORD",
        "DM_READ_ONLY",
        "DM_DEAD_MESSAGE_TTL_MS",
        "DM_SECRET",
    ):
        monkeypatch.delenv(name, raising=False)

    cfg = Config.from_env()
    assert cfg.redis_url is None
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 6379
    assert cfg.password is None
    assert cfg.ssl is False
    assert cfg.ssl_no_verify is False
    assert cfg.dbs == (0,)
    assert cfg.namespaces == ()
    assert cfg.base_path == ""
    assert cfg.auth_user is None
    assert cfg.auth_password is None
    assert cfg.read_only is False
    assert cfg.dead_message_ttl_ms is None
    assert cfg.secret is None


def test_from_env_parses_values(monkeypatch):
    monkeypatch.setenv("DM_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DM_REDIS_HOST", "10.0.0.5")
    monkeypatch.setenv("DM_REDIS_PORT", "6380")
    monkeypatch.setenv("DM_REDIS_PASSWORD", "secretpw")
    monkeypatch.setenv("DM_REDIS_SSL", "true")
    monkeypatch.setenv("DM_REDIS_SSL_NO_VERIFY", "yes")
    monkeypatch.setenv("DM_NAMESPACES", "dramatiq-dev, dramatiq-test")
    monkeypatch.setenv("DM_BASE_PATH", "/monitor")
    monkeypatch.setenv("DM_AUTH_USER", "admin")
    monkeypatch.setenv("DM_AUTH_PASSWORD", "hunter2")
    monkeypatch.setenv("DM_READ_ONLY", "1")
    monkeypatch.setenv("DM_DEAD_MESSAGE_TTL_MS", "7200000")
    monkeypatch.setenv("DM_SECRET", "abc123")

    cfg = Config.from_env()
    assert cfg.redis_url == "redis://localhost:6379/0"
    assert cfg.host == "10.0.0.5"
    assert cfg.port == 6380
    assert cfg.password == "secretpw"
    assert cfg.ssl is True
    assert cfg.ssl_no_verify is True
    assert cfg.namespaces == ("dramatiq-dev", "dramatiq-test")
    assert cfg.base_path == "/monitor"
    assert cfg.auth_user == "admin"
    assert cfg.auth_password == "hunter2"
    assert cfg.read_only is True
    assert cfg.dead_message_ttl_ms == 7200000
    assert cfg.secret == "abc123"


def test_from_env_dbs_list_parse(monkeypatch):
    monkeypatch.setenv("DM_DBS", "0,1")
    cfg = Config.from_env()
    assert cfg.dbs == (0, 1)


def test_config_dbs_default():
    cfg = Config()
    assert cfg.dbs == (0,)
    assert cfg.stale_worker_s == 60
    assert cfg.scan_count == 10000
