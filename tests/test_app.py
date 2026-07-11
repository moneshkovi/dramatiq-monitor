from __future__ import annotations

import re

import fakeredis
import pytest
from starlette.testclient import TestClient

from dramatiq_monitor.app import create_app
from dramatiq_monitor.config import Config

from conftest import seed_queue


def _client_for(clients, dbs=(0,)):
    config = Config(dbs=dbs)
    app = create_app(config, clients=clients)
    return TestClient(app)


def test_index_redirects_to_only_namespace():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/", follow_redirects=False)

    assert resp.status_code in (302, 307)
    assert resp.headers["location"].endswith("/ns/0/dramatiq-dev/")


def test_index_redirects_to_namespaces_when_multiple():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    seed_queue(r, "dramatiq-prod", "orders", waiting=1, heartbeats={"w2": 0})
    client = _client_for({0: r})

    resp = client.get("/", follow_redirects=False)

    assert resp.status_code in (302, 307)
    assert resp.headers["location"].endswith("/namespaces")


def test_namespaces_page_lists_seeded_namespace():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=2, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/namespaces")

    assert resp.status_code == 200
    assert "dramatiq-dev" in resp.text


def test_overview_shows_queue_names_and_header():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=3, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/ns/0/dramatiq-dev/")

    assert resp.status_code == 200
    assert "orders" in resp.text
    assert "dramatiq-dev" in resp.text
    assert "db 0" in resp.text


def test_fragment_queues_returns_rows_only():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=3, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/fragments/ns/0/dramatiq-dev/queues")

    assert resp.status_code == 200
    assert "<html" not in resp.text.lower()
    assert "<body" not in resp.text.lower()
    assert "orders" in resp.text


def test_fragment_workers_returns_rows_only():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/fragments/ns/0/dramatiq-dev/workers")

    assert resp.status_code == 200
    assert "<html" not in resp.text.lower()
    assert "<body" not in resp.text.lower()
    assert "w1"[:8] in resp.text


def test_healthz_ok():
    r = fakeredis.FakeRedis(decode_responses=False)
    client = _client_for({0: r})

    resp = client.get("/healthz")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_healthz_reports_503_when_ping_fails(monkeypatch):
    r = fakeredis.FakeRedis(decode_responses=False)

    def _boom(*args, **kwargs):
        raise ConnectionError("nope")

    monkeypatch.setattr(r, "ping", _boom)
    client = _client_for({0: r})

    resp = client.get("/healthz")

    assert resp.status_code == 503


def test_unknown_db_404():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/ns/7/dramatiq-dev/")

    assert resp.status_code == 404


def test_static_assets_served():
    r = fakeredis.FakeRedis(decode_responses=False)
    client = _client_for({0: r})

    resp = client.get("/static/style.css")
    assert resp.status_code == 200

    resp = client.get("/static/htmx.min.js")
    assert resp.status_code == 200


def test_messages_page_200_for_each_state():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(
        r,
        "dramatiq-dev",
        "orders",
        waiting=2,
        delayed_list=1,
        dead=1,
        inflight=[("w1", 1)],
        heartbeats={"w1": 0},
    )
    client = _client_for({0: r})

    for state in ("waiting", "delayed", "dead", "inflight"):
        resp = client.get(f"/ns/0/dramatiq-dev/queues/orders/{state}")
        assert resp.status_code == 200, state
        assert "orders" in resp.text


def test_messages_page_enqueued_age_is_recent_not_epoch_age():
    # Regression: enqueued_ms is an absolute epoch timestamp, not a
    # duration; rendering it straight through fmt_age (instead of
    # fmt_age_since(now_ms)) produced bogus multi-decade ages like "20644d".
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    rid = created["waiting"][0]
    client = _client_for({0: r})

    resp = client.get("/ns/0/dramatiq-dev/queues/orders/waiting")
    assert resp.status_code == 200
    assert not re.search(r"\b\d{3,}d\b", resp.text)

    resp = client.get(f"/ns/0/dramatiq-dev/queues/orders/waiting/msg/{rid}")
    assert resp.status_code == 200
    assert not re.search(r"\b\d{3,}d\b", resp.text)


def test_messages_page_unknown_state_404():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/ns/0/dramatiq-dev/queues/orders/bogus")

    assert resp.status_code == 404


def test_fragment_message_rows_returns_rows_only():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=2, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/fragments/ns/0/dramatiq-dev/queues/orders/waiting/rows")

    assert resp.status_code == 200
    assert "<html" not in resp.text.lower()
    assert "<body" not in resp.text.lower()


def test_message_detail_page_200():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    rid = created["waiting"][0]
    client = _client_for({0: r})

    resp = client.get(f"/ns/0/dramatiq-dev/queues/orders/waiting/msg/{rid}")

    assert resp.status_code == 200
    assert rid in resp.text


def test_message_detail_page_missing_redirects_with_flash():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get(
        "/ns/0/dramatiq-dev/queues/orders/waiting/msg/nonexistent-rid",
        follow_redirects=False,
    )

    assert resp.status_code in (302, 307)
    assert resp.headers["location"].endswith(
        "/ns/0/dramatiq-dev/queues/orders/waiting?flash=gone"
    )


def test_api_namespaces():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/api/namespaces")

    assert resp.status_code == 200
    body = resp.json()
    assert body == [{"db": 0, "ns": "dramatiq-dev"}]


def test_api_queues():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=2, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/api/ns/0/dramatiq-dev/queues")

    assert resp.status_code == 200
    body = resp.json()
    assert body[0]["name"] == "orders"
    assert body[0]["waiting"] == 2


def test_api_workers():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/api/ns/0/dramatiq-dev/workers")

    assert resp.status_code == 200
    body = resp.json()
    assert body[0]["worker_id"] == "w1"


def test_api_messages_shape_and_cursor():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=3, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/api/ns/0/dramatiq-dev/queues/orders/waiting?n=2")

    assert resp.status_code == 200
    body = resp.json()
    assert set(body.keys()) == {"items", "next_cursor", "total"}
    assert len(body["items"]) == 2
    assert body["total"] == 3
    assert body["next_cursor"] == "2"


def test_api_message_detail():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    rid = created["waiting"][0]
    client = _client_for({0: r})

    resp = client.get(f"/api/ns/0/dramatiq-dev/queues/orders/waiting/msg/{rid}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["rid"] == rid
    assert body["state"] == "waiting"


def test_api_message_detail_404_for_unknown_rid():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/api/ns/0/dramatiq-dev/queues/orders/waiting/msg/nonexistent-rid")

    assert resp.status_code == 404


def test_api_messages_unknown_state_404():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    client = _client_for({0: r})

    resp = client.get("/api/ns/0/dramatiq-dev/queues/orders/bogus")

    assert resp.status_code == 404
