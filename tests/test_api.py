from __future__ import annotations

import fakeredis
from starlette.testclient import TestClient

from dramatiq_monitor.app import create_app
from dramatiq_monitor.config import Config

from conftest import seed_queue


def _client_for(clients, **config_kwargs):
    config = Config(dbs=(0,), secret="testsecret", **config_kwargs)
    app = create_app(config, clients=clients)
    return TestClient(app)


def test_api_requeue_dead_returns_new_rid():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(r, "dramatiq-dev", "orders", dead=1)
    rid = created["dead"][0]
    client = _client_for({0: r})

    resp = client.post(
        f"/api/ns/0/dramatiq-dev/queues/orders/dead/{rid}/requeue",
        json={},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["new_rid"] != rid


def test_api_requeue_dead_404_for_unknown_rid():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", dead=1)
    client = _client_for({0: r})

    resp = client.post(
        "/api/ns/0/dramatiq-dev/queues/orders/dead/nonexistent-rid/requeue",
        json={},
    )

    assert resp.status_code == 404
    assert resp.json()["ok"] is False


def test_api_delete_message_waiting():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(r, "dramatiq-dev", "orders", waiting=1)
    rid = created["waiting"][0]
    client = _client_for({0: r})

    resp = client.post(
        f"/api/ns/0/dramatiq-dev/queues/orders/waiting/{rid}/delete",
        json={},
    )

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_api_delete_message_404_for_unknown_rid():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1)
    client = _client_for({0: r})

    resp = client.post(
        "/api/ns/0/dramatiq-dev/queues/orders/waiting/nonexistent-rid/delete",
        json={},
    )

    assert resp.status_code == 404


def test_api_delete_inflight_live_worker_409():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(
        r, "dramatiq-dev", "orders", inflight=[("live", 1)], heartbeats={"live": 0}
    )
    rid = created["inflight"][0]
    client = _client_for({0: r})

    resp = client.post(
        f"/api/ns/0/dramatiq-dev/queues/orders/inflight/{rid}/delete",
        json={},
    )

    assert resp.status_code == 409


def test_api_delete_inflight_stale_worker_ok():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(
        r,
        "dramatiq-dev",
        "orders",
        inflight=[("stale", 1)],
        heartbeats={"stale": 120_000},
    )
    rid = created["inflight"][0]
    client = _client_for({0: r})

    resp = client.post(
        f"/api/ns/0/dramatiq-dev/queues/orders/inflight/{rid}/delete",
        json={},
    )

    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_api_requeue_all_dead_requires_confirm():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", dead=3)
    client = _client_for({0: r})

    resp = client.post(
        "/api/ns/0/dramatiq-dev/queues/orders/dead/requeue-all",
        json={"confirm": "wrong-name"},
    )

    assert resp.status_code == 400


def test_api_requeue_all_dead_with_confirm():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", dead=3)
    client = _client_for({0: r})

    resp = client.post(
        "/api/ns/0/dramatiq-dev/queues/orders/dead/requeue-all",
        json={"confirm": "orders"},
    )

    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "count": 3}


def test_api_delete_all_dead_with_confirm():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", dead=2)
    client = _client_for({0: r})

    resp = client.post(
        "/api/ns/0/dramatiq-dev/queues/orders/dead/delete-all",
        json={"confirm": "orders"},
    )

    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "count": 2}


def test_api_purge_queue_requires_confirm():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=4)
    client = _client_for({0: r})

    resp = client.post(
        "/api/ns/0/dramatiq-dev/queues/orders/purge",
        json={"confirm": "nope"},
    )

    assert resp.status_code == 400


def test_api_purge_queue_with_confirm():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=4)
    client = _client_for({0: r})

    resp = client.post(
        "/api/ns/0/dramatiq-dev/queues/orders/purge",
        json={"confirm": "orders"},
    )

    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "count": 4}
