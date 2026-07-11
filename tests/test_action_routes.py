from __future__ import annotations

import fakeredis
from starlette.testclient import TestClient

from dramatiq_monitor import keys as k
from dramatiq_monitor.app import create_app
from dramatiq_monitor.config import Config

from conftest import seed_queue


def _client_for(clients, **config_kwargs):
    config = Config(dbs=(0,), secret="testsecret", **config_kwargs)
    app = create_app(config, clients=clients)
    return TestClient(app)


def _csrf_token(client: TestClient, path: str) -> str:
    resp = client.get(path)
    return resp.cookies.get("dm_csrf")


def test_requeue_dead_page_redirects_with_flash():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(r, "dramatiq-dev", "orders", dead=1)
    rid = created["dead"][0]
    client = _client_for({0: r})
    token = _csrf_token(client, "/ns/0/dramatiq-dev/queues/orders/dead")

    resp = client.post(
        f"/ns/0/dramatiq-dev/queues/orders/dead/{rid}/requeue",
        data={"csrf_token": token},
        follow_redirects=False,
    )

    assert resp.status_code == 303
    location = resp.headers["location"]
    assert "/ns/0/dramatiq-dev/queues/orders/dead" in location
    assert "flash=requeued" in location


def test_requeue_dead_page_raced_gone_flash():
    # Race: message already swept between page load and POST.
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(r, "dramatiq-dev", "orders", dead=1)
    rid = created["dead"][0]
    client = _client_for({0: r})
    token = _csrf_token(client, "/ns/0/dramatiq-dev/queues/orders/dead")

    r.hdel(k.xq_msgs_key("dramatiq-dev", "orders"), rid)

    resp = client.post(
        f"/ns/0/dramatiq-dev/queues/orders/dead/{rid}/requeue",
        data={"csrf_token": token},
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "flash=gone" in resp.headers["location"]


def test_delete_message_page_redirects_with_flash():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(r, "dramatiq-dev", "orders", waiting=1)
    rid = created["waiting"][0]
    client = _client_for({0: r})
    token = _csrf_token(client, "/ns/0/dramatiq-dev/queues/orders/waiting")

    resp = client.post(
        f"/ns/0/dramatiq-dev/queues/orders/waiting/{rid}/delete",
        data={"csrf_token": token},
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "flash=deleted" in resp.headers["location"]


def test_requeue_all_dead_page_requires_confirm():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", dead=3)
    client = _client_for({0: r})
    token = _csrf_token(client, "/ns/0/dramatiq-dev/queues/orders/dead")

    resp = client.post(
        "/ns/0/dramatiq-dev/queues/orders/dead/requeue-all",
        data={"csrf_token": token, "confirm": "wrong"},
    )

    assert resp.status_code == 400


def test_requeue_all_dead_page_with_confirm_redirects():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", dead=3)
    client = _client_for({0: r})
    token = _csrf_token(client, "/ns/0/dramatiq-dev/queues/orders/dead")

    resp = client.post(
        "/ns/0/dramatiq-dev/queues/orders/dead/requeue-all",
        data={"csrf_token": token, "confirm": "orders"},
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "flash=requeued+3" in resp.headers["location"]


def test_delete_all_dead_page_with_confirm_redirects():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", dead=2)
    client = _client_for({0: r})
    token = _csrf_token(client, "/ns/0/dramatiq-dev/queues/orders/dead")

    resp = client.post(
        "/ns/0/dramatiq-dev/queues/orders/dead/delete-all",
        data={"csrf_token": token, "confirm": "orders"},
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "flash=deleted+2" in resp.headers["location"]


def test_purge_queue_page_requires_confirm():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=4)
    client = _client_for({0: r})
    token = _csrf_token(client, "/ns/0/dramatiq-dev/queues/orders/waiting")

    resp = client.post(
        "/ns/0/dramatiq-dev/queues/orders/purge",
        data={"csrf_token": token, "confirm": "nope"},
    )

    assert resp.status_code == 400


def test_purge_queue_page_with_confirm_redirects():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=4)
    client = _client_for({0: r})
    token = _csrf_token(client, "/ns/0/dramatiq-dev/queues/orders/waiting")

    resp = client.post(
        "/ns/0/dramatiq-dev/queues/orders/purge",
        data={"csrf_token": token, "confirm": "orders"},
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "flash=purged+4" in resp.headers["location"]


def test_delete_inflight_live_worker_page_409():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(
        r, "dramatiq-dev", "orders", inflight=[("live", 1)], heartbeats={"live": 0}
    )
    rid = created["inflight"][0]
    client = _client_for({0: r})
    token = _csrf_token(client, "/ns/0/dramatiq-dev/queues/orders/inflight")

    resp = client.post(
        f"/ns/0/dramatiq-dev/queues/orders/inflight/{rid}/delete",
        data={"csrf_token": token},
    )

    assert resp.status_code == 409
