from __future__ import annotations

import base64

import fakeredis
from starlette.testclient import TestClient

from dramatiq_monitor.app import create_app
from dramatiq_monitor.config import Config

from conftest import seed_queue


def _basic_header(user: str, password: str) -> dict:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def _client_for(clients, **config_kwargs):
    config = Config(dbs=(0,), secret="testsecret", **config_kwargs)
    app = create_app(config, clients=clients)
    return TestClient(app)


# --- BasicAuth --------------------------------------------------------

def test_basic_auth_401_without_credentials():
    r = fakeredis.FakeRedis(decode_responses=False)
    client = _client_for({0: r}, auth_user="admin", auth_password="hunter2")

    resp = client.get("/namespaces")

    assert resp.status_code == 401
    assert resp.headers["www-authenticate"] == 'Basic realm="dramatiq-monitor"'


def test_basic_auth_401_with_wrong_credentials():
    r = fakeredis.FakeRedis(decode_responses=False)
    client = _client_for({0: r}, auth_user="admin", auth_password="hunter2")

    resp = client.get("/namespaces", headers=_basic_header("admin", "wrong"))

    assert resp.status_code == 401


def test_basic_auth_200_with_correct_credentials():
    r = fakeredis.FakeRedis(decode_responses=False)
    client = _client_for({0: r}, auth_user="admin", auth_password="hunter2")

    resp = client.get("/namespaces", headers=_basic_header("admin", "hunter2"))

    assert resp.status_code == 200


def test_basic_auth_exempts_healthz():
    r = fakeredis.FakeRedis(decode_responses=False)
    client = _client_for({0: r}, auth_user="admin", auth_password="hunter2")

    resp = client.get("/healthz")

    assert resp.status_code == 200


def test_no_auth_configured_allows_all():
    r = fakeredis.FakeRedis(decode_responses=False)
    client = _client_for({0: r})

    resp = client.get("/namespaces")

    assert resp.status_code == 200


# --- ReadOnly -----------------------------------------------------------

def test_read_only_blocks_post_page_route():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(r, "dramatiq-dev", "orders", dead=1)
    rid = created["dead"][0]
    client = _client_for({0: r}, read_only=True)

    resp = client.post(f"/ns/0/dramatiq-dev/queues/orders/dead/{rid}/requeue")

    assert resp.status_code == 403


def test_read_only_blocks_post_api_route_with_json_body():
    r = fakeredis.FakeRedis(decode_responses=False)
    client = _client_for({0: r}, read_only=True)

    resp = client.post(
        "/api/ns/0/dramatiq-dev/queues/orders/purge",
        json={"confirm": "orders"},
    )

    assert resp.status_code == 403
    assert resp.json()["ok"] is False


def test_read_only_hides_action_buttons_from_html():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", dead=1, waiting=1, heartbeats={"w1": 0})
    client = _client_for({0: r}, read_only=True)

    resp = client.get("/ns/0/dramatiq-dev/queues/orders/dead")
    assert resp.status_code == 200
    assert "Requeue all" not in resp.text
    assert "Delete all" not in resp.text

    resp = client.get("/ns/0/dramatiq-dev/queues/orders/waiting")
    assert "Purge" not in resp.text

    resp = client.get("/ns/0/dramatiq-dev/queues/orders/dead")
    assert 'badge-readonly' in resp.text or "read-only" in resp.text


def test_not_read_only_shows_action_buttons_in_html():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", dead=1, waiting=1, heartbeats={"w1": 0})
    client = _client_for({0: r}, read_only=False)

    resp = client.get("/ns/0/dramatiq-dev/queues/orders/dead")
    assert "Requeue all" in resp.text
    assert "Delete all" in resp.text

    resp = client.get("/ns/0/dramatiq-dev/queues/orders/waiting")
    assert "Purge" in resp.text


def test_read_only_get_still_allowed():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1, heartbeats={"w1": 0})
    client = _client_for({0: r}, read_only=True)

    resp = client.get("/ns/0/dramatiq-dev/")

    assert resp.status_code == 200


# --- CSRF -----------------------------------------------------------------

def _get_csrf_token(client: TestClient, path: str) -> str:
    resp = client.get(path)
    return resp.cookies.get("dm_csrf")


def test_csrf_post_without_token_403():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(r, "dramatiq-dev", "orders", dead=1)
    rid = created["dead"][0]
    client = _client_for({0: r})
    _get_csrf_token(client, "/ns/0/dramatiq-dev/queues/orders/dead")

    resp = client.post(
        f"/ns/0/dramatiq-dev/queues/orders/dead/{rid}/requeue",
        follow_redirects=False,
    )

    assert resp.status_code == 403


def test_csrf_post_with_cookie_and_form_field_succeeds():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(r, "dramatiq-dev", "orders", dead=1)
    rid = created["dead"][0]
    client = _client_for({0: r})
    token = _get_csrf_token(client, "/ns/0/dramatiq-dev/queues/orders/dead")

    resp = client.post(
        f"/ns/0/dramatiq-dev/queues/orders/dead/{rid}/requeue",
        data={"csrf_token": token},
        follow_redirects=False,
    )

    assert resp.status_code == 303


def test_csrf_post_with_cookie_and_header_succeeds():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(r, "dramatiq-dev", "orders", dead=1)
    rid = created["dead"][0]
    client = _client_for({0: r})
    token = _get_csrf_token(client, "/ns/0/dramatiq-dev/queues/orders/dead")

    resp = client.post(
        f"/ns/0/dramatiq-dev/queues/orders/dead/{rid}/requeue",
        headers={"X-CSRF-Token": token},
        follow_redirects=False,
    )

    assert resp.status_code == 303


def test_csrf_post_with_mismatched_token_403():
    r = fakeredis.FakeRedis(decode_responses=False)
    created = seed_queue(r, "dramatiq-dev", "orders", dead=1)
    rid = created["dead"][0]
    client = _client_for({0: r})
    _get_csrf_token(client, "/ns/0/dramatiq-dev/queues/orders/dead")

    resp = client.post(
        f"/ns/0/dramatiq-dev/queues/orders/dead/{rid}/requeue",
        data={"csrf_token": "totally-bogus"},
        follow_redirects=False,
    )

    assert resp.status_code == 403


def test_csrf_api_post_without_json_content_type_403():
    r = fakeredis.FakeRedis(decode_responses=False)
    client = _client_for({0: r})

    resp = client.post(
        "/api/ns/0/dramatiq-dev/queues/orders/purge",
        data={"confirm": "orders"},
    )

    assert resp.status_code == 403


def test_csrf_api_post_with_json_content_type_no_token_needed():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1)
    client = _client_for({0: r})

    resp = client.post(
        "/api/ns/0/dramatiq-dev/queues/orders/purge",
        json={"confirm": "orders"},
    )

    assert resp.status_code == 200
    assert resp.json()["ok"] is True
