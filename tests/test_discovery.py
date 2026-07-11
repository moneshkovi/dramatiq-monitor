from __future__ import annotations

import fakeredis

from dramatiq_monitor.config import Config
from dramatiq_monitor.models import NamespaceRef
from dramatiq_monitor.redis_ops import discovery as discovery_mod

from conftest import seed_queue


def _two_dbs():
    return {0: fakeredis.FakeRedis(decode_responses=False), 1: fakeredis.FakeRedis(decode_responses=False)}


def test_discover_namespaces_across_two_dbs():
    clients = _two_dbs()
    seed_queue(clients[0], "dramatiq-prod", "orders", waiting=1, heartbeats={"w1": 0})
    seed_queue(clients[1], "dramatiq-dev", "orders", waiting=1, heartbeats={"w2": 0})

    config = Config()
    refs = discovery_mod.discover_namespaces(config, clients)

    assert NamespaceRef(db=0, ns="dramatiq-prod") in refs
    assert NamespaceRef(db=1, ns="dramatiq-dev") in refs
    assert len(refs) == 2


def test_discover_namespaces_allowlist_union():
    clients = _two_dbs()
    seed_queue(clients[0], "dramatiq-prod", "orders", waiting=1, heartbeats={"w1": 0})

    config = Config(namespaces=("dramatiq-beta",))
    refs = discovery_mod.discover_namespaces(config, clients)

    assert NamespaceRef(db=0, ns="dramatiq-prod") in refs
    assert NamespaceRef(db=0, ns="dramatiq-beta") in refs
    # allowlist is unioned onto every configured db
    assert NamespaceRef(db=1, ns="dramatiq-beta") in refs


def test_discover_namespaces_ttl_cache_honored(monkeypatch):
    clients = _two_dbs()
    seed_queue(clients[0], "dramatiq-prod", "orders", waiting=1, heartbeats={"w1": 0})

    fake_now = [1000.0]
    monkeypatch.setattr(discovery_mod.time, "monotonic", lambda: fake_now[0])

    config = Config()
    refs = discovery_mod.discover_namespaces(config, {0: clients[0]})
    assert len(refs) == 1

    # write a new namespace's heartbeat key; cache should still be warm (stale)
    seed_queue(clients[0], "dramatiq-new", "orders", waiting=1, heartbeats={"w2": 0})
    refs_still_cached = discovery_mod.discover_namespaces(config, {0: clients[0]})
    assert len(refs_still_cached) == 1
    assert NamespaceRef(db=0, ns="dramatiq-new") not in refs_still_cached

    # advance past the TTL: cache expires, new namespace is discovered
    fake_now[0] += discovery_mod._NS_CACHE_TTL_S + 1
    refs_after_expiry = discovery_mod.discover_namespaces(config, {0: clients[0]})
    assert NamespaceRef(db=0, ns="dramatiq-new") in refs_after_expiry
    assert len(refs_after_expiry) == 2


def test_discover_queues_collapse_msgs_variants():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1, delayed_list=1, dead=1)

    config = Config()
    queues, _ack_keys = discovery_mod.discover_queues(config, r, 0, "dramatiq-dev")

    assert queues == ["orders"]


def test_discover_queues_ack_only_queue():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "reports", inflight=[("w1", 2)])

    config = Config()
    queues, ack_keys = discovery_mod.discover_queues(config, r, 0, "dramatiq-dev")

    assert queues == ["reports"]
    assert len(ack_keys) == 1
    worker_id, queue, _key = ack_keys[0]
    assert worker_id == "w1"
    assert queue == "reports"


def test_discover_queues_no_cross_namespace_bleed():
    r = fakeredis.FakeRedis(decode_responses=False)
    seed_queue(r, "dramatiq-dev", "orders", waiting=1, inflight=[("w1", 1)])
    seed_queue(r, "other-ns", "orders", waiting=1, inflight=[("w2", 1)])

    config = Config()
    queues, ack_keys = discovery_mod.discover_queues(config, r, 0, "dramatiq-dev")

    assert queues == ["orders"]
    assert len(ack_keys) == 1
    assert ack_keys[0][0] == "w1"
