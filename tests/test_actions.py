from __future__ import annotations

import json

from dramatiq_monitor import keys as k
from dramatiq_monitor.redis_ops import actions as actions_mod

from conftest import make_message, seed_queue


# --- requeue_dead: byte-exact semantics ------------------------------------

def test_requeue_dead_byte_exact_semantics(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    rid, orig_payload, encoded = make_message(
        "some_actor",
        q,
        message_id="fixed-message-id",
        message_timestamp=12345,
        options={"retries": 3, "traceback": "boom", "eta": 999},
        args=[1, 2],
        kwargs={"a": "b"},
    )
    r.hset(k.xq_msgs_key(ns, q), rid, encoded)
    r.zadd(k.xq_key(ns, q), {rid: 5000})

    new_rid = actions_mod.requeue_dead(r, ns, q, rid)

    assert new_rid is not None
    assert new_rid != rid

    # Old rid fully gone from XQ + XQ.msgs.
    assert r.zscore(k.xq_key(ns, q), rid) is None
    assert r.hget(k.xq_msgs_key(ns, q), rid) is None

    # New rid at the head of the waiting list (LPUSH => LPOS 0).
    assert r.lindex(k.queue_key(ns, q), 0).decode() == new_rid

    new_body_raw = r.hget(k.msgs_key(ns, q), new_rid)
    assert new_body_raw is not None
    new_payload = json.loads(new_body_raw)

    expected = dict(orig_payload)
    expected["options"] = dict(orig_payload["options"])
    expected["options"]["redis_message_id"] = new_rid
    expected["options"]["retries"] = 0
    expected["options"].pop("traceback", None)

    assert new_payload == expected
    # message_id / message_timestamp preserved for external correlation.
    assert new_payload["message_id"] == "fixed-message-id"
    assert new_payload["message_timestamp"] == 12345
    # eta (any other option) untouched.
    assert new_payload["options"]["eta"] == 999
    assert "traceback" not in new_payload["options"]
    assert new_payload["options"]["retries"] == 0

    # Compact separators, matching dramatiq's own encoding.
    assert new_body_raw == json.dumps(new_payload, separators=(",", ":")).encode()


def test_requeue_dead_after_hdel_returns_none(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    created = seed_queue(r, ns, q, dead=1)
    rid = created["dead"][0]

    # Simulate the dead-letter TTL sweep racing us: payload already gone.
    r.hdel(k.xq_msgs_key(ns, q), rid)

    result = actions_mod.requeue_dead(r, ns, q, rid)

    assert result is None


# --- delete_message: per state --------------------------------------------

def test_delete_message_waiting(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    created = seed_queue(r, ns, q, waiting=2)
    rid = created["waiting"][0]

    deleted = actions_mod.delete_message(r, ns, q, "waiting", rid)

    assert deleted is True
    assert rid.encode() not in r.lrange(k.queue_key(ns, q), 0, -1)
    assert r.hget(k.msgs_key(ns, q), rid) is None


def test_delete_message_waiting_missing_returns_false(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    seed_queue(r, ns, q, waiting=1)

    deleted = actions_mod.delete_message(r, ns, q, "waiting", "nonexistent-rid")

    assert deleted is False


def test_delete_message_delayed_list(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    created = seed_queue(r, ns, q, delayed_list=2)
    rid = created["delayed"][0]

    deleted = actions_mod.delete_message(r, ns, q, "delayed", rid)

    assert deleted is True
    assert r.hget(k.dq_msgs_key(ns, q), rid) is None


def test_delete_message_delayed_zset(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    created = seed_queue(r, ns, q, delayed_zset=2)
    rid = created["delayed"][0]

    deleted = actions_mod.delete_message(r, ns, q, "delayed", rid)

    assert deleted is True
    assert r.zscore(k.dq_key(ns, q), rid) is None
    assert r.hget(k.dq_msgs_key(ns, q), rid) is None


def test_delete_message_dead(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    created = seed_queue(r, ns, q, dead=2)
    rid = created["dead"][0]

    deleted = actions_mod.delete_message(r, ns, q, "dead", rid)

    assert deleted is True
    assert r.zscore(k.xq_key(ns, q), rid) is None
    assert r.hget(k.xq_msgs_key(ns, q), rid) is None


def test_delete_message_inflight_stale_worker_succeeds(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    created = seed_queue(
        r, ns, q, inflight=[("stale-worker", 1)], heartbeats={"stale-worker": 120_000}
    )
    rid = created["inflight"][0]

    deleted = actions_mod.delete_message(
        r, ns, q, "inflight", rid, stale_worker_ids={"stale-worker"}
    )

    assert deleted is True
    ack_key = f"{ns}:__acks__.stale-worker.{q}"
    assert not r.sismember(ack_key, rid)
    assert r.hget(k.msgs_key(ns, q), rid) is None


def test_delete_message_inflight_live_worker_raises_conflict(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    created = seed_queue(
        r, ns, q, inflight=[("live-worker", 1)], heartbeats={"live-worker": 0}
    )
    rid = created["inflight"][0]

    try:
        actions_mod.delete_message(r, ns, q, "inflight", rid, stale_worker_ids=set())
        assert False, "expected ActionConflict"
    except actions_mod.ActionConflict:
        pass

    # Message untouched.
    ack_key = f"{ns}:__acks__.live-worker.{q}"
    assert r.sismember(ack_key, rid)


def test_delete_message_inflight_unknown_rid_returns_false(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    seed_queue(r, ns, q, inflight=[("w1", 1)], heartbeats={"w1": 0})

    deleted = actions_mod.delete_message(
        r, ns, q, "inflight", "nonexistent-rid", stale_worker_ids=set()
    )

    assert deleted is False


# --- bulk requeue/delete all dead ------------------------------------------

def test_requeue_all_dead_counts_and_moves_to_waiting(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    seed_queue(r, ns, q, dead=5)

    count = actions_mod.requeue_all_dead(r, ns, q)

    assert count == 5
    assert r.zcard(k.xq_key(ns, q)) == 0
    assert r.hlen(k.xq_msgs_key(ns, q)) == 0
    assert r.llen(k.queue_key(ns, q)) == 5


def test_requeue_all_dead_includes_drift_leftovers(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    created = seed_queue(r, ns, q, dead=3)
    drifted_rid = created["dead"][0]
    # Drift: index entry gone, payload hash entry remains.
    r.zrem(k.xq_key(ns, q), drifted_rid)

    count = actions_mod.requeue_all_dead(r, ns, q)

    assert count == 3
    assert r.hlen(k.xq_msgs_key(ns, q)) == 0
    assert r.llen(k.queue_key(ns, q)) == 3


def test_delete_all_dead_counts_and_clears(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    seed_queue(r, ns, q, dead=4)

    count = actions_mod.delete_all_dead(r, ns, q)

    assert count == 4
    assert r.zcard(k.xq_key(ns, q)) == 0
    assert r.hlen(k.xq_msgs_key(ns, q)) == 0


def test_delete_all_dead_includes_drift_leftovers(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    created = seed_queue(r, ns, q, dead=3)
    drifted_rid = created["dead"][0]
    r.zrem(k.xq_key(ns, q), drifted_rid)

    count = actions_mod.delete_all_dead(r, ns, q)

    assert count == 3
    assert r.hlen(k.xq_msgs_key(ns, q)) == 0


def test_requeue_all_dead_empty_queue_returns_zero(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"

    count = actions_mod.requeue_all_dead(r, ns, q)

    assert count == 0


# --- purge_queue ------------------------------------------------------------

def test_purge_queue_drops_waiting_preserves_inflight(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    created = seed_queue(
        r, ns, q, waiting=3, inflight=[("w1", 2)], heartbeats={"w1": 0}
    )
    inflight_rids = created["inflight"]

    count = actions_mod.purge_queue(r, ns, q)

    assert count == 3
    assert r.llen(k.queue_key(ns, q)) == 0

    # In-flight payloads (sharing the same .msgs hash) must survive.
    for rid in inflight_rids:
        assert r.hget(k.msgs_key(ns, q), rid) is not None

    ack_key = f"{ns}:__acks__.w1.{q}"
    assert r.scard(ack_key) == 2


def test_purge_queue_never_touches_delayed_or_dead(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    seed_queue(r, ns, q, waiting=2, delayed_list=1, dead=1)

    actions_mod.purge_queue(r, ns, q)

    assert r.llen(k.dq_key(ns, q)) == 1
    assert r.zcard(k.xq_key(ns, q)) == 1


def test_purge_queue_empty_returns_zero(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"

    count = actions_mod.purge_queue(r, ns, q)

    assert count == 0


def test_purge_queue_large_batch_spans_multiple_lpop_calls(fake_redis):
    r = fake_redis
    ns, q = "dramatiq-dev", "orders"
    seed_queue(r, ns, q, waiting=550)  # > _PURGE_BATCH (500)

    count = actions_mod.purge_queue(r, ns, q)

    assert count == 550
    assert r.llen(k.queue_key(ns, q)) == 0
    assert r.hlen(k.msgs_key(ns, q)) == 0
