from __future__ import annotations

import json

import fakeredis

from dramatiq_monitor import keys as k
from dramatiq_monitor.config import Config
from dramatiq_monitor.redis_ops import stats as stats_mod

from conftest import seed_queue


def test_queue_stats_exact_counts_delayed_list(fake_redis):
    seed_queue(
        fake_redis,
        "dramatiq-dev",
        "orders",
        waiting=3,
        delayed_list=2,
        dead=1,
    )
    config = Config()
    result = stats_mod.queue_stats(config, fake_redis, 0, "dramatiq-dev")

    assert len(result) == 1
    row = result[0]
    assert row.name == "orders"
    assert row.waiting == 3
    assert row.delayed == 2
    assert row.failed == 1
    assert row.failed_drift is False


def test_queue_stats_exact_counts_delayed_zset(fake_redis):
    seed_queue(
        fake_redis,
        "dramatiq-dev",
        "orders",
        waiting=2,
        delayed_zset=4,
        dead=2,
    )
    config = Config()
    result = stats_mod.queue_stats(config, fake_redis, 0, "dramatiq-dev")

    assert len(result) == 1
    row = result[0]
    assert row.waiting == 2
    assert row.delayed == 4
    assert row.failed == 2


def test_queue_stats_live_vs_orphaned_boundary(fake_redis):
    # heartbeat age 59s => live; 61s => orphaned (stale_worker_s=60)
    seed_queue(
        fake_redis,
        "dramatiq-dev",
        "orders",
        waiting=1,
        inflight=[("fresh-worker", 2), ("stale-worker", 3)],
        heartbeats={"fresh-worker": 59_000, "stale-worker": 61_000},
    )
    config = Config()
    result = stats_mod.queue_stats(config, fake_redis, 0, "dramatiq-dev")

    row = result[0]
    assert row.live == 2
    assert row.orphaned == 3


def test_queue_stats_worker_absent_from_heartbeats_counts_orphaned(fake_redis):
    seed_queue(
        fake_redis,
        "dramatiq-dev",
        "orders",
        waiting=1,
        inflight=[("ghost-worker", 4)],
    )
    config = Config()
    result = stats_mod.queue_stats(config, fake_redis, 0, "dramatiq-dev")

    row = result[0]
    assert row.live == 0
    assert row.orphaned == 4


def test_worker_stats_absent_from_heartbeats_has_none_age(fake_redis):
    seed_queue(
        fake_redis,
        "dramatiq-dev",
        "orders",
        waiting=1,
        inflight=[("ghost-worker", 4)],
    )
    config = Config()
    workers = stats_mod.worker_stats(config, fake_redis, 0, "dramatiq-dev")

    assert len(workers) == 1
    worker = workers[0]
    assert worker.worker_id == "ghost-worker"
    assert worker.heartbeat_age_ms is None
    assert worker.inflight == 4


def test_oldest_waiting_age_ms_from_tail_message(fake_redis):
    # seed_queue LPUSHes with increasing timestamps as i increases; the
    # first LPUSHed message (i=0, oldest timestamp) ends up at the list tail.
    waiting = 5
    seed_queue(fake_redis, "dramatiq-dev", "orders", waiting=waiting)

    config = Config()
    result = stats_mod.queue_stats(config, fake_redis, 0, "dramatiq-dev")

    row = result[0]
    assert row.oldest_waiting_age_ms is not None
    expected_age_ms = waiting * 1000
    tolerance_ms = 200
    assert abs(row.oldest_waiting_age_ms - expected_age_ms) <= tolerance_ms


def test_failed_drift_true_when_xq_and_xq_msgs_disagree(fake_redis):
    created = seed_queue(fake_redis, "dramatiq-dev", "orders", waiting=1, dead=2)
    dead_rid = created["dead"][0]
    fake_redis.hdel(k.xq_msgs_key("dramatiq-dev", "orders"), dead_rid)

    config = Config()
    result = stats_mod.queue_stats(config, fake_redis, 0, "dramatiq-dev")

    row = result[0]
    assert row.failed_drift is True
    assert row.failed == 2  # max(ZCARD XQ=2, HLEN XQ.msgs=1)


def test_queue_stats_empty_namespace_returns_empty_list(fake_redis):
    config = Config()
    result = stats_mod.queue_stats(config, fake_redis, 0, "dramatiq-empty")
    assert result == []


def test_worker_stats_meta_present_and_bad_json(fake_redis):
    seed_queue(
        fake_redis,
        "dramatiq-dev",
        "orders",
        waiting=1,
        inflight=[("w-good", 1), ("w-bad", 1)],
        heartbeats={"w-good": 0, "w-bad": 0},
    )
    meta_key = k.worker_meta_key("dramatiq-dev")
    fake_redis.hset(meta_key, "w-good", json.dumps({"host": "h1", "pid": 123}))
    fake_redis.hset(meta_key, "w-bad", b"{not-json")

    config = Config()
    workers = stats_mod.worker_stats(config, fake_redis, 0, "dramatiq-dev")
    by_id = {w.worker_id: w for w in workers}

    assert by_id["w-good"].meta == {"host": "h1", "pid": 123}
    assert by_id["w-bad"].meta is None
