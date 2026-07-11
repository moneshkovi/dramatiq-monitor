from __future__ import annotations

import fakeredis

from dramatiq_monitor import keys as k
from dramatiq_monitor.config import Config
from dramatiq_monitor.redis_ops import messages as messages_mod

from conftest import seed_queue


def test_waiting_pagination_across_three_pages(fake_redis):
    seed_queue(fake_redis, "dramatiq-dev", "orders", waiting=5)
    config = Config()

    page1 = messages_mod.list_messages(
        config, fake_redis, "dramatiq-dev", "orders", "waiting", n=2
    )
    assert len(page1.items) == 2
    assert page1.total == 5
    assert page1.next_cursor == "2"

    page2 = messages_mod.list_messages(
        config, fake_redis, "dramatiq-dev", "orders", "waiting", cursor=page1.next_cursor, n=2
    )
    assert len(page2.items) == 2
    assert page2.next_cursor == "4"

    page3 = messages_mod.list_messages(
        config, fake_redis, "dramatiq-dev", "orders", "waiting", cursor=page2.next_cursor, n=2
    )
    assert len(page3.items) == 1
    assert page3.next_cursor is None

    all_rids = {item.rid for item in page1.items + page2.items + page3.items}
    assert len(all_rids) == 5


def test_delayed_list_variant(fake_redis):
    seed_queue(fake_redis, "dramatiq-dev", "orders", delayed_list=3)
    config = Config()

    page = messages_mod.list_messages(config, fake_redis, "dramatiq-dev", "orders", "delayed", n=10)

    assert len(page.items) == 3
    assert page.total == 3
    assert all("dq_score" not in item.extra for item in page.items)


def test_delayed_zset_variant_exposes_score(fake_redis):
    seed_queue(fake_redis, "dramatiq-dev", "orders", delayed_zset=3)
    config = Config()

    page = messages_mod.list_messages(config, fake_redis, "dramatiq-dev", "orders", "delayed", n=10)

    assert len(page.items) == 3
    assert page.total == 3
    for item in page.items:
        assert "dq_score" in item.extra


def test_delayed_zset_falls_back_to_msgs_key_on_missing_dq_payload(fake_redis):
    created = seed_queue(fake_redis, "dramatiq-dev", "orders", delayed_zset=1)
    rid = created["delayed"][0]
    # Simulate the payload only existing in the shared msgs hash.
    payload = fake_redis.hget(k.dq_msgs_key("dramatiq-dev", "orders"), rid)
    fake_redis.hdel(k.dq_msgs_key("dramatiq-dev", "orders"), rid)
    fake_redis.hset(k.msgs_key("dramatiq-dev", "orders"), rid, payload)

    config = Config()
    page = messages_mod.list_messages(config, fake_redis, "dramatiq-dev", "orders", "delayed", n=10)

    assert len(page.items) == 1
    assert page.items[0].rid == rid
    assert page.items[0].actor_name == "some_actor"


def test_dead_ordering_newest_death_first(fake_redis):
    seed_queue(fake_redis, "dramatiq-dev", "orders", dead=3)
    config = Config()

    page = messages_mod.list_messages(config, fake_redis, "dramatiq-dev", "orders", "dead", n=10)

    died_at = [item.extra["died_at_ms"] for item in page.items]
    assert died_at == sorted(died_at, reverse=True)


def test_dead_drift_fallback_uses_hscan(fake_redis):
    seed_queue(fake_redis, "dramatiq-dev", "orders", dead=3)
    # Drift: delete the ZSET index but keep the payload hash.
    fake_redis.delete(k.xq_key("dramatiq-dev", "orders"))

    config = Config()
    page = messages_mod.list_messages(config, fake_redis, "dramatiq-dev", "orders", "dead", n=2)

    assert len(page.items) == 2
    assert page.total == 3
    assert page.next_cursor is not None

    page2 = messages_mod.list_messages(
        config, fake_redis, "dramatiq-dev", "orders", "dead", cursor=page.next_cursor, n=2
    )
    assert len(page2.items) == 1
    assert page2.next_cursor is None


def test_inflight_tags_worker_and_stale_flag(fake_redis):
    seed_queue(
        fake_redis,
        "dramatiq-dev",
        "orders",
        inflight=[("fresh-worker", 1), ("stale-worker", 1)],
        heartbeats={"fresh-worker": 1_000, "stale-worker": 120_000},
    )
    config = Config()

    page = messages_mod.list_messages(config, fake_redis, "dramatiq-dev", "orders", "inflight", n=10)

    assert page.next_cursor is None
    by_worker = {item.extra["worker_id"]: item for item in page.items}
    assert by_worker["fresh-worker"].extra["stale"] is False
    assert by_worker["stale-worker"].extra["stale"] is True


def test_missing_payload_row(fake_redis):
    created = seed_queue(fake_redis, "dramatiq-dev", "orders", waiting=2)
    missing_rid = created["waiting"][0]
    fake_redis.hdel(k.msgs_key("dramatiq-dev", "orders"), missing_rid)

    config = Config()
    page = messages_mod.list_messages(config, fake_redis, "dramatiq-dev", "orders", "waiting", n=10)

    by_rid = {item.rid: item for item in page.items}
    missing_item = by_rid[missing_rid]
    assert missing_item.actor_name is None
    assert missing_item.extra["missing_payload"] is True


def test_get_message_dead_detail_fields(fake_redis):
    created = seed_queue(fake_redis, "dramatiq-dev", "orders", dead=1)
    rid = created["dead"][0]
    fake_redis.expire(k.xq_msgs_key("dramatiq-dev", "orders"), 3600)

    config = Config(dead_message_ttl_ms=7_200_000)
    detail = messages_mod.get_message(config, fake_redis, "dramatiq-dev", "orders", "dead", rid)

    assert detail is not None
    assert detail.rid == rid
    assert detail.state == "dead"
    assert detail.died_at_ms is not None
    assert detail.msgs_key_ttl_s is not None
    assert 3500 <= detail.msgs_key_ttl_s <= 3600
    assert detail.remaining_ttl_hint_ms is not None
    assert detail.remaining_ttl_hint_ms <= 7_200_000
    assert detail.raw_size_bytes is not None
    assert detail.payload_pretty is not None
    assert "actor_name" in detail.payload_pretty


def test_get_message_returns_none_for_unknown_rid(fake_redis):
    seed_queue(fake_redis, "dramatiq-dev", "orders", waiting=1)
    config = Config()

    detail = messages_mod.get_message(
        config, fake_redis, "dramatiq-dev", "orders", "waiting", "nonexistent-rid"
    )

    assert detail is None


def test_get_message_waiting_shares_msgs_key(fake_redis):
    created = seed_queue(fake_redis, "dramatiq-dev", "orders", waiting=1)
    rid = created["waiting"][0]
    config = Config()

    detail = messages_mod.get_message(config, fake_redis, "dramatiq-dev", "orders", "waiting", rid)

    assert detail is not None
    assert detail.died_at_ms is None
    assert detail.remaining_ttl_hint_ms is None
