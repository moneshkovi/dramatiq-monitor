from __future__ import annotations

import uuid

from dramatiq_monitor import keys as k


def test_queue_key_roundtrip():
    assert k.queue_key("dramatiq-dev", "orders") == "dramatiq-dev:orders"


def test_msgs_key_roundtrip():
    assert k.msgs_key("dramatiq-dev", "orders") == "dramatiq-dev:orders.msgs"
    queue, kind = k.queue_from_msgs_key("dramatiq-dev", "dramatiq-dev:orders.msgs")
    assert (queue, kind) == ("orders", "waiting")


def test_dq_key_roundtrip():
    assert k.dq_key("dramatiq-dev", "orders") == "dramatiq-dev:orders.DQ"
    assert k.dq_msgs_key("dramatiq-dev", "orders") == "dramatiq-dev:orders.DQ.msgs"
    queue, kind = k.queue_from_msgs_key("dramatiq-dev", "dramatiq-dev:orders.DQ.msgs")
    assert (queue, kind) == ("orders", "delayed")


def test_xq_key_roundtrip():
    assert k.xq_key("dramatiq-dev", "orders") == "dramatiq-dev:orders.XQ"
    assert k.xq_msgs_key("dramatiq-dev", "orders") == "dramatiq-dev:orders.XQ.msgs"
    queue, kind = k.queue_from_msgs_key("dramatiq-dev", "dramatiq-dev:orders.XQ.msgs")
    assert (queue, kind) == ("orders", "dead")


def test_acks_pattern():
    assert k.acks_pattern("dramatiq-dev") == "dramatiq-dev:__acks__.*"


def test_heartbeats_key():
    assert k.heartbeats_key("dramatiq-dev") == "dramatiq-dev:__heartbeats__"


def test_ns_from_heartbeats_key():
    assert k.ns_from_heartbeats_key("dramatiq-dev:__heartbeats__") == "dramatiq-dev"


def test_worker_meta_key():
    assert k.worker_meta_key("dramatiq-dev") == "dramatiq-dev:__worker_meta__"


def test_parse_ack_key_with_uuid4():
    worker_id = str(uuid.uuid4())
    key = f"dramatiq-dev:__acks__.{worker_id}.orders"
    parsed_worker, parsed_queue = k.parse_ack_key("dramatiq-dev", key)
    assert parsed_worker == worker_id
    assert parsed_queue == "orders"


def test_parse_ack_key_queue_with_underscores():
    worker_id = str(uuid.uuid4())
    key = f"dramatiq-dev:__acks__.{worker_id}.transition_scenario"
    parsed_worker, parsed_queue = k.parse_ack_key("dramatiq-dev", key)
    assert parsed_worker == worker_id
    assert parsed_queue == "transition_scenario"


def test_parse_ack_key_fallback_non_uuid4():
    key = "dramatiq-dev:__acks__.shortid.orders"
    parsed_worker, parsed_queue = k.parse_ack_key("dramatiq-dev", key)
    assert parsed_worker == "shortid"
    assert parsed_queue == "orders"


def test_queue_from_msgs_key_kinds():
    ns = "dramatiq-prod"
    cases = [
        (f"{ns}:rebalance.msgs", ("rebalance", "waiting")),
        (f"{ns}:rebalance.DQ.msgs", ("rebalance", "delayed")),
        (f"{ns}:rebalance.XQ.msgs", ("rebalance", "dead")),
    ]
    for key, expected in cases:
        assert k.queue_from_msgs_key(ns, key) == expected
