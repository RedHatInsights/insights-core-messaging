"""
Tests for KafkaMetrics Prometheus gauge updates.
"""

import json

import pytest


@pytest.mark.parametrize(
    ("gauge_attr", "label_kwargs", "expected"),
    [
        pytest.param(
            "KAFKA_CONSUMER_REBALANCE_COUNT",
            {"type": "consumer", "client_id": "test-client-1"},
            42,
            id="rebalance_count",
        ),
        pytest.param(
            "KAFKA_CONSUMER_REPLY_QUEUE_SIZE",
            {"type": "consumer", "client_id": "test-client-1"},
            7,
            id="reply_queue_size",
        ),
        pytest.param(
            "KAFKA_CONSUMER_REBALANCE_AGE",
            {"type": "consumer", "client_id": "test-client-1"},
            5000,
            id="rebalance_age",
        ),
    ],
)
def test_stats_to_metrics_sets_gauge(kafka_metrics, gauge_attr, label_kwargs, expected):
    """stats_to_metrics must parse the JSON stats blob and update Prometheus gauges."""
    stats = {
        "type": "consumer",
        "client_id": "test-client-1",
        "cgrp": {
            "rebalance_cnt": 42,
            "rebalance_age": 5000,
            "state": "up",
        },
        "replyq": 7,
    }

    kafka_metrics.stats_to_metrics(json.dumps(stats))

    gauge = getattr(kafka_metrics, gauge_attr)
    sample = gauge.labels(**label_kwargs)._value.get()
    assert sample == expected, f"{gauge_attr} should be {expected}, got {sample}"


def test_rebalance_count_labels_exclude_state(kafka_metrics):
    """KAFKA_CONSUMER_REBALANCE_COUNT must not include 'state' in labelnames."""
    labelnames = kafka_metrics.KAFKA_CONSUMER_REBALANCE_COUNT._labelnames

    assert "state" not in labelnames, (
        "KAFKA_CONSUMER_REBALANCE_COUNT should not have 'state' in its "
        "labelnames to prevent unbounded metric cardinality growth. "
        f"Found labelnames: {list(labelnames)}"
    )
    assert "type" in labelnames, "KAFKA_CONSUMER_REBALANCE_COUNT should have 'type' in labelnames"
    assert "client_id" in labelnames, (
        "KAFKA_CONSUMER_REBALANCE_COUNT should have 'client_id' in labelnames"
    )


def test_consumer_state_metric_exists(kafka_metrics):
    """KAFKA_CONSUMER_STATE gauge must exist with type and client_id labels."""
    assert hasattr(kafka_metrics, "KAFKA_CONSUMER_STATE"), (
        "KafkaMetrics should have a KAFKA_CONSUMER_STATE gauge for "
        "tracking consumer state separately from rebalance count"
    )

    labelnames = kafka_metrics.KAFKA_CONSUMER_STATE._labelnames
    assert "type" in labelnames, "KAFKA_CONSUMER_STATE should have 'type' in labelnames"
    assert "client_id" in labelnames, "KAFKA_CONSUMER_STATE should have 'client_id' in labelnames"
    assert "state" not in labelnames, (
        "KAFKA_CONSUMER_STATE should not have 'state' in labelnames — "
        "the value is set on the gauge itself, not as a label"
    )


def test_metrics_cardinality_fixed_across_callbacks(kafka_metrics):
    """Metrics child count must stay constant regardless of state changes in stats callbacks."""
    # Simulate 50 stats callbacks — in production this fires every 10s
    for i in range(50):
        stats = {
            "type": "consumer",
            "client_id": "test-client-memleak",
            "cgrp": {
                "state": ["up", "rebalancing", "init"][i % 3],
                "rebalance_cnt": i,
                "rebalance_age": i * 1000,
            },
            "replyq": 10,
        }
        kafka_metrics.stats_to_metrics(json.dumps(stats))

    # With fixed labels, there should be exactly 1 child metric per gauge
    # for this specific type+client_id combo.
    rebalance_children = kafka_metrics.KAFKA_CONSUMER_REBALANCE_COUNT._metrics
    memleak_keys = [k for k in rebalance_children if "test-client-memleak" in str(k)]
    assert len(memleak_keys) == 1, (
        f"Expected 1 child metric for test-client-memleak in "
        f"KAFKA_CONSUMER_REBALANCE_COUNT, but found {len(memleak_keys)}. "
        "This suggests 'state' or another variable label is still present, "
        "causing unbounded cardinality growth."
    )
