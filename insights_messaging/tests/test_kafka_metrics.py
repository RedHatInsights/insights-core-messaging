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
            {"type": "consumer", "client_id": "test-client-1", "state": "up"},
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
