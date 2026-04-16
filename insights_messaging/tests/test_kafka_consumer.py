"""
Tests for Kafka consumer components.

This module tests the ``KafkaMetrics`` stats callback, the
``update_archive_context_ids`` helper, and the ``ArchiveContextIdsInjectingFilter``
logging filter — all from ``insights_messaging.consumers.kafka``.

Note: The ``Kafka`` consumer class itself requires a live confluent-kafka
connection and is tested via integration tests, not here.
"""

import json
import logging

import pytest

from insights_messaging.consumers import (
    ArchiveContextIdsInjectingFilter,
    archive_context_var,
)
from insights_messaging.consumers.kafka import update_archive_context_ids


@pytest.fixture(autouse=True)
def _reset_archive_context():
    """Reset archive_context_var before and after each test."""
    archive_context_var.set({})
    yield
    archive_context_var.set({})


def _make_log_record(msg="test"):
    return logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg=msg,
        args=(),
        exc_info=None,
    )


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        pytest.param(
            {"platform_metadata": {"request_id": "req-123"}, "host": {"id": "host-456"}},
            {"request_id": "req-123", "inventory_id": "host-456"},
            id="present_keys",
        ),
        pytest.param(
            {"platform_metadata": {}, "host": {}},
            {},
            id="missing_keys",
        ),
    ],
)
def test_update_context_ids_sets_fields(payload, expected):
    """update_archive_context_ids must extract IDs when present, skip when absent."""
    update_archive_context_ids(payload)
    ctx = archive_context_var.get()
    assert ctx == expected


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(None, id="none_payload"),
        pytest.param({"other_key": "value"}, id="missing_structure"),
    ],
)
def test_update_context_ids_noop_for_invalid_payload(payload):
    """update_archive_context_ids must be a safe no-op for invalid payloads.

    The Kafka consumer may receive tombstone messages (None), deserialization
    failures, or messages from different topics with different schemas.
    The function must leave the context unchanged rather than raising.
    """
    update_archive_context_ids(payload)
    assert archive_context_var.get() == {}, f"Context should remain empty for payload {payload!r}"


def test_filter_injects_context_ids():
    """ArchiveContextIdsInjectingFilter must copy context IDs onto log records.

    When context is populated, the filter sets request_id and inventory_id
    as attributes on each LogRecord.  When context is empty (between
    messages or during startup), it must not add attributes with None
    values — that would cause format string errors in log formatters.
    """
    # --- With context ---
    archive_context_var.set({"request_id": "req-abc", "inventory_id": "inv-def"})
    f = ArchiveContextIdsInjectingFilter()
    record = _make_log_record()

    result = f.filter(record)

    assert result is True
    assert record.request_id == "req-abc"
    assert record.inventory_id == "inv-def"

    # --- Empty context ---
    archive_context_var.set({})
    record = _make_log_record()
    result = f.filter(record)
    assert result is True
    assert not hasattr(record, "request_id")
    assert not hasattr(record, "inventory_id")


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
    """stats_to_metrics must parse the JSON stats blob and update Prometheus gauges.

    Note: kafka_metrics is session-scoped because prometheus_client forbids
    re-registering gauges.  Tests use the same client_id so gauge values
    are overwritten, not accumulated.
    """
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
