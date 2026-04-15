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

# ---------------------------------------------------------------------------
# update_archive_context_ids tests
# ---------------------------------------------------------------------------


def test_update_context_ids_sets_fields():
    """update_archive_context_ids must extract request_id and inventory_id from the payload.

    These IDs are injected into every log message via ArchiveContextIdsInjectingFilter,
    enabling correlation of log lines to specific archive processing requests
    across distributed services.  When the payload keys are absent, the
    context must remain empty to avoid polluting log output.
    """
    # --- Present keys ---
    payload = {
        "platform_metadata": {"request_id": "req-123"},
        "host": {"id": "host-456"},
    }
    update_archive_context_ids(payload)

    ctx = archive_context_var.get()
    assert ctx["request_id"] == "req-123", "request_id should be extracted from platform_metadata"
    assert ctx["inventory_id"] == "host-456", "inventory_id should be extracted from host.id"
    archive_context_var.set({})  # cleanup

    # --- Missing keys ---
    update_archive_context_ids({"platform_metadata": {}, "host": {}})

    ctx = archive_context_var.get()
    assert "request_id" not in ctx, "request_id should not be set when missing from payload"
    assert "inventory_id" not in ctx, "inventory_id should not be set when missing from payload"
    archive_context_var.set({})  # cleanup


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
    archive_context_var.set({})
    update_archive_context_ids(payload)
    assert archive_context_var.get() == {}, f"Context should remain empty for payload {payload!r}"


# ---------------------------------------------------------------------------
# ArchiveContextIdsInjectingFilter tests
# ---------------------------------------------------------------------------


def test_filter_injects_context_ids():
    """ArchiveContextIdsInjectingFilter must copy context IDs onto log records.

    The logging filter reads request_id and inventory_id from the
    thread-local ContextVar and sets them as attributes on each
    LogRecord.  Log formatters then include these IDs in output,
    enabling log correlation across services.
    """
    archive_context_var.set(
        {
            "request_id": "req-abc",
            "inventory_id": "inv-def",
        }
    )

    f = ArchiveContextIdsInjectingFilter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="test message",
        args=(),
        exc_info=None,
    )

    result = f.filter(record)

    assert result is True, "Filter should always return True (pass the record)"
    assert record.request_id == "req-abc", "Filter should inject request_id into the log record"
    assert record.inventory_id == "inv-def", "Filter should inject inventory_id into the log record"
    archive_context_var.set({})  # cleanup


def test_filter_handles_empty_context():
    """ArchiveContextIdsInjectingFilter must not add attributes when context is empty.

    Between messages (or during startup), the ContextVar is empty.
    The filter must still return True (pass the record) and must not
    add request_id/inventory_id attributes with None values — that
    would cause format string errors in log formatters.
    """
    archive_context_var.set({})

    f = ArchiveContextIdsInjectingFilter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="test",
        args=(),
        exc_info=None,
    )

    result = f.filter(record)
    assert result is True
    assert not hasattr(record, "request_id")


# ---------------------------------------------------------------------------
# KafkaMetrics.stats_to_metrics tests
# ---------------------------------------------------------------------------


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

    confluent-kafka emits a JSON stats string via the stats_cb callback
    every statistics.interval.ms.  KafkaMetrics parses this and sets
    gauges for rebalance count, rebalance age, and reply queue size.
    These metrics drive alerting for consumer instability and lag.
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
