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


def test_update_context_ids_noop_for_none():
    """update_archive_context_ids must be a safe no-op when payload is None.

    The Kafka consumer may receive tombstone messages or deserialization
    failures that result in a None payload.  The function must not raise.
    """
    archive_context_var.set({})
    update_archive_context_ids(None)
    assert archive_context_var.get() == {}, "Context should remain empty for None payload"


def test_update_context_ids_noop_for_missing_structure():
    """update_archive_context_ids must handle payloads missing expected top-level keys.

    Messages from different Kafka topics may have different schemas.
    When platform_metadata or host keys are absent, the function must
    leave the context unchanged rather than raising KeyError.
    """
    archive_context_var.set({})
    update_archive_context_ids({"other_key": "value"})
    assert archive_context_var.get() == {}, (
        "Context should remain empty when payload lacks required keys"
    )


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


def test_stats_to_metrics_parses_json(kafka_metrics):
    """stats_to_metrics must parse the JSON stats blob and set rebalance count.

    confluent-kafka emits a JSON stats string via the stats_cb callback
    every statistics.interval.ms.  KafkaMetrics parses this and updates
    Prometheus gauges.  This test verifies the rebalance count gauge is
    set from cgrp.rebalance_cnt.
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

    # Verify the rebalance count was set correctly
    sample = kafka_metrics.KAFKA_CONSUMER_REBALANCE_COUNT.labels(
        type="consumer", client_id="test-client-1", state="up"
    )._value.get()
    assert sample == 42, f"Rebalance count should be 42, got {sample}"


def test_stats_to_metrics_sets_reply_queue(kafka_metrics):
    """stats_to_metrics must record the reply queue size from the stats blob.

    The reply queue (replyq) indicates how many responses are waiting
    to be delivered to the application.  A growing queue signals that
    the consumer is falling behind, making this a key health metric.
    """
    stats = {
        "type": "consumer",
        "client_id": "test-client-2",
        "cgrp": {
            "rebalance_cnt": 0,
            "rebalance_age": 0,
            "state": "up",
        },
        "replyq": 15,
    }

    kafka_metrics.stats_to_metrics(json.dumps(stats))

    sample = kafka_metrics.KAFKA_CONSUMER_REPLY_QUEUE_SIZE.labels(
        type="consumer", client_id="test-client-2"
    )._value.get()
    assert sample == 15, f"Reply queue size should be 15, got {sample}"


def test_stats_to_metrics_sets_rebalance_age(kafka_metrics):
    """stats_to_metrics must record the rebalance age from the stats blob.

    Rebalance age (cgrp.rebalance_age) indicates milliseconds since the
    last rebalance.  Frequent rebalances (low age) suggest consumer
    instability — this metric is used for alerting.
    """
    stats = {
        "type": "consumer",
        "client_id": "test-client-3",
        "cgrp": {
            "rebalance_cnt": 1,
            "rebalance_age": 12345,
            "state": "up",
        },
        "replyq": 0,
    }

    kafka_metrics.stats_to_metrics(json.dumps(stats))

    sample = kafka_metrics.KAFKA_CONSUMER_REBALANCE_AGE.labels(
        type="consumer", client_id="test-client-3"
    )._value.get()
    assert sample == 12345, f"Rebalance age should be 12345, got {sample}"
