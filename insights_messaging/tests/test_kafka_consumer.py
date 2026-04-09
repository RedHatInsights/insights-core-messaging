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

def test_update_context_ids_sets_request_id():
    """Verify that update_archive_context_ids sets request_id from payload."""
    payload = {
        "platform_metadata": {"request_id": "req-123"},
        "host": {"id": "host-456"},
    }
    update_archive_context_ids(payload)

    ctx = archive_context_var.get()
    assert ctx["request_id"] == "req-123", (
        "request_id should be extracted from platform_metadata"
    )
    archive_context_var.set({})  # cleanup


def test_update_context_ids_sets_inventory_id():
    """Verify that update_archive_context_ids sets inventory_id from payload."""
    payload = {
        "platform_metadata": {"request_id": "req-123"},
        "host": {"id": "host-456"},
    }
    update_archive_context_ids(payload)

    ctx = archive_context_var.get()
    assert ctx["inventory_id"] == "host-456", (
        "inventory_id should be extracted from host.id"
    )
    archive_context_var.set({})  # cleanup


def test_update_context_ids_handles_missing_keys():
    """Verify that update_archive_context_ids handles missing optional keys."""
    payload = {
        "platform_metadata": {},
        "host": {},
    }
    update_archive_context_ids(payload)

    ctx = archive_context_var.get()
    assert "request_id" not in ctx, (
        "request_id should not be set when missing from payload"
    )
    assert "inventory_id" not in ctx, (
        "inventory_id should not be set when missing from payload"
    )
    archive_context_var.set({})  # cleanup


def test_update_context_ids_noop_for_none():
    """Verify that update_archive_context_ids is safe with None payload."""
    archive_context_var.set({})
    update_archive_context_ids(None)
    assert archive_context_var.get() == {}, (
        "Context should remain empty for None payload"
    )


def test_update_context_ids_noop_for_missing_structure():
    """Verify safety when payload lacks platform_metadata or host."""
    archive_context_var.set({})
    update_archive_context_ids({"other_key": "value"})
    assert archive_context_var.get() == {}, (
        "Context should remain empty when payload lacks required keys"
    )


# ---------------------------------------------------------------------------
# ArchiveContextIdsInjectingFilter tests
# ---------------------------------------------------------------------------

def test_filter_injects_context_ids():
    """Verify that the logging filter injects context IDs into log records."""
    archive_context_var.set({
        "request_id": "req-abc",
        "inventory_id": "inv-def",
    })

    f = ArchiveContextIdsInjectingFilter()
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="test message", args=(), exc_info=None,
    )

    result = f.filter(record)

    assert result is True, "Filter should always return True (pass the record)"
    assert record.request_id == "req-abc", (
        "Filter should inject request_id into the log record"
    )
    assert record.inventory_id == "inv-def", (
        "Filter should inject inventory_id into the log record"
    )
    archive_context_var.set({})  # cleanup


def test_filter_handles_empty_context():
    """Verify that the filter works when context is empty."""
    archive_context_var.set({})

    f = ArchiveContextIdsInjectingFilter()
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="test", args=(), exc_info=None,
    )

    result = f.filter(record)
    assert result is True
    assert not hasattr(record, "request_id")


# ---------------------------------------------------------------------------
# KafkaMetrics.stats_to_metrics tests
# ---------------------------------------------------------------------------

def test_stats_to_metrics_parses_json(kafka_metrics):
    """Verify that stats_to_metrics correctly parses and records stats."""
    stats = {
        "type": "consumer",
        "client_id": "test-client-1",
        "cgrp": {
            "rebalance_cnt": 42,
            "rebalance_age": 5000,
        },
        "replyq": 7,
    }

    kafka_metrics.stats_to_metrics(json.dumps(stats))

    # Verify the rebalance count was set correctly
    sample = kafka_metrics.KAFKA_CONSUMER_REBALANCE_COUNT.labels(
        type="consumer", client_id="test-client-1"
    )._value.get()
    assert sample == 42, (
        "Rebalance count should be 42, got %s" % sample
    )


def test_stats_to_metrics_sets_reply_queue(kafka_metrics):
    """Verify that reply queue size is recorded correctly."""
    stats = {
        "type": "consumer",
        "client_id": "test-client-2",
        "cgrp": {
            "rebalance_cnt": 0,
            "rebalance_age": 0,
        },
        "replyq": 15,
    }

    kafka_metrics.stats_to_metrics(json.dumps(stats))

    sample = kafka_metrics.KAFKA_CONSUMER_REPLY_QUEUE_SIZE.labels(
        type="consumer", client_id="test-client-2"
    )._value.get()
    assert sample == 15, (
        "Reply queue size should be 15, got %s" % sample
    )


def test_stats_to_metrics_sets_rebalance_age(kafka_metrics):
    """Verify that rebalance age is recorded correctly."""
    stats = {
        "type": "consumer",
        "client_id": "test-client-3",
        "cgrp": {
            "rebalance_cnt": 1,
            "rebalance_age": 12345,
        },
        "replyq": 0,
    }

    kafka_metrics.stats_to_metrics(json.dumps(stats))

    sample = kafka_metrics.KAFKA_CONSUMER_REBALANCE_AGE.labels(
        type="consumer", client_id="test-client-3"
    )._value.get()
    assert sample == 12345, (
        "Rebalance age should be 12345, got %s" % sample
    )
