"""
Tests for Kafka consumer context helpers.

This module tests the ``update_archive_context_ids`` helper and the
``ArchiveContextIdsInjectingFilter`` logging filter.

Note: The ``Kafka`` consumer class itself requires a live confluent-kafka
connection and is tested via integration tests, not here.
"""

import logging

import pytest

from insights_messaging.consumers import (
    ArchiveContextIdsInjectingFilter,
    archive_context_var,
)
from insights_messaging.consumers.kafka import update_archive_context_ids


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
def test_update_context_ids_sets_fields(request, payload, expected):
    """update_archive_context_ids must extract IDs when present, skip when absent."""
    update_archive_context_ids(payload)
    ctx = archive_context_var.get()
    request.addfinalizer(lambda: archive_context_var.set({}))
    assert ctx == expected


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(None, id="none_payload"),
        pytest.param({"other_key": "value"}, id="missing_structure"),
    ],
)
def test_update_context_ids_noop_for_invalid_payload(request, payload):
    """update_archive_context_ids must be a safe no-op for invalid payloads.

    The Kafka consumer may receive tombstone messages (None), deserialization
    failures, or messages from different topics with different schemas.
    The function must leave the context unchanged rather than raising.
    """
    update_archive_context_ids(payload)
    request.addfinalizer(lambda: archive_context_var.set({}))
    assert archive_context_var.get() == {}, f"Context should remain empty for payload {payload!r}"


def test_filter_injects_context_ids(request):
    """ArchiveContextIdsInjectingFilter must copy context IDs onto log records.

    When context is populated, the filter sets request_id and inventory_id
    as attributes on each LogRecord.  When context is empty (between
    messages or during startup), it must not add attributes with None
    values — that would cause format string errors in log formatters.
    """
    # --- With context ---
    archive_context_var.set({"request_id": "req-abc", "inventory_id": "inv-def"})
    request.addfinalizer(lambda: archive_context_var.set({}))
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
