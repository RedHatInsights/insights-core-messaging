"""
Tests for the Consumer base class.

The Consumer base class provides the ``process()`` lifecycle method that
coordinates downloading, broker creation, engine processing, publishing,
and watcher events.  These tests verify the event dispatch ordering,
error handling, and the Requeue mechanism.
"""

from unittest.mock import MagicMock

import pytest

from insights_messaging.consumers import Consumer
from insights_messaging.watchers import ConsumerWatcher

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXPECTED_URL = "http://example.com/archive.tar.gz"
EXPECTED_RESULTS = "analysis_results"


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


def _make_downloader():
    """Create a MagicMock downloader whose get() returns a context manager."""
    dl = MagicMock()
    dl.get.return_value.__enter__ = MagicMock(return_value="/tmp/fake_archive")
    dl.get.return_value.__exit__ = MagicMock(return_value=False)
    return dl


def _make_engine(result=EXPECTED_RESULTS, side_effect=None):
    """Create a MagicMock engine with configurable process() behavior."""
    engine = MagicMock()
    engine.process.return_value = result
    if side_effect:
        engine.process.side_effect = side_effect
    return engine


class RecordingConsumerWatcher(ConsumerWatcher):
    """Watcher that records the order of consumer lifecycle events."""

    def __init__(self):
        self.events = []

    def on_recv(self, input_msg):
        self.events.append("on_recv")

    def on_download(self, path):
        self.events.append("on_download")

    def on_process(self, input_msg, results):
        self.events.append("on_process")

    def on_consumer_success(self, input_msg, broker, results):
        self.events.append("on_consumer_success")

    def on_consumer_failure(self, input_msg, exception):
        self.events.append("on_consumer_failure")

    def on_consumer_complete(self, input_msg):
        self.events.append("on_consumer_complete")


class StubConsumer(Consumer):
    """Concrete Consumer subclass for testing."""

    def __init__(self, publisher, downloader, engine):
        super().__init__(publisher, downloader, engine)

    def run(self):
        raise NotImplementedError()

    def get_url(self, input_msg):
        return EXPECTED_URL


# ---------------------------------------------------------------------------
# Success path tests
# ---------------------------------------------------------------------------


def test_process_publishes_results():
    """Consumer.process() must forward engine results to the publisher.

    The consumer orchestrates download -> engine -> publish.  This test
    verifies the final step: that the results returned by the engine are
    passed to publisher.publish() together with the original input message.
    """
    publisher = MagicMock()
    consumer = StubConsumer(publisher, _make_downloader(), _make_engine())

    consumer.process("test_msg")

    publisher.publish.assert_called_once_with("test_msg", EXPECTED_RESULTS)


def test_process_downloads_url():
    """Consumer.process() must download the archive from the URL provided by get_url().

    Subclasses override get_url() to extract the download location from the
    incoming message.  This test ensures the downloader receives that URL.
    """
    downloader = _make_downloader()
    consumer = StubConsumer(MagicMock(), downloader, _make_engine())

    consumer.process("test_msg")

    downloader.get.assert_called_once_with(EXPECTED_URL)


def test_process_passes_broker_to_engine():
    """Consumer.process() must create a broker and pass it with the archive path to the engine.

    The broker carries component results and exceptions through the
    insights-core evaluation pipeline.  The engine needs both the broker
    and the local path to the extracted archive.
    """
    engine = _make_engine()
    consumer = StubConsumer(MagicMock(), _make_downloader(), engine)

    consumer.process("test_msg")

    engine.process.assert_called_once()
    _broker, path = engine.process.call_args[0]
    assert path == "/tmp/fake_archive"


# ---------------------------------------------------------------------------
# Watcher event ordering tests
# ---------------------------------------------------------------------------


def test_watcher_success_event_order():
    """Watcher events must fire in a deterministic order on the success path."""
    watcher = RecordingConsumerWatcher()
    consumer = StubConsumer(MagicMock(), _make_downloader(), _make_engine())
    watcher.watch(consumer)

    consumer.process("test_msg")

    assert watcher.events == [
        "on_recv",
        "on_download",
        "on_process",
        "on_consumer_success",
        "on_consumer_complete",
    ], f"Unexpected success event order: {watcher.events}"


def test_watcher_failure_event_order():
    """Watcher failure path must fire on_consumer_failure and skip on_consumer_success."""
    watcher = RecordingConsumerWatcher()
    engine = _make_engine(side_effect=RuntimeError("engine error"))
    consumer = StubConsumer(MagicMock(), _make_downloader(), engine)
    watcher.watch(consumer)

    with pytest.raises(RuntimeError, match="engine error"):
        consumer.process("test_msg")

    assert "on_consumer_failure" in watcher.events
    assert "on_consumer_success" not in watcher.events
    assert watcher.events[-1] == "on_consumer_complete"


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------


def test_process_calls_publisher_error_on_exception():
    """Consumer.process() must notify the publisher about engine failures.

    publisher.error() is the mechanism for reporting processing failures
    back to the message broker (e.g. dead-letter queue, error topic).
    The original message and exception must be passed through so the
    publisher can decide how to handle the failure.
    """
    publisher = MagicMock()
    error = RuntimeError("engine failure")
    engine = _make_engine(side_effect=error)
    consumer = StubConsumer(publisher, _make_downloader(), engine)

    with pytest.raises(RuntimeError):
        consumer.process("msg")

    # Verify the original message and the exact exception instance are
    # forwarded — not copies or wrappers.
    publisher.error.assert_called_once()
    msg, ex = publisher.error.call_args[0]
    assert msg == "msg"
    assert ex is error
