"""
Tests for the Consumer base class.

The Consumer base class provides the ``process()`` lifecycle method that
coordinates downloading, broker creation, engine processing, publishing,
and watcher events.  These tests verify the event dispatch ordering,
error handling, and the Requeue mechanism.
"""

from collections import defaultdict
from contextlib import contextmanager

import pytest

from insights_messaging.consumers import Consumer, Requeue
from insights_messaging.watchers import ConsumerWatcher

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXPECTED_URL = "http://example.com/archive.tar.gz"
EXPECTED_RESULTS = "analysis_results"


# ---------------------------------------------------------------------------
# Mock infrastructure
# ---------------------------------------------------------------------------


class MockBroker:
    """Minimal broker mock matching the interface used by Consumer.process()."""

    def __init__(self):
        self.exceptions = defaultdict(list)
        self.tracebacks = {}
        self.instances = {}


class MockPublisher:
    """Publisher that records publish/error calls."""

    def __init__(self):
        self.published = []
        self.errors = []

    def publish(self, input_msg, results):
        self.published.append((input_msg, results))

    def error(self, input_msg, ex):
        self.errors.append((input_msg, ex))


class MockEngine:
    """Engine that returns canned results."""

    def __init__(self, result=EXPECTED_RESULTS, should_raise=None):
        self._result = result
        self._should_raise = should_raise
        self.processed = []

    def process(self, broker, path):
        self.processed.append((broker, path))
        if self._should_raise:
            raise self._should_raise
        return self._result


@contextmanager
def _mock_download():
    yield "/tmp/fake_archive"


class MockDownloader:
    def __init__(self):
        self.downloaded = []

    def get(self, url):
        self.downloaded.append(url)
        return _mock_download()


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

    def __init__(self, publisher, downloader, engine, broker_factory=None):
        super().__init__(publisher, downloader, engine)
        self._broker_factory = broker_factory or MockBroker

    def run(self):
        raise NotImplementedError()

    def get_url(self, input_msg):
        return EXPECTED_URL

    def create_broker(self, input_msg):
        return self._broker_factory()


# ---------------------------------------------------------------------------
# Success path tests
# ---------------------------------------------------------------------------


def test_process_publishes_results():
    """Verify that process() publishes engine results on success."""
    publisher = MockPublisher()
    consumer = StubConsumer(publisher, MockDownloader(), MockEngine())

    consumer.process("test_msg")

    assert len(publisher.published) == 1, (
        f"Expected exactly one publish call, got {len(publisher.published)}"
    )
    msg, results = publisher.published[0]
    assert msg == "test_msg"
    assert results == EXPECTED_RESULTS


def test_process_downloads_url():
    """Verify that process() downloads from the URL returned by get_url()."""
    downloader = MockDownloader()
    consumer = StubConsumer(MockPublisher(), downloader, MockEngine())

    consumer.process("test_msg")

    assert downloader.downloaded == [EXPECTED_URL], (
        f"Expected download of {EXPECTED_URL!r}, got {downloader.downloaded!r}"
    )


def test_process_passes_broker_to_engine():
    """Verify that process() creates a broker and passes it to the engine."""
    engine = MockEngine()
    consumer = StubConsumer(MockPublisher(), MockDownloader(), engine)

    consumer.process("test_msg")

    assert len(engine.processed) == 1, "Expected engine.process() to be called once"
    broker, path = engine.processed[0]
    assert isinstance(broker, MockBroker), "Broker should be a MockBroker instance"
    assert path == "/tmp/fake_archive"


# ---------------------------------------------------------------------------
# Watcher event ordering tests
# ---------------------------------------------------------------------------


def test_watcher_event_order_on_success():
    """Verify the order of watcher events during successful processing.

    The expected order is: on_recv -> on_download -> on_process ->
    on_consumer_success -> on_consumer_complete.
    """
    watcher = RecordingConsumerWatcher()
    consumer = StubConsumer(MockPublisher(), MockDownloader(), MockEngine())
    watcher.watch(consumer)

    consumer.process("test_msg")

    assert watcher.events == [
        "on_recv",
        "on_download",
        "on_process",
        "on_consumer_success",
        "on_consumer_complete",
    ], f"Unexpected watcher event order: {watcher.events}"


def test_watcher_event_order_on_failure():
    """Verify the order of watcher events when engine processing fails.

    The expected order is: on_recv -> on_download -> on_consumer_failure ->
    on_consumer_complete.  on_process and on_consumer_success must NOT fire.
    """
    watcher = RecordingConsumerWatcher()
    engine = MockEngine(should_raise=RuntimeError("engine error"))
    consumer = StubConsumer(MockPublisher(), MockDownloader(), engine)
    watcher.watch(consumer)

    with pytest.raises(RuntimeError, match="engine error"):
        consumer.process("test_msg")

    assert "on_consumer_failure" in watcher.events, (
        "on_consumer_failure should fire when engine raises"
    )
    assert "on_consumer_complete" in watcher.events, "on_consumer_complete should always fire"
    assert "on_consumer_success" not in watcher.events, (
        "on_consumer_success should not fire when engine raises"
    )


def test_on_consumer_complete_fires_on_exception():
    """Verify that on_consumer_complete fires even when process() raises."""
    watcher = RecordingConsumerWatcher()
    engine = MockEngine(should_raise=ValueError("boom"))
    consumer = StubConsumer(MockPublisher(), MockDownloader(), engine)
    watcher.watch(consumer)

    with pytest.raises(ValueError):
        consumer.process("msg")

    assert watcher.events[-1] == "on_consumer_complete", (
        "on_consumer_complete should be the last event, even on failure"
    )


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------


def test_process_calls_publisher_error_on_exception():
    """Verify that process() calls publisher.error() when an exception occurs."""
    publisher = MockPublisher()
    error = RuntimeError("engine failure")
    engine = MockEngine(should_raise=error)
    consumer = StubConsumer(publisher, MockDownloader(), engine)

    with pytest.raises(RuntimeError):
        consumer.process("msg")

    assert len(publisher.errors) == 1, "publisher.error() should be called once on engine failure"
    msg, ex = publisher.errors[0]
    assert msg == "msg"
    assert ex is error


# ---------------------------------------------------------------------------
# Requeue tests
# ---------------------------------------------------------------------------


def test_requeue_exception_exists():
    """Verify that Requeue is a proper exception class."""
    assert issubclass(Requeue, Exception), "Requeue should be a subclass of Exception"
    ex = Requeue("requeue reason")
    assert str(ex) == "requeue reason"
