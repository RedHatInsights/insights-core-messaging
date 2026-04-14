"""
Tests for consumer and engine watcher lifecycle integration.

Verifies that the Consumer.process() method fires watcher events in the
correct order and that EngineWatcher/ConsumerWatcher interfaces work
correctly with the Watched event system.
"""

from unittest.mock import MagicMock, patch, PropertyMock

from insights_messaging.consumers import Consumer
from insights_messaging.watchers import (
    ConsumerWatcher,
    EngineWatcher,
    Watched,
)


# ---------------------------------------------------------------------------
# ConsumerWatcher lifecycle tests
# ---------------------------------------------------------------------------

class RecordingConsumerWatcher(ConsumerWatcher):
    """A watcher that records the order of events fired."""

    def __init__(self):
        self.events = []

    def on_recv(self, input_msg):
        self.events.append(("on_recv", input_msg))

    def on_download(self, path):
        self.events.append(("on_download", path))

    def on_process(self, input_msg, results):
        self.events.append(("on_process", input_msg, results))

    def on_consumer_success(self, input_msg, broker, results):
        self.events.append(("on_consumer_success", input_msg))

    def on_consumer_failure(self, input_msg, exception):
        self.events.append(("on_consumer_failure", input_msg, str(exception)))

    def on_consumer_complete(self, input_msg):
        self.events.append(("on_consumer_complete", input_msg))


class StubConsumer(Consumer):
    """Minimal Consumer subclass for testing."""

    def run(self):
        pass

    def get_url(self, input_msg):
        return input_msg.get("url", "/tmp/test")


def _make_consumer(watcher):
    publisher = MagicMock()
    downloader = MagicMock()
    downloader.get.return_value.__enter__ = MagicMock(return_value="/tmp/downloaded")
    downloader.get.return_value.__exit__ = MagicMock(return_value=False)

    engine = MagicMock()
    engine.process.return_value = {"results": "ok"}

    consumer = StubConsumer(publisher, downloader, engine)
    watcher.watch(consumer)
    return consumer


def test_consumer_watcher_success_lifecycle():
    """Verify watcher events fire in correct order on successful processing."""
    watcher = RecordingConsumerWatcher()
    consumer = _make_consumer(watcher)

    msg = {"url": "http://example.com/archive.tar.gz"}
    consumer.process(msg)

    event_names = [e[0] for e in watcher.events]
    assert event_names == [
        "on_recv",
        "on_download",
        "on_process",
        "on_consumer_success",
        "on_consumer_complete",
    ], f"Expected success lifecycle events, got {event_names}"


def test_consumer_watcher_failure_lifecycle():
    """Verify watcher events fire correctly when processing fails."""
    watcher = RecordingConsumerWatcher()
    consumer = _make_consumer(watcher)

    # Make engine.process raise an exception
    consumer.engine.process.side_effect = RuntimeError("engine failed")

    msg = {"url": "http://example.com/archive.tar.gz"}
    try:
        consumer.process(msg)
    except RuntimeError:
        pass

    event_names = [e[0] for e in watcher.events]
    assert "on_recv" in event_names, "on_recv should fire before failure"
    assert "on_consumer_failure" in event_names, "on_consumer_failure should fire on error"
    assert "on_consumer_complete" in event_names, "on_consumer_complete should always fire"
    assert "on_consumer_success" not in event_names, "on_consumer_success should not fire on error"


def test_consumer_watcher_complete_fires_on_failure():
    """Verify on_consumer_complete fires even when processing raises."""
    watcher = RecordingConsumerWatcher()
    consumer = _make_consumer(watcher)

    consumer.downloader.get.return_value.__enter__.side_effect = RuntimeError("download failed")

    msg = {"url": "http://example.com/archive.tar.gz"}
    try:
        consumer.process(msg)
    except RuntimeError:
        pass

    event_names = [e[0] for e in watcher.events]
    assert event_names[-1] == "on_consumer_complete", (
        "on_consumer_complete should be the last event even on failure"
    )


# ---------------------------------------------------------------------------
# EngineWatcher interface tests
# ---------------------------------------------------------------------------

def test_engine_watcher_default_methods_are_noop():
    """Verify EngineWatcher methods are safe no-ops by default."""
    watcher = EngineWatcher()
    # These should not raise
    watcher.watch_broker(MagicMock())
    watcher.pre_extract(MagicMock(), "/path")
    watcher.on_extract(MagicMock(), MagicMock(), MagicMock())
    watcher.on_engine_failure(MagicMock(), RuntimeError("test"))
    watcher.on_engine_complete(MagicMock())


def test_consumer_watcher_default_methods_are_noop():
    """Verify ConsumerWatcher methods are safe no-ops by default."""
    watcher = ConsumerWatcher()
    # These should not raise
    watcher.on_recv("msg")
    watcher.on_download("/path")
    watcher.on_process("msg", {})
    watcher.on_consumer_success("msg", MagicMock(), {})
    watcher.on_consumer_failure("msg", RuntimeError("test"))
    watcher.on_consumer_complete("msg")


# ---------------------------------------------------------------------------
# Multiple watchers isolation tests
# ---------------------------------------------------------------------------

def test_multiple_watchers_all_receive_events():
    """Verify that multiple watchers all receive events."""
    watcher1 = RecordingConsumerWatcher()
    watcher2 = RecordingConsumerWatcher()
    consumer = _make_consumer(watcher1)
    watcher2.watch(consumer)

    msg = {"url": "http://example.com/archive.tar.gz"}
    consumer.process(msg)

    assert len(watcher1.events) > 0, "First watcher should receive events"
    assert len(watcher2.events) > 0, "Second watcher should receive events"
    assert len(watcher1.events) == len(watcher2.events), (
        "Both watchers should receive the same number of events"
    )


def test_failing_watcher_does_not_block_others():
    """Verify a watcher exception doesn't prevent other watchers from firing."""
    failing_watcher = ConsumerWatcher()
    failing_watcher.on_recv = MagicMock(side_effect=RuntimeError("watcher error"))

    recording_watcher = RecordingConsumerWatcher()

    watched = Watched()
    failing_watcher.watch(watched)
    recording_watcher.watch(watched)

    watched.fire("on_recv", "test-msg")

    assert len(recording_watcher.events) == 1, (
        "Second watcher should still receive events even if first watcher fails"
    )
