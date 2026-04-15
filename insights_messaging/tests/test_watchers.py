"""
Tests for the watcher system (Watched, Watcher, EngineWatcher, ConsumerWatcher).

The watcher system provides an observer pattern used by consumers and engines
to fire lifecycle events (on_recv, on_download, on_engine_complete, etc.).
These tests verify event dispatching, error isolation, and the watcher
registration API.
"""

import logging

from insights_messaging.watchers import (
    ConsumerWatcher,
    EngineWatcher,
    Watched,
    Watcher,
)

# ---------------------------------------------------------------------------
# Recording watcher for test assertions
# ---------------------------------------------------------------------------


class RecordingWatcher:
    """Watcher that records all events it receives."""

    def __init__(self):
        self.events = []

    def on_recv(self, input_msg):
        self.events.append(("on_recv", input_msg))

    def on_download(self, path):
        self.events.append(("on_download", path))

    def on_process(self, input_msg, results):
        self.events.append(("on_process", input_msg, results))

    def on_engine_complete(self, broker):
        self.events.append(("on_engine_complete", broker))


class FailingWatcher:
    """Watcher that raises on every event."""

    def on_recv(self, input_msg):
        raise RuntimeError("watcher failure")

    def on_engine_complete(self, broker):
        raise RuntimeError("watcher failure")


# ---------------------------------------------------------------------------
# Watched base class tests
# ---------------------------------------------------------------------------


def test_fire_dispatches_to_watchers():
    """Verify that fire() calls the matching method on all watchers."""
    watched = Watched()
    w1 = RecordingWatcher()
    w2 = RecordingWatcher()
    watched.add_watcher(w1)
    watched.add_watcher(w2)

    watched.fire("on_recv", "test_msg")

    assert w1.events == [("on_recv", "test_msg")], (
        "First watcher should have received the on_recv event"
    )
    assert w2.events == [("on_recv", "test_msg")], (
        "Second watcher should have received the on_recv event"
    )


def test_fire_with_multiple_args():
    """Verify that fire() passes all args to the watcher method."""
    watched = Watched()
    w = RecordingWatcher()
    watched.add_watcher(w)

    watched.fire("on_process", "msg", "results")

    assert w.events == [("on_process", "msg", "results")], (
        "Watcher should receive all arguments passed to fire()"
    )


def test_fire_ignores_missing_event():
    """Verify that fire() does not crash when the watcher lacks the method."""
    watched = Watched()
    w = RecordingWatcher()
    watched.add_watcher(w)

    # RecordingWatcher has no "on_nonexistent" method — should not raise.
    watched.fire("on_nonexistent", "arg")
    assert w.events == [], "No events should be recorded for missing methods"


def test_fire_isolates_watcher_exceptions(caplog):
    """Verify that a failing watcher does not prevent others from running.

    If one watcher raises an exception, the remaining watchers must still
    receive the event.  The exception is logged but not propagated.
    """
    watched = Watched()
    failing = FailingWatcher()
    recording = RecordingWatcher()
    watched.add_watcher(failing)
    watched.add_watcher(recording)

    with caplog.at_level(logging.ERROR):
        watched.fire("on_recv", "msg")

    assert recording.events == [("on_recv", "msg")], (
        "Recording watcher should still receive event despite earlier watcher failing"
    )
    assert "watcher failure" in caplog.text, "The failing watcher's exception should be logged"


def test_fire_with_no_watchers():
    """Verify that fire() does nothing when there are no watchers."""
    watched = Watched()
    # Should not raise.
    watched.fire("on_recv", "msg")


# ---------------------------------------------------------------------------
# Watcher registration tests
# ---------------------------------------------------------------------------


def test_watcher_watch_adds_to_watched():
    """Verify that Watcher.watch() adds itself to the Watched's watcher list."""
    watched = Watched()
    watcher = Watcher()
    watcher.watch(watched)

    assert watcher in watched.watchers, "Watcher.watch() should add the watcher to watched.watchers"


def test_engine_watcher_defaults_are_noop():
    """Verify that EngineWatcher's default methods don't raise."""
    w = EngineWatcher()
    # All default methods should be callable without raising.
    w.watch_broker(None)
    w.pre_extract(None, None)
    w.on_extract(None, None, None)
    w.on_engine_failure(None, None)
    w.on_engine_complete(None)


def test_consumer_watcher_defaults_are_noop():
    """Verify that ConsumerWatcher's default methods don't raise."""
    w = ConsumerWatcher()
    w.on_recv(None)
    w.on_download(None)
    w.on_process(None, None)
    w.on_consumer_success(None, None, None)
    w.on_consumer_failure(None, None)
    w.on_consumer_complete(None)
