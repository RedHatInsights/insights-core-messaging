"""
Tests for the watcher system (Watched, Watcher).

These tests verify event dispatching, error isolation, and the watcher
registration API.  EngineWatcher/ConsumerWatcher noop defaults are tested
in test_consumer_watchers.py.
"""

import logging
from unittest.mock import Mock

from insights_messaging.watchers import (
    Watched,
    Watcher,
)

# ---------------------------------------------------------------------------
# Watched base class tests
# ---------------------------------------------------------------------------


def test_fire_dispatches_to_watchers():
    """Verify that fire() calls the matching method on all watchers."""
    watched = Watched()
    w1 = Mock()
    w2 = Mock()
    watched.add_watcher(w1)
    watched.add_watcher(w2)

    watched.fire("on_recv", "test_msg")

    w1.on_recv.assert_called_once_with("test_msg")
    w2.on_recv.assert_called_once_with("test_msg")


def test_fire_with_multiple_args():
    """Verify that fire() passes all args to the watcher method."""
    watched = Watched()
    w = Mock()
    watched.add_watcher(w)

    watched.fire("on_process", "msg", "results")

    w.on_process.assert_called_once_with("msg", "results")


def test_fire_ignores_missing_event():
    """Verify that fire() does not crash when the watcher lacks the method."""
    watched = Watched()
    w = Mock(spec=[])  # spec=[] means no attributes/methods
    watched.add_watcher(w)

    # Watcher has no "on_nonexistent" method — should not raise.
    watched.fire("on_nonexistent", "arg")


def test_fire_isolates_watcher_exceptions(caplog):
    """Verify that a failing watcher does not prevent others from running.

    If one watcher raises an exception, the remaining watchers must still
    receive the event.  The exception is logged but not propagated.
    """
    watched = Watched()
    failing = Mock()
    failing.on_recv.side_effect = RuntimeError("watcher failure")
    recording = Mock()
    watched.add_watcher(failing)
    watched.add_watcher(recording)

    with caplog.at_level(logging.ERROR):
        watched.fire("on_recv", "msg")

    recording.on_recv.assert_called_once_with("msg")
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
