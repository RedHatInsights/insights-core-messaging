"""
Tests for the watcher system (Watched, Watcher).

These tests verify event dispatching, error isolation, and the watcher
registration API.  EngineWatcher/ConsumerWatcher noop defaults are tested
in test_consumer_watchers.py.
"""

import logging
from unittest.mock import Mock

from insights_messaging.watchers import Watched

# ---------------------------------------------------------------------------
# Watched base class tests
# ---------------------------------------------------------------------------


def test_fire_dispatches_to_watchers():
    """Watched.fire() must call the named method on every registered watcher.

    Multiple watchers (metrics, logging, stats) can be attached to a
    single consumer or engine.  fire() must fan out to all of them so
    that no observer is silently skipped.
    """
    watched = Watched()
    w1 = Mock()
    w2 = Mock()
    watched.add_watcher(w1)
    watched.add_watcher(w2)

    watched.fire("on_recv", "test_msg")

    w1.on_recv.assert_called_once_with("test_msg")
    w2.on_recv.assert_called_once_with("test_msg")


def test_fire_ignores_missing_event():
    """Watched.fire() must silently skip watchers that lack the fired method.

    Not every watcher implements every event.  An EngineWatcher may not
    have on_recv, and a ConsumerWatcher may not have on_engine_complete.
    fire() must use getattr with a fallback to avoid AttributeError.
    """
    watched = Watched()
    w = Mock(spec=[])  # spec=[] means no attributes/methods
    watched.add_watcher(w)

    # Watcher has no "on_nonexistent" method — should not raise.
    watched.fire("on_nonexistent", "arg")


def test_fire_isolates_watcher_exceptions(caplog):
    """Watched.fire() must isolate exceptions so one failing watcher cannot block others.

    In production, a bug in a stats watcher must not prevent the logging
    watcher from recording the event.  fire() catches exceptions per
    watcher, logs them, and continues to the next watcher.  This is
    critical for reliability — a watcher failure should never break
    message processing.
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
    assert "watcher failure" in caplog.text
