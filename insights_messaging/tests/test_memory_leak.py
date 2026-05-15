"""
Tests for broker memory leak fixes in Consumer.process().

Covers traceback clearing, broker dict cleanup, safe handling when
broker is None, and GC collection via reference counting.
"""

import gc
import platform
import traceback
import weakref
from collections import defaultdict
from contextlib import suppress
from unittest.mock import MagicMock

import pytest

from insights_messaging.consumers import Consumer

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXPECTED_MSG = "memory leak test exception"
EXPECTED_COMPONENT = "test_component"


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


class MockBroker:
    """Minimal broker mock with exceptions, tracebacks, and instances dicts."""

    def __init__(self):
        self.exceptions = defaultdict(list)
        self.tracebacks = {}
        self.instances = {}

    def add_exception(self, component, ex, tb=None):
        self.exceptions[component].append(ex)
        self.tracebacks[ex] = tb


class MockEngine:
    """Engine that populates the broker with a caught exception."""

    def __init__(self, should_raise=False):
        self.should_raise = should_raise

    def process(self, broker, path):
        try:
            raise Exception(EXPECTED_MSG)
        except Exception as ex:
            tb = traceback.format_exc()
            broker.add_exception(EXPECTED_COMPONENT, ex, tb)

        for i in range(10):
            broker.instances[f"component_{i}"] = f"result_{i}"

        if self.should_raise:
            raise RuntimeError("engine failure")

        return "test_results"


def _make_downloader():
    """Return a MagicMock downloader whose get() yields a fake path."""
    dl = MagicMock()
    dl.get.return_value.__enter__ = MagicMock(return_value="/tmp/fake_archive")
    dl.get.return_value.__exit__ = MagicMock(return_value=False)
    return dl


def _make_failing_downloader():
    """Return a MagicMock downloader whose get() raises OSError."""
    dl = MagicMock()
    dl.get.side_effect = OSError("download failed")
    return dl


class StubConsumer(Consumer):
    """Concrete Consumer subclass for testing."""

    def __init__(self, publisher, downloader, engine, broker_factory=None):
        super().__init__(publisher, downloader, engine)
        self._broker_factory = broker_factory or MockBroker

    def run(self):
        raise NotImplementedError()

    def get_url(self, input_msg):
        return "http://example.com/archive.tar.gz"

    def create_broker(self, input_msg):
        return self._broker_factory()


# ---------------------------------------------------------------------------
# Helper: run process() and capture broker state before cleanup
# ---------------------------------------------------------------------------


def _run_process_capturing_broker(consumer, input_msg="test_msg"):
    """Run consumer.process() and return the broker with pre-cleanup snapshots."""
    broker_ref = {}
    pre_cleanup = {}

    original_create = consumer.create_broker

    def capturing_create(msg):
        broker = original_create(msg)
        broker_ref["broker"] = broker
        return broker

    consumer.create_broker = capturing_create

    original_process = consumer.engine.process

    def capturing_process(broker, path):
        result = original_process(broker, path)
        pre_cleanup["exceptions"] = {comp: list(exs) for comp, exs in broker.exceptions.items()}
        pre_cleanup["tracebacks"] = dict(broker.tracebacks)
        pre_cleanup["instances"] = dict(broker.instances)
        return result

    consumer.engine.process = capturing_process

    with suppress(Exception):
        consumer.process(input_msg)

    broker = broker_ref.get("broker")
    if broker is not None:
        broker._pre_cleanup = pre_cleanup
    return broker


# ---------------------------------------------------------------------------
# Tests for exception __traceback__ clearing
# ---------------------------------------------------------------------------


def test_traceback_cleared_after_process():
    """__traceback__ must be None on all stored exceptions after process()."""
    consumer = StubConsumer(MagicMock(), _make_downloader(), MockEngine())
    broker = _run_process_capturing_broker(consumer)

    assert broker is not None

    pre = broker._pre_cleanup
    all_exceptions = []
    for ex_list in pre["exceptions"].values():
        all_exceptions.extend(ex_list)

    assert len(all_exceptions) >= 1

    for ex in all_exceptions:
        assert ex.__traceback__ is None


def test_traceback_string_preserved_before_cleanup():
    """Formatted traceback strings must be captured before cleanup clears them."""
    consumer = StubConsumer(MagicMock(), _make_downloader(), MockEngine())
    broker = _run_process_capturing_broker(consumer)

    assert broker is not None

    pre = broker._pre_cleanup
    for _ex, tb_string in pre["tracebacks"].items():
        assert tb_string is not None
        assert EXPECTED_MSG in tb_string
        assert "Traceback" in tb_string


# ---------------------------------------------------------------------------
# Tests for broker dict cleanup
# ---------------------------------------------------------------------------


def test_broker_dicts_cleared_after_process():
    """broker.exceptions, tracebacks, and instances must be empty after process()."""
    consumer = StubConsumer(MagicMock(), _make_downloader(), MockEngine())
    broker = _run_process_capturing_broker(consumer)

    assert broker is not None

    pre = broker._pre_cleanup
    assert len(pre["exceptions"]) > 0
    assert len(pre["instances"]) > 0

    assert len(broker.exceptions) == 0
    assert len(broker.tracebacks) == 0
    assert len(broker.instances) == 0


# ---------------------------------------------------------------------------
# Tests for cleanup on exception path
# ---------------------------------------------------------------------------


def test_cleanup_on_engine_exception():
    """Broker cleanup must happen even when engine.process() raises."""
    engine = MockEngine(should_raise=True)
    consumer = StubConsumer(MagicMock(), _make_downloader(), engine)

    broker_ref = {}
    original_create = consumer.create_broker

    def capturing_create(msg):
        broker = original_create(msg)
        broker_ref["broker"] = broker
        return broker

    consumer.create_broker = capturing_create

    with pytest.raises(RuntimeError, match="engine failure"):
        consumer.process("test_msg")

    broker = broker_ref.get("broker")
    assert broker is not None

    assert len(broker.exceptions) == 0
    assert len(broker.tracebacks) == 0
    assert len(broker.instances) == 0


def test_no_crash_when_broker_is_none():
    """process() must not crash in the finally block when broker is None."""
    consumer = StubConsumer(MagicMock(), _make_failing_downloader(), MockEngine())

    with pytest.raises(OSError, match="download failed"):
        consumer.process("test_msg")


# ---------------------------------------------------------------------------
# GC collection test
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    platform.python_implementation() != "CPython",
    reason="Relies on CPython reference-counting semantics",
)
def test_broker_collected_after_process():
    """Brokers must be GC-collectible by refcounting alone (no cyclic GC needed)."""
    n_runs = 5
    refs = []

    gc.collect()
    was_enabled = gc.isenabled()
    gc.disable()

    try:
        for _ in range(n_runs):
            consumer = StubConsumer(MagicMock(), _make_downloader(), MockEngine())
            consumer.process("test_msg")
            broker = MockBroker()
            broker_ref = weakref.ref(broker)
            consumer._broker_factory = lambda b=broker: b
            consumer.process("test_msg")
            refs.append(broker_ref)
            del broker
            del consumer

        collected = sum(1 for ref in refs if ref() is None)
        assert collected >= n_runs - 1, (
            f"Only {collected}/{n_runs} brokers were collected by refcounting alone"
        )
    finally:
        if was_enabled:
            gc.enable()
        gc.collect()
