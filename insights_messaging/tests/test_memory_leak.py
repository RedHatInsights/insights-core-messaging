"""
Regression tests for CCXDEV-15098: Memory leak fixes in insights-core-messaging.

This module tests two separate memory leak fixes in the messaging layer:

1. **Broker cleanup in Consumer.process()** — When a component raises an
   exception during ``dr.run()``, Python attaches the live traceback object
   to ``exception.__traceback__``.  That traceback holds a reference to the
   stack frame, which references local variables — including the ``broker``.
   This creates a circular reference chain::

       Broker -> exceptions -> exception -> __traceback__ -> frame -> Broker

   CPython's reference-counting collector cannot break cycles, so the broker
   and all of its data (the ``instances`` dict with ~500 component results)
   stays alive until the cyclic GC runs.  In long-running services this
   manifests as a steady memory leak (~8 MB/hr in production).

   The fix in ``Consumer.process()`` clears ``ex.__traceback__`` on every
   stored exception and then clears the broker's ``exceptions``,
   ``tracebacks``, and ``instances`` dicts in the ``finally`` block,
   breaking the cycle without losing any debugging information (the
   formatted traceback string is already captured before cleanup).

2. **Kafka metrics label cardinality** — The ``KafkaMetrics`` class
   previously included a ``state`` label on ``KAFKA_CONSUMER_REBALANCE_COUNT``.
   Since ``state`` changes over time (e.g. "up", "rebalancing", "init"),
   each unique label combination created a new child metric object stored
   permanently in prometheus_client's internal ``_metrics`` dict — never
   garbage collected.  With ``stats_cb`` firing every 10 seconds, this
   caused ~1 MB/hr of memory growth.

   The fix removes ``state`` from the rebalance count labelnames and tracks
   consumer state separately via ``KAFKA_CONSUMER_STATE`` with
   fixed-cardinality labels (type + client_id only).

These tests cover:

- Exception ``__traceback__`` clearing after ``Consumer.process()``
- Broker dict cleanup (exceptions, tracebacks, instances)
- Cleanup on both success and failure paths
- Safe handling when broker is ``None`` (download failure)
- GC collection of brokers via reference counting alone (CPython)
- Kafka metrics label cardinality (no ``state`` label leak)
"""

import gc
import json
import platform
import traceback
import weakref
from collections import defaultdict
from contextlib import contextmanager
import pytest

from insights_messaging.consumers import Consumer


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXPECTED_MSG = "memory leak test exception"
EXPECTED_COMPONENT = "test_component"


# ---------------------------------------------------------------------------
# Mock infrastructure for Consumer.process() tests
# ---------------------------------------------------------------------------

class MockBroker:
    """Minimal broker mock matching the attributes used by Consumer.process().

    Replicates the relevant interface of ``insights.core.dr.Broker``:
    ``exceptions`` (defaultdict(list)), ``tracebacks`` (dict), and
    ``instances`` (dict).
    """
    def __init__(self):
        self.exceptions = defaultdict(list)
        self.tracebacks = {}
        self.instances = {}

    def add_exception(self, component, ex, tb=None):
        """Store an exception and its formatted traceback string."""
        self.exceptions[component].append(ex)
        self.tracebacks[ex] = tb


class MockPublisher:
    """Publisher that records calls without side effects."""
    def publish(self, input_msg, results):
        pass

    def error(self, input_msg, ex):
        pass


class MockEngine:
    """Engine mock that populates the broker with exceptions.

    When ``process()`` is called, it adds a test exception to the broker
    to simulate what ``dr.run_components()`` does when a component fails.
    The exception will have a live ``__traceback__`` (created by raising
    and catching it), mimicking the real circular reference scenario.
    """
    def __init__(self, should_raise=False):
        self.should_raise = should_raise

    def process(self, broker, path):
        # Simulate a component raising during dr.run_components().
        # The exception gets a real __traceback__ from being raised.
        try:
            raise Exception(EXPECTED_MSG)
        except Exception as ex:
            tb = traceback.format_exc()
            broker.add_exception(EXPECTED_COMPONENT, ex, tb)

        # Also populate instances to simulate real broker state.
        for i in range(10):
            broker.instances[f"component_{i}"] = f"result_{i}"

        if self.should_raise:
            raise RuntimeError("engine failure")

        return "test_results"


@contextmanager
def _mock_download():
    """Context manager that yields a fake path, simulating downloader.get()."""
    yield "/tmp/fake_archive"


class MockDownloader:
    """Downloader that returns a fake path without touching the filesystem."""
    def get(self, url):
        return _mock_download()


class FailingDownloader:
    """Downloader that raises before the broker is created.

    Used to test the edge case where broker remains None in the finally block.
    """
    def get(self, url):
        raise IOError("download failed")


class StubConsumer(Consumer):
    """Concrete Consumer subclass for testing.

    Provides the minimal implementations of abstract methods (``run``,
    ``get_url``) and exposes the broker created during ``process()`` so
    tests can inspect it before cleanup.
    """
    def __init__(self, publisher, downloader, engine, broker_factory=None):
        super().__init__(publisher, downloader, engine)
        self._broker_factory = broker_factory or MockBroker
        self._broker_before_cleanup = None

    def run(self):
        raise NotImplementedError()

    def get_url(self, input_msg):
        return "http://example.com/archive.tar.gz"

    def create_broker(self, input_msg):
        return self._broker_factory()


def _find_exceptions(broker, exc_type=Exception):
    """Return all exceptions of exc_type from any component in the broker.

    Searches all components to avoid false-pass from a missed key lookup.
    """
    found = []
    for comp, ex_list in broker.exceptions.items():
        for ex in ex_list:
            if isinstance(ex, exc_type):
                found.append(ex)
    return found


# ---------------------------------------------------------------------------
# Helper: run process() and capture broker state before cleanup
# ---------------------------------------------------------------------------

def _run_process_capturing_broker(consumer, input_msg="test_msg"):
    """Run consumer.process() and return the broker.

    The broker's dicts are cleared in the finally block, so we capture
    the pre-cleanup state by monkey-patching the consumer. The returned
    broker will have cleared dicts (post-cleanup), but we store
    pre-cleanup snapshots as attributes for assertions.
    """
    broker_ref = {}
    pre_cleanup = {}

    original_create = consumer.create_broker

    def capturing_create(msg):
        broker = original_create(msg)
        broker_ref["broker"] = broker
        return broker

    consumer.create_broker = capturing_create

    # Monkey-patch to capture state before cleanup
    original_process = consumer.engine.process

    def capturing_process(broker, path):
        result = original_process(broker, path)
        # Snapshot broker state before the finally block clears it
        pre_cleanup["exceptions"] = {
            comp: list(exs) for comp, exs in broker.exceptions.items()
        }
        pre_cleanup["tracebacks"] = dict(broker.tracebacks)
        pre_cleanup["instances"] = dict(broker.instances)
        return result

    consumer.engine.process = capturing_process

    try:
        consumer.process(input_msg)
    except Exception:
        pass  # Some tests intentionally cause exceptions

    broker = broker_ref.get("broker")
    if broker is not None:
        broker._pre_cleanup = pre_cleanup
    return broker


# ---------------------------------------------------------------------------
# Tests for exception __traceback__ clearing
# ---------------------------------------------------------------------------

def test_traceback_cleared_after_process():
    """Verify that __traceback__ is None for all exceptions after process().

    Without the fix, exceptions stored in broker.exceptions retain a
    __traceback__ reference to the stack frame, creating a circular
    reference chain that prevents the broker from being garbage collected:

        Broker -> exceptions -> ex -> __traceback__ -> frame -> Broker
    """
    consumer = StubConsumer(MockPublisher(), MockDownloader(), MockEngine())
    broker = _run_process_capturing_broker(consumer)

    assert broker is not None, "Broker should have been created during process()"

    # Check pre-cleanup state had exceptions with tracebacks
    pre = broker._pre_cleanup
    all_exceptions = []
    for ex_list in pre["exceptions"].values():
        all_exceptions.extend(ex_list)

    assert len(all_exceptions) >= 1, (
        "Expected at least one exception in broker.exceptions before cleanup, "
        "found none. Keys: %s" % list(pre["exceptions"].keys())
    )

    for ex in all_exceptions:
        # The fix: __traceback__ must be cleared to break the circular ref.
        # This happens even though broker.exceptions is also cleared,
        # because the exception objects may be referenced elsewhere.
        assert ex.__traceback__ is None, (
            "ex.__traceback__ should be None after process() to prevent "
            "circular reference: Broker -> exceptions -> ex -> "
            "__traceback__ -> frame -> Broker"
        )


def test_traceback_string_preserved_before_cleanup():
    """Verify that formatted traceback strings were captured before cleanup.

    The formatted traceback string is the primary debugging artifact.
    It must be captured in broker.tracebacks *before* the finally block
    clears the dicts.  This test verifies no debugging information is
    lost by the cleanup.
    """
    consumer = StubConsumer(MockPublisher(), MockDownloader(), MockEngine())
    broker = _run_process_capturing_broker(consumer)

    assert broker is not None, "Broker should have been created during process()"

    pre = broker._pre_cleanup
    for ex, tb_string in pre["tracebacks"].items():
        assert tb_string is not None, (
            "Formatted traceback string should be captured before cleanup"
        )
        assert EXPECTED_MSG in tb_string, (
            "Formatted traceback should contain the exception message %r, "
            "got: %s" % (EXPECTED_MSG, tb_string[:200])
        )
        assert "Traceback" in tb_string, (
            "Formatted traceback should contain 'Traceback' header"
        )


# ---------------------------------------------------------------------------
# Tests for broker dict cleanup
# ---------------------------------------------------------------------------

def test_broker_dicts_cleared_after_process():
    """Verify that broker.exceptions, tracebacks, and instances are all
    cleared after process() completes.

    Without clearing these dicts, the broker retains references to all
    component results (~500 objects in production) and exception objects,
    preventing them from being garbage collected even after the message
    has been fully processed and published.
    """
    consumer = StubConsumer(MockPublisher(), MockDownloader(), MockEngine())
    broker = _run_process_capturing_broker(consumer)

    assert broker is not None, "Broker should have been created during process()"

    # Verify pre-cleanup state had data
    pre = broker._pre_cleanup
    assert len(pre["exceptions"]) > 0, (
        "Pre-cleanup broker should have had exceptions"
    )
    assert len(pre["instances"]) > 0, (
        "Pre-cleanup broker should have had instances"
    )

    # Verify post-cleanup state is empty
    assert len(broker.exceptions) == 0, (
        "broker.exceptions should be empty after process() cleanup, "
        "found %d entries" % len(broker.exceptions)
    )
    assert len(broker.tracebacks) == 0, (
        "broker.tracebacks should be empty after process() cleanup, "
        "found %d entries" % len(broker.tracebacks)
    )
    assert len(broker.instances) == 0, (
        "broker.instances should be empty after process() cleanup, "
        "found %d entries" % len(broker.instances)
    )


# ---------------------------------------------------------------------------
# Tests for cleanup on exception path
# ---------------------------------------------------------------------------

def test_cleanup_on_engine_exception():
    """Verify that broker cleanup still happens when engine.process() raises.

    The cleanup code is in the ``finally`` block, so it must execute
    regardless of whether processing succeeds or fails.  Without this,
    failed messages would leak the entire broker.
    """
    engine = MockEngine(should_raise=True)
    consumer = StubConsumer(MockPublisher(), MockDownloader(), engine)

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
    assert broker is not None, "Broker should have been created before engine failure"

    # Even after an exception, cleanup must happen
    assert len(broker.exceptions) == 0, (
        "broker.exceptions should be cleared even when engine.process() raises"
    )
    assert len(broker.tracebacks) == 0, (
        "broker.tracebacks should be cleared even when engine.process() raises"
    )
    assert len(broker.instances) == 0, (
        "broker.instances should be cleared even when engine.process() raises"
    )


def test_no_crash_when_broker_is_none():
    """Verify that process() handles the case where broker is never created.

    If the download fails before create_broker() is called, broker remains
    None.  The cleanup code in the finally block must not crash in this case.
    """
    consumer = StubConsumer(
        MockPublisher(), FailingDownloader(), MockEngine()
    )

    # Should not raise — the IOError from download is caught and re-raised,
    # but the finally block must not add a secondary exception.
    with pytest.raises(IOError, match="download failed"):
        consumer.process("test_msg")


# ---------------------------------------------------------------------------
# GC collection test
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    platform.python_implementation() != "CPython",
    reason="Relies on CPython reference-counting semantics"
)
def test_broker_collected_after_process():
    """Verify that brokers are garbage collected after processing.

    Without the fix, circular references from exception.__traceback__ keep
    brokers alive indefinitely, causing memory to grow over time.

    We disable the cyclic GC during the test so that only reference-counting
    frees the brokers.  With the fix, clearing __traceback__ and the broker
    dicts breaks all cycles and reference counting alone is sufficient.
    Without the fix, the circular reference keeps the broker alive.
    """
    n_runs = 5
    refs = []

    # Disable cyclic GC so circular references are NOT collected.
    # This makes the test deterministic: brokers survive IFF there
    # is a reference cycle.
    gc.collect()
    was_enabled = gc.isenabled()
    gc.disable()

    try:
        for _ in range(n_runs):
            consumer = StubConsumer(
                MockPublisher(), MockDownloader(), MockEngine()
            )
            # Call process() directly — don't use the capturing helper
            # because its closures would hold extra references to the broker.
            consumer.process("test_msg")
            # Access the broker via the consumer's create_broker path.
            # After process() returns, the broker local is out of scope,
            # but we need a weakref to it.  Re-create and process to get one.
            broker = MockBroker()
            broker_ref = weakref.ref(broker)
            consumer._broker_factory = lambda b=broker: b
            consumer.process("test_msg")
            refs.append(broker_ref)
            del broker
            del consumer

        collected = sum(1 for ref in refs if ref() is None)
        assert collected >= n_runs - 1, (
            f"Only {collected}/{n_runs} brokers were garbage collected "
            "by reference counting alone (cyclic GC disabled). "
            "Remaining brokers are held by circular references "
            "from exception.__traceback__ -> frame -> broker."
        )
    finally:
        # Restore GC state exactly as it was before the test.
        if was_enabled:
            gc.enable()
        gc.collect()


# ---------------------------------------------------------------------------
# Tests for KafkaMetrics label cardinality
# ---------------------------------------------------------------------------

def test_rebalance_count_labels_exclude_state(kafka_metrics):
    """Verify that KAFKA_CONSUMER_REBALANCE_COUNT does not include 'state'.

    Previously, labelnames included ['type', 'client_id', 'state'].  Since
    ``state`` changes over time (e.g. "up", "rebalancing", "init"), each
    unique label combination created a new child metric object stored
    permanently in prometheus_client's internal ``_metrics`` dict — never
    garbage collected.  This caused ~1 MB/hr of memory growth.

    The fix removes 'state' from the labelnames to keep metric cardinality
    fixed (one child per type+client_id pair).
    """
    labelnames = kafka_metrics.KAFKA_CONSUMER_REBALANCE_COUNT._labelnames

    assert "state" not in labelnames, (
        "KAFKA_CONSUMER_REBALANCE_COUNT should not have 'state' in its "
        "labelnames to prevent unbounded metric cardinality growth. "
        "Found labelnames: %s" % list(labelnames)
    )
    assert "type" in labelnames, (
        "KAFKA_CONSUMER_REBALANCE_COUNT should have 'type' in labelnames"
    )
    assert "client_id" in labelnames, (
        "KAFKA_CONSUMER_REBALANCE_COUNT should have 'client_id' in labelnames"
    )


def test_consumer_state_metric_exists(kafka_metrics):
    """Verify that KAFKA_CONSUMER_STATE gauge exists with correct labels.

    Consumer state was previously embedded in the rebalance count metric
    as a label, causing cardinality explosion.  It is now tracked
    separately via KAFKA_CONSUMER_STATE with fixed-cardinality labels.
    """
    assert hasattr(kafka_metrics, "KAFKA_CONSUMER_STATE"), (
        "KafkaMetrics should have a KAFKA_CONSUMER_STATE gauge for "
        "tracking consumer state separately from rebalance count"
    )

    labelnames = kafka_metrics.KAFKA_CONSUMER_STATE._labelnames
    assert "type" in labelnames, (
        "KAFKA_CONSUMER_STATE should have 'type' in labelnames"
    )
    assert "client_id" in labelnames, (
        "KAFKA_CONSUMER_STATE should have 'client_id' in labelnames"
    )
    assert "state" not in labelnames, (
        "KAFKA_CONSUMER_STATE should not have 'state' in labelnames — "
        "the value is set on the gauge itself, not as a label"
    )


def test_metrics_cardinality_fixed_across_callbacks(kafka_metrics):
    """Verify that metrics child count stays constant across stats callbacks.

    Without the fix, each ``stats_cb`` invocation with a different ``state``
    value would create a new child metric object in prometheus_client's
    internal ``_metrics`` dict.  With the fix, the label set is fixed
    (type + client_id only), so the child count should remain constant
    regardless of how many callbacks fire.
    """
    # Simulate 50 stats callbacks — in production this fires every 10s
    for i in range(50):
        stats = {
            "type": "consumer",
            "client_id": "test-client-memleak",
            "cgrp": {
                "state": ["up", "rebalancing", "init"][i % 3],
                "rebalance_cnt": i,
                "rebalance_age": i * 1000,
            },
            "replyq": 10,
        }
        kafka_metrics.stats_to_metrics(json.dumps(stats))

    # With fixed labels, there should be exactly 1 child metric per gauge
    # for this specific type+client_id combo.
    rebalance_children = kafka_metrics.KAFKA_CONSUMER_REBALANCE_COUNT._metrics
    memleak_keys = [k for k in rebalance_children if "test-client-memleak" in str(k)]
    assert len(memleak_keys) == 1, (
        f"Expected 1 child metric for test-client-memleak in "
        f"KAFKA_CONSUMER_REBALANCE_COUNT, but found {len(memleak_keys)}. "
        "This suggests 'state' or another variable label is still present, "
        "causing unbounded cardinality growth."
    )
