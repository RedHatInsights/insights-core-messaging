"""
Tests for publisher implementations.

Publishers handle the output of processed results.  The base Publisher
class is a no-op, and StdOut prints results to stdout.
"""

from insights_messaging.publishers import Publisher
from insights_messaging.publishers.cli import StdOut

# ---------------------------------------------------------------------------
# Base Publisher tests
# ---------------------------------------------------------------------------


def test_publisher_publish_is_noop():
    """Verify that the base Publisher.publish() does not raise."""
    pub = Publisher()
    pub.publish("input_msg", "response")


def test_publisher_error_is_noop():
    """Verify that the base Publisher.error() does not raise."""
    pub = Publisher()
    pub.error("input_msg", RuntimeError("test"))


# ---------------------------------------------------------------------------
# StdOut publisher tests
# ---------------------------------------------------------------------------


def test_stdout_publish_prints_results(capsys):
    """Verify that StdOut.publish() prints the results to stdout."""
    pub = StdOut()
    pub.publish("input_msg", "test results")

    captured = capsys.readouterr()
    assert "test results" in captured.out, (
        f"StdOut.publish() should print results to stdout, got: {captured.out!r}"
    )


def test_stdout_publish_multiple_calls(capsys):
    """Verify that StdOut.publish() works correctly across multiple calls."""
    pub = StdOut()
    pub.publish("msg1", "result1")
    pub.publish("msg2", "result2")

    captured = capsys.readouterr()
    assert "result1" in captured.out
    assert "result2" in captured.out
