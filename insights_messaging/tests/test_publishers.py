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


def test_publisher_noop_methods():
    """Base Publisher.publish() and error() must be safe no-ops.

    The base Publisher is an abstract-like class that concrete publishers
    (Kafka, RabbitMQ, StdOut) override.  The default implementations must
    not raise so that the consumer can function with a no-op publisher
    during development or testing.
    """
    pub = Publisher()
    pub.publish("input_msg", "response")
    pub.error("input_msg", RuntimeError("test"))


# ---------------------------------------------------------------------------
# StdOut publisher tests
# ---------------------------------------------------------------------------


def test_stdout_publish_prints_results(capsys):
    """StdOut.publish() must print each result to stdout.

    StdOut is the simplest publisher, used for local development and
    the interactive CLI consumer.  Each call to publish() must produce
    output so the developer can see results in the terminal.
    """
    pub = StdOut()
    pub.publish("msg1", "result1")
    pub.publish("msg2", "result2")

    captured = capsys.readouterr()
    assert "result1" in captured.out
    assert "result2" in captured.out
