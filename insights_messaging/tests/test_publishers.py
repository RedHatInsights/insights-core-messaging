"""
Tests for publisher implementations.

Publishers handle the output of processed results.  The base Publisher
class is a no-op, and StdOut prints results to stdout.
"""

from insights_messaging.publishers.cli import StdOut


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
