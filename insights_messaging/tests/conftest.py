"""
Shared test fixtures for insights-core-messaging tests.
"""

import pytest

from insights_messaging.consumers.kafka import KafkaMetrics


@pytest.fixture(scope="session")
def kafka_metrics():
    """Provide a shared KafkaMetrics instance.

    prometheus_client raises ValueError if a metric with the same name
    is registered twice in the default CollectorRegistry.  Since
    KafkaMetrics registers gauges at __init__ time, we must reuse a
    single instance across all tests.
    """
    return KafkaMetrics()
