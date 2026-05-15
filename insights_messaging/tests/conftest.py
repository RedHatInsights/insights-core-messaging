"""
Shared test fixtures for insights-core-messaging tests.
"""

import pytest
from prometheus_client import CollectorRegistry

from insights_messaging.consumers.kafka import KafkaMetrics


@pytest.fixture
def kafka_metrics():
    """Provide an isolated KafkaMetrics instance with its own registry."""
    return KafkaMetrics(registry=CollectorRegistry())
