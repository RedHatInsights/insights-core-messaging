import logging

from confluent_kafka import Producer as ConfluentProducer

from . import Requeuer

log = logging.getLogger(__name__)


class KafkaRequeuer(Requeuer):
    def __init__(self, topic, group_id, bootstrap_servers, **kwargs):
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers

        config = kwargs.copy()
        config["group.id"] = group_id
        config["bootstrap.servers"] = ",".join(bootstrap_servers)
        log.info("config", extra={"config": config})

        self.producer = ConfluentProducer(config)

    def requeue(self, msg, req):
        self.producer.produce(self.topic, msg)
