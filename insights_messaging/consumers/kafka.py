import logging
import os

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import Producer as ConfluentProducer
from insights_messaging.consumers import Consumer

log = logging.getLogger(__name__)


class Requeue(Exception):
    '''
    An exception used to signal a requeue request.
    '''
    pass


class Kafka(Consumer):
    def __init__(
        self,
        publisher,
        downloader,
        engine,
        incoming_topic,
        group_id,
        bootstrap_servers,
        **kwargs
    ):

        super().__init__(publisher, downloader, engine)
        self.topic = incoming_topic
        config = kwargs.copy()
        config["group.id"] = group_id
        config["bootstrap.servers"] = ",".join(bootstrap_servers)
        config["group.instance.id"] = kwargs.get("group.instance.id", os.environ.get("HOSTNAME"))
        log.info("config", extra={"config": config})

        self.auto_commit = kwargs.get("enable.auto.commit", True)
        self.consumer = ConfluentConsumer(config)

        self.consumer.subscribe([self.topic])
        log.info("subscribing to %s: %s", self.topic, self.consumer)
        self.requeuer = ConfluentProducer(config)

    def deserialize(self, bytes_):
        raise NotImplementedError()

    def handles(self, input_msg):
        return True

    def run(self):
        while True:
            msg = self.consumer.poll(1)
            if msg is None:
                continue

            err = msg.error()
            if err is not None:
                if not self.auto_commit:
                    self.consumer.commit(msg)
                log.exception(err)
                continue

            val = msg.value()
            if val is not None:
                try:
                    payload = self.deserialize(val)
                    if self.handles(payload):
                        self.process(payload)
                except Requeue:
                    log.info("produce() asked to requeue the message")
                    self.requeuer.produce(self.topic, val)
                except Exception as ex:
                    log.exception(ex)
                finally:
                    if not self.auto_commit:
                        self.consumer.commit(msg)
