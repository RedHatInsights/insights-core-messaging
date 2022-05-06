import logging
import os

from confluent_kafka import Consumer as ConfluentConsumer
from insights_messaging.consumers import Consumer, Requeue

log = logging.getLogger(__name__)


class Kafka(Consumer):
    def __init__(
        self,
        publisher,
        downloader,
        engine,
        incoming_topic,
        group_id,
        bootstrap_servers,
        requeuer=None,
        **kwargs
    ):

        super().__init__(publisher, downloader, engine)
        config = kwargs.copy()
        config["group.id"] = group_id
        config["bootstrap.servers"] = ",".join(bootstrap_servers)
        config["group.instance.id"] = kwargs.get("group.instance.id", os.environ.get("HOSTNAME"))

        self.auto_commit = kwargs.get("enable.auto.commit", True)
        self.consumer = ConfluentConsumer(config)

        self.consumer.subscribe([incoming_topic])
        log.info("subscribing to %s: %s", incoming_topic, self.consumer)
        self.requeuer = requeuer

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
                except Requeue as req:
                    if not self.requeuer:
                        raise Exception("Requeue request with no requeuer configured.")
                    self.requeuer.requeue(val, req)
                except Exception as ex:
                    log.exception(ex)
                finally:
                    if not self.auto_commit:
                        self.consumer.commit(msg)
