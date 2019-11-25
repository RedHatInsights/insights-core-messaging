import json
import logging
from kafka import KafkaConsumer
from . import Consumer

log = logging.getLogger(__name__)


class Kafka(Consumer):
    def __self__(self,
            publisher,
            downloader,
            engine,
            incoming_topic="",
            bootstrap_servers="",
            group_id="",
            retry_backoff_ms=1000):

        super().__init__(publisher, downloader, engine)
        self.consumer = KafkaConsumer(
            incoming_topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=self.deserialize,
            retry_backoff_ms=retry_backoff_ms,
        )

    def deserialize(self, bytes_):
        return json.loads(bytes_.decode("utf-8"))

    def handles(self, input_msg):
        return True

    def run(self):
        for msg in self.consumer:
            log.debug("recv", extra=msg.value)
            if self.handles(msg.value):
                self.process(msg.value)
