import logging
from kafka import KafkaConsumer
from . import Consumer

log = logging.getLogger(__name__)


class Kafka(Consumer):
    def __self__(self,
                 publisher,
                 downloader,
                 engine,
                 incoming_topic,
                 group_id,
                 bootstrap_servers,
                 retry_backoff_ms=1000):

        super().__init__(publisher, downloader, engine)
        self.consumer = KafkaConsumer(
            incoming_topic,
            group_id=group_id,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=self.deserialize,
            retry_backoff_ms=retry_backoff_ms,
        )

    def deserialize(self, bytes_):
        raise NotImplementedError()

    def handles(self, input_msg):
        return True

    def run(self):
        for msg in self.consumer:
            log.debug("recv", extra=msg.value)
            if self.handles(msg.value):
                self.process(msg.value)
