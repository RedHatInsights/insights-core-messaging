import logging
from kafka import KafkaConsumer
from . import Consumer

log = logging.getLogger(__name__)


class Kafka(Consumer):
    def __init__(self,
                 publisher,
                 downloader,
                 engine,
                 incoming_topic,
                 group_id,
                 bootstrap_servers,
                 **kwargs):

        retry_backoff_ms = kwargs.pop("retry_backoff_ms", 1000)

        super().__init__(publisher, downloader, engine)
        self.consumer = KafkaConsumer(
            incoming_topic,
            group_id=group_id,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=self.deserialize,
            retry_backoff_ms=retry_backoff_ms,
            **kwargs
        )

    def deserialize(self, bytes_):
        raise NotImplementedError()

    def handles(self, input_msg):
        return True

    def run(self):
        for msg in self.consumer:
            try:
                if self.handles(msg):
                    self.process(msg)
            except Exception as ex:
                log.exception(ex)
