import json
from kafka import KafkaConsumer


class Kafka(object):
    def __self__(self,
            downloader,
            engine,
            incoming_topic="",
            outgoing_topic="",
            bootstrap_servers="",
            group_id="",
            retry_backoff_ms=1000):

        self.downloader = downloader
        self.engine = engine
        self.consumer = KafkaConsumer(
            incoming_topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=self.deserialize,
            retry_backoff_ms=retry_backoff_ms,
        )

    @classmethod
    def deserialize(cls, bytes_):
        return json.loads(bytes_.decode("utf-8"))
