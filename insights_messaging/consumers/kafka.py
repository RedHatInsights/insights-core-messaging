import logging

from confluent_kafka import Consumer as ConfluentConsumer
from insights_messaging.consumers import Consumer

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
        **kwargs
    ):

        super().__init__(publisher, downloader, engine)
        config = kwargs.copy()
        config["group.id"] = group_id
        config["bootstrap.servers"] = ",".join(bootstrap_servers)
        log.info("config", extra={"config": config})

        self.auto_commit = kwargs.get("enable.auto.commit", True)
        self.consumer = ConfluentConsumer(config)

        if type(incoming_topic) == list:
            self.consumer.subscribe(incoming_topic)
            log.info("subscribing to topics with consumer %s", self.consumer)
            for topic in incoming_topic:
                log.info(" - %s", topic)
        else:   
            self.consumer.subscribe([incoming_topic])
            log.info("subscribing to %s: %s", incoming_topic, self.consumer)

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
                # TODO: Should msg be committed?
                log.exception(err)
                continue

            val = msg.value()
            if val is not None:
                try:
                    payload = self.deserialize(val)
                    if self.handles(payload):
                        self.process(payload)
                except Exception as ex:
                    log.exception(ex)
                finally:
                    if not self.auto_commit:
                        self.consumer.commit(msg)
