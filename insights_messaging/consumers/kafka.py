import logging

from confluent_kafka import Consumer as ConfluentConsumer
from prometheus_client import Counter, Summary

from insights_messaging.consumers import Consumer

log = logging.getLogger(__name__)

PREFIX = __name__.replace(".", "_")

POLL_TIME = Summary(f"{PREFIX}_poll_time", "Time spent waiting on consumer.poll()")
PROCESS_TIME = Summary(f"{PREFIX}_process_time", "Time spent in process()")
NO_MESSAGE = Counter(
    f"{PREFIX}_no_message", "Count of times consumer.poll() returned no message"
)
ERR_MESSAGE = Counter(
    f"{PREFIX}_err_message",
    "Count of times consumer.poll() returned a message with an error",
)
PROCESS_EXCEPTION = Counter(
    f"{PREFIX}_process_exception", "Count of times that process raised an exception"
)


class Kafka(Consumer):
    def __init__(
        self,
        publisher,
        downloader,
        engine,
        incoming_topic,
        group_id,
        bootstrap_servers,
        **kwargs,
    ):

        super().__init__(publisher, downloader, engine)
        config = kwargs.copy()
        config["group.id"] = group_id
        config["bootstrap.servers"] = ",".join(bootstrap_servers)
        log.info("config", extra={"config": config})

        self.auto_commit = kwargs.get("enable.auto.commit", True)
        self.consumer = ConfluentConsumer(config)

        self.consumer.subscribe([incoming_topic])
        log.info("subscribing to %s: %s", incoming_topic, self.consumer)

    def deserialize(self, bytes_):
        raise NotImplementedError()

    def handles(self, input_msg):
        return True

    @POLL_TIME.time()
    def _poll(self):
        return self.consumer.poll(1)

    @PROCESS_TIME.time()
    def _process(self, payload):
        self.process(payload)

    def run(self):
        while True:
            msg = self._poll()
            if msg is None:
                NO_MESSAGE.inc()
                continue

            err = msg.error()
            if err is not None:
                # TODO: Should msg be committed?
                ERR_MESSAGE.inc()
                log.exception(err)
                continue

            val = msg.value()
            if val is not None:
                try:
                    payload = self.deserialize(val)
                    if self.handles(payload):
                        self._process(payload)
                except Exception as ex:
                    PROCESS_EXCEPTION.inc()
                    log.exception(ex)
                finally:
                    if not self.auto_commit:
                        self.consumer.commit(msg)
