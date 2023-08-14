import logging
import os

from confluent_kafka import Consumer as ConfluentConsumer
from insights_messaging.consumers import Consumer, Requeue, archive_context_var

log = logging.getLogger(__name__)

def update_archive_context_ids(payload):
    if payload and "platform_metadata" in payload and 'host' in payload:
        payload_id_dict = {}
        if 'request_id' in payload["platform_metadata"]:
            payload_id_dict['request_id'] = payload["platform_metadata"].get("request_id")
        if 'id' in payload["host"]:
            payload_id_dict['inventory_id'] = payload["host"].get("id")
        archive_context_var.set(payload_id_dict)

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
                    # set the context var according payload info
                    update_archive_context_ids(payload)
                    if self.handles(payload):
                        log.info("Start to process one payload")
                        self.process(payload)
                        log.info("Completed one payload")
                except Requeue as req:
                    if not self.requeuer:
                        raise Exception("Requeue request with no requeuer configured.")
                    self.requeuer.requeue(val, req)
                except Exception as ex:
                    log.exception(ex)
                finally:
                    archive_context_var.set({})  # reset the contextvar
                    if not self.auto_commit:
                        self.consumer.commit(msg)
