import logging
import pika
from . import Consumer

log = logging.getLogger(__name__)


class RabbitMQ(Consumer):
    def __init__(self, publisher, downloader, engine, queue, conn_params,
                 auth=None, durable=False, prefetch_count=1):
        super().__init__(publisher, downloader, engine)

        if auth is not None:
            creds = pika.credentials.PlainCredentials(**auth)
        else:
            creds = None

        if creds is not None:
            conn_params["credentials"] = creds

        params = pika.ConnectionParameters(**conn_params)
        connection = pika.BlockingConnection(params)

        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=durable)
        channel.basic_qos(prefetch_count=prefetch_count)
        channel.basic_consume(queue=queue, on_message_callback=self._callback)
        self.channel = channel

    def _callback(self, ch, method, properties, body):
        input_msg = self.deserialize(body)
        log.debug("recv", extra=input_msg)
        try:
            self.process(input_msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as ex:
            log.exception(ex)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def deserialize(self, bytes_):
        raise NotImplementedError()

    def get_url(self, input_msg):
        raise NotImplementedError()

    def run(self):
        self.channel.start_consuming()
