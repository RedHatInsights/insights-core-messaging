import logging
import pika
from retry import retry
from . import Consumer

log = logging.getLogger(__name__)


class RabbitMQ(Consumer):
    def __init__(
        self,
        publisher,
        downloader,
        engine,
        redis,
        queue,
        conn_params,
        auth=None,
        durable=False,
        prefetch_count=1,
    ):
        super().__init__(publisher, downloader, engine, redis)
        self.queue = queue
        self.prefetch_count = prefetch_count
        self.durable = durable

        creds = None if auth is None else pika.credentials.PlainCredentials(**auth)
        if creds is not None:
            conn_params["credentials"] = creds

        self.params = pika.ConnectionParameters(**conn_params)

    def open(self):
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue, durable=self.durable)
        self.channel.basic_qos(prefetch_count=self.prefetch_count)
        self.channel.basic_consume(queue=self.queue, on_message_callback=self._callback)

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

    @retry(pika.exceptions.AMQPConnectionError, delay=1, jitter=(1, 3))
    def run(self):
        try:
            self.open()
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
            self.connection.close()
        except pika.exceptions.ConnectionClosedByBroker:
            pass
