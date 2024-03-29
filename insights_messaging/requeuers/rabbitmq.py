import logging
import pika
from . import Requeuer
from utils import retry

log = logging.getLogger(__name__)


class RabbitMQ(Requeuer):
    def __init__(
        self, queue, conn_params, exchange="", auth=None, durable=False,
    ):
        self.queue = queue
        self.exchange = exchange
        self.durable = durable
        self.properties = (
            pika.BasicProperties(delivery_mode=2) if self.durable else None
        )

        creds = None if auth is None else pika.credentials.PlainCredentials(**auth)
        if creds is not None:
            conn_params["credentials"] = creds
        self.params = pika.ConnectionParameters(**conn_params)
        self.open()

    def open(self):
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue, durable=self.durable)

    def send(self, msg):
        if not self.connection.is_open:
            self.open()
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.queue,
            properties=self.properties,
            body=msg,
        )

    @retry
    def requeue(self, msg):
        try:
            self.send(msg)
        except pika.exceptions.ConnectionClosedByBroker:
            pass
