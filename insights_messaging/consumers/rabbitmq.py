import logging
from . import Consumer

log = logging.getLogger(__name__)


class RabbitMQ(Consumer):
    def __init__(self, publisher, downloader, engine):
        super().__init__(publisher, downloader, engine)

    def run(self):
        pass

    def get_url(self, input_msg):
        return input_msg["url"]
