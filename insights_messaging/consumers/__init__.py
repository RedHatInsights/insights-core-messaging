import logging
from insights import dr
from insights_messaging.watchers import Watched


log = logging.getLogger(__name__)


class Consumer(Watched):
    def __init__(self, publisher, downloader, engine):
        super().__init__()
        self.publisher = publisher
        self.downloader = downloader
        self.engine = engine

    def run(self):
        raise NotImplementedError()

    def process(self, input_msg):
        try:
            self.fire("on_recv", input_msg)

            url = self.get_url(input_msg)
            log.debug("Downloading %s", url)
            with self.downloader.get(url) as path:
                log.debug("Saved %s to %s", url, path)
                self.fire("on_download", path)

                broker = self.create_broker(input_msg)
                results = self.engine.process(broker, path)
                self.fire("on_process", input_msg, results)

                self.publisher.publish(input_msg, results)
                self.fire("on_consumer_success", input_msg, broker, results)
        except Exception as ex:
            self.publisher.error(input_msg, ex)
            self.fire("on_consumer_failure", input_msg, ex)
            raise
        finally:
            self.fire("on_consumer_complete", input_msg)

    def get_url(self, input_msg):
        raise NotImplementedError()

    def create_broker(self, input_msg):
        return dr.Broker()
