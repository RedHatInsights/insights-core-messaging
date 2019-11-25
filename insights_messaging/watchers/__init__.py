import logging

log = logging.getLogger(__name__)


class Watched(object):
    def __init__(self):
        self.watchers = []

    def add_watcher(self, w):
        self.watchers.append(w)

    def fire(self, event, *args):
        for w in self.watchers:
            try:
                func = getattr(w, event, None)
                if func is not None:
                    func(*args)
            except Exception as ex:
                log.exception(ex)


class Watcher(object):
    def watch(self, watched):
        watched.add_watcher(self)


class EngineWatcher(Watcher):
    def watch_broker(self, broker):
        pass

    def pre_extract(self, broker, path):
        pass

    def on_extract(self, ctx, broker, extraction):
        pass

    def on_engine_failure(self, broker):
        pass

    def on_engine_complete(self, broker):
        pass


class ConsumerWatcher(Watcher):
    def on_recv(self, input_msg):
        pass

    def on_download(self, path):
        pass

    def on_consumer_success(self, input_msg, broker, response):
        pass

    def on_consumer_failure(self, input_msg, exception):
        pass

    def on_consumer_complete(self, input_msg):
        pass
