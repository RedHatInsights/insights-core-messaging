import logging

log = logging.getLogger(__name__)


class Watched:
    """
    Generic base class for subclasses that support notifying a list of event
    watchers.
    """

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


class Watcher:
    def watch(self, watched):
        watched.add_watcher(self)


class EngineWatcher(Watcher):
    def watch_broker(self, broker):
        """
        Used to give the watcher an opportunity to register Broker observers
        before analysis.
        """

    def pre_extract(self, broker, archive):
        """
        Fired before the archive is extracted.
        """

    def on_extract(self, ctx, broker, extraction):
        """
        Fired just after the archive is extracted but before any analysis.
        """

    def on_engine_failure(self, broker, ex):
        """
        Fired if any exceptions occur during archive identification, extraction,
        or result formatting. Exceptions raised by insights components are
        not intercepted by this function. Use :py:method:`watch_broker` to
        register callbacks on the insights Broker to look for those kinds of
        errors.
        """

    def on_engine_complete(self, broker):
        """
        Fired when an analysis is complete regardless of outcome.
        """


class ConsumerWatcher(Watcher):
    def on_recv(self, input_msg):
        """
        Fired after the consumer receives a message to process.
        """

    def on_download(self, path):
        """
        Fired after the consumer downloads an archive to path.
        """

    def on_pre_process(self, input_msg):
        """
        Fired before the consumer processes downloaded data.
        """

    def on_process(self, input_msg, results):
        """
        Fired after the consumer finishes processing downloaded data.
        """

    def on_consumer_success(self, input_msg, broker, results):
        """
        Fired after an archive was successfully processed.
        """

    def on_consumer_failure(self, input_msg, exception):
        """
        Fired after an archive fails processing.
        """

    def on_consumer_complete(self, input_msg):
        """
        Fired after an archive processes regardless of outcome.
        """
