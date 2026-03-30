from contextvars import ContextVar
import logging
from insights import dr
from insights_messaging.watchers import Watched

log = logging.getLogger(__name__)
archive_context_var = ContextVar('archive_context_ids', default={})

class ArchiveContextIdsInjectingFilter(logging.Filter):
    """
    A filter which injects context-specific (inventory id, account id, request id) information into logs.
    """
    def filter(self, record):
        ids_dict = archive_context_var.get()
        for k, v in ids_dict.items():
            setattr(record, k, v)
        return True

class Requeue(Exception):
    """
    An Exception to mesasge a requeue request.
    """


class Consumer(Watched):
    def __init__(self, publisher, downloader, engine, requeuer=None):
        super().__init__()
        self.publisher = publisher
        self.downloader = downloader
        self.engine = engine
        self.requeuer = requeuer

    def run(self):
        raise NotImplementedError()

    def process(self, input_msg):
        broker = None
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
            # CCXDEV-15098: Break circular references to prevent memory leak.
            #
            # When a component raises during dr.run(), Python attaches the
            # traceback to the exception via ex.__traceback__. That traceback
            # holds a reference to the stack frame, which references local
            # variables — including the broker itself. This creates a circular
            # reference chain:
            #
            #   Broker -> exceptions dict -> exception -> __traceback__
            #          -> frame -> local vars -> Broker
            #
            # Because the broker's dicts keep these objects reachable, CPython's
            # reference-counting GC cannot free them. The cyclic GC can
            # eventually collect them, but in practice the broker and all its
            # data (instances dict with ~500 component results) stay alive far
            # longer than needed, causing steady memory growth (~8 MB/hr in
            # production for rules-processing).
            #
            # The fix:
            # 1. Clear ex.__traceback__ on every stored exception to sever the
            #    frame reference. The formatted traceback string is already
            #    saved in broker.tracebacks before this point, so no debugging
            #    information is lost.
            # 2. Clear the broker's large dicts (exceptions, tracebacks,
            #    instances) so the broker object becomes lightweight and can be
            #    collected promptly by reference counting.
            if broker is not None:
                for ex_list in broker.exceptions.values():
                    for ex in ex_list:
                        ex.__traceback__ = None
                broker.exceptions.clear()
                broker.tracebacks.clear()
                broker.instances.clear()

    def get_url(self, input_msg):
        raise NotImplementedError()

    def create_broker(self, input_msg):
        return dr.Broker()
