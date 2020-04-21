import inspect
import logging

from contextlib import contextmanager

log = logging.getLogger(__name__)


class Watched:
    """
    Base class for subclasses that support notifying a list of event watchers.
    """

    def __init__(self):
        self.watchers = []

    def add_watcher(self, w):
        self.watchers.append(w)

    def fire(self, event, *args, **kwargs):
        for w in self.watchers:
            try:
                func = getattr(w, event)
                func(*args, **kwargs)
            except AttributeError:
                pass
            except Exception as ex:
                log.exception(ex)

    @contextmanager
    def context_event(self, name, *args, **kwargs):
        # Run the watch functions up to their yields. Do this is reverse so the
        # first thing registered is the last thing called and therefore closest
        # to the watched code.
        alive = []
        for watcher in reversed(self.watchers):
            try:
                func = getattr(watcher, name)
                if inspect.isgeneratorfunction(func):
                    gen = func(*args, **kwargs)
                    next(gen)
                    alive.append(gen)
                else:
                    log.warn(f"{func} should yield.")
            except AttributeError:
                pass
            except Exception as ex:
                log.exception(ex)

        # Give control back to the caller. If it raises an exception, pass
        # that exception along to all watchers.
        ex = None
        try:
            yield
        except Exception as e:
            ex = e
            raise
        finally:
            # Allow the watch functions to complete. The last thing in
            # self.watchers will be the first thing in alive since we reversed
            # the watcher list when running them above.
            for gen in alive:
                try:
                    gen.send(ex)
                except StopIteration:
                    pass
                except Exception as ex:
                    log.exception(ex)


class Watcher:
    """
    Watchers should implement functions named the same as events they care
    about.
    """

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
