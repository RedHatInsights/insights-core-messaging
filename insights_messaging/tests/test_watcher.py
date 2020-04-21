from insights_messaging.watchers import Watched, Watcher


class Thing(Watched):

    def foo(self):
        self.fire("on_foo", self)

    def bar(self):
        # context for the "downloading" event
        with self.context_event("downloading", self):
            pass


class ThingWatcher(Watcher):

    def __init__(self):
        self.foo_fired = False
        self.downloading_fired = False

    def on_foo(self, thing):
        self.foo_fired = True

    # Wrap around the "downloading" event. Called when we enter the "downloading"
    # context in Thing.bar above.
    def downloading(self, thing):

        # we must yield control back to the main flow.
        # when we get control back, save any exception the main flow raised. It
        # gets sent back to us as the value of the yield.
        ex = yield
        assert ex is None

        # After the yield, we're exiting the context of the event.
        self.downloading_fired = True


def test_thingwatcher():
    thing = Thing()
    watcher = ThingWatcher()
    thing.add_watcher(watcher)

    thing.foo()
    thing.bar()

    assert watcher.foo_fired
    assert watcher.downloading_fired
