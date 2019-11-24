from collections import defaultdict
from insights import dr


class StatWatcher(object):
    def __init__(self):
        self.scoreboard = defaultdict(int)

    def keep_score(self, comp, broker):
        print(f"Saw {dr.get_name(comp)}")

    def watch_broker(self, broker):
        broker.add_observer(self.keep_score)

    def watch_engine(self, engine):
        engine.add_watcher(self)

    def watch_service(self, service):
        service.add_watcher(self)

    def on_recv(self, msg):
        self.scoreboard = defaultdict(int)

    def on_download(self, path):
        pass

    def on_process_complete(self, broker):
        print(dict(self.scoreboard))

    def on_run_complete(self):
        pass
