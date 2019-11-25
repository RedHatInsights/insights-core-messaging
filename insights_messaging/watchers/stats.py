from pprint import pprint
from insights import dr
from insights_messaging.watcher import EngineWatcher


class LocalStatWatcher(EngineWatcher):
    def __init__(self):
        self.archives = 0

    def on_engine_complete(self, broker):
        self.archives += 1
        times = {dr.get_name(k): v for k, v in broker.exec_times.items() if k in broker}
        pprint({"times": times, "archives": self.archives})
