import traceback

from insights import dr
from insights_messaging.watcher import Watched


class Interactive(Watched):
    def __init__(self, downloader, engine):
        super().__init__()
        self.downloader = downloader
        self.engine = engine

    def run(self):
        while True:
            archive = input("Input Archive Name: ")
            if not archive:
                break
            try:
                self.fire("on_recv", archive)
                with self.downloader.get(archive) as path:
                    self.fire("on_download", path)
                    broker = dr.Broker()
                    results = self.engine.process(broker, path)
                    print(results)
                    self.fire("on_run_success", archive, results)
            except Exception as ex:
                traceback.print_exc()
                self.fire("on_run_failure", archive, ex)
            finally:
                self.fire("on_run_complete")
