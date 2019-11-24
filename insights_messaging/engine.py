from io import StringIO
from insights.core import dr
from insights.core.archives import extract
from insights.core.hydration import create_context
from insights.formats import Formatter
from insights_messaging.watcher import Watched


class Engine(Watched):
    def __init__(self, comps=None, Format=Formatter):
        super().__init__()
        self.comps = comps
        self.Format = Format

    def process(self, broker, path):
        for w in self.watchers:
            w.watch_broker(broker)

        try:
            with extract(path) as e:
                ctx = create_context(e.tmp_dir)
                broker[ctx.__class__] = ctx
                self.fire("on_archive_extract", ctx, broker, e)
                output = StringIO()
                with self.Format(broker, stream=output):
                    dr.run(self.comps, broker=broker)
                output.seek(0)
                return output.read()
        finally:
            self.fire("on_process_complete", broker)
