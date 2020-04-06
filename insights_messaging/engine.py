import logging
import time
from io import StringIO

from insights.core import dr
from insights.core.archives import extract
from insights.core.hydration import create_context
from insights.formats import Formatter
from prometheus_client import Summary

from insights_messaging.watchers import Watched

log = logging.getLogger(__name__)

PREFIX = __name__.replace(".", "_")

EXTRACTION_TIME = Summary(f"{PREFIX}_extraction_time", "Time spent extracting")
RUN_TIME = Summary(f"{PREFIX}_run_time", "Time spent executing rule processing")


class Engine(Watched):
    def __init__(self, comps=None, Format=Formatter, timeout=None, tmp_dir=None):
        super().__init__()
        self.comps = comps
        self.Format = Format
        self.timeout = timeout
        self.tmp_dir = tmp_dir

    def process(self, broker, path):
        for w in self.watchers:
            w.watch_broker(broker)

        result = None

        try:
            self.fire("pre_extract", broker, path)

            start = time.time()
            with extract(
                path, timeout=self.timeout, extract_dir=self.tmp_dir
            ) as extraction:
                EXTRACTION_TIME.observe(time.time() - start)
                ctx = create_context(extraction.tmp_dir)
                broker[ctx.__class__] = ctx

                self.fire("on_extract", ctx, broker, extraction)

                output = StringIO()
                with self.Format(broker, stream=output):
                    start = time.time()
                    dr.run(self.comps, broker=broker)
                    RUN_TIME.observe(time.time() - start)
                output.seek(0)
                result = output.read()
                self.fire("on_engine_success", broker, result)
                return result
        except Exception as ex:
            self.fire("on_engine_failure", broker, ex)
            raise
        finally:
            self.fire("on_engine_complete", broker)
