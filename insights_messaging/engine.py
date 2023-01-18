import logging
from io import StringIO

from insights.core import dr
from insights.core.archives import extract
from insights.core.hydration import initialize_broker
from insights.formats.text import HumanReadableFormat
from insights_messaging.watchers import Watched

log = logging.getLogger(__name__)


class Engine(Watched):
    def __init__(
        self,
        formatter,
        target_components=None,
        extract_timeout=None,
        extract_tmp_dir=None,
    ):
        super().__init__()
        self.Formatter = formatter or HumanReadableFormat
        self.components_dict = dr.determine_components(target_components or dr.COMPONENTS[dr.GROUPS.single])
        self.target_components = dr.toposort_flatten(self.components_dict, sort=False)
        self.extract_timeout = extract_timeout
        self.extract_tmp_dir = extract_tmp_dir

    def process(self, broker, path):
        for w in self.watchers:
            w.watch_broker(broker)

        result = None

        try:
            self.fire("pre_extract", broker, path)

            with extract(
                path, timeout=self.extract_timeout, extract_dir=self.extract_tmp_dir
            ) as extraction:
                ctx, broker = initialize_broker(extraction.tmp_dir, broker=broker)

                self.fire("on_extract", ctx, broker, extraction)

                output = StringIO()
                with self.Formatter(broker, stream=output):
                    dr.run_components(self.target_components, self.components_dict, broker=broker)
                output.seek(0)
                result = output.read()
                self.fire("on_engine_success", broker, result)
                return result
        except Exception as ex:
            self.fire("on_engine_failure", broker, ex)
            raise
        finally:
            self.fire("on_engine_complete", broker)
