import logging
from io import StringIO
from pathlib import Path

from insights.core import dr
from insights.core.archives import extract
from insights.core.hydration import initialize_broker
from insights.formats.text import HumanReadableFormat
from insights_messaging.watchers import Watched

logger = logging.getLogger(__name__)

class Engine(Watched):
    def __init__(
        self,
        formatter,
        target_components=None,
        extract_timeout=None,
        unpacked_archive_size_limit=None,
        extract_tmp_dir=None,
    ):
        super().__init__()
        self.Formatter = formatter or HumanReadableFormat
        self.components_dict = dr.determine_components(target_components or dr.COMPONENTS[dr.GROUPS.single])
        self.target_components = dr.toposort_flatten(self.components_dict, sort=False)
        self.extract_timeout = extract_timeout
        # if there is no limit setting in config.yaml, set default limit to 4G
        self.unpacked_archive_size_limit = unpacked_archive_size_limit if unpacked_archive_size_limit else 4000000000
        self.extract_tmp_dir = extract_tmp_dir

    def validate_size(self, i_path):
        """
        reject payloads where the extracted size exceeds the configured max
        """
        total_size = sum(p.stat().st_size for p in Path(i_path).rglob('*'))
        if total_size >= self.unpacked_archive_size_limit:
            logger.warning("Unpacked archive exceeds extracted file size limit of {}".format(self.unpacked_archive_size_limit))
            return False
        return True

    def process(self, broker, path):
        for w in self.watchers:
            w.watch_broker(broker)

        result = None

        try:
            self.fire("pre_extract", broker, path)
            with extract(
                path, timeout=self.extract_timeout, extract_dir=self.extract_tmp_dir
            ) as extraction:
                if self.validate_size(extraction.tmp_dir):
                    ctx, broker = initialize_broker(extraction.tmp_dir, broker=broker)
                    self.fire("on_extract", ctx, broker, extraction)
                    output = StringIO()
                    with self.Formatter(broker, stream=output):
                        dr.run_components(self.target_components, self.components_dict, broker=broker)
                    output.seek(0)
                    result = output.read()
                    self.fire("on_engine_success", broker, result)
                    return result
                else:
                    raise Exception("Unpacked archive exceeds the size limit {}".format(self.unpacked_archive_size_limit))
        except Exception as ex:
            self.fire("on_engine_failure", broker, ex)
            raise ex
        finally:
            self.fire("on_engine_complete", broker)
