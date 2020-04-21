import logging
import logging.config
import os

import yaml

from insights import apply_configs, apply_default_enabled, dr
from insights.formats.text import HumanReadableFormat

from .consumers.cli import Interactive
from .downloaders.localfs import LocalFS
from .engine import Engine
from .publishers.cli import StdOut
from .template import DefaultingTemplate as Template
from .watchers import ConsumerWatcher, EngineWatcher

Loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)


def resolve_variables(v, env=os.environ):
    if isinstance(v, str):
        return Template(v).safe_substitute(env)

    if isinstance(v, dict):
        for k in v:
            v[k] = resolve_variables(v[k], env)
        return v

    if isinstance(v, list):
        return [resolve_variables(i, env) for i in v]

    return v


class AppBuilder:
    def __init__(self, manifest):
        if not isinstance(manifest, dict):
            manifest = yaml.load(manifest, Loader=Loader)

        manifest = resolve_variables(manifest)

        self.plugins = manifest.get("plugins", {})
        self.service = manifest.get("service", {})
        self.configs = manifest.get("configs", {})

    def _load_packages(self, pkgs):
        for p in pkgs:
            dr.load_components(p, continue_on_error=False)

    def _load_plugins(self):
        self._load_packages(self.plugins.get("packages", []))

    def _get_format(self):
        if "format" not in self.service:
            return HumanReadableFormat
        name = self.service["format"]
        fmt = dr.get_component(name)
        if fmt is None:
            raise Exception(f"Couldn't find {name}.")
        return fmt

    def _get_consumer(self, publisher, downloader, engine):
        if "consumer" not in self.service:
            return Interactive(publisher, downloader, engine)
        spec = self.service["consumer"]
        Consumer = dr.get_component(spec["name"])
        if Consumer is None:
            raise Exception(f"Couldn't find {spec['name']}.")
        args = spec.get("args", [])
        kwargs = spec.get("kwargs", {})
        return Consumer(publisher, downloader, engine, *args, **kwargs)

    def _get_publisher(self):
        if "publisher" not in self.service:
            return StdOut()
        spec = self.service["publisher"]
        Publisher = dr.get_component(spec["name"])
        if Publisher is None:
            raise Exception(f"Couldn't find {spec['name']}.")
        args = spec.get("args", [])
        kwargs = spec.get("kwargs", {})
        return Publisher(*args, **kwargs)

    def _load(self, spec):
        comp = dr.get_component(spec["name"])
        if comp is None:
            raise Exception(f"Couldn't find {spec['name']}.")
        args = spec.get("args", [])
        kwargs = spec.get("kwargs", {})
        return comp(*args, **kwargs)

    def _get_downloader(self):
        if "downloader" not in self.service:
            return LocalFS
        return self._load(self.service["downloader"])

    def _get_watchers(self):
        if "watchers" not in self.service:
            return []
        return [self._load(w) for w in self.service["watchers"]]

    def _get_target_components(self):
        tc = tuple(self.service.get("target_components", []))
        if not tc:
            return
        graph = {}
        for c in dr.DELEGATES:
            if dr.get_name(c).startswith(tc):
                graph.update(dr.get_dependency_graph(c))
        return graph or None

    def _get_extract_timeout(self):
        return self.service.get("extract_timeout")

    def _get_extract_tmp_dir(self):
        return self.service.get("extract_tmp_dir")

    def _get_engine(self, components, formatter, timeout=None, tmp_dir=None):
        cls = self._load(self.service["engine"]) if "engine" in self.service else Engine
        return cls(components, formatter, timeout=timeout, tmp_dir=tmp_dir)

    def _get_log_config(self):
        return self.service.get("logging", {})

    def build_app(self):
        log_config = self._get_log_config()
        if log_config:
            logging.config.dictConfig(log_config)
        else:
            logging.basicConfig(level=logging.DEBUG)

        self._load_plugins()
        apply_default_enabled(self.plugins)
        apply_configs(self.plugins)

        target_components = self._get_target_components()
        publisher = self._get_publisher()
        downloader = self._get_downloader()
        timeout = self._get_extract_timeout()
        tmp_dir = self._get_extract_tmp_dir()
        engine = self._get_engine(
            target_components, self._get_format(), timeout=timeout, tmp_dir=tmp_dir
        )
        consumer = self._get_consumer(publisher, downloader, engine)

        for w in self._get_watchers():
            if isinstance(w, EngineWatcher):
                w.watch(engine)
            if isinstance(w, ConsumerWatcher):
                w.watch(consumer)

        return consumer
