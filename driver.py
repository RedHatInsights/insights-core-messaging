#!/usr/bin/env python
import yaml

from insights import dr, apply_default_enabled, apply_configs
from insights.formats._json import JsonFormat
from insights_messaging.downloaders.localfs import LocalFS
from insights_messaging.engine import Engine
from insights_messaging.services.cli import Interactive


class AppBuilder(object):
    default_manifest = """
    plugins:
        default_component_enabled: true
        packages:
            - insights.specs.default
            - insights.specs.insights_archive
            - examples.rules.bash_version
    configs:
        - name: examples.rules.bash_version.report
          enabled: true
    service:
        format: insights.formats.text.HumanReadableFormat
        downloader:
            name: insights_messaging.downloaders.localfs.LocalFS
        service:
            name: insights_messaging.services.cli.Interactive
        watchers: []
        target_components:
            - examples.rules.bash_version.report
    """

    def __init__(self, manifest=None):
        if manifest is None:
            manifest = self.default_manifest
        if not isinstance(manifest, dict):
            manifest = yaml.load(manifest, Loader=yaml.CSafeLoader)
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
            return JsonFormat
        return dr.get_component(self.service["format"])

    def _get_service(self, downloader, engine):
        if "service" not in self.service:
            return Interactive(downloader, engine)
        spec = self.service["service"]
        Service = dr.get_component(spec["name"])
        args = spec.get("args", [])
        kwargs = spec.get("kwargs", {})
        return Service(downloader, engine, *args, **kwargs)

    def _load(self, spec):
        comp = dr.get_component(spec["name"])
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

    def build_app(self):
        self._load_plugins()
        apply_default_enabled(self.plugins)
        apply_configs(self.plugins)

        downloader = self._get_downloader()
        target_components = self._get_target_components()
        engine = Engine(target_components, self._get_format())
        service = self._get_service(downloader, engine)

        for w in self._get_watchers():
            if hasattr(w, "watch_engine"):
                w.watch_engine(engine)
            if hasattr(w, "watch_service"):
                w.watch_service(service)

        return service

if __name__ == "__main__":
    AppBuilder().build_app().run()
