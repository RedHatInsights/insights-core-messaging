import logging
import logging.config
import os

import yaml

from insights import apply_configs, apply_default_enabled, dr

from .consumers.cli import Interactive
from .downloaders.localfs import LocalFS
from .engine import Engine
from .publishers.cli import StdOut
from .template import DefaultingTemplate as Template
from .watchers import ConsumerWatcher, EngineWatcher

Loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)

CLOWDER_ENABLED = os.environ.get('CLOWDER_ENABLED', False)

if CLOWDER_ENABLED:
    print("Clowder IS ENABLED")
    from app_common_python import LoadedConfig, KafkaTopics


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

    def _load_packages(self, pkgs):
        for p in pkgs:
            dr.load_components(p, continue_on_error=False)

    def _load_plugins(self):
        self._load_packages(self.plugins.get("packages", []))

    def _get_consumer_clowder(self, publisher, downloader, engine, redis=None):
        if "consumer" not in self.service:
            return Interactive(publisher, downloader, engine)
        spec = self.service["consumer"]
        Consumer = dr.get_component(spec["name"])
        if Consumer is None:
            raise Exception(f"Couldn't find {spec['name']}.")
        args = spec.get("args", {})
        kwargs = spec.get("kwargs", {})
        clowder_kwargs = {
            "bootstrap_servers": [
                f"{LoadedConfig.kafka.brokers[0].hostname}:{LoadedConfig.kafka.brokers[0].port}"
                ],
            "incoming_topic": KafkaTopics["platform.inventory.events"].name,
            #"group_id": KafkaTopics["platform.inventory.events"].consumerGroup
        }
        kwargs.update(clowder_kwargs)
        return Consumer(publisher, downloader, engine, redis=redis, *args, **kwargs)

    def _get_consumer(self, publisher, downloader, engine, redis=None):
        if "consumer" not in self.service:
            return Interactive(publisher, downloader, engine)
        spec = self.service["consumer"]
        Consumer = dr.get_component(spec["name"])
        if Consumer is None:
            raise Exception(f"Couldn't find {spec['name']}.")
        args = spec.get("args", [])
        kwargs = spec.get("kwargs", {})
        return Consumer(publisher, downloader, engine, redis=redis, *args, **kwargs)

    def _get_publisher_clowder(self):
        if "publisher" not in self.service:
            return StdOut()
        spec = self.service["publisher"]
        Publisher = dr.get_component(spec["name"])
        if Publisher is None:
            raise Exception(f"Couldn't find {spec['name']}.")
        args = spec.get("args", [])
        kwargs = spec.get("kwargs", {})
        clowder_kwargs = {
            "bootstrap_servers": [
                f"{LoadedConfig.kafka.brokers[0].hostname}:{LoadedConfig.kafka.brokers[0].port}"
                ],
            "topic": KafkaTopics["platform.engine.results"].name,
        }
        kwargs.update(clowder_kwargs)
        return Publisher(*args, **kwargs)

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

    def _get_graphs(self, target_components):
        graph = {}
        tc = tuple(target_components or [])
        if tc:
            for c in dr.DELEGATES:
                if dr.get_name(c).startswith(tc):
                    graph.update(dr.get_dependency_graph(c))
        return graph

    def _resolve_engine_config(self, config):
        return {
            "formatter": dr.get_component(config.get("format")),
            "target_components": self._get_graphs(config.get("target_components", [])),
            "extract_timeout": config.get("extract_timeout"),
            "extract_tmp_dir": config.get("extract_tmp_dir"),
        }

    def _get_engine(self):
        engine_config = self._resolve_engine_config(self.service)

        if "engine" not in self.service:
            return Engine(**engine_config)

        spec = self.service["engine"]
        kwargs = spec.get("kwargs", {})
        engine_config.update(self._resolve_engine_config(kwargs))

        EngineCls = dr.get_component(spec["name"])
        if EngineCls is None:
            raise Exception(f"Couldn't find {spec['name']}.")
        return EngineCls(**engine_config)

    def _get_redis(self):
        if "redis" in self.service:
            import redis
            cfg = self.service["redis"]
            hostname = cfg.get("hostname") if cfg.get("hostname") is not None else os.environ.get("REDIS_HOSTNAME")
            password = cfg.get("password") if cfg.get("password") is not None else os.environ.get("REDIS_PASSWORD")
            return redis.Redis(host=hostname,
                               port=cfg.get("port", 6379),
                               password=password,
                               decode_responses=cfg.get("decode_responses"))

    def _load(self, spec):
        if CLOWDER_ENABLED:
            return self._load_clowder(spec)
        else:
            return self._load_non_clowder(spec)

    def _load_non_clowder(self, spec):
        comp = dr.get_component(spec["name"])
        if comp is None:
            raise Exception(f"Couldn't find {spec['name']}.")
        args = spec.get("args", [])
        kwargs = spec.get("kwargs", {})
        return comp(*args, **kwargs)

    def _load_clowder(self, spec):
        comp = dr.get_component(spec["name"])
        if comp is None:
            raise Exception(f"Couldn't find {spec['name']}.")
        args = spec.get("args", [])
        kwargs = spec.get("kwargs", {})
        if spec.get("needs_kafka", False):
            clowder_kwargs = {
                "bootstrap_servers": [
                    f"{LoadedConfig.kafka.brokers[0].hostname}:{LoadedConfig.kafka.brokers[0].port}"
                    ],
                "topic": KafkaTopics["platform.payload-status"].name,
            }
            kwargs.update(clowder_kwargs)
        return comp(*args, **kwargs)

#platform.payload-status
    def _get_downloader(self):
        if "downloader" not in self.service:
            return LocalFS
        return self._load(self.service["downloader"])

    def _get_watchers(self):
        if "watchers" not in self.service:
            return []
        return [self._load(w) for w in self.service["watchers"]]

    def _get_log_config(self):
        return self.service.get("logging", {})

    def build_app(self):
        log_config = self._get_log_config()
        if log_config:
            if CLOWDER_ENABLED:
                cw_cfg = LoadedConfig.logging.cloudwatch
                from boto3.session import Session
                if "handlers" in log_config and "watchtower" in log_config["handlers"]:
                    log_config["handlers"]["watchtower"]["boto3_session"] = Session(
                        aws_access_key_id=cw_cfg.accessKeyId,
                        aws_secret_access_key=cw_cfg.secretAccessKey,
                        region_name=cw_cfg.region
                    )
                    log_config["handlers"]["watchtower"]["log_group"] = cw_cfg.logGroup
            logging.config.dictConfig(log_config)
        else:
            logging.basicConfig(level=logging.DEBUG)

        self._load_plugins()
        apply_default_enabled(self.plugins)
        apply_configs(self.plugins)

        downloader = self._get_downloader()
        engine = self._get_engine()
        if CLOWDER_ENABLED:
            publisher = self._get_publisher_clowder()
            redis = self._get_redis()
            consumer = self._get_consumer_clowder(publisher, downloader, engine, redis=redis)
        else:
            publisher = self._get_publisher()
            redis = self._get_redis()
            consumer = self._get_consumer(publisher, downloader, engine, redis=redis)

        for w in self._get_watchers():
            if isinstance(w, EngineWatcher):
                w.watch(engine)
            if isinstance(w, ConsumerWatcher):
                w.watch(consumer)

        return consumer
