import attr
import json

from insights import condition, incident, parser, rule
from insights.core import dr
from insights.parsers.redhat_release import RedhatRelease
from insights.formats import Formatter


@attr.s
class StatResponse:
    name = attr.ib(default=None)
    component = attr.ib(default=None)
    fired = attr.ib(default=False)
    value = attr.ib(default=None)
    errors = attr.ib(default=[])
    response_type = attr.ib(default=None)


class Stats(Formatter):
    def __init__(self, broker, **kwargs):
        super().__init__(broker, **kwargs)
        self.product = None
        self.major = None
        self.minor = None
        self.results = []

    def get_system_info(self, comp, broker):
        if comp is RedhatRelease and comp in broker:
            rel = broker[comp]
            self.product = rel.product
            self.major = rel.major
            self.minor = rel.minor

    def rule_res(self, val, res):
        res.response_type = val.response_type
        if val.response_type != "skip":
            res.fired = True
            res.value = val.get_key()

    def bool_res(self, val, res):
        res.fired = True
        res.value = None if val is None else bool(val)

    def collector(self, stat_filler):
        def inner(comp, broker):
            ct = dr.get_delegate(comp).__class__.__name__
            res = StatResponse(name=dr.get_name(comp), component=ct)
            if comp in broker:
                stat_filler(broker[comp], res)
            elif comp in broker.exceptions:
                for e in broker.exceptions[comp]:
                    res.errors.append(broker.tracebacks[e])
            self.results.append(attr.asdict(res))
        return inner

    def get_response(self):
        return {
            "system": {
                "product": self.product,
                "major": self.major,
                "minor": self.minor,
            },
            "results": self.results
        }

    def preprocess(self):
        self.broker.add_observer(self.collector(self.rule_res), rule)
        self.broker.add_observer(self.collector(self.bool_res), condition)
        self.broker.add_observer(self.collector(self.bool_res), incident)
        self.broker.add_observer(self.get_system_info, parser)

    def postprocess(self):
        json.dump(self.get_response(), self.stream)
