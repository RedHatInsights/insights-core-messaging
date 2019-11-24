
_registry = {}


def register(name):
    def inner(factory):
        _registry[name] = factory
        return factory
    return inner


def lookup(name):
    return _registry[name]
