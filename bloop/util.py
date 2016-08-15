import blinker
import weakref

__all__ = ["Sentinel", "WeakDefaultDictionary", "missing", "ordered", "signal", "walk_subclasses"]

# Isolate to avoid collisions with other modules
# Don't expose the namespace.
__signals = blinker.Namespace()
signal = __signals.signal


def ordered(obj):
    """
    Return sorted version of nested dicts/lists for comparing.

    http://stackoverflow.com/a/25851972
    """
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


def walk_subclasses(cls):
    classes = {cls}
    visited = set()
    while classes:
        cls = classes.pop()
        # Testing this branch would require checking walk_subclass(object)
        if cls is not type:  # pragma: no branch
            classes.update(cls.__subclasses__())
            visited.add(cls)
            yield cls


class WeakDefaultDictionary(weakref.WeakKeyDictionary):
    def __init__(self, default_factory):
        self.default_factory = default_factory
        super().__init__()

    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        self[key] = value = self.default_factory()
        return value

_symbols = {}


class Sentinel:
    def __new__(cls, name, *args, **kwargs):
        name = name.lower()
        sentinel = _symbols.get(name, None)
        if sentinel is None:
            sentinel = _symbols[name] = super().__new__(cls)
        return sentinel

    def __init__(self, name):
        self.name = name

    def __repr__(self):  # pragma: no cover
        return "<Sentinel({})>".format(self.name)

missing = Sentinel("missing")
