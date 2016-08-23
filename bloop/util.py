import collections.abc
import weakref

import blinker


__all__ = [
    "Sentinel",
    "WeakDefaultDictionary",
    "missing",
    "ordered",
    "printable_column_name",
    "printable_query",
    "signal",
    "walk_subclasses",
    "unpack_from_dynamodb"
]

# De-dupe dict for Sentinel
_symbols = {}

# Isolate to avoid collisions with other modules
# Don't expose the namespace.
__signals = blinker.Namespace()
signal = __signals.signal


def ordered(obj):
    """
    Return sorted version of nested dicts/lists for comparing.

    Modified from:
    http://stackoverflow.com/a/25851972
    """
    if isinstance(obj, collections.abc.Mapping):
        return sorted((k, ordered(v)) for k, v in obj.items())
    # Special case str since it's a collections.abc.Iterable,
    # and causes infinite recursion
    elif isinstance(obj, str):
        return obj
    elif isinstance(obj, collections.abc.Iterable):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


def printable_column_name(column, path=None):
    """Provided for debug output when rendering conditions"""
    model_name = column.model.__name__
    name = "{}.{}".format(model_name, column.model_name)
    pieces = [name]
    for segment in (path or []):
        if isinstance(segment, str):
            pieces.append(segment)
        else:
            pieces[-1] += "[{}]".format(segment)
    return ".".join(pieces)


def printable_query(query_on):
    # Model.Meta -> Model
    if getattr(query_on, "__name__", "") == "Meta":
        return query_on.model
    # Index -> Index
    return query_on


def unpack_from_dynamodb(*, attrs, expected, model=None, obj=None, engine=None, context=None, **kwargs):
    """Push values by dynamo_name into an object"""
    context = context or {"engine": engine}
    engine = engine or context.get("engine", None)
    if not engine:
        raise ValueError("You must provide engine or a context with an engine.")
    if model is None and obj is None:
        raise ValueError("You must provide a model or obj to unpack.")
    if model is not None and obj is not None:
        raise ValueError("Only specify model or obj.")
    if model:
        obj = model.Meta.init()

    load = context["engine"]._load
    for column in expected:
        value = attrs.get(column.dynamo_name, None)
        value = load(column.typedef, value, context=context, **kwargs)
        setattr(obj, column.model_name, value)
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


class Sentinel:
    def __new__(cls, name, *args, **kwargs):
        name = name.lower()
        sentinel = _symbols.get(name, None)
        if sentinel is None:
            sentinel = _symbols[name] = super().__new__(cls)
        return sentinel

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<Sentinel[{}]>".format(self.name)


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

missing = Sentinel("missing")
