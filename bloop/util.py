import collections.abc
import weakref

import blinker


__all__ = ["signal"]

# De-dupe dict for Sentinel
_symbols = {}

# Isolate to avoid collisions with other modules.
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
    # Special case str since it's a collections.abc.Iterable
    elif isinstance(obj, str):
        return obj
    elif isinstance(obj, collections.abc.Iterable):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


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

    for column in expected:
        value = attrs.get(column.dynamo_name, None)
        value = engine._load(column.typedef, value, context=context, **kwargs)
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
    """Simple string-based placeholders for missing or special values.

    Names are unique, and instances are re-used for the same name:

    .. code-block:: pycon

        >>> from bloop.util import Sentinel
        >>> empty = Sentinel("empty")
        >>> empty
        <Sentinel[empty]>
        >>> same_token = Sentinel("empty")
        >>> empty is same_token
        True

    This removes the need to import the same signal or placeholder value everywhere; two modules can create
    ``Sentinel("some-value")`` and refer to the same object.  This is especially helpful where ``None`` is a possible
    value, and so can't be used to indicate omission of an optional parameter.

    Implements \_\_repr\_\_ to render nicely in function signatures.  Standard object-based sentinels:

    .. code-block:: pycon

        >>> missing = object()
        >>> def some_func(optional=missing):
        ...     pass
        ...
        >>> help(some_func)
        Help on function some_func in module __main__:

        some_func(optional=<object object at 0x7f0f3f29e5d0>)

    With the Sentinel class:

    .. code-block:: pycon

        >>> from bloop.util import Sentinel
        >>> missing = Sentinel("Missing")
        >>> def some_func(optional=missing):
        ...     pass
        ...
        >>> help(some_func)
        Help on function some_func in module __main__:

        some_func(optional=<Sentinel[Missing]>)

    :param str name: The name for this sentinel.
    """
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
    """The cross product of :class:`weakref.WeakKeyDictionary` and :class:`collections.defaultdict`."""
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

    __iter__ = weakref.WeakKeyDictionary.__iter__

missing = Sentinel("missing")
