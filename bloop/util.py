import collections.abc
import weakref

import blinker


__all__ = ["Sentinel", "ordered", "signal", "walk_subclasses"]

# De-dupe dict for Sentinel
_symbols = {}

# Isolate to avoid collisions with other modules.
# Don't expose the namespace.
__signals = blinker.Namespace()
signal = __signals.signal


def index(objects, attr):
    """
    Generate a mapping of a list of objects indexed by the given attr.

    Parameters
    ----------
    objects : :class:`list`, iterable
    attr : string
        The attribute to index the list of objects by

    Returns
    -------
    dictionary : dict
        keys are the value of each object's attr, and values are from objects

    Example
    -------

    class Person(object):
        def __init__(self, name, email, age):
            self.name = name
            self.email = email
            self.age = age

    people = [
        Person('one', 'one@people.com', 1),
        Person('two', 'two@people.com', 2),
        Person('three', 'three@people.com', 3)
    ]

    by_email = index(people, 'email')
    by_name = index(people, 'name')

    assert by_name['one'] is people[0]
    assert by_email['two@people.com'] is people[1]

    """
    return {getattr(obj, attr): obj for obj in objects}


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


def walk_subclasses(root):
    """Does not yield the input class"""
    classes = [root]
    visited = set()
    while classes:
        cls = classes.pop()
        if cls is type or cls in visited:
            continue
        classes.extend(cls.__subclasses__())
        visited.add(cls)
        if cls is not root:
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
