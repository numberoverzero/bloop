import collections.abc

from .exceptions import MissingKey


__all__ = [
    "Sentinel",
    "dump_key", "extract_key", "get_table_name",
    "index_for", "missing", "ordered",
    "value_of", "walk_subclasses",
]

# De-dupe dict for Sentinel
_symbols = {}


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


def value_of(column):
    """value_of({'S': 'Space Invaders'}) -> 'Space Invaders'"""
    return next(iter(column.values()))


def index_for(key):
    """stable hashable tuple of object keys for indexing an item in constant time.

    usage::

        index_for({'id': {'S': 'foo'}, 'range': {'S': 'bar'}}) -> ('bar', 'foo')
    """
    return tuple(sorted(value_of(k) for k in key.values()))


def extract_key(key_shape, item):
    """
    construct a key according to key_shape for building an index

    usage::

        key_shape = "foo", "bar"
        item = {"baz": 1, "bar": 2, "foo": 3}
        extract_key(key_shape, item) -> {"foo": 3, "bar": 2}
    """
    return {field: item[field] for field in key_shape}


def dump_key(engine, obj):
    """dump the hash (and range, if there is one) key(s) of an object into
    a dynamo-friendly format.

    returns {dynamo_name: {type: value} for dynamo_name in hash/range keys}
    """
    key = {}
    for key_column in obj.Meta.keys:
        key_value = getattr(obj, key_column.name, missing)
        if key_value is missing:
            raise MissingKey("{!r} is missing {}: {!r}".format(
                obj, "hash_key" if key_column.hash_key else "range_key",
                key_column.name
            ))
        # noinspection PyProtectedMember
        key_value = engine._dump(key_column.typedef, key_value)
        key[key_column.dynamo_name] = key_value
    return key


def get_table_name(engine, obj):
    """return the table name for an object as seen by a given engine"""
    # noinspection PyProtectedMember
    return engine._compute_table_name(obj.__class__)


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

    Implements ``__repr__`` to render nicely in function signatures.  Standard object-based sentinels:

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


missing = Sentinel("missing")
