import collections
import gc

import pytest

from bloop.models import BaseModel, Column
from bloop.types import Integer
from bloop.util import (
    Sentinel,
    WeakDefaultDictionary,
    index,
    ordered,
    walk_subclasses,
)


def test_index():
    """Index by each object's value for an attribute"""
    class Person:
        def __init__(self, name):
            self.name = name

    p1, p2, p3 = Person("foo"), Person("bar"), Person("baz")
    assert index([p1, p2, p3], "name") == {
        "foo": p1,
        "bar": p2,
        "baz": p3
    }


@pytest.mark.parametrize("obj", [None, object(), 2, False, "abc"])
def test_ordered_basic_objects(obj):
    """Things that don't need to be unpacked or flattened for comparison"""
    assert ordered(obj) is obj


@pytest.mark.parametrize("it", [
    iter(list("bac")),
    ["b", "c", "a"],
    ("c", "a", "b"),
    (x for x in "cba"),
    {"a", "c", "b"}
])
def test_ordered_iterable(it):
    """Any non-mapping iterable is sorted, even if it's consumable"""
    expected = ["a", "b", "c"]
    assert ordered(it) == expected


@pytest.mark.parametrize("mapping", [
    {"b": True, "a": "zebra", "c": None},
    collections.OrderedDict([("c", None), ("b", True), ("a", "zebra")])
])
def test_ordered_mapping(mapping):
    """Mappings are flattened into (key, value) tuples and then those tuples are sorted"""
    expected = [
        ("a", "zebra"),
        ("b", True),
        ("c", None)
    ]
    assert ordered(mapping) == expected


@pytest.mark.parametrize("obj, expected", [
    # mapping int -> set(str)
    ({3: {"a", "b"}, 2: {"c", "b"}, 1: {"a", "c"}}, [(1, ["a", "c"]), (2, ["b", "c"]), (3, ["a", "b"])]),
    # mapping str -> list(int)
    ({"b": [1, 2], "a": [3, 2], "c": [1, 3]}, [("a", [2, 3]), ("b", [1, 2]), ("c", [1, 3])]),
    # list(set(bool))
    ([{False}, {True}], [[False], [True]]),
])
def test_ordered_recursion(obj, expected):
    """Mappings and iterables inside each other are sorted and flattened"""
    assert ordered(obj) == expected


def test_walk_subclasses():
    class A:
        pass

    class B:  # Not included
        pass

    class C(A):
        pass

    class D(A):
        pass

    class E(C, A):  # would be visited twice without dedupe
        pass

    class F(D, A):  # would be visited twice without dedupe
        pass

    # list instead of set ensures we don't false succeed on duplicates
    subclasses = sorted(walk_subclasses(A), key=lambda c: c.__name__)
    assert subclasses == [C, D, E, F]


def test_sentinel_uniqueness():
    sentinel = Sentinel("name")
    same_sentinel = Sentinel("NAME")
    assert sentinel is same_sentinel


def test_sentinel_repr():
    foo = Sentinel("foo")
    assert repr(foo) == "<Sentinel[foo]>"


def test_weakref_default_dict():
    """Provides defaultdict behavior for a WeakKeyDictionary"""
    class MyModel(BaseModel):
        id = Column(Integer, hash_key=True)
        data = Column(Integer)

    def new(i):
        obj = MyModel(id=i, data=2 * i)
        return obj

    weak_dict = WeakDefaultDictionary(lambda: {"foo": "bar"})

    n_objs = 10
    objs = [new(i) for i in range(n_objs)]

    for obj in objs:
        # default_factory is called
        assert weak_dict[obj] == {"foo": "bar"}
    # don't keep a reference to the last obj, throws off the count below
    del obj

    calls = 0
    while weak_dict:
        del objs[0]
        gc.collect()
        calls += 1
        assert len(weak_dict) == len(objs)
    assert calls == n_objs
