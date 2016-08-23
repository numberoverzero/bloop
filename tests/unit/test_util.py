import collections
import gc

import pytest
from bloop.util import (
    Sentinel,
    WeakDefaultDictionary,
    ordered,
    printable_column_name,
    printable_query,
    unpack_from_dynamodb,
    walk_subclasses,
)

from ..helpers.models import User


@pytest.fixture
def unpack_kwargs(engine):
    return {
        "attrs": {"name": {"S": "numberoverzero"}},
        "expected": {User.name, User.joined},
        "model": User,
        "engine": engine,
        "context": {"engine": engine, "extra": "foo"},
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


def test_printable_column_no_path():
    """Model.column"""
    assert printable_column_name(User.email) == "User.email"


def test_printable_column_mixed_path():
    """Model.column[3].foo[1]"""
    assert printable_column_name(User.id, path=[3, "foo", "bar", 0, 1]) == "User.id[3].foo.bar[0][1]"


@pytest.mark.parametrize("query_on, expected", [
    (User.Meta, User),
    (User.by_email, User.by_email)
])
def test_printable_query(query_on, expected):
    """Unpacks Model.Meta into Model, Index into Index for consistent attribute lookup"""
    assert printable_query(query_on) is expected


def test_unpack_no_engine(unpack_kwargs):
    del unpack_kwargs["engine"]
    del unpack_kwargs["context"]["engine"]

    with pytest.raises(ValueError):
        unpack_from_dynamodb(**unpack_kwargs)


def test_unpack_no_obj_or_model(unpack_kwargs):
    del unpack_kwargs["model"]
    with pytest.raises(ValueError):
        unpack_from_dynamodb(**unpack_kwargs)


def test_unpack_obj_and_model(unpack_kwargs):
    unpack_kwargs["obj"] = User()
    with pytest.raises(ValueError):
        unpack_from_dynamodb(**unpack_kwargs)


def test_unpack_model(unpack_kwargs):
    result = unpack_from_dynamodb(**unpack_kwargs)
    assert result.name == "numberoverzero"
    assert result.joined is None


def test_unpack_obj(unpack_kwargs):
    del unpack_kwargs["model"]
    unpack_kwargs["obj"] = User()
    result = unpack_from_dynamodb(**unpack_kwargs)
    assert result.name == "numberoverzero"
    assert result.joined is None


def test_walk_subclasses():
    class A:
        pass

    class B:  # Not included
        pass

    class C(A):
        pass

    class D(B, C, A):
        pass

    assert set(walk_subclasses(A)) == {A, C, D}


def test_sentinel_uniqueness():
    sentinel = Sentinel("name")
    same_sentinel = Sentinel("NAME")
    assert sentinel is same_sentinel


def test_sentinel_repr():
    foo = Sentinel("foo")
    assert repr(foo) == "<Sentinel[foo]>"


def test_weakref_default_dict():
    """Provides defaultdict behavior for a WeakKeyDictionary"""
    class Object:
        pass

    def counter():
        current = 0
        while True:
            yield current
            current += 1

    weak_dict = WeakDefaultDictionary(counter().__next__)
    objs = [Object() for _ in range(3)]

    for i, obj in enumerate(objs):
        # default_factory is called
        assert weak_dict[obj] == i

    # Interesting: deleting objs[-1] won't work here because the for loop above
    # has a ref to that object stored in the `obj` variable, which gets leaked
    # :(

    del objs[0]
    gc.collect()
    # Properly cleaning up data when gc'd
    assert len(weak_dict) == 2
