import collections

import pytest
from tests.helpers.models import User

from bloop.engine import Engine
from bloop.exceptions import MissingKey
from bloop.models import BaseModel, Column
from bloop.types import Integer
from bloop.util import (
    Sentinel,
    dump_key,
    extract_key,
    get_table_name,
    index,
    index_for,
    ordered,
    value_of,
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


def test_dump_key(engine):
    class HashAndRange(BaseModel):
        foo = Column(Integer, hash_key=True)
        bar = Column(Integer, range_key=True)
    engine.bind(HashAndRange)

    user = User(id="foo")
    user_key = {"id": {"S": "foo"}}
    assert dump_key(engine, user) == user_key

    obj = HashAndRange(foo=4, bar=5)
    obj_key = {"bar": {"N": "5"}, "foo": {"N": "4"}}
    assert dump_key(engine, obj) == obj_key


def test_dump_key_missing(engine):
    class HashAndRange(BaseModel):
        foo = Column(Integer, hash_key=True)
        bar = Column(Integer, range_key=True)
    engine.bind(HashAndRange)

    obj = HashAndRange()
    with pytest.raises(MissingKey):
        dump_key(engine, obj)


def test_extract_key():
    key_shape = "foo", "bar"
    item = {"baz": 1, "bar": 2, "foo": 3}
    expected = {"foo": 3, "bar": 2}
    assert extract_key(key_shape, item) == expected


def test_get_table_name(dynamodb, dynamodbstreams):
    def transform_table_name(model):
        return f"transform.{model.Meta.table_name}"

    class HashAndRange(BaseModel):
        class Meta:
            table_name = "custom.name"
        foo = Column(Integer, hash_key=True)
        bar = Column(Integer, range_key=True)

    engine = Engine(
        dynamodb=dynamodb, dynamodbstreams=dynamodbstreams,
        table_name_template=transform_table_name)
    obj = HashAndRange()
    assert get_table_name(engine, obj) == "transform.custom.name"


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


def test_value_of():
    column = {"S": "Space Invaders"}
    assert value_of(column) == "Space Invaders"


def test_index_for_sorts():
    key = {
        "f": {"S": "foo"},
        "b": {"S": "bar"},
    }
    same_key = {
        "b": {"S": "bar"},
        "f": {"S": "foo"},
    }
    assert index_for(key) == index_for(same_key)


def test_sentinel_uniqueness():
    sentinel = Sentinel("name")
    same_sentinel = Sentinel("NAME")
    assert sentinel is same_sentinel


def test_sentinel_repr():
    foo = Sentinel("foo")
    assert repr(foo) == "<Sentinel[foo]>"
