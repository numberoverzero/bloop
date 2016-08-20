import gc

import pytest
from bloop.util import (
    Sentinel,
    WeakDefaultDictionary,
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
