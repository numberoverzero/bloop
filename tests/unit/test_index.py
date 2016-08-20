import pytest
from bloop.models import (
    BaseModel,
    Column,
    GlobalSecondaryIndex,
    Index,
    LocalSecondaryIndex,
)
from bloop.types import String

from ..helpers.models import User


def test_dynamo_name():
    """returns model name unless dynamo name is specified"""
    index = Index(projection="keys")
    # Normally set when a class is defined
    index.model_name = "foo"
    assert index.dynamo_name == "foo"

    index = Index(name="foo", projection="keys")
    index.model_name = "bar"
    assert index.dynamo_name == "foo"


def test_projection_validation():
    """should be all, keys, or a list of column model names"""
    with pytest.raises(ValueError):
        Index(projection="foo")
    with pytest.raises(ValueError):
        Index(projection=object())
    with pytest.raises(ValueError):
        Index(projection=["only strings", 1, None])

    index = Index(projection="all")
    assert index.projection == "all"
    assert index.projected_columns is None

    index = Index(projection="keys")
    assert index.projection == "keys"
    assert index.projected_columns is None

    index = Index(projection=["foo", "bar"])
    assert index.projection == "include"
    assert index.projected_columns == ["foo", "bar"]


def test_lsi_specifies_hash_key():
    with pytest.raises(ValueError):
        LocalSecondaryIndex(hash_key="blah", range_key="foo", projection="keys")


def test_lsi_init_throughput():
    """Can't set throughput when creating an LSI"""
    with pytest.raises(ValueError):
        LocalSecondaryIndex(range_key="range", projection="keys", write_units=1)

    with pytest.raises(ValueError):
        LocalSecondaryIndex(range_key="range", projection="keys", read_units=1)


def test_lsi_delegates_throughput():
    """LSI read_units, write_units delegate to model.Meta"""
    class Model(BaseModel):
        name = Column(String, hash_key=True)
        other = Column(String, range_key=True)
        joined = Column(String)
        by_joined = LocalSecondaryIndex(range_key="joined", projection="keys")

    meta = Model.Meta
    lsi = Model.by_joined

    # Getters pass through
    meta.write_units = "meta.write_units"
    meta.read_units = "meta.read_units"
    assert lsi.write_units == meta.write_units
    assert lsi.read_units == meta.read_units

    # Setters pass through
    lsi.write_units = "lsi.write_units"
    lsi.read_units = "lsi.read_units"
    assert lsi.write_units == meta.write_units
    assert lsi.read_units == meta.read_units


def test_repr_index():
    index = Index(projection="all", name="f")
    index.model = User
    index.model_name = "by_foo"
    assert repr(index) == "<Index[User.by_foo=all]>"

    index.projection = "keys"
    assert repr(index) == "<Index[User.by_foo=keys]>"

    index.projection = "include"
    assert repr(index) == "<Index[User.by_foo=include]>"


def test_repr_lsi():
    index = LocalSecondaryIndex(projection="all", range_key="key", name="f")
    index.model = User
    index.model_name = "by_foo"
    assert repr(index) == "<LSI[User.by_foo=all]>"


def test_repr_gsi():
    index = GlobalSecondaryIndex(projection="all", hash_key="key", name="f")
    index.model = User
    index.model_name = "by_foo"
    assert repr(index) == "<GSI[User.by_foo=all]>"
