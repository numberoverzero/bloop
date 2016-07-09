import bloop.index
import pytest


def test_dynamo_name():
    """ Returns model name unless dynamo name is specified """
    index = bloop.index._Index()
    # Normally set when a class is defined
    index.model_name = "foo"
    assert index.dynamo_name == "foo"

    index = bloop.index._Index(name="foo")
    index.model_name = "bar"
    assert index.dynamo_name == "foo"


def test_projection_validation():
    """ should be all, keys, or a list of column model names """
    Index = bloop.index._Index

    with pytest.raises(ValueError):
        Index(projection="foo")
    with pytest.raises(ValueError):
        Index(projection=object())
    with pytest.raises(ValueError):
        Index(projection=["only strings", 1, None])

    assert Index(projection="all").projection == "ALL"
    # This won't be changed to the DynamoDB value "KEYS_ONLY" until the index is bound to the model
    assert Index(projection="keys").projection == "KEYS"
    assert Index(projection=["foo", "bar"]).projection == ["foo", "bar"]


def test_gsi_missing_hash_key():
    with pytest.raises(ValueError):
        bloop.index.GlobalSecondaryIndex(range_key="blah")


def test_lsi_specifies_hash_key():
    with pytest.raises(ValueError):
        bloop.index.LocalSecondaryIndex(hash_key="blah")


def test_lsi_missing_range_key():
    with pytest.raises(ValueError):
        bloop.index.LocalSecondaryIndex()


def test_lsi_init_throughput():
    """Can't set throughput when creating an LSI"""
    with pytest.raises(ValueError):
        bloop.index.LocalSecondaryIndex(range_key="range", write_units=1)

    with pytest.raises(ValueError):
        bloop.index.LocalSecondaryIndex(range_key="range", read_units=1)


def test_lsi_delegates_throughput():
    """LSI read_units, write_units delegate to model.Meta"""
    class Model(bloop.new_base()):
        name = bloop.Column(bloop.String, hash_key=True)
        other = bloop.Column(bloop.String, range_key=True)
        joined = bloop.Column(bloop.String)
        by_joined = bloop.LocalSecondaryIndex(range_key="joined")

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
