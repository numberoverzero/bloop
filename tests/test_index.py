import bloop.index
import pytest


def test_dynamo_name():
    """ Returns model name unless dynamo name is specified """
    index = bloop.index._Index(bloop.Integer)
    # Normally set when a class is defined
    index.model_name = "foo"
    assert index.dynamo_name == "foo"

    index = bloop.index._Index(bloop.Integer, name="foo")
    index.model_name = "bar"
    assert index.dynamo_name == "foo"


def test_projection_validation():
    """ should be all, keys_only, or a list of column model names """
    Index = bloop.index._Index

    with pytest.raises(ValueError):
        Index(projection="foo")
    with pytest.raises(ValueError):
        Index(projection=object())
    with pytest.raises(ValueError):
        Index(projection=["only strings", 1, None])

    assert Index(projection="all").projection == "ALL"
    assert Index(projection="keys_only").projection == "KEYS_ONLY"
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


def test_lsi_read_write_units():
    with pytest.raises(ValueError):
        bloop.index.LocalSecondaryIndex(range_key="range", write_units=1)

    with pytest.raises(ValueError):
        bloop.index.LocalSecondaryIndex(range_key="range", read_units=1)
