import uuid

import arrow
import pytest
from bloop.models import BaseModel, Column, GlobalSecondaryIndex, LocalSecondaryIndex
from bloop.types import UUID, Boolean, DateTime, String

from ..helpers.models import User


def test_default_model_init():
    """Missing attributes are set to `None`"""
    user = User(id=uuid.uuid4(), email="user@domain.com")
    assert user.email == "user@domain.com"
    assert not hasattr(user, "name")


def test_load_default_init(engine):
    """The default model loader uses the model's __init__ method"""
    init_called = False

    class Blob(BaseModel):
        def __init__(self, **kwargs):
            nonlocal init_called
            init_called = True
            super().__init__(**kwargs)
        id = Column(String, hash_key=True)
        data = Column(String)
    engine.bind(base=Blob)

    assert Blob.Meta.init is Blob

    blob = {
        "id": {"S": "foo"},
        "data": {"S": "data"},
        "extra_field": {"N": "0.125"}
    }

    loaded_blob = engine._load(Blob, blob)
    assert loaded_blob.id == "foo"
    assert loaded_blob.data == "data"
    assert not hasattr(loaded_blob, "extra_field")
    # No args, kwargs provided to custom init function
    assert init_called


def test_load_dump(engine):
    """_load and _dump should be symmetric"""
    user = User(id=uuid.uuid4(), name="test-name", email="email@domain.com", age=31,
                joined=arrow.now())
    serialized = {
        "id": {"S": str(user.id)},
        "age": {"N": "31"},
        "name": {"S": "test-name"},
        "email": {"S": "email@domain.com"},
        "j": {"S": user.joined.to("utc").isoformat()}
    }

    loaded_user = engine._load(User, serialized)

    missing = object()
    for attr in (c.model_name for c in User.Meta.columns):
        assert getattr(loaded_user, attr, missing) == getattr(user, attr, missing)

    assert engine._dump(User, user) == serialized
    assert engine._dump(User, loaded_user) == serialized


def test_load_dump_none(engine):
    user = User()
    assert engine._dump(User, user) is None
    assert engine._dump(User, None) is None

    # Loaded instances have None attributes, unlike newly created instances
    # which don't have those attributes.  That is, `not hasattr(user, "id")`
    # whereas `getattr(loaded_user, "id") is None`
    loaded_user = engine._load(User, None)
    for attr in (c.model_name for c in User.Meta.columns):
        assert getattr(loaded_user, attr) is None

    loaded_user = engine._load(User, {})
    for attr in (c.model_name for c in User.Meta.columns):
        assert getattr(loaded_user, attr) is None


def test_meta_read_write_units():
    """If `read_units` or `write_units` is missing from a model's Meta, it defaults to 1"""
    class Model(BaseModel):
        id = Column(UUID, hash_key=True)

    assert Model.Meta.write_units == 1
    assert Model.Meta.read_units == 1

    class Other(BaseModel):
        class Meta:
            read_units = 2
            write_units = 3
        id = Column(UUID, hash_key=True)

    assert Other.Meta.write_units == 3
    assert Other.Meta.read_units == 2


def test_meta_indexes_columns():
    """An index should not be considered a Column, even if it subclasses"""
    assert User.by_email not in set(User.Meta.columns)
    assert User.by_email in set(User.Meta.indexes)


def test_invalid_model_keys():
    with pytest.raises(ValueError):
        class DoubleHash(BaseModel):
            hash1 = Column(UUID, hash_key=True)
            hash2 = Column(UUID, hash_key=True)

    with pytest.raises(ValueError):
        class DoubleRange(BaseModel):
            id = Column(UUID, hash_key=True)
            range1 = Column(UUID, range_key=True)
            range2 = Column(UUID, range_key=True)

    with pytest.raises(ValueError):
        class NoHash(BaseModel):
            other = Column(UUID, range_key=True)

    with pytest.raises(ValueError):
        class SharedHashRange(BaseModel):
            both = Column(UUID, hash_key=True, range_key=True)


def test_invalid_local_index():
    with pytest.raises(ValueError):
        class InvalidIndex(BaseModel):
            id = Column(UUID, hash_key=True)
            index = LocalSecondaryIndex(range_key="id", projection="keys")


def test_index_keys():
    """Make sure index hash and range keys are objects, not strings"""
    class Model(BaseModel):
        id = Column(UUID, hash_key=True)
        other = Column(DateTime, range_key=True)
        another = Column(UUID)
        last = Column(String)

        by_last = GlobalSecondaryIndex(hash_key="another", range_key="last", projection="keys")
        by_another = LocalSecondaryIndex(range_key="last", projection="keys")

    assert Model.by_last.hash_key is Model.another
    assert Model.by_last.range_key is Model.last

    assert Model.by_another.hash_key is Model.id
    assert Model.by_another.range_key is Model.last


def test_local_index_no_range_key():
    """A table range_key is required to specify a LocalSecondaryIndex"""
    with pytest.raises(ValueError):
        class Model(BaseModel):
            id = Column(UUID, hash_key=True)
            another = Column(UUID)
            by_another = LocalSecondaryIndex(range_key="another", projection="keys")


def test_index_projections():
    """Make sure index projections are calculated to include table keys"""

    class Model(BaseModel):
        id = Column(UUID, hash_key=True)
        other = Column(UUID, range_key=True)
        another = Column(UUID)
        date = Column(DateTime)
        boolean = Column(Boolean)

        g_all = GlobalSecondaryIndex(hash_key="another", range_key="date", projection="all")
        g_key = GlobalSecondaryIndex(hash_key="another", projection="keys")
        g_inc = GlobalSecondaryIndex(hash_key="other", projection=["another", "date"])

        l_all = LocalSecondaryIndex(range_key="another", projection="all")
        l_key = LocalSecondaryIndex(range_key="another", projection="keys")
        l_inc = LocalSecondaryIndex(range_key="another", projection=["date"])

    uuids = {Model.id, Model.other, Model.another}
    no_boolean = set(Model.Meta.columns)
    no_boolean.remove(Model.boolean)

    assert Model.g_all.projection == "ALL"
    assert Model.g_all.projection_attributes == set(Model.Meta.columns)
    assert Model.g_key.projection == "KEYS_ONLY"
    assert Model.g_key.projection_attributes == uuids
    assert Model.g_inc.projection == "INCLUDE"
    assert Model.g_inc.projection_attributes == no_boolean

    assert Model.l_all.projection == "ALL"
    assert Model.l_all.projection_attributes == set(Model.Meta.columns)
    assert Model.l_key.projection == "KEYS_ONLY"
    assert Model.l_key.projection_attributes == uuids
    assert Model.l_inc.projection == "INCLUDE"
    assert Model.l_inc.projection_attributes == no_boolean


def test_meta_table_name():
    """If table_name is missing from a model's Meta, use the model's __name__"""
    class Model(BaseModel):
        id = Column(UUID, hash_key=True)

    assert Model.Meta.table_name == "Model"

    class Other(BaseModel):
        class Meta:
            table_name = "table_name"
            write_units = 3
        id = Column(UUID, hash_key=True)

    assert Other.Meta.table_name == "table_name"


def test_abstract_not_inherited():
    class Concrete(BaseModel):
        id = Column(UUID, hash_key=True)

    assert BaseModel.Meta.abstract
    assert not Concrete.Meta.abstract


def test_str(engine):
    """Different strings for None and missing"""
    new_user = User()
    loaded_empty_user = engine._load(User, None)

    # No values to show
    assert str(new_user) == "User()"
    # Values set to None
    assert str(loaded_empty_user) == "User(age=None, email=None, id=None, joined=None, name=None)"
