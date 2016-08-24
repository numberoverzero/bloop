import operator
import uuid

import arrow
import pytest
from bloop.exceptions import InvalidIndex, InvalidModel
from bloop.models import (
    BaseModel,
    Column,
    GlobalSecondaryIndex,
    Index,
    LocalSecondaryIndex,
    model_created,
    object_modified
)
from bloop.types import UUID, Boolean, DateTime, Integer, String

from ..helpers.models import User


operations = [
    (operator.ne, "!="),
    (operator.eq, "=="),
    (operator.lt, "<"),
    (operator.le, "<="),
    (operator.gt, ">"),
    (operator.ge, ">=")
]


# BASE MODEL =============================================================================================== BASE MODEL


def test_default_model_init():
    """Missing attributes are set to `None`"""
    user = User(id=uuid.uuid4(), email="user@domain.com")
    assert user.email == "user@domain.com"
    assert not hasattr(user, "name")


def test_load_default_init(engine):
    """The default model loader uses the model's __init__ method"""
    init_called = False

    class Blob(BaseModel):
        def __init__(self, *args, **kwargs):
            nonlocal init_called
            # No args, kwargs provided to custom init function
            assert not args
            assert not kwargs
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

    assert init_called

    assert loaded_blob.id == "foo"
    assert loaded_blob.data == "data"
    assert not hasattr(loaded_blob, "extra_field")


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
    with pytest.raises(InvalidModel):
        class DoubleHash(BaseModel):
            hash1 = Column(UUID, hash_key=True)
            hash2 = Column(UUID, hash_key=True)

    with pytest.raises(InvalidModel):
        class DoubleRange(BaseModel):
            id = Column(UUID, hash_key=True)
            range1 = Column(UUID, range_key=True)
            range2 = Column(UUID, range_key=True)

    with pytest.raises(InvalidModel):
        class NoHash(BaseModel):
            other = Column(UUID, range_key=True)

    with pytest.raises(InvalidModel):
        class SharedHashRange(BaseModel):
            both = Column(UUID, hash_key=True, range_key=True)


def test_invalid_local_index():
    with pytest.raises(InvalidIndex):
        class InvalidLSI(BaseModel):
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
    with pytest.raises(InvalidIndex):
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
        l_not_strict = LocalSecondaryIndex(range_key="another", projection=["date"], strict=False)

    uuids = {Model.id, Model.other, Model.another}
    no_boolean = set(Model.Meta.columns)
    no_boolean.remove(Model.boolean)

    assert Model.g_all.projection["mode"] == "all"
    assert Model.g_all.projection["included"] == Model.Meta.columns
    assert Model.g_all.projection["available"] == Model.Meta.columns

    assert Model.g_key.projection["mode"] == "keys"
    assert Model.g_key.projection["included"] == uuids
    assert Model.g_key.projection["available"] == uuids

    assert Model.g_inc.projection["mode"] == "include"
    assert Model.g_inc.projection["included"] == no_boolean
    assert Model.g_inc.projection["available"] == no_boolean

    assert Model.l_all.projection["mode"] == "all"
    assert Model.l_all.projection["included"] == Model.Meta.columns
    assert Model.l_all.projection["available"] == Model.Meta.columns

    assert Model.l_key.projection["mode"] == "keys"
    assert Model.l_key.projection["included"] == uuids
    assert Model.l_key.projection["available"] == uuids

    assert Model.l_inc.projection["mode"] == "include"
    assert Model.l_inc.projection["included"] == no_boolean
    assert Model.l_inc.projection["available"] == no_boolean

    assert Model.l_not_strict.projection["mode"] == "include"
    assert Model.l_not_strict.projection["included"] == no_boolean
    assert Model.l_not_strict.projection["available"] == Model.Meta.columns


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


def test_model_str(engine):
    """Different strings for None and missing"""
    new_user = User()
    loaded_empty_user = engine._load(User, None)

    # No values to show
    assert str(new_user) == "User()"
    # Values set to None
    assert str(loaded_empty_user) == "User(age=None, email=None, id=None, joined=None, name=None)"


def test_created_signal():
    """Emitted when a model is defined"""
    new_model = None

    @model_created.connect
    def verify_called(_, *, model):
        nonlocal new_model
        new_model = model

    # Should invoke verify_called by defining this model
    class SomeModel(BaseModel):
        # class is still being defined
        assert new_model is None
        id = Column(Integer, hash_key=True)

    assert new_model is SomeModel


# END BASE MODEL ======================================================================================= END BASE MODEL


# COLUMN ======================================================================================================= COLUMN


def test_column_dynamo_name():
    """ Returns model name unless dynamo name is specified """
    column = Column(Integer)
    # Normally set when a class is defined
    column.model_name = "foo"
    assert column.dynamo_name == "foo"

    column = Column(Integer, name="foo")
    column.model_name = "bar"
    assert column.dynamo_name == "foo"


def test_column_repr():
    column = Column(Integer, name="f")
    column.model = User
    column.model_name = "foo"
    assert repr(column) == "<Column[User.foo]>"

    column.hash_key = True
    assert repr(column) == "<Column[User.foo=hash]>"

    column.hash_key = False
    column.range_key = True
    assert repr(column) == "<Column[User.foo=range]>"


def test_column_repr_path():
    column = Column(Integer, name="f")
    column.model = User
    column.model_name = "foo"

    assert repr(column[3]["foo"]["bar"][2][1]) == "<Column[User.foo[3].foo.bar[2][1]]>"

    column.hash_key = True
    assert repr(column[3]["foo"]["bar"][2][1]) == "<Column[User.foo[3].foo.bar[2][1]=hash]>"


def test_modified_signal():
    """setting or deleting a column value fires an object_modified signal"""
    user = User()

    @object_modified.connect
    def verify_expected(_, *, obj, column, value):
        nonlocal called
        called = True
        assert obj is expected["obj"]
        assert column is expected["column"]
        assert value is expected["value"]

    # Set should send the new value
    called = False
    expected = {
        "obj": user,
        "column": User.age,
        "value": 10
    }

    user.age = 10
    assert called

    # Reset for delete test
    called = False
    expected = {
        "obj": user,
        "column": User.age,
        "value": None
    }

    del user.age
    assert called

    # Delete sends even if the attribute wasn't set before
    called = False
    expected = {
        "obj": user,
        "column": User.email,
        "value": None
    }

    # Raises because there's no value for email
    with pytest.raises(AttributeError):
        del user.email
    assert called

    # Setting to None looks exactly the same as deleting (since they're effectively identical)
    called = False
    expected = {
        "obj": user,
        "column": User.id,
        "value": None
    }

    user.id = None
    assert called


# END COLUMN =============================================================================================== END COLUMN


# INDEX ========================================================================================================= INDEX


def test_index_dynamo_name():
    """returns model name unless dynamo name is specified"""
    index = Index(projection="keys")
    # Normally set when a class is defined
    index.model_name = "foo"
    assert index.dynamo_name == "foo"

    index = Index(name="foo", projection="keys")
    index.model_name = "bar"
    assert index.dynamo_name == "foo"


def test_index_binds_model_names():
    """When a Model is created, the Index is bound and model names are resolved into columns."""
    class Model(BaseModel):
        id = Column(Integer, hash_key=True)
        foo = Column(Integer)
        bar = Column(Integer)
        baz = Column(Integer)

        by_foo = GlobalSecondaryIndex(projection=["foo"], hash_key=bar, range_key="baz")
        by_bar = GlobalSecondaryIndex(projection=[foo], hash_key="bar", range_key=baz)

    # hash key must be a string or column
    bad_index = Index(projection="all", hash_key=object())
    with pytest.raises(InvalidIndex):
        bad_index._bind(Model)
    bad_index = Index(projection="all", hash_key="foo", range_key=object())
    with pytest.raises(InvalidIndex):
        bad_index._bind(Model)


def test_index_projection_validation():
    """should be all, keys, a list of columns, or a list of column model names"""
    with pytest.raises(InvalidIndex):
        Index(projection="foo")
    with pytest.raises(InvalidIndex):
        Index(projection=object())
    with pytest.raises(InvalidIndex):
        Index(projection=["only strings", 1, None])
    with pytest.raises(InvalidIndex):
        Index(projection=["foo", User.joined])

    index = Index(projection="all")
    assert index.projection["mode"] == "all"
    assert index.projection["included"] is None
    assert index.projection["available"] is None

    index = Index(projection="keys")
    assert index.projection["mode"] == "keys"
    assert index.projection["included"] is None
    assert index.projection["available"] is None

    index = Index(projection=["foo", "bar"])
    assert index.projection["mode"] == "include"
    assert index.projection["included"] == ["foo", "bar"]
    assert index.projection["available"] is None

    index = Index(projection=[User.age, User.email])
    assert index.projection["mode"] == "include"
    assert index.projection["included"] == [User.age, User.email]
    assert index.projection["available"] is None


def test_lsi_specifies_hash_key():
    with pytest.raises(InvalidIndex):
        LocalSecondaryIndex(hash_key="blah", range_key="foo", projection="keys")


def test_lsi_init_throughput():
    """Can't set throughput when creating an LSI"""
    with pytest.raises(InvalidIndex):
        LocalSecondaryIndex(range_key="range", projection="keys", write_units=1)

    with pytest.raises(InvalidIndex):
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


@pytest.mark.parametrize("projection", ["all", "keys", ["foo"]])
def test_index_repr(projection):
    index = Index(projection=projection, name="f")
    index.model = User
    index.model_name = "by_foo"
    if isinstance(projection, list):
        projection = "include"
    assert repr(index) == "<Index[User.by_foo={}]>".format(projection)


def test_lsi_repr():
    index = LocalSecondaryIndex(projection="all", range_key="key", name="f")
    index.model = User
    index.model_name = "by_foo"
    assert repr(index) == "<LSI[User.by_foo=all]>"


def test_gsi_repr():
    index = GlobalSecondaryIndex(projection="all", hash_key="key", name="f")
    index.model = User
    index.model_name = "by_foo"
    assert repr(index) == "<GSI[User.by_foo=all]>"


# END INDEX ================================================================================================= END INDEX
