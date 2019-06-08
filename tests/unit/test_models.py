import datetime
import logging
import operator

import pytest

from bloop.conditions import ConditionRenderer
from bloop.exceptions import InvalidModel, InvalidStream
from bloop.models import (
    BaseModel,
    Column,
    GlobalSecondaryIndex,
    IMeta,
    Index,
    LocalSecondaryIndex,
    bind_column,
    bind_index,
    model_created,
    object_modified,
    unbind,
    unpack_from_dynamodb,
)
from bloop.types import (
    UUID,
    Boolean,
    DateTime,
    Integer,
    String,
    Timestamp,
    Type,
)

from ..helpers.models import User, VectorModel


operations = [
    (operator.ne, "!="),
    (operator.eq, "=="),
    (operator.lt, "<"),
    (operator.le, "<="),
    (operator.gt, ">"),
    (operator.ge, ">=")
]


@pytest.fixture
def unpack_kwargs(engine):
    return {
        "attrs": {"name": {"S": "numberoverzero"}},
        "expected": {User.name, User.joined},
        "model": User,
        "engine": engine,
        "context": {"engine": engine, "extra": "foo"},
    }


# BASE MODEL =============================================================================================== BASE MODEL


def test_default_model_init():
    """Missing attributes are set to `None`"""
    user = User(id="user_id", email="user@domain.com")
    assert user.id == "user_id"
    assert user.email == "user@domain.com"
    assert not hasattr(user, "name")


def test_load_default_init(engine):
    """The default model loader uses cls.__new__(cls) method"""
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
    engine.bind(Blob)

    assert isinstance(Blob.Meta.init(), Blob)

    blob = {
        "id": {"S": "foo"},
        "data": {"S": "data"},
        "extra_field": {"N": "0.125"}
    }

    engine._load(Blob, blob)

    assert init_called is False


def test_load_dump(engine):
    """_load and _dump should be symmetric"""
    user = User(id="user_id", name="test-name", email="email@domain.com", age=31,
                joined=datetime.datetime.now(datetime.timezone.utc))
    serialized = {
        "id": {"S": user.id},
        "age": {"N": "31"},
        "name": {"S": "test-name"},
        "email": {"S": "email@domain.com"},
        "j": {"S": user.joined.isoformat()}
    }

    loaded_user = engine._load(User, serialized)

    missing = object()
    for attr in (c.name for c in User.Meta.columns):
        assert getattr(loaded_user, attr, missing) == getattr(user, attr, missing)

    assert engine._dump(User, user) == serialized
    assert engine._dump(User, loaded_user) == serialized


def test_load_dump_none(engine):
    user = User()
    assert engine._dump(User, user) is None
    assert engine._dump(User, None) is None

    # Loaded instances have None or Falsey (in the case of String, Set, List, Map) attributes.
    # This is unlike newly created instances which don't have those attributes.
    loaded_user = engine._load(User, None)
    for attr in (c.name for c in User.Meta.columns):
        assert not getattr(loaded_user, attr)

    loaded_user = engine._load(User, {})
    for attr in (c.name for c in User.Meta.columns):
        assert not getattr(loaded_user, attr)


def test_meta_read_write_units():
    """If `read_units` or `write_units` is missing from a model's Meta, it defaults to None until bound"""
    class Model(BaseModel):
        id = Column(UUID, hash_key=True)

    assert Model.Meta.write_units is None
    assert Model.Meta.read_units is None

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


def test_invalid_model_duplicate_dynamo_name():
    """Two columns have the same dynamo_name, which is ambiguous"""
    with pytest.raises(InvalidModel):
        class SharedDynamoName(BaseModel):
            class Meta:
                abstract = True
            id = Column(UUID, hash_key=True)
            first = Column(String, dynamo_name="shared")
            second = Column(Integer, dynamo_name="shared")


def test_invalid_local_index():
    with pytest.raises(InvalidModel):
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
    with pytest.raises(InvalidModel):
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


def test_meta_not_class():
    """A model's Meta can be anything, not necessarily an inline class"""
    class MetaClass(IMeta):
        pass
    meta = MetaClass()
    meta.read_units = 3

    class Model(BaseModel):
        Meta = meta
        id = Column(UUID, hash_key=True)

    assert Model.Meta.read_units == 3
    assert not Model.Meta.indexes


def test_meta_default_stream():
    """By default, stream is None"""
    class Model(BaseModel):
        id = Column(UUID, hash_key=True)
    assert Model.Meta.stream is None

    class Other(BaseModel):
        class Meta:
            stream = None

        id = Column(UUID, hash_key=True)
    assert Other.Meta.stream is None


def test_meta_default_ttl():
    """By default, ttl is None"""
    class Model(BaseModel):
        id = Column(UUID, hash_key=True)
    assert Model.Meta.ttl is None

    class Other(BaseModel):
        class Meta:
            ttl = None

        id = Column(UUID, hash_key=True)
    assert Other.Meta.ttl is None


def test_meta_default_encryption():
    """By default, sse encryption is None"""
    class Model(BaseModel):
        id = Column(UUID, hash_key=True)
    assert Model.Meta.encryption is None

    class Other(BaseModel):
        class Meta:
            encryption = None

        id = Column(UUID, hash_key=True)
    assert Other.Meta.encryption is None


def test_abstract_not_inherited():
    """Meta.abstract isn't inherited, and by default subclasses are not abstract"""
    class Concrete(BaseModel):
        id = Column(UUID, hash_key=True)

    assert BaseModel.Meta.abstract
    assert not Concrete.Meta.abstract


def test_abstract_subclass():
    """Explicit abstract subclasses are fine, and don't require hash/range keys"""
    class Abstract(BaseModel):
        class Meta(IMeta):
            abstract = True
    assert not Abstract.Meta.keys


def test_model_str(engine):
    """Different strings for None and missing"""
    new_user = User()
    loaded_empty_user = engine._load(User, None)

    # No values to show
    assert str(new_user) == "User()"
    # Values set to None
    assert str(loaded_empty_user) == "User(age=None, email='', id='', joined=None, name='')"


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


@pytest.mark.parametrize("invalid_stream", [
    False,
    True,
    "new",
    ["old", "new"],
    {},
    {"include": "new"},
    {"include": []},
    {"include": ["keys", "old"]},
    {"include": ["keys", "new"]},
    {"include": ["KEYS"]},
])
def test_invalid_stream(invalid_stream):
    """Stream must be a dict with include a list containing keys or (new, old) or new or old"""
    with pytest.raises(InvalidStream):
        class Model(BaseModel):
            class Meta:
                stream = invalid_stream
            id = Column(Integer, hash_key=True)


@pytest.mark.parametrize("invalid_ttl", [
    False, True,
    {}, object(), {"ttl": User.age}, {"column": None}, {"column": []}
])
def test_invalid_ttl(invalid_ttl):
    """ttl must be a dict with 'column' a single Column object or Column name"""
    with pytest.raises(InvalidModel):
        class Model(BaseModel):
            class Meta:
                ttl = invalid_ttl
            id = Column(Integer, hash_key=True)


def test_invalid_ttl_datetime():
    """Special handling for the built-in DateTime to help with type confusion"""
    with pytest.raises(InvalidModel) as excinfo:
        class Model(BaseModel):
            class Meta:
                ttl = {"column": "expiry"}

            id = Column(Integer, hash_key=True)
            expiry = Column(DateTime)
    assert "Did you mean to use bloop.Timestamp?" in str(excinfo.value)


def test_invalid_ttl_backing_type():
    """TTL must be backed by 'N' dynamo type"""
    with pytest.raises(InvalidModel) as excinfo:
        class Model(BaseModel):
            class Meta:
                ttl = {"column": "expiry"}

            id = Column(Integer, hash_key=True)
            expiry = Column(UUID)
    assert "TTL column must be a unix timestamp with backing_type 'N'" in str(excinfo.value)


def test_ttl_late_binds_name():
    """TTL late binds name, even if instance is a column"""
    my_column = Column(Timestamp)

    class Model(BaseModel):
        class Meta:
            ttl = {"column": my_column}
        id = Column(Integer, hash_key=True)
        expiry = my_column
    assert Model.Meta.ttl["column"] is my_column


def test_ttl_by_name():
    """TTL can also take str of column name"""
    my_column = Column(Timestamp)

    class Model(BaseModel):
        class Meta:
            ttl = {"column": "expiry"}
        id = Column(Integer, hash_key=True)
        expiry = my_column
    assert Model.Meta.ttl["column"] is my_column


@pytest.mark.parametrize("invalid_encryption", [False, True, {}, object()])
def test_invalid_encryption(invalid_encryption):
    with pytest.raises(InvalidModel):
        class Model(BaseModel):
            class Meta:
                encryption = invalid_encryption
            id = Column(Integer, hash_key=True)


@pytest.mark.parametrize("invalid_backups", [False, True, {}, object()])
def test_invalid_backups(invalid_backups):
    with pytest.raises(InvalidModel):
        class Model(BaseModel):
            class Meta:
                backups = invalid_backups
            id = Column(Integer, hash_key=True)


@pytest.mark.parametrize("invalid_billing", [
    "provisioned", "on_demand",  # no bare specification
    ["provisioned"],  # must be a dict
    {},  # missing "mode" key
    {"mode": "unknown"},  # unsupported mode
    object()
])
def test_invalid_billing(invalid_billing):
    with pytest.raises(InvalidModel):
        class Model(BaseModel):
            class Meta:
                billing = invalid_billing
            id = Column(Integer, hash_key=True)


@pytest.mark.parametrize("valid_stream", [
    {"include": ["new"]},
    {"include": ["old"]},
    {"include": ["new", "old"]},
    {"include": ["keys"]},
])
def test_valid_stream(valid_stream):
    class Model(BaseModel):
        class Meta:
            stream = valid_stream

        id = Column(Integer, hash_key=True)
    assert Model.Meta.stream["include"] == set(valid_stream["include"])


@pytest.mark.parametrize("valid_encryption", [
    {"enabled": True},
    {"enabled": False},
    {"enabled": True, "unused": object()},
    None
])
def test_valid_encryption(valid_encryption):
    class Model(BaseModel):
        class Meta:
            encryption = valid_encryption

        id = Column(Integer, hash_key=True)
    assert Model.Meta.encryption is valid_encryption


@pytest.mark.parametrize("valid_backups", [
    {"enabled": True},
    {"enabled": False},
    {"enabled": True, "unused": object()},
    None
])
def test_valid_backups(valid_backups):
    class Model(BaseModel):
        class Meta:
            backups = valid_backups

        id = Column(Integer, hash_key=True)
    assert Model.Meta.backups is valid_backups


@pytest.mark.parametrize("valid_billing", [
    {"mode": "provisioned"},
    {"mode": "on_demand"},
    {"mode": "on_demand", "unused": object()},
    None,
])
def test_valid_billing(valid_billing):
    class Model(BaseModel):
        class Meta:
            billing = valid_billing

        id = Column(Integer, hash_key=True)
    assert Model.Meta.billing is valid_billing


def test_require_hash():
    """Models must be hashable"""
    class Model(BaseModel):
        id = Column(Integer, hash_key=True)
        __hash__ = None
    assert Model.__hash__ is object.__hash__


def test_custom_eq():
    """Custom eq method without __hash__ is ok; metaclass will find its parents __hash__"""
    class Model(BaseModel):
        id = Column(Integer, hash_key=True)

        def __eq__(self, other):
            return self.id == other.id
    same = Model(id=3)
    other = Model(id=3)
    assert same == other
    assert Model.__hash__ is object.__hash__


def test_defined_hash():
    """Custom __hash__ isn't replaced"""
    def hash_fn(self):
        return id(self)

    class Model(BaseModel):
        id = Column(Integer, hash_key=True)
        __hash__ = hash_fn
    assert Model.__hash__ is hash_fn


def test_parent_hash(caplog):
    """Parent __hash__ function is used, not object __hash__"""
    class OtherBase:
        # Explicit __eq__ prevents OtherBase from having a __hash__
        def __eq__(self, other):
            pass

    class BaseWithHash:
        def __hash__(self):
            return id(self)

    class MyModel(OtherBase, BaseWithHash, BaseModel):
        id = Column(Integer, hash_key=True)
    assert MyModel.__hash__ is BaseWithHash.__hash__

    assert caplog.record_tuples == [
        ("bloop.models", logging.INFO, "searching for nearest __hash__ impl in MyModel.__mro__"),
    ]


def test_mixins_dynamo_name_conflict():
    """A class derives from two mixins that alias the same dynamo_name"""
    class FooMixin(BaseModel):
        class Meta:
            abstract = True
        foo = Column(String, dynamo_name="shared")

    class BarMixin(BaseModel):
        class Meta:
            abstract = True
        bar = Column(String, dynamo_name="shared")

    with pytest.raises(InvalidModel) as excinfo:
        class SharedMixin(FooMixin, BarMixin):
            class Meta:
                abstract = True
    assert "conflicting column or index" in str(excinfo.value)


def test_mixins_hash_key_conflict():
    """A class derives from two mixins that define a hash_key using different names"""
    class FooHashMixin(BaseModel):
        class Meta:
            abstract = True
        foo_hash = Column(String, hash_key=True, dynamo_name="foo")

    class BarHashMixin(BaseModel):
        class Meta:
            abstract = True
        bar_hash = Column(String, hash_key=True, dynamo_name="bar")

    with pytest.raises(InvalidModel) as excinfo:
        class SharedMixin(FooHashMixin, BarHashMixin):
            class Meta:
                abstract = True
    expected = (
        "The model SharedMixin subclasses one or more models that declare multiple "
        "columns as the hash key: ['bar_hash', 'foo_hash']"
    )
    assert str(excinfo.value) == expected


def test_mixins_range_key_conflict():
    """A class derives from two mixins that define a range_key using different names"""
    class FooRangeMixin(BaseModel):
        class Meta:
            abstract = True
        foo_range = Column(String, range_key=True, dynamo_name="foo")

    class BarRangeMixin(BaseModel):
        class Meta:
            abstract = True
        bar_range = Column(String, range_key=True, dynamo_name="bar")

    with pytest.raises(InvalidModel) as excinfo:
        class SharedMixin(FooRangeMixin, BarRangeMixin):
            class Meta:
                abstract = True
    expected = (
        "The model SharedMixin subclasses one or more models that declare multiple "
        "columns as the range key: ['bar_range', 'foo_range']"
    )
    assert str(excinfo.value) == expected


def test_mixins_same_column_name():
    """If two mixins define columns with the same name, mro determines which the class uses"""
    class FooMixin(BaseModel):
        class Meta:
            abstract = True
        same = Column(String, dynamo_name="foo")

    class BarMixin(BaseModel):
        class Meta:
            abstract = True
        same = Column(String, dynamo_name="bar")

    class FooFirst(FooMixin, BarMixin):
        class Meta:
            abstract = True

    class BarFirst(BarMixin, FooMixin):
        class Meta:
            abstract = True

    assert FooFirst.same.dynamo_name == "foo"
    assert BarFirst.same.dynamo_name == "bar"


def test_mixins_same_index_name():
    """If two mixins define indexes with the same name, mro determines which the class uses"""
    class HasKeys(BaseModel):
        class Meta:
            abstract = True
        mixin_hash = Column(String, hash_key=True)
        mixin_range = Column(String, range_key=True)

    class FooMixin(HasKeys):
        class Meta:
            abstract = True
        data = Column(String, dynamo_name="foo-data")
        by_data = GlobalSecondaryIndex(projection="keys", hash_key=data, dynamo_name="index:foo")

    class BarMixin(HasKeys):
        class Meta:
            abstract = True

        data = Column(String, dynamo_name="bar-data")
        by_data = GlobalSecondaryIndex(projection="keys", hash_key=data, dynamo_name="index:bar")

    class FooFirst(FooMixin, BarMixin, BaseModel):
        class Meta:
            abstract = True

    class BarFirst(BarMixin, FooMixin):
        class Meta:
            abstract = True

    assert FooFirst.by_data.dynamo_name == "index:foo"
    assert BarFirst.by_data.dynamo_name == "index:bar"


def test_mixins_no_base_model():
    """Mixins don't need to subclass BaseModel to be inherited"""
    class HasKeys:
        id = Column(String, hash_key=True)
        range = Column(String, range_key=True)
        other_id = Column(String)

        by_other = GlobalSecondaryIndex(projection="keys", hash_key=other_id)
        sort_by_other = LocalSecondaryIndex(projection="all", range_key=other_id)

    class ParentData:
        data = Column(String, dynamo_name="parent-data")
        parent = Column(Integer)

    class ChildData(ParentData):
        data = Column(String, dynamo_name="child-data")
        child = Column(Integer)

    class BaseModelFirst(BaseModel, HasKeys, ChildData):
        pass

    assert BaseModelFirst.id
    assert BaseModelFirst.range
    assert BaseModelFirst.other_id
    assert BaseModelFirst.by_other
    assert BaseModelFirst.sort_by_other
    assert BaseModelFirst.parent
    assert BaseModelFirst.child
    assert BaseModelFirst.data.dynamo_name == "child-data"

    class BaseModelLast(HasKeys, ChildData, BaseModel):
        pass

    assert BaseModelLast.id
    assert BaseModelLast.range
    assert BaseModelLast.other_id
    assert BaseModelLast.by_other
    assert BaseModelLast.sort_by_other
    assert BaseModelLast.parent
    assert BaseModelLast.child
    assert BaseModelLast.data.dynamo_name == "child-data"


# END BASE MODEL ======================================================================================= END BASE MODEL


# COLUMN ======================================================================================================= COLUMN


def test_column_invalid_typedef():
    """Column typedef must be an instance or subclass of bloop.Type"""
    with pytest.raises(TypeError):
        Column(object())


def test_column_type_instantiation():
    """If a bloop.Type subclass is provided, Column calls __init__ with no args"""
    # noinspection PyAbstractClass
    class MyType(Type):
        def __init__(self, *args, **kwargs):
            assert not args
            assert not kwargs
            super().__init__()

    column = Column(MyType)
    assert isinstance(column.typedef, MyType)


def test_unbound_name():
    """A column created outside a subclass of BaseModel won't have a name, and can't get/set/del"""
    column = Column(Integer)

    class MyModel(BaseModel):
        class Meta:
            abstract = True
    MyModel.email = column

    obj = MyModel()

    with pytest.raises(AttributeError) as excinfo:
        obj.email = "foo"
    assert "without binding to model" in str(excinfo.value)
    with pytest.raises(AttributeError) as excinfo:
        getattr(obj, "email")
    assert "without binding to model" in str(excinfo.value)
    with pytest.raises(AttributeError) as excinfo:
        del obj.email
    assert "without binding to model" in str(excinfo.value)


def test_column_dynamo_name():
    """ Returns model name unless dynamo name is specified """
    column = Column(Integer)
    # Normally set when a class is defined
    column._name = "foo"
    assert column.dynamo_name == "foo"

    column = Column(Integer, dynamo_name="foo")
    column._name = "bar"
    assert column.dynamo_name == "foo"


def test_column_repr():
    column = Column(Integer, dynamo_name="f")
    column.model = User
    column._name = "foo"
    assert repr(column) == "<Column[User.foo]>"

    column.hash_key = True
    assert repr(column) == "<Column[User.foo=hash]>"

    column.hash_key = False
    column.range_key = True
    assert repr(column) == "<Column[User.foo=range]>"


def test_column_repr_path():
    column = Column(Integer, dynamo_name="f")
    column.model = User
    column._name = "foo"

    assert repr(column[3]["foo"]["bar"][2][1]) == "<Proxy[User.foo[3].foo.bar[2][1]]>"

    column.hash_key = True
    assert repr(column[3]["foo"]["bar"][2][1]) == "<Proxy[User.foo[3].foo.bar[2][1]]>"


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


@pytest.mark.parametrize("container_column", [VectorModel.list_str, VectorModel.set_str])
def test_contains_container_types(container_column, engine):
    """Contains should render with the column type's inner type"""
    renderer = ConditionRenderer(engine)
    condition = container_column.contains("foo")
    renderer.condition_expression(condition)

    expected = {
        'ExpressionAttributeValues': {':v1': {'S': "foo"}},
        'ConditionExpression': '(contains(#n0, :v1))',
        'ExpressionAttributeNames': {'#n0': container_column.dynamo_name}
    }
    assert renderer.output == expected


def test_column_defaults():

    # Set should send the new value
    num_called = 0
    received = {}

    @object_modified.connect
    def verify_expected(_, *, obj, column, value):
        nonlocal num_called, received
        num_called += 1
        received['obj'] = obj
        received['column'] = column
        received['value'] = value

    class ColumnDefaultModel(BaseModel):
        id = Column(Integer, hash_key=True, default=12)

    default_obj = ColumnDefaultModel()

    assert num_called == 1
    assert received["obj"] is default_obj
    assert received["column"] is ColumnDefaultModel.id
    assert received["value"] == 12


def test_column_default_func():

    # Set should send the new value
    num_called = 0
    received = {}

    @object_modified.connect
    def verify_expected(_, *, obj, column, value):
        nonlocal num_called, received
        num_called += 1
        received['obj'] = obj
        received['column'] = column
        received['value'] = value

    def return_number():
        return 123

    class ColumnDefaultFuncModel(BaseModel):
        id = Column(Integer, hash_key=True, default=return_number)

    default_obj = ColumnDefaultFuncModel()

    assert num_called == 1
    assert received["obj"] is default_obj
    assert received["column"] is ColumnDefaultFuncModel.id
    assert received["value"] == 123


# END COLUMN =============================================================================================== END COLUMN


# INDEX ========================================================================================================= INDEX

@pytest.mark.parametrize("key_type", ["hash_key", "range_key"])
@pytest.mark.parametrize("value, valid", [
    (3, False), (object(), False), (type, False),
    ("some_name", True), (Column(Integer), True)
])
def test_index_bad_key(key_type, value, valid):
    """__init__ raises when passing the wrong type"""
    kwargs = {
        "projection": "all",
        "hash_key": "valid",
        "range_key": "valid",
        key_type: value
    }
    run = lambda: Index(**kwargs)
    if valid:
        run()
    else:
        with pytest.raises(InvalidModel):
            run()


def test_index_dynamo_name():
    """returns model name unless dynamo name is specified"""
    index = Index(projection="keys")
    # Normally set when a class is defined
    index._name = "foo"
    assert index.dynamo_name == "foo"

    index = Index(dynamo_name="foo", projection="keys")
    index._name = "bar"
    assert index.dynamo_name == "foo"


def test_index_binds_names():
    """When a Model is created, the Index is bound and model names are resolved into columns."""
    class Model(BaseModel):
        id = Column(Integer, hash_key=True)
        foo = Column(Integer)
        bar = Column(Integer)
        baz = Column(Integer)

        by_foo = GlobalSecondaryIndex(projection=["foo"], hash_key=bar, range_key="baz")
        by_bar = GlobalSecondaryIndex(projection=[foo], hash_key="bar", range_key=baz)

    # hash key must be a string or column
    bad_index = Index(projection="all", hash_key="this_name_is_missing")
    with pytest.raises(InvalidModel):
        bind_index(Model, "another_index", bad_index)
    bad_index = Index(projection="all", hash_key="foo", range_key="this_name_is_missing")
    with pytest.raises(InvalidModel):
        bind_index(Model, "another_index", bad_index)


def test_index_projection_validation():
    """should be all, keys, a list of columns, or a list of column model names"""
    with pytest.raises(InvalidModel):
        Index(projection="foo")
    with pytest.raises(InvalidModel):
        Index(projection=object())
    with pytest.raises(InvalidModel):
        Index(projection=["only strings", 1, None])
    with pytest.raises(InvalidModel):
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


def test_index_unmodifiable():
    """Can't set/get/del an index"""
    class Model(BaseModel):
        id = Column(Integer, hash_key=True)
        other = Column(Integer)
        by_other = GlobalSecondaryIndex(projection="all", hash_key="other")
    obj = Model(by_other=2)
    assert not hasattr(obj, "by_other")

    with pytest.raises(AttributeError) as excinfo:
        getattr(obj, "by_other")
    assert "Model.by_other" in str(excinfo.value)

    with pytest.raises(AttributeError) as excinfo:
        setattr(obj, "by_other", "value")
    assert "Model.by_other" in str(excinfo.value)

    with pytest.raises(AttributeError) as excinfo:
        delattr(obj, "by_other")
    assert "Model.by_other" in str(excinfo.value)


def test_lsi_specifies_hash_key():
    with pytest.raises(InvalidModel):
        LocalSecondaryIndex(hash_key="blah", range_key="foo", projection="keys")


def test_lsi_init_throughput():
    """Can't set throughput when creating an LSI"""
    with pytest.raises(InvalidModel):
        LocalSecondaryIndex(range_key="range", projection="keys", write_units=1)

    with pytest.raises(InvalidModel):
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


def test_gsi_default_throughput():
    """When not specified, GSI read_units and write_units are None"""
    class Model(BaseModel):
        name = Column(String, hash_key=True)
        other = Column(String)
        by_joined = GlobalSecondaryIndex(hash_key="other", projection="keys")
    gsi = Model.by_joined
    assert gsi.read_units is gsi.write_units is None


@pytest.mark.parametrize("projection", ["all", "keys", ["foo"]])
def test_index_repr(projection):
    index = Index(projection=projection, dynamo_name="f")
    index.model = User
    index._name = "by_foo"
    if isinstance(projection, list):
        projection = "include"
    assert repr(index) == "<Index[User.by_foo={}]>".format(projection)


def test_lsi_repr():
    index = LocalSecondaryIndex(projection="all", range_key="key", dynamo_name="f")
    index.model = User
    index._name = "by_foo"
    assert repr(index) == "<LSI[User.by_foo=all]>"


def test_gsi_repr():
    index = GlobalSecondaryIndex(projection="all", hash_key="key", dynamo_name="f")
    index.model = User
    index._name = "by_foo"
    assert repr(index) == "<GSI[User.by_foo=all]>"


# END INDEX ================================================================================================= END INDEX


# BINDING ===================================================================================================== BINDING


def new_abstract_model(indexes=False):
    class MyModel(BaseModel):
        class Meta(IMeta):
            abstract = True
        data = Column(String, dynamo_name="dynamo-data")
        if indexes:
            my_index_hash = Column(Integer, hash_key=True)
            by_data = GlobalSecondaryIndex(
                projection="all", hash_key="data", dynamo_name="dynamo-by-data")
            email = Column(String, range_key=True)
            by_email = LocalSecondaryIndex(projection="all", range_key=data)
    return MyModel


def test_bind_non_model():
    """bind_column, bind_index take subclass of Model"""
    column = Column(String, dynamo_name="other")
    index = GlobalSecondaryIndex(projection="keys", hash_key="foo")

    class NotAModel:
        class Meta:
            pass

    with pytest.raises(InvalidModel):
        bind_column(NotAModel, "c", column)
    with pytest.raises(InvalidModel):
        bind_index(NotAModel, "i", index)


def test_bind_column_name_conflict_fails():
    """When a name conflicts and force=False, bind fails"""
    model = new_abstract_model()
    other = Column(String, dynamo_name="other")

    with pytest.raises(InvalidModel) as excinfo:
        bind_column(model, "data", other)
    assert "has the same name" in str(excinfo.value)


def test_bind_column_name_conflict_force():
    """When a name conflicts and force=True, the original column is overwritten"""
    model = new_abstract_model()
    other = Column(String, dynamo_name="other")

    bound = bind_column(model, "data", other, force=True)
    # not proxied
    assert bound is other
    assert len(model.Meta.columns) == 1
    assert model.data is other


def test_bind_column_dynamo_name_conflict_fails():
    """When a dynamo_name conflicts and force=False, bind fails"""
    model = new_abstract_model()
    other = Column(String, dynamo_name="dynamo-data")

    with pytest.raises(InvalidModel) as excinfo:
        bind_column(model, "other", other)
    assert "has the same dynamo_name" in str(excinfo.value)


def test_bind_column_dynamo_name_conflict_force():
    """When a dynamo_name conflicts and force=True, the original column is overwritten"""
    model = new_abstract_model()
    other = Column(String, dynamo_name="dynamo-data")

    bound = bind_column(model, "other", other, force=True)
    # not proxied
    assert bound is other
    assert len(model.Meta.columns) == 1
    assert model.other is other


def test_bind_column_force_keys():
    """When a column is bound with force, hash_keys and range_keys can be replaced"""
    model = new_abstract_model()
    assert not model.Meta.hash_key
    assert not model.Meta.range_key

    hash_key = Column(String, hash_key=True)
    bind_column(model, "data", hash_key, force=True)
    assert model.Meta.hash_key is hash_key
    assert not model.Meta.range_key
    assert model.Meta.keys == {hash_key}

    range_key = Column(String, range_key=True)
    bind_column(model, "data", range_key, force=True)
    assert not model.Meta.hash_key
    assert model.Meta.range_key is range_key
    assert model.Meta.keys == {range_key}

    # force rebind over a hash or range key
    new_hash_key = Column(Integer, hash_key=True)
    bound_nhk = bind_column(model, "data", new_hash_key, force=True, copy=True)
    assert model.Meta.hash_key is bound_nhk
    new_range_key = Column(Integer, range_key=True)
    bound_nrk = bind_column(model, "data", new_range_key, force=True, copy=True)
    assert model.Meta.range_key is bound_nrk


def test_bind_column_extra_hash_key():
    """When a column is bound without force and would replace a hash or range key, it fails"""
    model = new_abstract_model()
    hash_key = Column(String, hash_key=True)
    range_key = Column(String, range_key=True)

    # setup "existing" hash and range keys
    bind_column(model, "my_hash", hash_key, force=True)
    bind_column(model, "my_range", range_key, force=True)
    assert hash_key is model.my_hash is model.Meta.hash_key
    assert range_key is model.my_range is model.Meta.range_key
    assert model.Meta.keys == {hash_key, range_key}

    # binding another hash_key fails, even without name/dynamo_name collisions
    extra = Column(String, hash_key=True, dynamo_name="extra-hash")
    with pytest.raises(InvalidModel) as excinfo:
        bind_column(model, "extra", extra)
    assert "has a different hash_key" in str(excinfo.value)

    # binding another range_key fails
    extra = Column(String, range_key=True, dynamo_name="extra-range")
    with pytest.raises(InvalidModel) as excinfo:
        bind_column(model, "extra", extra)
    assert "has a different range_key" in str(excinfo.value)


def test_bind_column_recalculates_index_projection():
    """An existing index recalculates its projection when a new column is added"""
    model = new_abstract_model()
    hash_key = Column(String, hash_key=True)
    index = GlobalSecondaryIndex(projection="all", hash_key="data")

    bind_column(model, "id", hash_key)
    bind_index(model, "by_data", index)

    assert index.projection["included"] == {model.Meta.hash_key, model.data}

    # bind a new column, which will now be part of the projection
    new_column = Column(String)
    bind_column(model, "something", new_column)
    assert index.projection["included"] == {model.Meta.hash_key, model.data, model.something}


def test_bind_column_breaks_index_key():
    """An existing Index has a hash or range key which is removed when a new column is bound"""
    model = new_abstract_model(indexes=True)
    index_hash_key_dynamo_name = "dynamo-data"
    new_column = Column(String, dynamo_name=index_hash_key_dynamo_name)

    # Note that since this puts the model in an inconsistent state, even force=True
    # won't prevent the exception
    with pytest.raises(InvalidModel):
        bind_column(model, "different_name", new_column, force=True)


def test_bind_column_parent_class():
    """
    Binding to a parent class can optionally recurse through children, adding a
    copy of the column when there are no conflicts
    """

    class Parent(BaseModel):
        class Meta(IMeta):
            abstract = True

    class Child(Parent):
        id = Column(String, hash_key=True)
        NAME_CONFLICT = Column(String)

    class AnotherChild(Parent):
        id = Column(String, hash_key=True)
        another = Column(String, dynamo_name="DYNAMO-NAME-CONFLICT")

    class Grandchild(Child):
        pass

    no_conflict = Column(Integer)
    no_conflict_recursive = Column(Integer)
    name_conflict = Column(Integer, dynamo_name="DYNAMO-NAME-OK")
    dynamo_name_conflict = Column(Integer, dynamo_name="DYNAMO-NAME-CONFLICT")

    # 0. Non-recursive binds don't modify children
    parent_bind = bind_column(Parent, "parent_only", no_conflict)
    assert Parent.Meta.columns == {parent_bind}
    assert len(Child.Meta.columns) == 2
    assert len(AnotherChild.Meta.columns) == 2
    assert len(Grandchild.Meta.columns) == 2

    # 1. Recursive binds with no conflicts are applied to all descendants
    recursive_bind = bind_column(Parent, "all_children", no_conflict_recursive, recursive=True)
    assert Parent.Meta.columns == {parent_bind, recursive_bind}
    assert len(Child.Meta.columns) == 3
    assert len(AnotherChild.Meta.columns) == 3
    assert len(Grandchild.Meta.columns) == 3

    # 2. Recursive bind with name conflict isn't added to
    #    Child or Grandchild, but is added to AnotherChild
    first_conflict = bind_column(Parent, "NAME_CONFLICT", name_conflict, recursive=True)
    assert Parent.Meta.columns == {parent_bind, recursive_bind, first_conflict}
    assert len(Child.Meta.columns) == 3
    assert len(AnotherChild.Meta.columns) == 4
    assert len(Grandchild.Meta.columns) == 3

    # 3. Recursive bind with dynamo_name conflict isn't added to AnotherChild,
    #    but is added to Child and Grandchild
    second_conflict = bind_column(Parent, "NAME_OK", dynamo_name_conflict, recursive=True)
    assert Parent.Meta.columns == {parent_bind, recursive_bind, first_conflict, second_conflict}
    assert len(Child.Meta.columns) == 4
    assert len(AnotherChild.Meta.columns) == 4
    assert len(Grandchild.Meta.columns) == 4

    assert Child.NAME_CONFLICT is not first_conflict  # name conflict
    assert AnotherChild.another is not second_conflict  # dynamo_name conflict
    assert Grandchild.NAME_CONFLICT is not first_conflict


def test_bind_column_copy():
    """When copy=True, the given column isn't bound directly but a shallow copy is inserted"""
    model = new_abstract_model()
    other_model = new_abstract_model()
    assert model is not other_model  # guard against refactor to return the same model

    column = Column(String, dynamo_name="dynamo-name")

    bound = bind_column(model, "to_model", column, copy=True)
    assert bound is not column
    assert model.to_model is bound

    other_bound = bind_column(other_model, "to_other_model", column, copy=True)
    assert other_bound is not column
    assert other_model.to_other_model is other_bound

    assert bound is not other_bound

    # mutating the original does not change either copy
    column._dynamo_name = "new-dynamo-name"
    bound._dynamo_name = "bound-dynamo-name"
    assert column.dynamo_name == "new-dynamo-name"
    assert bound.dynamo_name == "bound-dynamo-name"
    assert other_bound.dynamo_name == "dynamo-name"

    # name is set correctly even though __copy__ clears it
    assert bound.name == "to_model"
    assert other_bound.name == "to_other_model"

    # bound to the correct model
    assert bound.model is model
    assert other_bound.model is other_model


def test_bind_column_no_copy():
    """Binding a column with copy=False mutates the original column"""
    model = new_abstract_model()
    column = Column(String, dynamo_name="dynamo-name")

    bound_column = bind_column(model, "another-name", column, copy=False)
    assert column is bound_column


def test_bind_index_name_conflict_fails():
    """When a name conflicts and force=False, bind fails"""
    model = new_abstract_model(indexes=True)
    other = GlobalSecondaryIndex(projection="all", hash_key="data")

    with pytest.raises(InvalidModel) as excinfo:
        bind_index(model, "by_data", other)
    assert "has the same name" in str(excinfo.value)


def test_bind_index_name_conflict_force():
    """When a name conflicts and force=True, the original index is overwritten"""
    model = new_abstract_model(indexes=True)
    other = LocalSecondaryIndex(projection="all", range_key="data")
    another = GlobalSecondaryIndex(projection="all", hash_key="data")

    bound = bind_index(model, "by_data", other, force=True)
    # not proxied
    assert bound is other
    assert len(model.Meta.indexes) == 2
    assert model.by_data is other
    assert bound in model.Meta.indexes
    assert bound in model.Meta.lsis

    new_bound = bind_index(model, "by_data", another, force=True)
    assert new_bound is another
    assert len(model.Meta.indexes) == 2
    assert model.by_data is another
    assert bound not in model.Meta.indexes
    assert bound not in model.Meta.lsis

    assert new_bound in model.Meta.indexes
    assert new_bound in model.Meta.gsis


def test_bind_index_dynamo_name_conflict_fails():
    """When a dynamo_name conflicts and force=False, bind fails"""
    model = new_abstract_model(indexes=True)
    other = GlobalSecondaryIndex(projection="all", hash_key="data", dynamo_name="dynamo-by-data")

    with pytest.raises(InvalidModel) as excinfo:
        bind_index(model, "other", other)
    assert "has the same dynamo_name" in str(excinfo.value)


def test_bind_index_dynamo_name_conflict_force():
    """When a dynamo_name conflicts and force=True, the original index is overwritten"""
    model = new_abstract_model(indexes=True)
    other = GlobalSecondaryIndex(projection="all", hash_key="data", dynamo_name="dynamo-by-data")

    bound = bind_index(model, "other", other, force=True)
    # not proxied
    assert bound is other
    assert len(model.Meta.indexes) == 2
    assert model.other is other


def test_bind_index_recalculates_index_projection():
    """An existing index recalculates its projection when bound"""
    model = new_abstract_model()
    old_data_column = model.data

    # bind the hash key with force since the model already has a column named "data"
    hash_key = Column(String, hash_key=True)
    bound_hash_key = bind_column(model, "data", hash_key, copy=True, force=True)

    # index points to an outdated column, but its name will be
    # used to resolve the current hash_key
    index = GlobalSecondaryIndex(projection="all", hash_key=old_data_column)
    bound_index = bind_index(model, "by_data", index, copy=True)

    assert bound_index.projection["included"] == {bound_hash_key}

    # bind a new column, which will now be part of the projection
    new_column = Column(String)
    bind_column(model, "something", new_column)
    assert bound_index.projection["included"] == {bound_hash_key, model.something}

    # because we used a copy, the original index should be unchanged
    assert index.projection == {
        "mode": "all",
        "included": None,
        "available": None,
        "strict": True
    }
    assert bound_index.hash_key is not old_data_column


def test_bind_index_parent_class():
    """
    Binding to a parent class can optionally recurse through children, adding a
    copy of the index when there are no conflicts
    """

    class Parent(BaseModel):
        class Meta(IMeta):
            abstract = True
        id = Column(String, hash_key=True)
        range = Column(String, range_key=True)

    class Child(Parent):
        NAME_CONFLICT = GlobalSecondaryIndex(projection="all", hash_key="id")

    class AnotherChild(Parent):
        another = GlobalSecondaryIndex(
            projection="all", hash_key="id",
            dynamo_name="DYNAMO-NAME-CONFLICT")

    class Grandchild(Child):
        pass

    no_conflict = GlobalSecondaryIndex(projection="all", hash_key="id")
    no_conflict_recursive = LocalSecondaryIndex(
        projection="all", range_key="id")
    name_conflict = GlobalSecondaryIndex(
        projection="all", hash_key="id", dynamo_name="DYNAMO-NAME-OK")
    dynamo_name_conflict = LocalSecondaryIndex(
        projection="all", range_key="id", dynamo_name="DYNAMO-NAME-CONFLICT")

    # 0. Non-recursive binds don't modify children
    parent_bind = bind_index(Parent, "parent_only", no_conflict)
    assert Parent.Meta.indexes == {parent_bind}
    assert len(Child.Meta.indexes) == 2
    assert len(AnotherChild.Meta.indexes) == 2
    assert len(Grandchild.Meta.indexes) == 2

    # 1. Recursive binds with no conflicts are applied to all descendants
    recursive_bind = bind_index(Parent, "all_children", no_conflict_recursive, recursive=True)
    assert Parent.Meta.indexes == {parent_bind, recursive_bind}
    assert len(Child.Meta.indexes) == 3
    assert len(AnotherChild.Meta.indexes) == 3
    assert len(Grandchild.Meta.indexes) == 3

    # 2. Recursive bind with name conflict isn't added to
    #    Child or Grandchild, but is added to AnotherChild
    first_conflict = bind_index(Parent, "NAME_CONFLICT", name_conflict, recursive=True)
    assert Parent.Meta.indexes == {parent_bind, recursive_bind, first_conflict}
    assert len(Child.Meta.indexes) == 3
    assert len(AnotherChild.Meta.indexes) == 4
    assert len(Grandchild.Meta.indexes) == 3

    # 3. Recursive bind with dynamo_name conflict isn't added to AnotherChild,
    #    but is added to Child and Grandchild
    second_conflict = bind_index(Parent, "NAME_OK", dynamo_name_conflict, recursive=True)
    assert Parent.Meta.indexes == {parent_bind, recursive_bind, first_conflict, second_conflict}
    assert len(Child.Meta.indexes) == 4
    assert len(AnotherChild.Meta.indexes) == 4
    assert len(Grandchild.Meta.indexes) == 4

    assert Child.NAME_CONFLICT is not first_conflict  # name conflict
    assert AnotherChild.another is not second_conflict  # dynamo_name conflict
    assert Grandchild.NAME_CONFLICT is not first_conflict


def test_bind_index_copy():
    """When copy=True, the given column isn't bound directly but a shallow copy is inserted"""
    model = new_abstract_model(indexes=True)
    other_model = new_abstract_model()
    assert model is not other_model  # guard against refactor to return the same model

    index = GlobalSecondaryIndex(projection="all", hash_key="data", dynamo_name="dynamo-name")
    bound = bind_index(model, "by_data_copy", index, copy=True)

    assert bound is not index
    assert model.by_data_copy is bound

    other_bound = bind_index(other_model, "by_data_other", index, copy=True)
    assert other_bound is not index
    assert other_model.by_data_other is other_bound

    assert bound is not other_bound

    # mutating the original does not change either copy
    index._dynamo_name = "new-dynamo-name"
    bound._dynamo_name = "bound-dynamo-name"
    assert index.dynamo_name == "new-dynamo-name"
    assert bound.dynamo_name == "bound-dynamo-name"
    assert other_bound.dynamo_name == "dynamo-name"

    # name is set correctly even though __copy__ clears it
    assert bound.name == "by_data_copy"
    assert other_bound.name == "by_data_other"

    # bound to the correct model
    assert bound.model is model
    assert other_bound.model is other_model


def test_unbind_bad_call():
    """unbind must be called with either name= or dynamo_name="""
    model = new_abstract_model()
    with pytest.raises(RuntimeError):
        unbind(model.Meta)


# END BINDING ============================================================================================= END BINDING


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
