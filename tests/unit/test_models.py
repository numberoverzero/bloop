import datetime
import logging
import operator

import pytest

from bloop.conditions import ConditionRenderer
from bloop.exceptions import InvalidIndex, InvalidModel, InvalidStream
from bloop.models import (
    BaseModel,
    Column,
    GlobalSecondaryIndex,
    Index,
    LocalSecondaryIndex,
    Proxy,
    bind_index,
    model_created,
    object_modified,
    unpack_from_dynamodb,
)
from bloop.types import UUID, Boolean, DateTime, Integer, String, Type

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
    engine.bind(Blob)

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

    # Loaded instances have None attributes, unlike newly created instances
    # which don't have those attributes.  That is, `not hasattr(user, "id")`
    # whereas `getattr(loaded_user, "id") is None`
    loaded_user = engine._load(User, None)
    for attr in (c.name for c in User.Meta.columns):
        assert getattr(loaded_user, attr) is None

    loaded_user = engine._load(User, {})
    for attr in (c.name for c in User.Meta.columns):
        assert getattr(loaded_user, attr) is None


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
    class MetaClass:
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


def test_abstract_not_inherited():
    """Meta.abstract isn't inherited, and by default subclasses are not abstract"""
    class Concrete(BaseModel):
        id = Column(UUID, hash_key=True)

    assert BaseModel.Meta.abstract
    assert not Concrete.Meta.abstract


def test_abstract_subclass():
    """Explicit abstract subclasses are fine, and don't require hash/range keys"""
    class Abstract(BaseModel):
        class Meta:
            abstract = True
    assert not Abstract.Meta.keys


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
    renderer.render_condition_expression(condition)

    expected = {
        'ExpressionAttributeValues': {':v1': {'S': "foo"}},
        'ConditionExpression': '(contains(#n0, :v1))',
        'ExpressionAttributeNames': {'#n0': container_column.dynamo_name}
    }
    assert renderer.rendered == expected


# END COLUMN =============================================================================================== END COLUMN


# INDEX ========================================================================================================= INDEX


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
    bad_index = Index(projection="all", hash_key=object())
    with pytest.raises(InvalidModel):
        bind_index(Model.Meta, "another_index", bad_index)
    bad_index = Index(projection="all", hash_key="foo", range_key=object())
    with pytest.raises(InvalidModel):
        bind_index(Model.Meta, "another_index", bad_index)


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


# PROXY ========================================================================================================= PROXY

def test_proxy_registration():
    """Subclasses of Proxies are automatically hooked up to Proxy.of for their __bases__"""
    class Original:
        pass

    Proxy.register(Original)

    obj = Original()
    proxy_obj = Proxy.of(obj)

    assert isinstance(proxy_obj, Proxy)
    assert proxy_obj._proxied_obj is obj


def test_proxy_unknown_class():
    """Proxy.of only proxies registered classes"""
    class Original:
        pass

    obj = Original()
    with pytest.raises(ValueError):
        Proxy.of(obj)


def test_proxy_nesting():
    """By default Proxy.of doesn't unwrap; proxying a proxy creates a nested proxy"""
    class Original:
        pass

    Proxy.register(Original)

    data = [3, 4, 5]
    obj = Original()
    obj.some_attr = data
    first_proxy = Proxy.of(obj)
    second_proxy = Proxy.of(first_proxy)
    assert isinstance(second_proxy, Proxy)
    assert second_proxy._proxied_obj is first_proxy
    assert second_proxy.some_attr is data


def test_proxy_unwrapping():
    """Proxy.of can unwrap proxied objects to prevent nesting"""
    class Original:
        pass

    Proxy.register(Original)

    obj = Original()
    first_proxy = Proxy.of(obj)
    second_proxy = Proxy.of(first_proxy, unwrap=True)

    assert isinstance(second_proxy, Proxy)
    assert second_proxy._proxied_obj is obj


def test_proxy_attr_intercept():
    """Proxy only intercepts whitelisted attributes, otherwise defers to the proxied object"""

    class Original:
        class_attr = [3, 4, 5]

        def __init__(self):
            self._name = "original"
            self._data = "blah"

        @property
        def read_only(self):
            return "read-only"

        @property
        def data(self):
            return self._data

        @data.setter
        def data(self, value):
            self._data = value

        def method(self, x, y):
            return str(x + y) in self.data

        def say_name(self):
            return f"I am {self._name}"

    Proxy.register(Original)

    obj = Original()
    proxy = Proxy.of(obj)

    # getattr on property
    assert proxy.data == "blah"
    obj.data = "foo"
    assert proxy.data == "foo"

    # setattr on property
    proxy.data = "hello, world"
    assert obj.data == "hello, world"

    # class attributes
    assert proxy.class_attr is Original.class_attr

    # method call
    assert not proxy.method(3, 4)
    proxy.data = "7"
    assert proxy.method(3, 4)

    # method call with reference to proxy attribute
    assert proxy.say_name() == "I am original"

    # setattr whitelisted attribute
    proxy._name = "proxy"
    assert proxy.say_name() == "I am proxy"
    assert obj.say_name() == "I am original"

    # delattr passes through
    del proxy._data
    assert not hasattr(obj, "_data")

    # delattr on whitelist attribute
    del proxy._name
    assert obj._name == "original"


def test_proxy_register_twice():
    """Multiple register calls is a no-op"""
    class Original:
        pass

    proxy_cls = Proxy.register(Original)
    same_proxy_cls = Proxy.register(Original)
    assert proxy_cls is same_proxy_cls


def test_proxy_register_name():
    """Proxy.register can provide a custom name for __repr__/__str__ purposes"""
    class Original:
        pass

    name = "MyProxyClassHere"
    proxy_cls = Proxy.register(Original, name=name)
    assert proxy_cls.__name__ == name


def test_proxy_register_custom_cls():
    """Proxy.register can provide an existing proxy class"""
    class Original:
        pass

    class MyCustomProxy:
        def __init__(self, my_obj):
            # Note that this doesn't follow the whitelist rules from
            # Proxy, since it's a custom class
            self.my_obj = my_obj

    proxy_cls = Proxy.register(Original, proxy_cls=MyCustomProxy)
    obj = Original()
    assert not hasattr(obj, "my_obj")
    proxy = Proxy.of(obj)

    assert proxy.my_obj is obj
    assert not hasattr(obj, "my_obj")

    assert not isinstance(proxy, Proxy)
    assert proxy_cls is MyCustomProxy


# END PROXY ================================================================================================= END PROXY


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
