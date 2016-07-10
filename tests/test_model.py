import arrow
import uuid
import pytest
from unittest.mock import Mock

from bloop import (Column, UUID, Boolean, DateTime, String,
                   LocalSecondaryIndex, GlobalSecondaryIndex, new_base)

from test_models import User


def test_default_model_init():
    """ Missing attributes are set to `None` """
    user = User(id=uuid.uuid4(), email="user@domain.com")
    assert user.email == "user@domain.com"
    assert not hasattr(user, "name")


def test_load_default_init(engine):
    """ The default model loader uses the model's __init__ method """
    User.Meta.init = Mock()
    User.Meta.init.return_value = User()

    user_id = uuid.uuid4()
    now = arrow.now()

    user = {
        "id": {"S": str(user_id)},
        "j": {"S": now.isoformat()},
        "extra_field": {"N": "0.125"}
    }

    loaded_user = User._load(user, context={"engine": engine})
    assert loaded_user.id == user_id
    assert loaded_user.joined == now
    assert not hasattr(loaded_user, "extra_field")
    # No args, kwargs provided to custom init function
    User.Meta.init.assert_called_once_with()


def test_load_dump(engine):
    """ _load and _dump should be symmetric """
    user = User(id=uuid.uuid4(), name="name", email="user@domain.com", age=25,
                joined=arrow.now())
    serialized = {
        "id": {"S": str(user.id)},
        "age": {"N": "25"},
        "name": {"S": "name"},
        "email": {"S": "user@domain.com"},
        "j": {"S": user.joined.to("utc").isoformat()}
    }

    assert User._load(serialized, context={"engine": engine}) == user
    assert User._dump(user, context={"engine": engine}) == serialized


def test_equality():
    user_id = uuid.uuid4()
    user = User(id=user_id, name="name", email="user@domain.com", age=25)
    same = User(id=user_id, name="name", email="user@domain.com", age=25)
    other = User(id=user_id, name="wrong", email="user@domain.com", age=25)
    another = User(id=user_id, email="user@domain.com", age=25)

    # Wrong type
    assert not(user == "foo")
    assert user != "foo"

    # Attr with different value
    assert not(user == other)
    assert user != other

    # Missing an attr
    assert not(user == another)
    assert user != another

    assert user == same


def test_meta_read_write_units():
    """
    If `read_units` or `write_units` is missing from a model's Meta,
    it defaults to 1
    """
    class Model(new_base()):
        id = Column(UUID, hash_key=True)

    assert Model.Meta.write_units == 1
    assert Model.Meta.read_units == 1

    class Other(new_base()):
        class Meta:
            read_units = 2
            write_units = 3
        id = Column(UUID, hash_key=True)

    assert Other.Meta.write_units == 3
    assert Other.Meta.read_units == 2


def test_meta_indexes_columns():
    """ An index should not be considered a Column, even if it subclasses """
    assert User.by_email not in set(User.Meta.columns)
    assert User.by_email in set(User.Meta.indexes)


def test_meta_keys():
    """ Various combinations of hash and range keys (some impossible) """
    def hash_column():
        return Column(UUID, hash_key=True)

    def range_column():
        return Column(UUID, range_key=True)

    class HashOnly(new_base()):
        h = hash_column()

    class RangeOnly(new_base()):
        r = range_column()

    class Neither(new_base()):
        pass

    class Both(new_base()):
        h = hash_column()
        r = range_column()

    expect = [
        (HashOnly, (HashOnly.h, None)),
        (RangeOnly, (None, RangeOnly.r)),
        (Neither, (None, None)),
        (Both, (Both.h, Both.r))
    ]

    for (model, (hash_key, range_key)) in expect:
        assert model.Meta.hash_key is hash_key
        assert model.Meta.range_key is range_key


def test_model_extra_keys():
    with pytest.raises(ValueError):
        class DoubleHash(new_base()):
            id = Column(UUID, hash_key=True)
            other = Column(UUID, hash_key=True)

    with pytest.raises(ValueError):
        class DoubleRange(new_base()):
            id = Column(UUID, range_key=True)
            other = Column(UUID, range_key=True)


def test_invalid_local_index():
    with pytest.raises(ValueError):
        class InvalidIndex(new_base()):
            id = Column(UUID, hash_key=True)
            index = LocalSecondaryIndex(range_key="id", projection="keys")


def test_index_keys():
    """ Make sure index hash and range keys are objects, not strings """
    class Model(new_base()):
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
    """ A table range_key is required to specify a LocalSecondaryIndex """
    with pytest.raises(ValueError):
        class Model(new_base()):
            id = Column(UUID, hash_key=True)
            another = Column(UUID)
            by_another = LocalSecondaryIndex(range_key="another", projection="keys")


def test_index_projections():
    """ Make sure index projections are calculated to include table keys """
    Global, Local = GlobalSecondaryIndex, LocalSecondaryIndex

    class Model(new_base()):
        id = Column(UUID, hash_key=True)
        other = Column(UUID, range_key=True)
        another = Column(UUID)
        date = Column(DateTime)
        boolean = Column(Boolean)

        g_all = Global(hash_key="another", range_key="date", projection="all")
        g_key = Global(hash_key="another", projection="keys")
        g_inc = Global(hash_key="other", projection=["another", "date"])

        l_all = Local(range_key="another", projection="all")
        l_key = Local(range_key="another", projection="keys")
        l_inc = Local(range_key="another", projection=["date"])

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
    """
    If table_name is missing from a model's Meta, use the model's __name__
    """
    class Model(new_base()):
        id = Column(UUID, hash_key=True)

    assert Model.Meta.table_name == "Model"

    class Other(new_base()):
        class Meta:
            table_name = "table_name"
            write_units = 3
        id = Column(UUID, hash_key=True)

    assert Other.Meta.table_name == "table_name"


def test_abstract_not_inherited():
    base = new_base()

    class Concrete(base):
        pass

    assert base.Meta.abstract
    assert not Concrete.Meta.abstract
