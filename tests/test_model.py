import uuid
import bloop.column
import bloop.index
import bloop.model
import pytest

from bloop import (Column, UUID, Boolean, DateTime, String,
                   LocalSecondaryIndex, GlobalSecondaryIndex)
missing = object()


def test_default_model_init(User):
    ''' Missing attributes aren't set to `None` or any other placeholder '''
    user = User(id=uuid.uuid4(), email='user@domain.com')
    assert user.email == 'user@domain.com'
    assert getattr(user, 'name', missing) is missing


def test_load_default_init(engine, local_bind):
    ''' The default model loader uses the model's __init__ method '''
    loader_calls = 0

    class CustomUser(engine.model):
        id = Column(UUID, hash_key=True)
        admin = Column(Boolean)
        joined = Column(DateTime)
        email = Column(String)
    engine.bind()

    def load_user(**kwargs):
        nonlocal loader_calls
        loader_calls += 1
        user = CustomUser()
        for key, value in kwargs.items():
            setattr(user, key, value)
        return user
    CustomUser.Meta.bloop_init = load_user

    user_id = uuid.uuid4()

    user = {
        'id': {'S': str(user_id)},
        'admin': {'BOOL': False},
        'extra_field': {'N': '0.125'}
    }

    loaded_user = CustomUser.__load__(user)
    assert loader_calls == 1
    assert loaded_user.id == user_id
    assert loaded_user.admin is False
    # Values that aren't explicitly described by the model aren't passed to
    # the custom loader
    assert getattr(loaded_user, 'extra_field', missing) is missing


def test_load_dump(User):
    ''' __load__ and __dump__ should be symmetric '''

    user_id = uuid.uuid4()
    user = User(id=user_id, name='name', email='user@domain.com', age=25)
    serialized_user = {
        'id': {'S': str(user_id)},
        'age': {'N': '25'},
        'name': {'S': 'name'},
        'email': {'S': 'user@domain.com'}
    }

    assert User.__load__(serialized_user) == user
    assert User.__dump__(user) == serialized_user


def test_equality(User):
    user_id = uuid.uuid4()
    user = User(id=user_id, name='name', email='user@domain.com', age=25)
    same = User(id=user_id, name='name', email='user@domain.com', age=25)
    other = User(id=user_id, name='wrong', email='user@domain.com', age=25)
    another = User(id=user_id, email='user@domain.com', age=25)

    # Wrong type
    assert not(user == 'foo')
    assert user != 'foo'

    # Attr with different value
    assert not(user == other)
    assert user != other

    # Missing an attr
    assert not(user == another)
    assert user != another

    assert user == same


def test_multiple_base_models(engine):
    ''' Once an engine has a `model` attr, BaseModel should always throw '''
    with pytest.raises(ValueError):
        bloop.model.BaseModel(engine)


def test_meta_read_write_units(engine):
    '''
    If `read_units` or `write_units` is missing from a model's Meta,
    it defaults to 1
    '''
    class Model(engine.model):
        id = Column(UUID, hash_key=True)

    assert Model.Meta.write_units == 1
    assert Model.Meta.read_units == 1

    class Other(engine.model):
        class Meta:
            read_units = 2
            write_units = 3
        id = Column(UUID, hash_key=True)

    assert Other.Meta.write_units == 3
    assert Other.Meta.read_units == 2


def test_meta_indexes_columns(User):
    ''' An index should not be considered a Column, even if it subclasses '''
    assert User.by_email not in set(User.Meta.columns)
    assert User.by_email in set(User.Meta.indexes)


def test_meta_indexed_columns_indexes(engine):
    column = Column(UUID, hash_key=True, name='dynamo_name')

    class Model(engine.model):
        model_name = column

    assert Model.Meta.columns_by_model_name == {'model_name': column}
    assert Model.Meta.columns_by_dynamo_name == {'dynamo_name': column}


def test_meta_keys(engine):
    ''' Various combinations of hash and range keys (some impossible) '''
    hash_column = lambda: Column(UUID, hash_key=True)
    range_column = lambda: Column(UUID, range_key=True)

    class HashOnly(engine.model):
        h = hash_column()

    class RangeOnly(engine.model):
        r = range_column()

    class Neither(engine.model):
        pass

    class Both(engine.model):
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


def test_model_extra_keys(engine):
    with pytest.raises(ValueError):
        class DoubleHash(engine.model):
            id = Column(UUID, hash_key=True)
            other = Column(UUID, hash_key=True)

    with pytest.raises(ValueError):
        class DoubleRange(engine.model):
            id = Column(UUID, range_key=True)
            other = Column(UUID, range_key=True)


def test_invalid_local_index(engine):
    with pytest.raises(ValueError):
        class InvalidIndex(engine.model):
            id = Column(UUID, hash_key=True)
            index = LocalSecondaryIndex(range_key='id')


def test_index_keys(engine):
    ''' Make sure index hash and range keys are objects, not strings '''
    class Model(engine.model):
        id = Column(UUID, hash_key=True)
        other = Column(DateTime, range_key=True)
        another = Column(UUID)
        last = Column(String)

        by_last = GlobalSecondaryIndex(hash_key='another', range_key='last')
        by_another = LocalSecondaryIndex(range_key='last')

    assert Model.by_last.hash_key is Model.another
    assert Model.by_last.range_key is Model.last

    assert Model.by_another.hash_key is Model.id
    assert Model.by_another.range_key is Model.last


def test_local_index_no_range_key(engine):
    ''' A table range_key is required to specify a LocalSecondaryIndex '''
    with pytest.raises(ValueError):
        class Model(engine.model):
            id = Column(UUID, hash_key=True)
            another = Column(UUID)
            by_another = LocalSecondaryIndex(range_key='another')


def test_abstract_index(engine):
    ''' Can't use a direct Index, since it's abstract '''
    with pytest.raises(ValueError):
        class Model(engine.model):
            id = Column(UUID, hash_key=True)
            another = Column(UUID)
            by_another = bloop.index.Index(hash_key='another')


def test_index_projections(engine):
    ''' Make sure index projections are calculated to include table keys '''
    Global, Local = GlobalSecondaryIndex, LocalSecondaryIndex

    class Model(engine.model):
        id = Column(UUID, hash_key=True)
        other = Column(UUID, range_key=True)
        another = Column(UUID)
        date = Column(DateTime)
        boolean = Column(Boolean)

        g_all = Global(hash_key='another', range_key='date', projection='all')
        g_key = Global(hash_key='another', projection='keys_only')
        g_inc = Global(hash_key='other', projection=['another', 'date'])

        l_all = Local(range_key='another', projection='all')
        l_key = Local(range_key='another', projection='keys_only')
        l_inc = Local(range_key='another', projection=['date'])

    uuids = set([Model.id, Model.other, Model.another])
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


def test_meta_table_name(engine):
    '''
    If table_name is missing from a model's Meta, use the model's __name__
    '''
    class Model(engine.model):
        id = Column(UUID, hash_key=True)

    assert Model.Meta.table_name == 'Model'

    class Other(engine.model):
        class Meta:
            table_name = 'table_name'
            write_units = 3
        id = Column(UUID, hash_key=True)

    assert Other.Meta.table_name == 'table_name'
