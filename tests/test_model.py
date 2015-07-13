import uuid
from bloop import Column, UUID, Boolean, DateTime, String
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
