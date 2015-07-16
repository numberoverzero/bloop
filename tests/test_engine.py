import bloop
import bloop.engine
import pytest
import uuid


def test_missing_objects(User, engine):
    '''
    When objects aren't loaded, ObjectsNotFound is raised with a list of
    missing objects
    '''
    # Patch batch_get_items to return no results
    engine.client.batch_get_items = lambda *a, **kw: {}

    users = [User(id=uuid.uuid4()) for _ in range(3)]

    with pytest.raises(bloop.engine.ObjectsNotFound) as excinfo:
        engine.load(users)

    assert set(excinfo.value.missing) == set(users)


def test_register_bound_model(User, engine):
    assert User in engine.models
    engine.register(User)
    assert User not in engine.unbound_models


def test_dump_key(User, engine, local_bind):
    class HashAndRange(engine.model):
        foo = bloop.Column(bloop.Integer, hash_key=True)
        bar = bloop.Column(bloop.Integer, range_key=True)
    engine.bind()

    user = User(id=uuid.uuid4())
    user_key = {'id': {'S': str(user.id)}}
    assert bloop.engine.dump_key(engine, user) == user_key

    obj = HashAndRange(foo=4, bar=5)
    obj_key = {'bar': {'N': '5'}, 'foo': {'N': '4'}}
    assert bloop.engine.dump_key(engine, obj) == obj_key


def test_load_object(User, engine):
    user_id = uuid.uuid4()
    expected = {'User': {'Keys': [{'id': {'S': str(user_id)}}],
                         'ConsistentRead': False}}
    response = {'User': [{'age': {'N': 5},
                          'name': {'S': 'foo'},
                          'id': {'S': str(user_id)}}]}

    def respond(input):
        assert input == expected
        return response
    engine.client.batch_get_items = respond

    user = User(id=user_id)
    engine.load(user)

    assert user.age == 5
    assert user.name == 'foo'
    assert user.id == user_id


def test_load_objects(User, engine, ordered):
    user1 = User(id=uuid.uuid4())
    user2 = User(id=uuid.uuid4())
    expected = {'User': {'Keys': [{'id': {'S': str(user1.id)}},
                                  {'id': {'S': str(user2.id)}}],
                         'ConsistentRead': False}}
    response = {'User': [{'age': {'N': 5},
                          'name': {'S': 'foo'},
                          'id': {'S': str(user1.id)}},
                         {'age': {'N': 10},
                          'name': {'S': 'bar'},
                          'id': {'S': str(user2.id)}}]}

    def respond(input):
        assert ordered(input) == ordered(expected)
        return response
    engine.client.batch_get_items = respond

    engine.load((user1, user2))

    assert user1.age == 5
    assert user1.name == 'foo'
    assert user2.age == 10
    assert user2.name == 'bar'


def test_load_missing_attrs(User, engine):
    '''
    When an instance of a Model is loaded into, existing attributes should be
    overwritten with new values, or if there is no new value, should be deleted
    '''
    obj = User(id=uuid.uuid4(), age=4, name='user')

    response = {'User': [{'age': {'N': 5},
                          'id': {'S': str(obj.id)}}]}

    engine.client.batch_get_items = lambda input: response
    engine.load(obj)
    assert obj.age == 5
    assert not hasattr(obj, 'name')


def test_load_dump_unbound(UnboundUser, engine):
    user_id = uuid.uuid4()
    user = UnboundUser(id=user_id, age=5, name='foo')
    value = {'User': [{'age': {'N': 5},
                       'name': {'S': 'foo'},
                       'id': {'S': str(user_id)}}]}

    with pytest.raises(RuntimeError):
        engine.__load__(UnboundUser, value)
    with pytest.raises(RuntimeError):
        engine.__dump__(UnboundUser, user)


def test_load_dump_unknown(engine):
    class NotModeled:
        pass
    obj = NotModeled()
    user_id = uuid.uuid4()
    value = {'User': [{'age': {'N': 5},
                       'name': {'S': 'foo'},
                       'id': {'S': str(user_id)}}]}

    with pytest.raises(ValueError):
        engine.__load__(NotModeled, value)
    with pytest.raises(ValueError):
        engine.__dump__(NotModeled, obj)


def test_illegal_save(User, engine):
    users = [User(id=uuid.uuid4()) for _ in range(3)]
    condition = User.id.is_(None)

    with pytest.raises(ValueError):
        engine.save(users, condition=condition)


def test_save_condition(User, engine):
    user_id = uuid.uuid4()
    user = User(id=user_id)
    condition = User.id.is_(None)
    expected = {'TableName': 'User',
                'ExpressionAttributeNames': {'#n0': 'id'},
                'ConditionExpression': '(attribute_not_exists(#n0))',
                'Item': {'id': {'S': str(user_id)}}}

    def validate(item):
        assert item == expected
    engine.client.put_item = validate
    engine.save(user, condition=condition)


def test_save_multiple(User, engine, ordered):
    user1 = User(id=uuid.uuid4())
    user2 = User(id=uuid.uuid4())

    expected = {'User': [
        {'PutRequest': {'Item': {'id': {'S': str(user1.id)}}}},
        {'PutRequest': {'Item': {'id': {'S': str(user2.id)}}}}]}

    def validate(items):
        assert ordered(items) == ordered(expected)
    engine.client.batch_write_items = validate
    engine.save((user1, user2))


def test_illegal_delete(User, engine):
    users = [User(id=uuid.uuid4()) for _ in range(3)]
    condition = User.id.is_(None)

    with pytest.raises(ValueError):
        engine.delete(users, condition=condition)


def test_delete_condition(User, engine):
    user_id = uuid.uuid4()
    user = User(id=user_id)
    condition = User.id.is_(None)
    expected = {'TableName': 'User',
                'ExpressionAttributeNames': {'#n0': 'id'},
                'ConditionExpression': '(attribute_not_exists(#n0))',
                'Key': {'id': {'S': str(user_id)}}}

    def validate(item):
        assert item == expected
    engine.client.delete_item = validate
    engine.delete(user, condition=condition)


def test_delete_multiple(User, engine, ordered):
    user1 = User(id=uuid.uuid4())
    user2 = User(id=uuid.uuid4())

    expected = {'User': [
        {'DeleteRequest': {'Key': {'id': {'S': str(user1.id)}}}},
        {'DeleteRequest': {'Key': {'id': {'S': str(user2.id)}}}}]}

    def validate(items):
        assert ordered(items) == ordered(expected)
    engine.client.batch_write_items = validate
    engine.delete((user1, user2))


def test_query(User, engine):
    ''' Engine.query supports model and index-based queries '''
    index_query = engine.query(User.by_email)
    assert index_query.model is User
    assert index_query.index is User.by_email

    model_query = engine.query(User)
    assert model_query.model is User
    assert model_query.index is None


def test_scan(User, engine):
    ''' Engine.scan supports model and index-based queries '''
    index_scan = engine.scan(User.by_email)
    assert index_scan.model is User
    assert index_scan.index is User.by_email

    model_scan = engine.scan(User)
    assert model_scan.model is User
    assert model_scan.index is None
