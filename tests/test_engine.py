import bloop
import bloop.engine
import bloop.exceptions
import bloop.tracking
import bloop.util
import pytest
import uuid


def test_missing_objects(User, engine):
    """
    When objects aren't loaded, ObjectsNotFound is raised with a list of
    missing objects
    """
    # Patch batch_get_items to return no results
    engine.client.batch_get_items = lambda *a, **kw: {}

    users = [User(id=uuid.uuid4()) for _ in range(3)]

    with pytest.raises(bloop.exceptions.NotModified) as excinfo:
        engine.load(users)

    assert set(excinfo.value.objects) == set(users)


def test_dump_key(User, engine, local_bind):
    base = bloop.new_base()

    class HashAndRange(base):
        foo = bloop.Column(bloop.Integer, hash_key=True)
        bar = bloop.Column(bloop.Integer, range_key=True)
    with local_bind():
        engine.bind(base=base)

    user = User(id=uuid.uuid4())
    user_key = {"id": {"S": str(user.id)}}
    assert bloop.engine._dump_key(engine, user) == user_key

    obj = HashAndRange(foo=4, bar=5)
    obj_key = {"bar": {"N": "5"}, "foo": {"N": "4"}}
    assert bloop.engine._dump_key(engine, obj) == obj_key


def test_load_object(User, engine):
    user_id = uuid.uuid4()
    expected = {"User": {"Keys": [{"id": {"S": str(user_id)}}],
                         "ConsistentRead": True}}
    response = {"User": [{"age": {"N": 5},
                          "name": {"S": "foo"},
                          "id": {"S": str(user_id)}}]}

    def respond(input):
        assert input == expected
        return response
    engine.client.batch_get_items = respond

    user = User(id=user_id)
    engine.load(user, consistent=True)

    assert user.age == 5
    assert user.name == "foo"
    assert user.id == user_id


def test_load_objects(User, engine):
    user1 = User(id=uuid.uuid4())
    user2 = User(id=uuid.uuid4())
    expected = {"User": {"Keys": [{"id": {"S": str(user1.id)}},
                                  {"id": {"S": str(user2.id)}}],
                         "ConsistentRead": False}}
    response = {"User": [{"age": {"N": 5},
                          "name": {"S": "foo"},
                          "id": {"S": str(user1.id)}},
                         {"age": {"N": 10},
                          "name": {"S": "bar"},
                          "id": {"S": str(user2.id)}}]}

    def respond(input):
        assert bloop.util.ordered(input) == bloop.util.ordered(expected)
        return response
    engine.client.batch_get_items = respond

    engine.load((user1, user2))

    assert user1.age == 5
    assert user1.name == "foo"
    assert user2.age == 10
    assert user2.name == "bar"


def test_load_duplicate_objects(User, engine):
    """Duplicate objects are handled correctly when loading"""
    user = User(id=uuid.uuid4())
    expected = {"User": {"Keys": [{"id": {"S": str(user.id)}}],
                         "ConsistentRead": False}}
    response = {"User": [{"age": {"N": 5},
                          "name": {"S": "foo"},
                          "id": {"S": str(user.id)}}]}

    def respond(input):
        assert bloop.util.ordered(input) == bloop.util.ordered(expected)
        return response
    engine.client.batch_get_items = respond

    engine.load((user, user))

    assert user.age == 5
    assert user.name == "foo"


def test_load_missing_attrs(User, engine):
    """
    When an instance of a Model is loaded into, existing attributes should be
    overwritten with new values, or if there is no new value, should be deleted
    """
    obj = User(id=uuid.uuid4(), age=4, name="user")

    response = {"User": [{"age": {"N": 5},
                          "id": {"S": str(obj.id)}}]}

    engine.client.batch_get_items = lambda input: response
    engine.load(obj)
    assert obj.age == 5
    assert obj.name is None


def test_load_dump_unbound(engine):
    class Model(bloop.new_base()):
        id = bloop.Column(bloop.UUID, hash_key=True)
        counter = bloop.Column(bloop.Integer)
    obj = Model(id=uuid.uuid4(), counter=5)
    value = {"User": [{"counter": {"N": 5}, "id": {"S": str(obj.id)}}]}

    with pytest.raises(bloop.exceptions.UnboundModel) as excinfo:
        engine._load(Model, value)
    assert excinfo.value.model is Model
    assert excinfo.value.obj is None

    with pytest.raises(bloop.exceptions.UnboundModel) as excinfo:
        engine._dump(Model, obj)
    assert excinfo.value.model is Model
    assert excinfo.value.obj is obj


def test_load_dump_unknown(engine):
    class NotModeled:
        pass
    obj = NotModeled()
    value = {"User": [{"age": {"N": 5},
                       "name": {"S": "foo"},
                       "id": {"S": str(uuid.uuid4())}}]}

    with pytest.raises(ValueError):
        engine._load(NotModeled, value)
    with pytest.raises(ValueError):
        engine._dump(NotModeled, obj)


def test_load_missing_key(engine, User, ComplexModel):
    """Trying to load objects with missing hash and range keys raises"""
    user = User(age=2)
    with pytest.raises(ValueError):
        engine.load(user)

    complex_models = [
        ComplexModel(),
        ComplexModel(name="no range"),
        ComplexModel(date="no hash")
    ]
    for model in complex_models:
        with pytest.raises(ValueError):
            engine.load(model)


def test_atomic_load(User, atomic, renderer):
    """Loading objects in an atomic context caches the loaded condition"""
    user_id = uuid.uuid4()
    obj = User(id=user_id)

    # Load may not return fields, in the case of missing data
    # (or non-mapped data, in the case of multi-view tables)
    response = {"User": [{"age": {"N": 5},
                          "id": {"S": str(obj.id)},
                          "extra_field": {"freeform data": "not parsed"}}]}

    atomic.client.batch_get_items = lambda input: response
    atomic.load(obj)

    condition = ('((#n0 = :v1) AND (attribute_not_exists(#n2)) '
                 'AND (#n3 = :v4) AND (attribute_not_exists(#n5))'
                 ' AND (attribute_not_exists(#n6)))')
    expected = {
        'ExpressionAttributeValues': {
            ':v4': {'S': str(user_id)},
            ':v1': {'N': '5'}},
        'ExpressionAttributeNames': {
            '#n2': 'email', '#n0': 'age', '#n5': 'j',
            '#n6': 'name', '#n3': 'id'},
        'ConditionExpression': condition}

    actual_condition = bloop.tracking.get_snapshot(obj)
    renderer.render(actual_condition, "condition")
    assert expected == renderer.rendered


def test_update_noop_save(engine, User):
    """ Saves should send all fields that have been set, every time """
    user = User(id=uuid.uuid4(), age=5)

    expected = {
        "Key": {"id": {"S": str(user.id)}},
        "TableName": "User",
        "ExpressionAttributeNames": {"#n0": "age"},
        "ExpressionAttributeValues": {":v1": {"N": "5"}},
        "UpdateExpression": "SET #n0=:v1"}
    calls = 0

    def validate(item):
        assert item == expected
        nonlocal calls
        calls += 1

    engine.client.update_item = validate
    engine.save(user)
    engine.save(user)
    assert calls == 2


def test_save_multiple_condition(User, engine):
    users = [User(id=uuid.uuid4()) for _ in range(3)]
    condition = User.id.is_(None)

    expected = [{"ConditionExpression": "(attribute_not_exists(#n0))",
                 "ExpressionAttributeNames": {"#n0": "id"},
                 "Key": {"id": {"S": str(user.id)}},
                 "TableName": "User"} for user in users]
    calls = 0

    def validate(item):
        assert item in expected
        nonlocal calls
        calls += 1

    engine.client.update_item = validate
    engine.save(users, condition=condition)
    assert calls == 3


def test_save_condition(User, engine):
    user_id = uuid.uuid4()
    user = User(id=user_id)
    condition = User.id.is_(None)
    expected = {"TableName": "User",
                "ExpressionAttributeNames": {"#n0": "id"},
                "ConditionExpression": "(attribute_not_exists(#n0))",
                "Key": {"id": {"S": str(user_id)}}}

    def validate(item):
        assert item == expected
    engine.client.update_item = validate
    engine.save(user, condition=condition)


def test_save_atomic_new(User, engine):
    """
    When an object is first created, an atomic save should expect no columns
    to exist.
    """
    user_id = uuid.uuid4()
    user = User(id=user_id)
    expected = {
        'ExpressionAttributeNames': {
            '#n0': 'age', '#n3': 'j', '#n1': 'email',
            '#n4': 'name', '#n2': 'id'},
        'Key': {'id': {'S': str(user_id)}},
        'TableName': 'User',
        'ConditionExpression': (
            '((attribute_not_exists(#n0)) AND (attribute_not_exists(#n1)) '
            'AND (attribute_not_exists(#n2)) AND (attribute_not_exists(#n3))'
            ' AND (attribute_not_exists(#n4)))')}

    def validate(item):
        assert item == expected
    engine.client.update_item = validate
    engine.config["atomic"] = True
    engine.save(user)


def test_save_atomic_update_condition(User, atomic):
    user_id = uuid.uuid4()
    user = User(id=user_id)
    bloop.tracking.sync(user, atomic)

    user.name = "new_foo"

    expected = {
        "ExpressionAttributeNames": {"#n2": "id", "#n0": "name"},
        "TableName": "User",
        "ExpressionAttributeValues": {":v4": {"S": "expect_foo"},
                                      ":v1": {"S": "new_foo"},
                                      ":v3": {"S": str(user_id)}},
        'ConditionExpression': "((#n2 = :v3) AND (#n0 = :v4))",
        "UpdateExpression": "SET #n0=:v1",
        "Key": {"id": {"S": str(user_id)}}}
    called = False

    def validate(item):
        nonlocal called
        called = True
        assert item == expected
    atomic.client.update_item = validate
    atomic.save(user, condition=User.name == "expect_foo")
    assert called


def test_save_multiple(User, engine):
    user1 = User(id=uuid.uuid4())
    user2 = User(id=uuid.uuid4())

    expected = [
        {"Key": {"id": {"S": str(user1.id)}}, "TableName": "User"},
        {"Key": {"id": {"S": str(user2.id)}}, "TableName": "User"}]
    calls = 0

    def validate(item):
        assert item in expected
        nonlocal calls
        calls += 1
    engine.client.update_item = validate
    engine.save((user1, user2))
    assert calls == 2


def test_save_update_condition_key_only(User, engine):
    """
    Even when the diff is empty, an UpdateItem should be issued
    (in case this is really a create - the item doesn't exist yet)
    """
    user = User(id=uuid.uuid4())
    condition = User.id.is_(None)
    expected = {"ConditionExpression": "(attribute_not_exists(#n0))",
                "TableName": "User",
                "ExpressionAttributeNames": {"#n0": "id"},
                "Key": {"id": {"S": str(user.id)}}}

    def validate(item):
        assert item == expected
    engine.client.update_item = validate
    engine.save(user, condition=condition)


def test_save_update_condition(User, engine):
    """
    Non-empty diff
    """
    user = User(id=uuid.uuid4(), age=4)
    condition = User.id.is_(None)
    expected = {"ConditionExpression": "(attribute_not_exists(#n2))",
                "ExpressionAttributeNames": {"#n2": "id", "#n0": "age"},
                "TableName": "User",
                "Key": {"id": {"S": str(user.id)}},
                "ExpressionAttributeValues": {":v1": {"N": "4"}},
                "UpdateExpression": "SET #n0=:v1"}

    def validate(item):
        assert item == expected
    engine.client.update_item = validate
    engine.save(user, condition=condition)


def test_save_update_multiple(User, engine):
    user1 = User(id=uuid.uuid4(), age=4)
    user2 = User(id=uuid.uuid4(), age=5)

    expected = [
        {"UpdateExpression": "SET #n0=:v1",
         "Key": {"id": {"S": str(user1.id)}},
         "TableName": "User",
         "ExpressionAttributeNames": {"#n0": "age"},
         "ExpressionAttributeValues": {":v1": {"N": "4"}}},
        {"UpdateExpression": "SET #n0=:v1",
         "Key": {"id": {"S": str(user2.id)}},
         "TableName": "User",
         "ExpressionAttributeNames": {"#n0": "age"},
         "ExpressionAttributeValues": {":v1": {"N": "5"}}}
    ]
    calls = 0

    def validate(item):
        nonlocal calls
        calls += 1
        assert item in expected
        expected.remove(item)
    engine.client.update_item = validate
    engine.save((user1, user2))
    assert calls == 2


def test_save_set_del_field(User, engine):
    """ UpdateItem can REMOVE fields as well as SET """
    user = User(id=uuid.uuid4(), age=4)

    for field in [User.id, User.age, User.email]:
        bloop.tracking.mark(user, field)

    # Expect to see a REMOVE on age, and a SET on email
    del user.age
    user.email = "foo@domain.com"

    expected = {"Key": {"id": {"S": str(user.id)}},
                "ExpressionAttributeNames": {"#n0": "email", "#n2": "age"},
                "TableName": "User",
                "UpdateExpression": "SET #n0=:v1 REMOVE #n2",
                "ExpressionAttributeValues": {":v1": {"S": "foo@domain.com"}}}

    def validate(item):
        assert item == expected
    engine.client.update_item = validate
    engine.save(user)


def test_save_update_del_field(User, engine):
    user = User(id=uuid.uuid4(), age=4)

    # Manually snapshot so we think age is persisted
    bloop.tracking.sync(user, engine)

    # Expect to see a REMOVE on age, and a SET on email
    del user.age

    expected = {"Key": {"id": {"S": str(user.id)}},
                "ExpressionAttributeNames": {"#n0": "age"},
                "TableName": "User",
                "UpdateExpression": "REMOVE #n0"}

    def validate(item):
        assert item == expected
    engine.client.update_item = validate
    engine.save(user)


def test_delete_multiple_condition(User, engine):
    users = [User(id=uuid.uuid4()) for _ in range(3)]
    condition = User.id == "foo"
    expected = [{"Key": {"id": {"S": str(user.id)}},
                 "ExpressionAttributeValues": {":v1": {"S": "foo"}},
                 "ExpressionAttributeNames": {"#n0": "id"},
                 "ConditionExpression": "(#n0 = :v1)",
                 "TableName": "User"} for user in users]
    calls = 0

    def validate(item):
        assert item in expected
        nonlocal calls
        calls += 1

    engine.client.delete_item = validate
    engine.delete(users, condition=condition)
    assert calls == 3


def test_delete_condition(User, engine):
    user_id = uuid.uuid4()
    user = User(id=user_id)
    condition = User.id.is_(None)
    expected = {"TableName": "User",
                "ExpressionAttributeNames": {"#n0": "id"},
                "ConditionExpression": "(attribute_not_exists(#n0))",
                "Key": {"id": {"S": str(user_id)}}}

    def validate(item):
        assert item == expected
    engine.client.delete_item = validate
    engine.delete(user, condition=condition)


def test_delete_multiple(User, engine):
    user1 = User(id=uuid.uuid4())
    user2 = User(id=uuid.uuid4())

    expected = [
        {"Key": {"id": {"S": str(user1.id)}}, "TableName": "User"},
        {"Key": {"id": {"S": str(user2.id)}}, "TableName": "User"}]
    calls = 0

    def validate(item):
        assert item in expected
        nonlocal calls
        calls += 1
    engine.client.delete_item = validate
    engine.delete((user1, user2))
    assert calls == 2


def test_delete_atomic(User, atomic):
    user_id = uuid.uuid4()
    user = User(id=user_id)

    # Manually snapshot so we think age is persisted
    bloop.tracking.sync(user, atomic)

    expected = {
        'ConditionExpression': '(#n0 = :v1)',
        'ExpressionAttributeValues': {':v1': {'S': str(user_id)}},
        'TableName': 'User',
        'Key': {'id': {'S': str(user_id)}},
        'ExpressionAttributeNames': {'#n0': 'id'}}
    called = False

    def validate(item):
        nonlocal called
        called = True
        assert item == expected
    atomic.client.delete_item = validate
    atomic.delete(user)
    assert called


def test_delete_atomic_new(User, engine):
    """
    When an object is first created, an atomic delete should expect
    no columns to exist.
    """
    user_id = uuid.uuid4()
    user = User(id=user_id)
    expected = {
        'TableName': 'User',
        'ExpressionAttributeNames': {
            '#n2': 'id', '#n0': 'age', '#n4': 'name',
            '#n3': 'j', '#n1': 'email'},
        'Key': {'id': {'S': str(user_id)}},
        'ConditionExpression': (
            '((attribute_not_exists(#n0)) AND (attribute_not_exists(#n1)) '
            'AND (attribute_not_exists(#n2)) AND (attribute_not_exists(#n3))'
            ' AND (attribute_not_exists(#n4)))')}

    def validate(item):
        assert item == expected
    engine.client.delete_item = validate
    engine.config["atomic"] = True
    engine.delete(user)


def test_delete_new(User, engine):
    """
    When an object is first created, a non-atomic delete shouldn't expect
    anything.
    """
    user_id = uuid.uuid4()
    user = User(id=user_id)
    expected = {
        'TableName': 'User',
        'Key': {'id': {'S': str(user_id)}}}

    def validate(item):
        assert item == expected
    engine.client.delete_item = validate
    engine.config["atomic"] = False
    engine.delete(user)


def test_delete_atomic_condition(User, atomic):
    user_id = uuid.uuid4()
    user = User(id=user_id, email='foo@bar.com')

    # Manually snapshot so we think age is persisted
    bloop.tracking.sync(user, atomic)

    expected = {
        'ExpressionAttributeNames': {
            '#n2': 'id', '#n4': 'name', '#n0': 'email'},
        'ConditionExpression':
            '((#n0 = :v1) AND (#n2 = :v3) AND (#n4 = :v5))',
        'TableName': 'User',
        'ExpressionAttributeValues': {
            ':v5': {'S': 'foo'}, ':v1': {'S': 'foo@bar.com'},
            ':v3': {'S': str(user_id)}},
        'Key': {'id': {'S': str(user_id)}}}
    called = False

    def validate(item):
        nonlocal called
        called = True
        assert item == expected
    atomic.client.delete_item = validate
    atomic.delete(user, condition=User.name.is_("foo"))
    assert called


def test_query(User, engine):
    """ Engine.query supports model and index-based queries """
    index_query = engine.query(User.by_email)
    assert index_query.model is User
    assert index_query.index is User.by_email

    model_query = engine.query(User)
    assert model_query.model is User
    assert model_query.index is None


def test_scan(User, engine):
    """ Engine.scan supports model and index-based queries """
    index_scan = engine.scan(User.by_email)
    assert index_scan.model is User
    assert index_scan.index is User.by_email

    model_scan = engine.scan(User)
    assert model_scan.model is User
    assert model_scan.index is None


def test_context(User, engine):
    engine.config["atomic"] = True
    user_id = uuid.uuid4()
    user = User(id=user_id, name="foo")

    expected = {"TableName": "User",
                "UpdateExpression": "SET #n0=:v1",
                "ExpressionAttributeValues": {":v1": {"S": "foo"}},
                "ExpressionAttributeNames": {"#n0": "name"},
                "Key": {"id": {"S": str(user_id)}}}

    def validate(item):
        assert item == expected
    engine.client.update_item = validate

    with engine.context(atomic=False) as eng:
        eng.save(user)

    # EngineViews can't bind
    with pytest.raises(RuntimeError):
        with engine.context() as eng:
            eng.bind(base=bloop.new_base())


def test_unbound_engine_view(engine):
    """Trying to mutate an unbound model through an EngineView fails"""
    class UnboundModel(bloop.new_base()):
        id = bloop.Column(bloop.String, hash_key=True)
    instance = UnboundModel(id="foo")

    with pytest.raises(bloop.exceptions.UnboundModel):
        with engine.context() as view:
            view._dump(UnboundModel, instance)
