import datetime
import logging
import uuid
from unittest.mock import Mock

import pytest
from tests.helpers.models import ComplexModel, User, VectorModel

from bloop.engine import Engine, dump_key
from bloop.exceptions import (
    InvalidModel,
    InvalidStream,
    InvalidTemplate,
    MissingKey,
    MissingObjects,
    UnknownType,
)
from bloop.models import BaseModel, Column, GlobalSecondaryIndex
from bloop.session import SessionWrapper
from bloop.signals import object_saved
from bloop.types import DateTime, Integer, String, Timestamp
from bloop.util import ordered


def test_default_table_name_template(dynamodb, dynamodbstreams, session):
    """When no table_name_template is provided, the default of '{table_name}' is used"""
    class LocalModel(BaseModel):
        class Meta:
            table_name = "my-table-name"
        id = Column(Integer, hash_key=True)

    engine = Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)
    # Replace mock clients immediately
    engine.session = session

    engine.bind(LocalModel)
    session.create_table.assert_called_once_with("my-table-name", LocalModel)
    session.validate_table.assert_called_once_with("my-table-name", LocalModel)


def test_str_table_name_template(dynamodb, dynamodbstreams, session):
    """When a string is provided for table_name_template, .format is called on it with the key table_name"""
    class LocalModel(BaseModel):
        class Meta:
            table_name = "my-table-name"
        id = Column(Integer, hash_key=True)

    template = "prefix-{table_name}"
    engine = Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams, table_name_template=template)
    # Replace mock clients immediately
    engine.session = session

    engine.bind(LocalModel)
    session.create_table.assert_called_once_with("prefix-my-table-name", LocalModel)
    session.validate_table.assert_called_once_with("prefix-my-table-name", LocalModel)


def test_malformed_table_name_template(dynamodb, dynamodbstreams):
    """When a string is provided that doesn't have the correct format key {table_name}, InvalidTemplate is raised"""
    template = "prefix-{wrong_key}"
    with pytest.raises(InvalidTemplate):
        Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams, table_name_template=template)


def test_wrong_type_table_name_template(dynamodb, dynamodbstreams):
    """When the template is neither a string nor a function is provided, ValueError is raised."""
    template = object()
    with pytest.raises(ValueError):
        # noinspection PyTypeChecker
        Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams, table_name_template=template)


def test_func_table_name_template(dynamodb, dynamodbstreams, session):
    """When a function is provided for table_name_template, it is called with the model as its sole argument."""
    class LocalModel(BaseModel):
        class Meta:
            table_name = "my-table-name"
        id = Column(Integer, hash_key=True)

    def template(model):
        assert issubclass(model, BaseModel)
        return "reverse-" + model.Meta.table_name[::-1]
    engine = Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams, table_name_template=template)
    # Replace mock clients immediately
    engine.session = session

    engine.bind(LocalModel)
    expected = "reverse-eman-elbat-ym"
    session.create_table.assert_called_once_with(expected, LocalModel)
    session.validate_table.assert_called_once_with(expected, LocalModel)


def test_missing_objects(engine, session, caplog):
    """When objects aren't loaded, MissingObjects is raised with a list of missing objects"""
    # Patch batch_get_items to return no results
    session.load_items.return_value = {}

    users = [User(id=str(i)) for i in range(3)]

    with pytest.raises(MissingObjects) as excinfo:
        engine.load(*users)
    assert set(excinfo.value.objects) == set(users)

    assert caplog.record_tuples == [
        ("bloop.engine", logging.WARNING, "loaded 0 of 3 objects")
    ]


def test_dump_key(engine):
    class HashAndRange(BaseModel):
        foo = Column(Integer, hash_key=True)
        bar = Column(Integer, range_key=True)
    engine.bind(HashAndRange)

    user = User(id="foo")
    user_key = {"id": {"S": "foo"}}
    assert dump_key(engine, user) == user_key

    obj = HashAndRange(foo=4, bar=5)
    obj_key = {"bar": {"N": "5"}, "foo": {"N": "4"}}
    assert dump_key(engine, obj) == obj_key


def test_load_object(engine, session):
    user_id = "user_id"
    expected = {
        "User": {
            "Keys": [{"id": {"S": "user_id"}}],
            "ConsistentRead": True
        }
    }
    response = {
        "User": [{"age": {"N": 5}, "name": {"S": "foo"}, "id": {"S": "user_id"}}]
    }

    def respond(RequestItems):
        assert RequestItems == expected
        return response

    session.load_items.side_effect = respond
    user = User(id=user_id)
    engine.load(user, consistent=True)

    assert user.age == 5
    assert user.name == "foo"
    assert user.id == user_id


def test_load_objects(engine, session):
    user1 = User(id="user1")
    user2 = User(id="user2")
    expected = {
        "User": {
            "Keys": [
                {"id": {"S": "user1"}},
                {"id": {"S": "user2"}}
            ],
            "ConsistentRead": False
        }
    }
    response = {
        "User": [
            {"age": {"N": 5}, "name": {"S": "foo"}, "id": {"S": "user1"}},
            {"age": {"N": 10}, "name": {"S": "bar"}, "id": {"S": "user2"}}
        ]
    }

    def respond(RequestItems):
        assert ordered(RequestItems) == ordered(expected)
        return response

    session.load_items.side_effect = respond
    engine.load(user1, user2)

    assert user1.age == 5
    assert user1.name == "foo"
    assert user2.age == 10
    assert user2.name == "bar"


def test_load_repeated_objects(engine, session):
    """The same object is only loaded once"""
    user = User(id="user_id")
    expected = {
        "User": {
            "Keys": [{"id": {"S": user.id}}],
            "ConsistentRead": False}
    }
    response = {
        "User": [{"age": {"N": 5}, "name": {"S": "foo"}, "id": {"S": user.id}}],
    }

    def respond(RequestItems):
        assert ordered(RequestItems) == ordered(expected)
        return response

    session.load_items.side_effect = respond
    engine.load(user, user)

    assert user.age == 5
    assert user.name == "foo"


def test_load_equivalent_objects(engine, session):
    """Two objects with the same key are both loaded"""
    user = User(id="user_id")
    same_user = User(id=user.id)

    expected = {
        "User": {
            "Keys": [{"id": {"S": user.id}}],
            "ConsistentRead": False
        }
    }
    response = {
        "User": [{"age": {"N": 5}, "name": {"S": "foo"}, "id": {"S": user.id}}]
    }

    def respond(RequestItems):
        assert ordered(RequestItems) == ordered(expected)
        return response

    session.load_items.side_effect = respond
    engine.load(user, same_user)

    assert user.age == 5
    assert user.name == "foo"
    assert same_user.age == 5
    assert same_user.name == "foo"


def test_load_shared_table(engine, session, caplog):
    """Two different models backed by the same table try to load the same hash key.
    They share the column "shared" but load the content differently
    """
    class FirstModel(BaseModel):
        class Meta:
            table_name = "SharedTable"
        id = Column(String, hash_key=True)
        range = Column(String, range_key=True)
        first = Column(String)
        as_date = Column(DateTime, dynamo_name="shared")

    class SecondModel(BaseModel):
        class Meta:
            table_name = "SharedTable"

        id = Column(String, hash_key=True)
        range = Column(String, range_key=True)
        second = Column(String)
        as_string = Column(String, dynamo_name="shared")
    engine.bind(BaseModel)

    id = "foo"
    range = "bar"
    now = datetime.datetime.now(datetime.timezone.utc)
    now_str = now.isoformat()
    session.load_items.return_value = {
        "SharedTable": [{
            "id": {"S": id},
            "range": {"S": range},
            "first": {"S": "first"},
            "second": {"S": "second"},
            "shared": {"S": now_str}}]
    }

    first = FirstModel(id=id, range=range)
    second = SecondModel(id=id, range=range)

    caplog.clear()
    engine.load(first, second)

    expected_first = FirstModel(id=id, range=range, first="first", as_date=now)
    expected_second = SecondModel(id=id, range=range, second="second", as_string=now_str)

    missing = object()
    for attr in (c.name for c in FirstModel.Meta.columns):
        assert getattr(first, attr, missing) == getattr(expected_first, attr, missing)
    for attr in (c.name for c in SecondModel.Meta.columns):
        assert getattr(second, attr, missing) == getattr(expected_second, attr, missing)
    assert not hasattr(first, "second")
    assert not hasattr(second, "first")

    assert caplog.record_tuples == [
        ("bloop.engine", logging.INFO, "successfully loaded 2 objects")
    ]


def test_load_missing_attrs(engine, session):
    """When an instance of a Model is loaded into, existing attributes should be
    overwritten with new values, or if there is no new value, should be deleted
    """
    obj = User(id="user_id", age=4, name="user")

    response = {
        "User": [{"age": {"N": 5}, "id": {"S": obj.id}}]
    }

    session.load_items.return_value = response
    engine.load(obj)
    assert obj.age == 5
    assert obj.name == ""


def test_load_dump_unbound(engine):
    class Model(BaseModel):
        id = Column(Integer, hash_key=True)
    obj = Model(id=5)
    value = {"id": {"N": "5"}}

    loaded = engine._load(Model, value)
    assert loaded.id == 5

    dumped = engine._dump(Model, obj)
    assert dumped == {"id": {"N": "5"}}


def test_load_dump_subclass(engine):
    """Both immediate and inherited Columns should be dumped"""

    class Admin(User):
        admin_id = Column(Integer, hash_key=True)
        other = Column(Integer)
    engine.bind(User)

    admin = Admin(admin_id=3)
    # Set an attribute for a column on the parent class
    admin.email = "admin@domain.com"

    dumped_admin = {"admin_id": {"N": "3"}, "email": {"S": "admin@domain.com"}}
    assert engine._dump(Admin, admin) == dumped_admin

    # Inject a value that for a column on the parent class
    dumped_admin["email"] = {"S": "support@foo.com"}
    same_admin = engine._load(Admin, dumped_admin)
    assert same_admin.email == "support@foo.com"


def test_load_dump_unknown(engine):
    class NotModeled:
        pass
    obj = NotModeled()
    value = {
        "age": {"N": 5},
        "name": {"S": "foo"},
        "id": {"S": "user_id"}
    }

    with pytest.raises(UnknownType):
        engine._load(NotModeled, value)
    with pytest.raises(UnknownType):
        engine._dump(NotModeled, obj)


def test_load_missing_key(engine):
    """Trying to load objects with missing hash and range keys raises"""
    user = User(age=2)
    with pytest.raises(MissingKey):
        engine.load(user)

    complex_models = [
        ComplexModel(),
        ComplexModel(name=uuid.uuid4()),
        ComplexModel(date="no hash")
    ]
    for model in complex_models:
        with pytest.raises(MissingKey):
            engine.load(model)


def test_save_twice(engine, session):
    """Save sends full local values, not just deltas from last save"""
    user = User(id="user_id", age=5)
    expected = {
        "Key": {"id": {"S": user.id}},
        "TableName": "User",
        "ExpressionAttributeNames": {"#n0": "age"},
        "ExpressionAttributeValues": {":v1": {"N": "5"}},
        "UpdateExpression": "SET #n0=:v1"}
    engine.save(user)
    engine.save(user)

    session.save_item.assert_called_with(expected)
    assert session.save_item.call_count == 2


def test_save_list_with_condition(engine, session, caplog):
    users = [User(id=str(i)) for i in range(3)]
    condition = User.id.is_(None)
    expected_calls = [
        {
            "ConditionExpression": "(attribute_not_exists(#n0))",
            "ExpressionAttributeNames": {"#n0": "id"},
            "Key": {"id": {"S": user.id}},
            "TableName": "User"}
        for user in users]
    engine.save(*users, condition=condition)
    for expected in expected_calls:
        session.save_item.assert_any_call(expected)
    assert session.save_item.call_count == 3

    assert caplog.record_tuples[-1] == ("bloop.engine", logging.INFO, "successfully saved 3 objects")


def test_save_single_with_condition(engine, session):
    user = User(id="user_id")
    condition = User.id.is_(None)
    expected = {"TableName": "User",
                "ExpressionAttributeNames": {"#n0": "id"},
                "ConditionExpression": "(attribute_not_exists(#n0))",
                "Key": {"id": {"S": user.id}}}
    engine.save(user, condition=condition)
    session.save_item.assert_called_once_with(expected)


def test_save_atomic_new(engine, session):
    """atomic save on new object should expect no columns to exist"""
    user = User(id="user_id")
    expected = {
        "ExpressionAttributeNames": {
            "#n0": "age", "#n6": "j", "#n2": "email",
            "#n8": "name", "#n4": "id"},
        "Key": {"id": {"S": user.id}},
        "TableName": "User",
        "ConditionExpression": (
            "((attribute_not_exists(#n0)) AND (attribute_not_exists(#n2)) "
            "AND (attribute_not_exists(#n4)) AND (attribute_not_exists(#n6))"
            " AND (attribute_not_exists(#n8)))")}
    engine.save(user, atomic=True)
    session.save_item.assert_called_once_with(expected)


def test_save_atomic_condition(engine, session):
    user = User(id="user_id")
    # Tell the tracking system the user's id was saved to DynamoDB
    object_saved.send(engine, engine=engine, obj=user)
    # Mutate a field; part of the update but not an expected condition
    user.name = "new_foo"
    # Condition on the mutated field with a different value
    condition = User.name == "expect_foo"

    expected = {
        "ConditionExpression": "((#n0 = :v1) AND (#n2 = :v3))",
        "ExpressionAttributeNames": {"#n0": "name", "#n2": "id"},
        "ExpressionAttributeValues": {
            ":v1": {"S": "expect_foo"},
            ":v3": {"S": user.id},
            ":v4": {"S": "new_foo"}},
        "Key": {"id": {"S": user.id}},
        "TableName": "User",
        "UpdateExpression": "SET #n0=:v4"
    }
    engine.save(user, condition=condition, atomic=True)
    session.save_item.assert_called_once_with(expected)


def test_save_condition_key_only(engine, session):
    """Even when the diff is empty, an UpdateItem should be issued
    (in case this is really a create - the item doesn't exist yet)
    """
    user = User(id="user_id")
    condition = User.id.is_(None)
    expected = {
        "ConditionExpression": "(attribute_not_exists(#n0))",
        "TableName": "User",
        "ExpressionAttributeNames": {"#n0": "id"},
        "Key": {"id": {"S": user.id}}}
    engine.save(user, condition=condition)
    session.save_item.assert_called_once_with(expected)


def test_save_set_only(engine, session):
    user = User(id="user_id")

    # Expect a SET on email
    user.email = "foo@domain.com"

    expected = {
        "Key": {"id": {"S": user.id}},
        "ExpressionAttributeNames": {"#n0": "email"},
        "TableName": "User",
        "UpdateExpression": "SET #n0=:v1",
        "ExpressionAttributeValues": {":v1": {"S": "foo@domain.com"}}}
    engine.save(user)
    session.save_item.assert_called_once_with(expected)


def test_save_del_only(engine, session):
    user = User(id="user_id", age=4)

    # Expect a REMOVE on age
    del user.age

    expected = {
        "Key": {"id": {"S": user.id}},
        "ExpressionAttributeNames": {"#n0": "age"},
        "TableName": "User",
        "UpdateExpression": "REMOVE #n0"}
    engine.save(user)
    session.save_item.assert_called_once_with(expected)


def test_delete_multiple_condition(engine, session, caplog):
    users = [User(id=str(i)) for i in range(3)]
    condition = User.id == "foo"
    expected_calls = [
        {"Key": {"id": {"S": user.id}},
         "ExpressionAttributeValues": {":v1": {"S": "foo"}},
         "ExpressionAttributeNames": {"#n0": "id"},
         "ConditionExpression": "(#n0 = :v1)",
         "TableName": "User"}
        for user in users]
    engine.delete(*users, condition=condition)
    for expected in expected_calls:
        session.delete_item.assert_any_call(expected)
    assert session.delete_item.call_count == 3

    assert caplog.record_tuples[-1] == ("bloop.engine", logging.INFO, "successfully deleted 3 objects")


def test_delete_atomic(engine, session):
    user = User(id="user_id")

    # Tell the tracking system the user's id was saved to DynamoDB
    object_saved.send(engine, engine=engine, obj=user)

    expected = {
        "ConditionExpression": "(#n0 = :v1)",
        "ExpressionAttributeValues": {":v1": {"S": user.id}},
        "TableName": "User",
        "Key": {"id": {"S": user.id}},
        "ExpressionAttributeNames": {"#n0": "id"}}
    engine.delete(user, atomic=True)
    session.delete_item.assert_called_once_with(expected)


def test_delete_atomic_new(engine, session):
    """atomic delete on new object should expect no columns to exist"""
    user = User(id="user_id")
    expected = {
        "TableName": "User",
        "ExpressionAttributeNames": {
            "#n4": "id", "#n0": "age", "#n8": "name",
            "#n6": "j", "#n2": "email"},
        "Key": {"id": {"S": user.id}},
        "ConditionExpression": (
            "((attribute_not_exists(#n0)) AND (attribute_not_exists(#n2)) "
            "AND (attribute_not_exists(#n4)) AND (attribute_not_exists(#n6))"
            " AND (attribute_not_exists(#n8)))")}
    engine.delete(user, atomic=True)
    session.delete_item.assert_called_once_with(expected)


def test_delete_new(engine, session):
    """When an object is first created, a non-atomic delete shouldn't expect anything."""
    user = User(id="user_id")
    expected = {
        "TableName": "User",
        "Key": {"id": {"S": user.id}}}
    engine.delete(user)
    session.delete_item.assert_called_once_with(expected)


def test_delete_atomic_condition(engine, session):
    user = User(id="user_id", email="foo@bar.com")

    # Tell the tracking system the user's id and email were saved to DynamoDB
    object_saved.send(engine, engine=engine, obj=user)

    expected = {
        "ConditionExpression": "((#n0 = :v1) AND (#n2 = :v3) AND (#n4 = :v5))",
        "ExpressionAttributeValues": {
            ":v1": {"S": "foo"},
            ":v3": {"S": "foo@bar.com"},
            ":v5": {"S": user.id}},
        "ExpressionAttributeNames": {"#n0": "name", "#n2": "email", "#n4": "id"},
        "Key": {"id": {"S": user.id}},
        "TableName": "User"
    }
    engine.delete(user, condition=User.name.is_("foo"), atomic=True)
    session.delete_item.assert_called_once_with(expected)


def test_query(engine):
    """Engine.query supports model and index-based queries"""
    index_query = engine.query(
        User.by_email,
        key=User.by_email.hash_key == "placeholder",
        forward=False
    )
    assert index_query.model is User
    assert index_query.index is User.by_email

    model_query = engine.query(User, key=User.Meta.hash_key == "other")
    assert model_query.model is User
    assert model_query.index is None


def test_scan(engine):
    """Engine.scan supports model and index-based queries"""
    index_scan = engine.scan(User.by_email, parallel=(1, 5))
    assert index_scan.model is User
    assert index_scan.index is User.by_email

    model_scan = engine.scan(User)
    assert model_scan.model is User
    assert model_scan.index is None


def test_stream(engine, session):
    class StreamModel(BaseModel):
        class Meta:
            stream = {
                "include": {"new"},
                "arn": "test-arn-manually-set"
            }
        id = Column(String, hash_key=True)
    engine.bind(StreamModel)
    session.describe_stream.return_value = {"Shards": []}

    stream = engine.stream(StreamModel, "latest")
    assert stream.model is StreamModel


def test_invalid_stream(engine, session):
    with pytest.raises(InvalidStream):
        engine.stream(User, "latest")


def test_bind_non_model(engine):
    """Can't bind things that don't subclass BaseModel"""
    with pytest.raises(InvalidModel):
        engine.bind(object())


def test_bind_skip_abstract_models(engine, session, caplog):
    class Abstract(BaseModel):
        class Meta:
            abstract = True
        id = Column(Integer, hash_key=True)

    class Concrete(Abstract):
        id = Column(Integer, hash_key=True)

    class AlsoAbstract(Concrete):
        class Meta:
            abstract = True
        id = Column(Integer, hash_key=True)

    class AlsoConcrete(AlsoAbstract):
        id = Column(Integer, hash_key=True)

    caplog.clear()
    engine.bind(Abstract)

    session.create_table.assert_any_call("Concrete", Concrete)
    session.validate_table.assert_any_call("Concrete", Concrete)
    session.create_table.assert_any_call("AlsoConcrete", AlsoConcrete)
    session.validate_table.assert_any_call("AlsoConcrete", AlsoConcrete)

    assert caplog.record_tuples == [
        ("bloop.engine", logging.DEBUG, "binding non-abstract models ['AlsoConcrete', 'Concrete']"),
        ("bloop.engine", logging.INFO, "successfully bound 2 models to the engine"),
    ]


def test_bind_concrete_base(engine, session):
    session.create_table.reset_mock()
    session.validate_table.reset_mock()

    class Concrete(BaseModel):
        id = Column(Integer, hash_key=True)
    engine.bind(Concrete)
    session.create_table.assert_called_once_with("Concrete", Concrete)
    session.validate_table.assert_called_once_with("Concrete", Concrete)


def test_bind_different_engines(dynamodb, dynamodbstreams):
    # Required so engine doesn't pass boto3 to the wrapper
    first_engine = Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)
    second_engine = Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)

    first_engine.session = Mock(spec=SessionWrapper)
    second_engine.session = Mock(spec=SessionWrapper)

    class Concrete(BaseModel):
        id = Column(Integer, hash_key=True)
    first_engine.bind(Concrete)
    second_engine.bind(Concrete)

    # Create/Validate are only called once per bind
    first_engine.session.create_table.assert_called_once_with("Concrete", Concrete)
    first_engine.session.validate_table.assert_called_once_with("Concrete", Concrete)
    second_engine.session.create_table.assert_called_once_with("Concrete", Concrete)
    second_engine.session.validate_table.assert_called_once_with("Concrete", Concrete)


def test_bind_skip_table_setup(engine, session, caplog):
    class MyUser(BaseModel):
        id = Column(Integer, hash_key=True)

    caplog.clear()

    engine.bind(MyUser, skip_table_setup=True)
    session.create_table.assert_not_called()
    session.validate_table.assert_not_called()

    assert caplog.record_tuples == [
        ("bloop.engine", logging.DEBUG, "binding non-abstract models ['MyUser']"),
        ("bloop.engine", logging.INFO,
         "skip_table_setup is True; not trying to create tables or validate models during bind"),
        ("bloop.engine", logging.INFO, "successfully bound 1 models to the engine"),
    ]


def test_bind_configures_ttl(engine, session):
    class MyUser(BaseModel):
        class Meta:
            ttl = {"column": "expiry"}
        id = Column(Integer, hash_key=True)
        expiry = Column(Timestamp)

    engine.bind(MyUser)
    session.describe_table.assert_called_once_with("MyUser")
    session.enable_ttl.assert_called_once_with("MyUser", MyUser)


def test_bind_configures_backups(engine, session):
    class MyUser(BaseModel):
        class Meta:
            backups = {"enabled": True}
        id = Column(Integer, hash_key=True)

    engine.bind(MyUser)
    session.describe_table.assert_called_once_with("MyUser")
    session.enable_backups.assert_called_once_with("MyUser", MyUser)


def test_bind_existing_table(engine, session):
    """Even though backups/ttl are specified, no UpdateTTL/etc calls made because the table exists"""
    class MyUser(BaseModel):
        class Meta:
            backups = {"enabled": True}
            ttl = {"column": "expiry"}

        id = Column(Integer, hash_key=True)
        expiry = Column(Timestamp)

    session.create_table.return_value = False
    engine.bind(MyUser)
    session.enable_ttl.assert_not_called()
    session.enable_backups.assert_not_called()
    session.validate_table.assert_called_once_with("MyUser", MyUser)


@pytest.mark.parametrize("op_name, plural", [("save", True), ("load", True), ("delete", True)], ids=str)
def test_abstract_object_operations_raise(engine, op_name, plural):
    class Abstract(BaseModel):
        class Meta:
            abstract = True
    engine.bind(Abstract)

    abstract = Abstract(id=5)
    concrete = User(age=5)

    with pytest.raises(InvalidModel):
        operation = getattr(engine, op_name)
        operation(abstract)
    if plural:
        with pytest.raises(InvalidModel):
            operation = getattr(engine, op_name)
            operation(abstract, concrete)


@pytest.mark.parametrize("op_name", ["scan", "query"])
def test_abstract_model_operations_raise(engine, op_name):
    class Abstract(BaseModel):
        class Meta:
            abstract = True
        id = Column(Integer, hash_key=True)
        other = Column(Integer)
        by_other = GlobalSecondaryIndex(projection="all", hash_key="other")
    args = [Abstract]
    if op_name == "query":
        args.append("KeyCondition")

    with pytest.raises(InvalidModel):
        operation = getattr(engine, op_name)
        operation(*args)


def test_load_missing_vector_types(engine, session):
    """None (or missing) for Set/List etc become actual objects on load"""

    # Only the hash key was persisted
    from_dynamo = {"VectorModel": [{"name": {"S": "foo"}}]}
    session.load_items.return_value = from_dynamo

    # Note that this goes through engine.load; engine._load would go through Model._load,
    # which can't set every column.  If it did, there would be no way to partially load objects
    # through
    obj = VectorModel(name="foo")
    engine.load(obj)

    assert obj.list_str == list()
    assert obj.set_str == set()
    assert obj.map_nested == {
        "str": "",
        "bytes": b"",
        "map": {
            "str": "",
            "int": None
        }
    }


def test_update_missing_vector_types(engine, session):
    """Empty Set/List are deleted, not-set values aren't specified during update"""
    obj = VectorModel(name="foo", list_str=list(), map_nested={"str": "bar"})

    expected = {
        "ExpressionAttributeNames": {"#n2": "map_nested", "#n0": "list_str"},
        "ExpressionAttributeValues": {":v3": {"M": {"str": {"S": "bar"}}}},
        "Key": {"name": {"S": "foo"}},
        "TableName": "VectorModel",
        # Map is set, but only with the key that has a value.
        # list is deleted, since it has no values.
        "UpdateExpression": "SET #n2=:v3 REMOVE #n0",
    }

    engine.save(obj)
    session.save_item.assert_called_once_with(expected)
