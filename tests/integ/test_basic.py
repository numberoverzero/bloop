"""Basic scenarios, symmetric tests"""
import uuid
from random import randint

import pytest

from bloop import (
    UUID,
    BaseModel,
    Column,
    GlobalSecondaryIndex,
    Integer,
    MissingObjects,
    String,
)

from .models import User


def test_crud(engine):
    engine.bind(User)

    user = User(email="user@domain.com", username="user", profile="first")
    engine.save(user)

    same_user = User(email=user.email, username=user.username)
    engine.load(same_user)
    assert user.profile == same_user.profile

    same_user.profile = "second"
    engine.save(same_user)

    engine.load(user, consistent=True)
    assert user.profile == same_user.profile

    engine.delete(user)

    with pytest.raises(MissingObjects) as excinfo:
        engine.load(same_user, consistent=True)
    assert [same_user] == excinfo.value.objects


def test_model_defaults(engine):
    class ColumnDefaultsModel(BaseModel):
        hash = Column(Integer, hash_key=True, default=12)
        range = Column(Integer, range_key=True, default=24)
        other = Column(Integer, default=48)
    engine.bind(ColumnDefaultsModel)

    obj = ColumnDefaultsModel()
    assert obj.hash == 12
    assert obj.range == 24
    assert obj.other == 48

    engine.save(obj)

    same_obj = ColumnDefaultsModel(hash=12, range=24)
    engine.load(same_obj)

    assert same_obj.hash == 12
    assert same_obj.range == 24
    assert same_obj.other == 48


def test_model_defaults_load(engine):
    class ColumnDefaultLoadModel(BaseModel):
        hash = Column(Integer, hash_key=True, default=12)
        range = Column(Integer, range_key=True, default=24)
        other = Column(Integer, default=48)
    engine.bind(ColumnDefaultLoadModel)

    obj = ColumnDefaultLoadModel(hash=333, range=333)
    engine.save(obj)

    same_obj = ColumnDefaultLoadModel(hash=obj.hash, range=obj.range)
    engine.load(same_obj)

    assert same_obj.hash == 333
    assert same_obj.range == 333
    assert same_obj.other == 48


def test_model_default_func(engine):
    get_int_called = 0
    get_randint_called = 0

    def get_int():
        nonlocal get_int_called
        get_int_called += 1
        return 404

    def random_int():
        nonlocal get_randint_called
        get_randint_called += 1
        return randint(1, 100)

    class ColumnDefaultFuncModel(BaseModel):
        hash = Column(UUID, hash_key=True, default=uuid.uuid4())
        range = Column(Integer, range_key=True, default=get_int)
        other = Column(Integer, default=random_int)
    engine.bind(ColumnDefaultFuncModel)

    obj = ColumnDefaultFuncModel()
    assert get_int_called == 1
    assert get_randint_called == 1
    assert isinstance(obj.hash, uuid.UUID)
    assert obj.range == 404

    engine.save(obj)

    # get_int shouldn't be called because we are passing that value in the constructor
    same_obj = ColumnDefaultFuncModel(hash=obj.hash, range=obj.range)
    engine.load(same_obj)

    assert get_int_called == 1
    assert get_randint_called == 2
    assert same_obj.hash == obj.hash
    assert same_obj.range == obj.range
    assert same_obj.other == obj.other


def test_model_default_projection(engine):
    def token_hex(prefix=None):
        if prefix:
            return prefix + uuid.uuid4().hex
        return uuid.uuid4().hex

    class MyModel(BaseModel):
        id = Column(Integer, hash_key=True)
        email = Column(String)

        password = Column(String, default=token_hex)

        by_email = GlobalSecondaryIndex(
            projection="keys",
            hash_key="email"
        )

    engine.bind(MyModel)

    expected_password = token_hex("RC_")
    instance = MyModel(
        id=3, email="u@d.com",
        password=expected_password
    )
    engine.save(instance)

    q = engine.query(MyModel.by_email, key=MyModel.email == "u@d.com")
    same_instance = q.first()

    assert not hasattr(same_instance, 'password')

    q = engine.query(MyModel, key=MyModel.id == 3)
    same_instance = q.first()

    assert same_instance.password == expected_password


def test_projection_overlap(engine):
    class ProjectionOverlap(BaseModel):
        hash = Column(Integer, hash_key=True)
        range = Column(Integer, range_key=True)
        other = Column(Integer)

        by_other = GlobalSecondaryIndex(projection=["other", "range"], hash_key="other")
    # by_other's projected attributes overlap with the model and its own keys
    engine.bind(ProjectionOverlap)


def test_stream_creation(engine):
    class StreamCreation(BaseModel):
        class Meta:
            stream = {
                "include": ["keys"]
            }
        hash = Column(Integer, hash_key=True)
    engine.bind(StreamCreation)
    assert "arn" in StreamCreation.Meta.stream


def test_stream_read(engine):
    class MyStreamReadModel(BaseModel):
        class Meta:
            stream = {
                "include": ["new", "old"]
            }
        id = Column(Integer, hash_key=True)
        data = Column(String)
    engine.bind(MyStreamReadModel)

    stream = engine.stream(MyStreamReadModel, "trim_horizon")
    assert next(stream) is None

    obj = MyStreamReadModel(id=3, data="hello, world")
    another = MyStreamReadModel(id=5, data="foobar")
    # Two calls to ensure ordering
    engine.save(obj)
    engine.save(another)
    for expected in obj, another:
        record = next(stream)
        assert record["new"].id == expected.id
        assert record["new"].data == expected.data
        assert record["old"] is None


# TODO enable when/if DynamoDBLocal supports DescribeTimeToLive, UpdateTimeToLive
# def test_ttl_enabled(engine):
#     class MyModel(BaseModel):
#         class Meta:
#             ttl = {"column": "expiry"}
#         id = Column(Integer, hash_key=True)
#         expiry = Column(Timestamp, dynamo_name='e')
#     engine.bind(MyModel)
#     assert MyModel.Meta.ttl["enabled"] == "enabled"


def test_model_overlap(dynamodb, engine):
    """Two models backed by the same table, with different indexes"""
    class FirstOverlap(BaseModel):
        class Meta:
            table_name = "overlap-table"
        id = Column(Integer, hash_key=True)
        first_value = Column(Integer)
        first_index = GlobalSecondaryIndex(projection="keys", hash_key="first_value")

    class SecondOverlap(BaseModel):
        class Meta:
            table_name = "overlap-table"
        id = Column(Integer, hash_key=True)
        second_value = Column(Integer)
        second_index = GlobalSecondaryIndex(projection="keys", hash_key="second_value")

    # bloop won't modify the table to match the expected value, so we need to
    # emulate someone setting up a table in the console or by hand.
    combined_table = {
        "ProvisionedThroughput": {"WriteCapacityUnits": 1, "ReadCapacityUnits": 1},
        "AttributeDefinitions": [
            {"AttributeType": "N", "AttributeName": "id"},
            {"AttributeType": "N", "AttributeName": "first_value"},
            {"AttributeType": "N", "AttributeName": "second_value"},
        ],
        "KeySchema": [{"KeyType": "HASH", "AttributeName": "id"}],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "first_index",
                "Projection": {"ProjectionType": "KEYS_ONLY"},
                "KeySchema": [{"KeyType": "HASH", "AttributeName": "first_value"}],
                "ProvisionedThroughput": {"WriteCapacityUnits": 1, "ReadCapacityUnits": 1}
            },
            {
                "IndexName": "second_index",
                "Projection": {"ProjectionType": "KEYS_ONLY"},
                "KeySchema": [{"KeyType": "HASH", "AttributeName": "second_value"}],
                "ProvisionedThroughput": {"WriteCapacityUnits": 1, "ReadCapacityUnits": 1}
            }
        ],
        # Can't use the fixed value above since it'll be modified by
        # the test framework to allow parallel runs
        "TableName": engine._compute_table_name(FirstOverlap)
    }
    dynamodb.create_table(**combined_table)

    # Now, both of these binds should see the particular subset of indexes/attribute names that they care about
    engine.bind(FirstOverlap)
    engine.bind(SecondOverlap)
    assert True


def test_unknown_throughput(dynamodb, engine):
    """A model doesn't have to specify read_units or write_units but will take the existing value"""
    class ExplicitValues(BaseModel):
        class Meta:
            read_units = 10
            write_units = 1
            table_name = "throughput-test"
        id = Column(Integer, hash_key=True)
        other = Column(Integer)
        by_other = GlobalSecondaryIndex(
            projection="keys", hash_key=other, read_units=11, write_units=1)

    class ImplicitValues(BaseModel):
        class Meta:
            write_units = 1
            table_name = "throughput-test"
        id = Column(Integer, hash_key=True)
        other = Column(Integer)
        by_other = GlobalSecondaryIndex(
            projection="keys", hash_key=other, write_units=1)

    engine.bind(ExplicitValues)
    assert ImplicitValues.Meta.read_units is None
    assert ImplicitValues.by_other.read_units is None

    # Now binding to the same table but not specifying read_units should have the same value
    engine.bind(ImplicitValues)
    assert ImplicitValues.Meta.read_units == 10
    assert ImplicitValues.by_other.read_units == 11


def test_partial_load_save(engine):
    engine.bind(User)

    obj = User(
        email="my-email@",
        username="my-username",
        profile="original-profile",
        data="original-data",
        extra="my-extra"
    )
    engine.save(obj)

    partial = engine.query(
        User.by_username,
        key=User.username == "my-username").one()
    assert getattr(partial, "profile", None) is None

    partial.data = "new-data"
    partial.extra = None
    engine.save(partial)

    same = User(
        email="my-email@",
        username="my-username"
    )
    engine.load(same)

    # never modified
    assert same.profile == "original-profile"
    # modified from partial
    assert same.data == "new-data"
    # deleted from partial
    assert getattr(same, "extra", None) is None
