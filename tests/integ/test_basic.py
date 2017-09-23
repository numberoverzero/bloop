"""Basic scenarios, symmetric tests"""
import pytest

from bloop import (
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
    class MyModel(BaseModel):
        class Meta:
            stream = {
                "include": ["new", "old"]
            }
        id = Column(Integer, hash_key=True)
        data = Column(String)
    engine.bind(MyModel)

    stream = engine.stream(MyModel, "trim_horizon")
    assert next(stream) is None

    obj = MyModel(id=3, data="hello, world")
    another = MyModel(id=5, data="foobar")
    # Two calls to ensure ordering
    engine.save(obj)
    engine.save(another)
    for expected in obj, another:
        record = next(stream)
        assert record["new"].id == expected.id
        assert record["new"].data == expected.data
        assert record["old"] is None


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
        "TableName": FirstOverlap.Meta.table_name
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
