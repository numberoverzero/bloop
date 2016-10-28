"""Basic scenarios, symmetric tests"""
import pytest
from bloop import (
    BaseModel,
    Column,
    GlobalSecondaryIndex,
    Integer,
    MissingObjects,
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
