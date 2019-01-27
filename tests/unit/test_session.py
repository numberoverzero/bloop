import logging
from unittest.mock import Mock

import botocore.exceptions
import pytest

from bloop.exceptions import (
    BloopException,
    ConstraintViolation,
    InvalidSearch,
    InvalidShardIterator,
    InvalidStream,
    RecordsExpired,
    ShardIteratorExpired,
    TableMismatch,
    TransactionCanceled,
)
from bloop.models import (
    BaseModel,
    Column,
    GlobalSecondaryIndex,
    LocalSecondaryIndex,
)
from bloop.session import (
    BATCH_GET_ITEM_CHUNK_SIZE,
    SessionWrapper,
    compare_tables,
    create_table_request,
    ready,
    sanitize_table_description,
    simple_table_status,
)
from bloop.types import String, Timestamp
from bloop.util import Sentinel, ordered

from ..helpers.models import ComplexModel, SimpleModel, User


missing = Sentinel("missing")


@pytest.fixture
def dynamodb():
    # No spec since clients are generated dynamically.
    # We could use botocore.client.BaseClient but it's so generic
    # that we don't gain any useful inspections
    return Mock()


@pytest.fixture
def dynamodbstreams():
    # No spec since clients are generated dynamically.
    return Mock()


@pytest.fixture
def session(dynamodb, dynamodbstreams):
    return SessionWrapper(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)


@pytest.fixture
def model():
    """Return a clean model so each test can mutate the model's Meta"""
    class MyModel(BaseModel):
        class Meta:
            backups = {"enabled": True}
            billing = {"mode": "provisioned"}
            encryption = {"enabled": True}
            stream = {"include": {"old", "new"}}
            ttl = {"column": "expiry"}
            read_units = 3
            write_units = 7

        id = Column(String, hash_key=True)
        range = Column(String, range_key=True)
        expiry = Column(Timestamp)
        email = Column(String)

        gsi_email_keys = GlobalSecondaryIndex(
            projection="keys", hash_key=email,
            read_units=13, write_units=17)
        gsi_email_specific = GlobalSecondaryIndex(
            projection=["expiry"], hash_key=email,
            read_units=23, write_units=27)
        gsi_email_all = GlobalSecondaryIndex(
            projection="all", hash_key=email,
            read_units=23, write_units=27)
        lsi_email_keys = LocalSecondaryIndex(projection="keys", range_key=email)
        lsi_email_specific = LocalSecondaryIndex(projection=["expiry"], range_key=email)
        lsi_email_all = LocalSecondaryIndex(projection="all", range_key=email)

    return MyModel


@pytest.fixture
def basic_model():
    class BasicModel(BaseModel):
        id = Column(String, hash_key=True)
    return BasicModel


@pytest.fixture
def logger(caplog):
    class CaplogWrapper:
        def __init__(self):
            self.caplog = caplog

        def assert_logged(self, msg, level=logging.DEBUG):
            assert ("bloop.session", level, msg) in self.caplog.record_tuples

        def assert_only_logged(self, msg, level=logging.DEBUG):
            self.assert_logged(msg, level=level)
            assert len(self.caplog.record_tuples) == 1

    return CaplogWrapper()


def description_for(cls, active=None):
    """Returns an exact description for the model"""
    description = create_table_request(cls.Meta.table_name, cls)
    if cls.Meta.encryption:
        description.pop("SSESpecification")
        description["SSEDescription"] = {"Status": "ENABLED"}
    if cls.Meta.ttl:
        description["TimeToLiveDescription"] = {
            "AttributeName": cls.Meta.ttl["column"].dynamo_name,
            "TimeToLiveStatus": "ENABLED",
        }
    if cls.Meta.backups:
        description["ContinuousBackupsDescription"] = {
            "ContinuousBackupsStatus": "ENABLED"
        }
    description["LatestStreamArn"] = "not-a-real-arn"

    # CreateTable::BillingMode -> DescribeTable::BillingModeSummary.BillingMode
    description["BillingModeSummary"] = {"BillingMode": description.pop("BillingMode")}

    description = sanitize_table_description(description)
    # post-sanitize because it strips TableStatus
    if active is not None:
        description["TableStatus"] = "ACTIVE" if active else "TEST-NOT-ACTIVE"
        for gsi in description["GlobalSecondaryIndexes"]:
            gsi["IndexStatus"] = "ACTIVE" if active else "TEST-NOT-ACTIVE"
    return description


def build_describe_stream_response(shards=missing, next_id=missing):
    description = {
        "StreamDescription": {
            "CreationRequestDateTime": "now",
            "KeySchema": [{"AttributeName": "string", "KeyType": "string"}],
            "LastEvaluatedShardId": next_id,
            "Shards": shards,
            "StreamArn": "string",
            "StreamLabel": "string",
            "StreamStatus": "string",
            "StreamViewType": "string",
            "TableName": "string"
        }
    }
    if shards is missing:
        description["StreamDescription"].pop("Shards")
    if next_id is missing:
        description["StreamDescription"].pop("LastEvaluatedShardId")
    return description


def client_error(code):
    error_response = {
        "Error": {
            "Code": code,
            "Message": "FooMessage"}}
    operation_name = "OperationName"
    return botocore.exceptions.ClientError(error_response, operation_name)


def minimal_description(active=True):
    description = sanitize_table_description({})
    description["TableStatus"] = "ACTIVE" if active else "TEST-NOT-ACTIVE"
    return description


# SAVE ITEM ================================================================================================ SAVE ITEM


def test_save_item(session, dynamodb):
    request = {"foo": "bar"}
    session.save_item(request)
    dynamodb.update_item.assert_called_once_with(**request)


def test_save_item_unknown_error(session, dynamodb):
    request = {"foo": "bar"}
    cause = dynamodb.update_item.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.save_item(request)
    assert excinfo.value.__cause__ is cause
    dynamodb.update_item.assert_called_once_with(**request)


def test_save_item_condition_failed(session, dynamodb):
    request = {"foo": "bar"}
    dynamodb.update_item.side_effect = client_error("ConditionalCheckFailedException")
    with pytest.raises(ConstraintViolation):
        session.save_item(request)
    dynamodb.update_item.assert_called_once_with(**request)


# END SAVE ITEM ======================================================================================== END SAVE ITEM


# DELETE ITEM ============================================================================================ DELETE ITEM


def test_delete_item(session, dynamodb):
    request = {"foo": "bar"}
    session.delete_item(request)
    dynamodb.delete_item.assert_called_once_with(**request)


def test_delete_item_unknown_error(session, dynamodb):
    request = {"foo": "bar"}
    cause = dynamodb.delete_item.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.delete_item(request)
    assert excinfo.value.__cause__ is cause
    dynamodb.delete_item.assert_called_once_with(**request)


def test_delete_item_condition_failed(session, dynamodb):
    request = {"foo": "bar"}
    dynamodb.delete_item.side_effect = client_error("ConditionalCheckFailedException")
    with pytest.raises(ConstraintViolation):
        session.delete_item(request)
    dynamodb.delete_item.assert_called_once_with(**request)


# END DELETE ITEM ==================================================================================== END DELETE ITEM


# LOAD ITEMS ============================================================================================== LOAD ITEMS


def test_batch_get_raises(session, dynamodb):
    cause = dynamodb.batch_get_item.side_effect = client_error("FooError")
    request = {"TableName": {"Keys": ["key"], "ConsistentRead": False}}
    with pytest.raises(BloopException) as excinfo:
        session.load_items(request)
    assert excinfo.value.__cause__ is cause


def test_batch_get_one_item(session, dynamodb):
    """A single call for a single item"""
    user = User(id="user_id")

    request = {"User": {"Keys": [{"id": {"S": user.id}}],
                        "ConsistentRead": False}}
    # When batching input with less keys than the batch size, the request
    # will look identical
    expected_request = request
    response = {
        "Responses": {
            "User": [{"id": {"S": user.id}, "age": {"N": "4"}}]
        },
        "UnprocessedKeys": {}
    }
    # Expected response is a single list of users
    expected_response = {"User": [{"id": {"S": user.id}, "age": {"N": "4"}}]}

    def handle(RequestItems):
        assert RequestItems == expected_request
        return response
    dynamodb.batch_get_item.side_effect = handle

    response = session.load_items(request)
    assert response == expected_response
    dynamodb.batch_get_item.assert_called_once_with(RequestItems=expected_request)


def test_batch_get_one_batch(session, dynamodb):
    """A single call when the number of requested items is <= batch size"""
    users = [User(id=str(i)) for i in range(BATCH_GET_ITEM_CHUNK_SIZE)]

    # Request to the bloop client
    client_request = {
        "User": {
            "Keys": [
                {"id": {"S": user.id}}
                for user in users
            ],
            "ConsistentRead": False
        }
    }

    boto3_client_response = {
        "Responses": {
            "User": [
                {"id": {"S": user.id}, "age": {"N": "4"}}
                for user in users
            ]
        },
        "UnprocessedKeys": {}
    }

    # The response that the bloop client should return
    expected_client_response = boto3_client_response["Responses"]

    dynamodb.batch_get_item.return_value = boto3_client_response
    response = session.load_items(client_request)

    dynamodb.batch_get_item.assert_called_once_with(RequestItems=client_request)
    assert response == expected_client_response


def test_batch_get_paginated(session, dynamodb):
    """Paginate requests to fit within the max batch size"""
    users = [User(id=str(i)) for i in range(BATCH_GET_ITEM_CHUNK_SIZE + 1)]
    keys = [
        {"id": {"S": user.id}}
        for user in users
    ]

    # Request with BATCH_GET_ITEM_CHUNK_SIZE + 1 items sent to the bloop client
    client_request = {"User": {"Keys": keys, "ConsistentRead": False}}

    # The two responses that boto3 would return to the bloop client
    batched_responses = [
        # [0, BATCH_GET_ITEM_CHUNK_SIZE] items
        {
            "Responses": {
                "User": [
                    {"id": {"S": user.id}, "age": {"N": "4"}}
                    for user in users[:BATCH_GET_ITEM_CHUNK_SIZE]
                ]
            },
            "UnprocessedKeys": {}
        },
        # [BATCH_GET_ITEM_CHUNK_SIZE+1, ] items
        {
            "Responses": {
                "User": [
                    {"id": {"S": user.id}, "age": {"N": "4"}}
                    for user in users[BATCH_GET_ITEM_CHUNK_SIZE:]
                ]
            },
            "UnprocessedKeys": {}
        }
    ]

    # The response that the bloop client should return (all items)
    expected_client_response = {
        "User": [
            {"id": {"S": user.id}, "age": {"N": "4"}}
            for user in users
        ]
    }

    dynamodb.batch_get_item.side_effect = batched_responses
    response = session.load_items(client_request)

    assert dynamodb.batch_get_item.call_count == 2
    assert response == expected_client_response


def test_batch_get_unprocessed(session, dynamodb):
    """ Re-request unprocessed keys """
    user = User(id="user_id")

    request = {
        "User": {
            "Keys": [{"id": {"S": user.id}}],
            "ConsistentRead": False
        }
    }
    expected_requests = [{
        "User": {
            "Keys": [{"id": {"S": user.id}}],
            "ConsistentRead": False}
    }, {
        "User": {
            "Keys": [{"id": {"S": user.id}}],
            "ConsistentRead": False}
    }]

    responses = [{
        "UnprocessedKeys": {
            "User": {
                "Keys": [{"id": {"S": user.id}}],
                "ConsistentRead": False}}
    }, {
        "Responses": {
            "User": [{"id": {"S": user.id}, "age": {"N": "4"}}]
        },
        "UnprocessedKeys": {}
    }]

    expected_response = {"User": [{"id": {"S": user.id}, "age": {"N": "4"}}]}
    calls = 0

    def handle(RequestItems):
        nonlocal calls
        expected = expected_requests[calls]
        response = responses[calls]
        calls += 1
        assert RequestItems == expected
        return response
    dynamodb.batch_get_item = handle

    response = session.load_items(request)

    assert calls == 2
    assert response == expected_response


# END LOAD ITEMS ====================================================================================== END LOAD ITEMS


# QUERY SCAN SEARCH ================================================================================ QUERY SCAN SEARCH


@pytest.mark.parametrize("response, expected", [
    ({}, (0, 0)),
    ({"Count": -1}, (-1, -1)),
    ({"ScannedCount": -1}, (0, -1)),
    ({"Count": 1, "ScannedCount": 2}, (1, 2))
], ids=str)
def test_query_scan(session, dynamodb, response, expected):
    dynamodb.query.return_value = response
    dynamodb.scan.return_value = response
    expected = {"Count": expected[0], "ScannedCount": expected[1]}
    assert session.query_items({}) == expected
    assert session.scan_items({}) == expected


def test_query_scan_raise(session, dynamodb):
    cause = dynamodb.query.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.query_items({})
    assert excinfo.value.__cause__ is cause

    cause = dynamodb.scan.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.scan_items({})
    assert excinfo.value.__cause__ is cause


def test_search_unknown(session):
    with pytest.raises(InvalidSearch) as excinfo:
        session.search_items(mode="foo", request={})
    assert "foo" in str(excinfo.value)


# END QUERY SCAN SEARCH ======================================================================== END QUERY SCAN SEARCH


# CREATE TABLE ========================================================================================== CREATE TABLE


def test_create_table(session, dynamodb):
    expected = {
        "LocalSecondaryIndexes": [
            {"Projection": {"NonKeyAttributes": ["date", "name",
                                                 "email", "joined"],
                            "ProjectionType": "INCLUDE"},
             "IndexName": "by_joined",
             "KeySchema": [
                {"KeyType": "HASH", "AttributeName": "name"},
                {"KeyType": "RANGE", "AttributeName": "joined"}]}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 3,
                                  "WriteCapacityUnits": 2},
        "GlobalSecondaryIndexes": [
            {"Projection": {"ProjectionType": "ALL"},
             "IndexName": "by_email",
             "ProvisionedThroughput": {"ReadCapacityUnits": 4,
                                       "WriteCapacityUnits": 5},
             "KeySchema": [{"KeyType": "HASH", "AttributeName": "email"}]}],
        "TableName": "LocalTableName",
        "KeySchema": [
            {"KeyType": "HASH", "AttributeName": "name"},
            {"KeyType": "RANGE", "AttributeName": "date"}],
        "AttributeDefinitions": [
            {"AttributeType": "S", "AttributeName": "date"},
            {"AttributeType": "S", "AttributeName": "name"},
            {"AttributeType": "S", "AttributeName": "joined"},
            {"AttributeType": "S", "AttributeName": "email"}],
        'BillingMode': 'PROVISIONED',
    }

    def handle(**table):
        assert ordered(table) == ordered(expected)
    dynamodb.create_table.side_effect = handle
    session.create_table("LocalTableName", ComplexModel)
    assert dynamodb.create_table.call_count == 1


def test_create_subclass(session, dynamodb):
    """Creating a subclass should include all parent models' columns in the request"""
    class SubModel(User):
        my_id = Column(String, hash_key=True)

    expected = {
        'AttributeDefinitions': [
            {'AttributeName': 'my_id', 'AttributeType': 'S'},
            {'AttributeName': 'email', 'AttributeType': 'S'},
        ],
        'BillingMode': 'PROVISIONED',
        'KeySchema': [{'AttributeName': 'my_id', 'KeyType': 'HASH'}],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1},
        'TableName': 'LocalTableName',
        'GlobalSecondaryIndexes': [
            {'IndexName': 'by_email', 'KeySchema': [{'AttributeName': 'email', 'KeyType': 'HASH'}],
             'Projection': {'ProjectionType': 'ALL'},
             'ProvisionedThroughput': {'WriteCapacityUnits': 1, 'ReadCapacityUnits': 1}}]
    }

    def handle(**table):
        assert ordered(table) == ordered(expected)

    dynamodb.create_table.side_effect = handle
    session.create_table("LocalTableName", SubModel)
    assert dynamodb.create_table.call_count == 1


def test_create_raises_unknown(session, dynamodb):
    cause = dynamodb.create_table.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.create_table("User", User)
    assert excinfo.value.__cause__ is cause
    assert dynamodb.create_table.call_count == 1


def test_create_already_exists(session, dynamodb):
    dynamodb.create_table.side_effect = client_error("ResourceInUseException")
    session.create_table("User", User)
    assert dynamodb.create_table.call_count == 1


# END CREATE TABLE ================================================================================== END CREATE TABLE


# DESCRIBE TABLE ====================================================================================== DESCRIBE TABLE


def test_describe_table_raises_unknown(session, dynamodb):
    cause = dynamodb.describe_table.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.describe_table("User")
    assert excinfo.value.__cause__ is cause
    assert dynamodb.describe_table.call_count == 1
    assert dynamodb.describe_time_to_live.call_count == 0


def test_describe_ttl_raises_unknown(session, dynamodb):
    dynamodb.describe_table.return_value = {"Table": minimal_description(True)}
    cause = dynamodb.describe_time_to_live.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.describe_table("User")
    assert excinfo.value.__cause__ is cause
    assert dynamodb.describe_table.call_count == 1
    assert dynamodb.describe_time_to_live.call_count == 1


def test_describe_backups_raises_unknown(session, dynamodb):
    dynamodb.describe_table.return_value = {"Table": minimal_description(True)}
    dynamodb.describe_time_to_live.return_value = {}
    cause = dynamodb.describe_continuous_backups.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.describe_table("User")
    assert excinfo.value.__cause__ is cause
    assert dynamodb.describe_table.call_count == 1
    assert dynamodb.describe_time_to_live.call_count == 1
    assert dynamodb.describe_continuous_backups.call_count == 1


def test_describe_table_polls_status(session, dynamodb):
    dynamodb.describe_table.side_effect = [
        {"Table": minimal_description(False)},
        {"Table": minimal_description(True)}
    ]
    dynamodb.describe_time_to_live.return_value = {"TimeToLiveDescription": {}}
    dynamodb.describe_continuous_backups.return_value = {"ContinuousBackupsDescription": {}}
    description = session.describe_table("User")
    # table status is filtered out
    assert "TableStatus" not in description
    assert dynamodb.describe_table.call_count == 2
    assert dynamodb.describe_time_to_live.call_count == 1
    assert dynamodb.describe_continuous_backups.call_count == 1


def test_describe_table_sanitizes(session, dynamodb, caplog):
    responses = dynamodb.describe_table.side_effect = [
        {"Table": minimal_description(False)},
        {"Table": minimal_description(True)}
    ]
    # New/unknown fields are filtered out
    responses[-1]["Table"]["UnknownField"] = "Something"
    # Missing fields are added
    responses[-1]["Table"].pop("GlobalSecondaryIndexes")

    dynamodb.describe_time_to_live.return_value = {"TimeToLiveDescription": {}}
    dynamodb.describe_continuous_backups.return_value = {"ContinuousBackupsDescription": {}}
    description = session.describe_table("User")
    assert "UnknownField" not in description
    assert description["GlobalSecondaryIndexes"] == []
    assert caplog.record_tuples == [
        ("bloop.session", logging.DEBUG,
         "describe_table: table \"User\" was in ACTIVE state after 2 calls"),
    ]


def test_describe_table_caches_responses(session, dynamodb):
    dynamodb.describe_table.side_effect = [
        {"Table": minimal_description(True)},
        {"Table": minimal_description(True)}
    ]

    dynamodb.describe_time_to_live.return_value = {"TimeToLiveDescription": {}}
    dynamodb.describe_continuous_backups.return_value = {"ContinuousBackupsDescription": {}}

    first_description = session.describe_table("User")
    second_description = session.describe_table("User")

    assert first_description is second_description
    assert dynamodb.describe_table.call_count == 1
    assert dynamodb.describe_time_to_live.call_count == 1
    assert dynamodb.describe_continuous_backups.call_count == 1

    session.clear_cache()
    session.describe_table("User")

    assert dynamodb.describe_table.call_count == 2
    assert dynamodb.describe_time_to_live.call_count == 2
    assert dynamodb.describe_continuous_backups.call_count == 2


# END DESCRIBE TABLE ============================================================================== END DESCRIBE TABLE


def test_enable_ttl(session, dynamodb):
    class Model(BaseModel):
        class Meta:
            ttl = {"column": "expiry"}
        id = Column(String, hash_key=True)
        expiry = Column(Timestamp, dynamo_name="e!!")
    session.enable_ttl("LocalTableName", Model)
    expected = {
        "TableName": "LocalTableName",
        "TimeToLiveSpecification": {
            "AttributeName": "e!!",
            "Enabled": True
        }
    }
    dynamodb.update_time_to_live.assert_called_once_with(**expected)


def test_enable_ttl_wraps_exception(session, dynamodb):
    class Model(BaseModel):
        class Meta:
            ttl = {"column": "expiry"}
        id = Column(String, hash_key=True)
        expiry = Column(Timestamp, dynamo_name="e!!")
    dynamodb.update_time_to_live.side_effect = expected = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.enable_ttl("LocalTableName", Model)
    assert excinfo.value.__cause__ is expected


def test_enable_backups(session, dynamodb):
    class Model(BaseModel):
        class Meta:
            backups = {"enabled": True}
        id = Column(String, hash_key=True)
    session.enable_backups("LocalTableName", Model)
    expected = {
        "TableName": "LocalTableName",
        "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": True}
    }
    dynamodb.update_continuous_backups.assert_called_once_with(**expected)


def test_enable_backups_wraps_exception(session, dynamodb):
    class Model(BaseModel):
        class Meta:
            backups = {"enabled": True}
        id = Column(String, hash_key=True)
    dynamodb.update_continuous_backups.side_effect = expected = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.enable_backups("LocalTableName", Model)
    assert excinfo.value.__cause__ is expected

# VALIDATE TABLE ====================================================================================== VALIDATE TABLE


def test_validate_table_all_meta(model, session, dynamodb, logger):
    description = description_for(model, active=True)
    dynamodb.describe_table.return_value = {"Table": description}
    dynamodb.describe_time_to_live.return_value = {
        "TimeToLiveDescription": {
            "AttributeName": model.Meta.ttl["column"].dynamo_name,
            "TimeToLiveStatus": "ENABLED"
        }
    }
    dynamodb.describe_continuous_backups.return_value = {
        "ContinuousBackupsDescription": {
            "ContinuousBackupsStatus": "ENABLED"
        }
    }
    session.validate_table(model.Meta.table_name, model)


def test_validate_table_mismatch(basic_model, session, dynamodb, logger):
    description = description_for(basic_model, active=True)
    description["AttributeDefinitions"] = []
    dynamodb.describe_table.return_value = {"Table": description}
    dynamodb.describe_time_to_live.return_value = {}
    dynamodb.describe_continuous_backups.return_value = {}
    with pytest.raises(TableMismatch) as excinfo:
        session.validate_table(basic_model.Meta.table_name, basic_model)
    assert str(excinfo.value) == "The expected and actual tables for 'BasicModel' do not match."
    logger.assert_logged("Table is missing expected attribute 'id'")


def test_validate_table_sets_stream_arn(model, session, dynamodb, logger):
    # isolate the Meta component we're trying to observe
    model.Meta.billing = None
    # model.Meta.stream = None
    model.Meta.ttl = None
    model.Meta.encryption = None
    model.Meta.backups = None

    description = description_for(model, active=True)
    dynamodb.describe_table.return_value = {"Table": description}
    dynamodb.describe_time_to_live.return_value = {}
    dynamodb.describe_continuous_backups.return_value = {}
    session.validate_table(model.Meta.table_name, model)
    assert model.Meta.stream["arn"] == "not-a-real-arn"
    logger.assert_logged("Set MyModel.Meta.stream['arn'] to 'not-a-real-arn' from DescribeTable response")


def test_validate_table_sets_ttl(model, session, dynamodb, logger):
    # isolate the Meta component we're trying to observe
    model.Meta.billing = None
    model.Meta.stream = None
    # model.Meta.ttl = None
    model.Meta.encryption = None
    model.Meta.backups = None

    description = description_for(model, active=True)
    dynamodb.describe_table.return_value = {"Table": description}
    dynamodb.describe_time_to_live.return_value = {
        "TimeToLiveDescription": {
            "AttributeName": model.Meta.ttl["column"].dynamo_name,
            "TimeToLiveStatus": "ENABLED"
        }
    }
    dynamodb.describe_continuous_backups.return_value = {}

    session.validate_table(model.Meta.table_name, model)
    assert model.Meta.ttl["enabled"] is True
    logger.assert_logged("Set MyModel.Meta.ttl['enabled'] to 'True' from DescribeTable response")


def test_validate_table_sets_encryption(model, session, dynamodb, logger):
    # isolate the Meta component we're trying to observe
    model.Meta.billing = None
    model.Meta.stream = None
    model.Meta.ttl = None
    # model.Meta.encryption = None
    model.Meta.backups = None

    description = description_for(model, active=True)
    dynamodb.describe_table.return_value = {"Table": description}
    dynamodb.describe_time_to_live.return_value = {}
    dynamodb.describe_continuous_backups.return_value = {}

    # clear the Meta value so validate_table can set it
    model.Meta.encryption = None

    session.validate_table(model.Meta.table_name, model)
    assert model.Meta.encryption["enabled"] is True
    logger.assert_logged("Set MyModel.Meta.encryption['enabled'] to 'True' from DescribeTable response")


def test_validate_table_sets_backups(model, session, dynamodb, logger):
    # isolate the Meta component we're trying to observe
    model.Meta.billing = None
    model.Meta.stream = None
    model.Meta.ttl = None
    model.Meta.encryption = None
    # model.Meta.backups = None

    description = description_for(model, active=True)
    dynamodb.describe_table.return_value = {"Table": description}
    dynamodb.describe_time_to_live.return_value = {}
    dynamodb.describe_continuous_backups.return_value = {
        "ContinuousBackupsDescription": {
            "ContinuousBackupsStatus": "ENABLED"
        }
    }

    # clear the Meta value so validate_table can set it
    model.Meta.backups = None

    session.validate_table(model.Meta.table_name, model)
    assert model.Meta.backups == {"enabled": True}
    logger.assert_logged("Set MyModel.Meta.backups['enabled'] to 'True' from DescribeTable response")


@pytest.mark.parametrize("billing_mode", ["provisioned", "on_demand"])
def test_validate_table_sets_billing_mode(billing_mode, model, session, dynamodb, logger):
    # isolate the Meta components we're trying to observe
    # model.Meta.billing = None
    model.Meta.stream = None
    model.Meta.ttl = None
    model.Meta.encryption = None
    model.Meta.backups = None

    model.Meta.billing["mode"] = billing_mode

    description = description_for(model, active=True)
    dynamodb.describe_table.return_value = {"Table": description}
    dynamodb.describe_time_to_live.return_value = {}
    dynamodb.describe_continuous_backups.return_value = {}

    # clear the Meta value so validate_table can set it
    model.Meta.billing = None

    session.validate_table(model.Meta.table_name, model)
    assert model.Meta.billing["mode"] == billing_mode
    logger.assert_logged(f"Set MyModel.Meta.billing['mode'] to '{billing_mode}' from DescribeTable response")


def test_validate_table_sets_table_throughput(model, session, dynamodb, logger):
    # isolate the Meta components we're trying to observe
    model.Meta.billing = None
    model.Meta.stream = None
    model.Meta.ttl = None
    model.Meta.encryption = None
    model.Meta.backups = None

    description = description_for(model, active=True)
    dynamodb.describe_table.return_value = {"Table": description}
    dynamodb.describe_time_to_live.return_value = {}
    dynamodb.describe_continuous_backups.return_value = {}

    # tell the model to stop tracking read/write units so that we can see it's added back
    expected_read_units = model.Meta.read_units
    expected_write_units = model.Meta.write_units
    model.Meta.read_units = model.Meta.write_units = None

    session.validate_table(model.Meta.table_name, model)
    assert model.Meta.read_units == expected_read_units
    assert model.Meta.write_units == expected_write_units
    logger.assert_logged("Set MyModel.Meta.read_units to 3 from DescribeTable response")
    logger.assert_logged("Set MyModel.Meta.write_units to 7 from DescribeTable response")


def test_validate_table_sets_gsi_throughput(model, session, dynamodb, logger):
    # isolate the Meta components we're trying to observe
    model.Meta.billing = None
    model.Meta.stream = None
    model.Meta.ttl = None
    model.Meta.encryption = None
    model.Meta.backups = None

    description = description_for(model, active=True)
    dynamodb.describe_table.return_value = {"Table": description}
    dynamodb.describe_time_to_live.return_value = {}
    dynamodb.describe_continuous_backups.return_value = {}

    # tell the model to stop tracking read/write units so that we can see it's added back
    index = any_index(model, "gsis")
    expected_read_units = index.read_units
    expected_write_units = index.write_units
    index.read_units = index.write_units = None

    session.validate_table(model.Meta.table_name, model)
    assert index.read_units == expected_read_units
    assert index.write_units == expected_write_units
    logger.assert_logged(f"Set MyModel.{index.name}.read_units to {expected_read_units} from DescribeTable response")
    logger.assert_logged(f"Set MyModel.{index.name}.write_units to {expected_write_units} from DescribeTable response")

# END VALIDATE TABLE ============================================================================== END VALIDATE TABLE


# DESCRIBE STREAM ==================================================================================== DESCRIBE STREAM


def test_describe_stream_unknown_error(session, dynamodbstreams):
    request = {"StreamArn": "arn", "ExclusiveStartShardId": "shard id"}
    cause = dynamodbstreams.describe_stream.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.describe_stream("arn", "shard id")
    assert excinfo.value.__cause__ is cause
    dynamodbstreams.describe_stream.assert_called_once_with(**request)


def test_describe_stream_not_found(session, dynamodbstreams):
    request = {"StreamArn": "arn", "ExclusiveStartShardId": "shard id"}
    cause = dynamodbstreams.describe_stream.side_effect = client_error("ResourceNotFoundException")
    with pytest.raises(InvalidStream) as excinfo:
        session.describe_stream("arn", "shard id")
    assert excinfo.value.__cause__ is cause
    dynamodbstreams.describe_stream.assert_called_once_with(**request)


@pytest.mark.parametrize("no_shards", [missing, list()])
@pytest.mark.parametrize("next_ids", [(missing, ), (None, ), ("two pages", None)])
def test_describe_stream_no_results(no_shards, next_ids, session, dynamodbstreams):
    stream_arn = "arn"
    responses = [build_describe_stream_response(shards=no_shards, next_id=next_id) for next_id in next_ids]
    dynamodbstreams.describe_stream.side_effect = responses

    description = session.describe_stream(stream_arn=stream_arn, first_shard="first-token")
    assert description["Shards"] == []

    empty_response = build_describe_stream_response(shards=[])["StreamDescription"]
    assert ordered(description) == ordered(empty_response)

    dynamodbstreams.describe_stream.assert_any_call(StreamArn=stream_arn, ExclusiveStartShardId="first-token")
    assert dynamodbstreams.describe_stream.call_count == len(next_ids)


@pytest.mark.parametrize("shard_list", [
    # results followed by empty
    (["first", "second"], []),
    # empty followed by results
    ([], ["first", "second"])
])
def test_describe_stream_combines_results(shard_list, session, dynamodbstreams):
    stream_arn = "arn"
    responses = [build_describe_stream_response(shards=shard_list[0], next_id="second-token"),
                 build_describe_stream_response(shards=shard_list[1], next_id=missing)]
    dynamodbstreams.describe_stream.side_effect = responses

    description = session.describe_stream(stream_arn)
    assert description["Shards"] == ["first", "second"]

    assert dynamodbstreams.describe_stream.call_count == 2
    dynamodbstreams.describe_stream.assert_any_call(StreamArn=stream_arn)
    dynamodbstreams.describe_stream.assert_any_call(StreamArn=stream_arn, ExclusiveStartShardId="second-token")


# END DESCRIBE STREAM ============================================================================ END DESCRIBE STREAM


# GET SHARD ITERATOR ============================================================================= GET SHARD ITERATOR


def test_get_unknown_shard_iterator(dynamodbstreams, session):
    unknown_type = "foo123"
    with pytest.raises(InvalidShardIterator) as excinfo:
        session.get_shard_iterator(
            stream_arn="arn",
            shard_id="shard_id",
            iterator_type=unknown_type,
            sequence_number=None
        )
    assert unknown_type in str(excinfo.value)
    dynamodbstreams.get_shard_iterator.assert_not_called()


def test_get_trimmed_shard_iterator(dynamodbstreams, session):
    dynamodbstreams.get_shard_iterator.side_effect = client_error("TrimmedDataAccessException")
    with pytest.raises(RecordsExpired):
        session.get_shard_iterator(
            stream_arn="arn",
            shard_id="shard_id",
            iterator_type="at_sequence",
            sequence_number="sequence-123"
        )
    dynamodbstreams.get_shard_iterator.assert_called_once_with(
        StreamArn="arn",
        ShardId="shard_id",
        ShardIteratorType="AT_SEQUENCE_NUMBER",
        SequenceNumber="sequence-123"
    )


def test_get_shard_iterator_unknown_error(dynamodbstreams, session):
    cause = dynamodbstreams.get_shard_iterator.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.get_shard_iterator(stream_arn="arn", shard_id="shard_id", iterator_type="at_sequence")
    assert excinfo.value.__cause__ is cause


def test_get_shard_iterator_after_sequence(dynamodbstreams, session):
    dynamodbstreams.get_shard_iterator.return_value = {"ShardIterator": "return value"}

    shard_iterator = session.get_shard_iterator(
        stream_arn="arn",
        shard_id="shard_id",
        iterator_type="after_sequence",
        sequence_number="sequence-123"
    )
    assert shard_iterator == "return value"
    dynamodbstreams.get_shard_iterator.assert_called_once_with(
        StreamArn="arn",
        ShardId="shard_id",
        ShardIteratorType="AFTER_SEQUENCE_NUMBER",
        SequenceNumber="sequence-123"
    )


def test_get_shard_iterator_latest(dynamodbstreams, session):
    dynamodbstreams.get_shard_iterator.return_value = {"ShardIterator": "return value"}

    shard_iterator = session.get_shard_iterator(
        stream_arn="arn",
        shard_id="shard_id",
        iterator_type="latest"
    )
    assert shard_iterator == "return value"
    dynamodbstreams.get_shard_iterator.assert_called_once_with(
        StreamArn="arn",
        ShardId="shard_id",
        ShardIteratorType="LATEST"
    )


# END GET SHARD ITERATOR ====================================================================== END GET SHARD ITERATOR


# GET STREAM RECORDS ============================================================================== GET STREAM RECORDS


def test_get_trimmed_records(dynamodbstreams, session):
    dynamodbstreams.get_records.side_effect = client_error("TrimmedDataAccessException")
    with pytest.raises(RecordsExpired):
        session.get_stream_records(iterator_id="iterator-123")


def test_get_records_expired_iterator(dynamodbstreams, session):
    dynamodbstreams.get_records.side_effect = client_error("ExpiredIteratorException")
    with pytest.raises(ShardIteratorExpired):
        session.get_stream_records("some-iterator")


def test_get_shard_records_unknown_error(dynamodbstreams, session):
    cause = dynamodbstreams.get_records.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.get_stream_records("iterator-123")
    assert excinfo.value.__cause__ is cause


def test_get_records(dynamodbstreams, session):
    # Return structure isn't important, since it's just a passthrough
    response = dynamodbstreams.get_records.return_value = {"return": "value"}
    records = session.get_stream_records(iterator_id="some-iterator")
    assert records is response
    dynamodbstreams.get_records.assert_called_once_with(ShardIterator="some-iterator")


# END GET STREAM RECORDS ====================================================================== END GET STREAM RECORDS


# TRANSACTION READ ================================================================================== TRANSACTION READ


def test_transaction_read(dynamodb, session):
    response = dynamodb.transact_get_items.return_value = {"Responses": ["placeholder"]}
    result = session.transaction_read("some-items")
    assert result is response
    dynamodb.transact_get_items.assert_called_once_with(TransactItems="some-items")


def test_transaction_read_canceled(dynamodb, session):
    cause = dynamodb.transact_get_items.side_effect = client_error("TransactionCanceledException")
    with pytest.raises(TransactionCanceled) as excinfo:
        session.transaction_read("some-items")
    assert excinfo.value.__cause__ is cause


def test_transaction_read_unknown_error(dynamodb, session):
    cause = dynamodb.transact_get_items.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.transaction_read("some-items")
    assert excinfo.value.__cause__ is cause


# END TRANSACTION READ ========================================================================== END TRANSACTION READ


# TRANSACTION WRITE ================================================================================ TRANSACTION WRITE


def test_transaction_write(dynamodb, session):
    session.transaction_write("some-items", "some-token")
    dynamodb.transact_write_items.assert_called_once_with(TransactItems="some-items", ClientRequestToken="some-token")


def test_transaction_write_canceled(dynamodb, session):
    cause = dynamodb.transact_write_items.side_effect = client_error("TransactionCanceledException")
    with pytest.raises(TransactionCanceled) as excinfo:
        session.transaction_write("some-items", "some-token")
    assert excinfo.value.__cause__ is cause


def test_transaction_write_unknown_error(dynamodb, session):
    cause = dynamodb.transact_write_items.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.transaction_write("some-items", "some-token")
    assert excinfo.value.__cause__ is cause


# END TRANSACTION WRITE ======================================================================== END TRANSACTION WRITE


# COMPARE TABLES ====================================================================================== COMPARE TABLES


def remove_index_by_name(description, to_remove):
    for index_type in ["GlobalSecondaryIndexes", "LocalSecondaryIndexes"]:
        description[index_type] = [
            index
            for index in description[index_type]
            if index["IndexName"] != to_remove
        ]


def find_index(description, index_name):
    for index_type in ["GlobalSecondaryIndexes", "LocalSecondaryIndexes"]:
        for index in description[index_type]:
            if index["IndexName"] == index_name:
                return index
    raise RuntimeError("test setup failed to find expected index by name")


def any_index(model, index_type, require_attributes=False):
    indexes = getattr(model.Meta, index_type)
    for index in indexes:
        if index.projection["mode"] != "keys" or not require_attributes:
            return index
    raise RuntimeError("test setup failed to find a usable index")


def test_compare_table_sanity_check(model, logger):
    """By default the test setup should provide a fully-valid description of the table.

    Without this sanity check, any test that makes the description slightly invalid wouldn't actually
    verify the compare_table method is failing as expected.
    """
    description = description_for(model)
    assert compare_tables(model, description)
    assert not logger.caplog.record_tuples


def test_compare_table_simple(basic_model):
    """A minimal model that doesn't care about streaming, ttl, or encryption and has no indexes"""
    description = description_for(basic_model)
    assert compare_tables(basic_model, description)


def test_compare_table_wrong_encryption_enabled(model, logger):
    description = description_for(model)
    description["SSEDescription"]["Status"] = "DISABLED"
    assert not compare_tables(model, description)
    logger.assert_only_logged("Model expects SSE to be 'ENABLED' but was 'DISABLED'")


def test_compare_table_wrong_backups_enabled(model, logger):
    description = description_for(model)
    description["ContinuousBackupsDescription"]["ContinuousBackupsStatus"] = "DISABLED"
    assert not compare_tables(model, description)
    logger.assert_only_logged("Model expects backups to be 'ENABLED' but was 'DISABLED'")


def test_compare_table_wrong_stream_enabled(model, logger):
    description = description_for(model)
    description["StreamSpecification"]["StreamEnabled"] = False
    assert not compare_tables(model, description)
    logger.assert_only_logged("Model expects streaming but streaming is not enabled")


def test_compare_table_wrong_stream_type(model, logger):
    description = description_for(model)
    description["StreamSpecification"]["StreamViewType"] = "UNKNOWN"
    assert not compare_tables(model, description)
    logger.assert_only_logged("Model expects StreamViewType 'NEW_AND_OLD_IMAGES' but was 'UNKNOWN'")


def test_compare_table_wrong_ttl_enabled(model, logger):
    description = description_for(model)
    description["TimeToLiveDescription"]["TimeToLiveStatus"] = "DISABLED"
    assert not compare_tables(model, description)
    logger.assert_only_logged("Model expects ttl but ttl is not enabled")


def test_compare_table_wrong_ttl_column(model, logger):
    description = description_for(model)
    description["TimeToLiveDescription"]["AttributeName"] = "wrong_column"
    assert not compare_tables(model, description)
    logger.assert_only_logged("Model expects ttl column to be 'expiry' but was 'wrong_column'")


@pytest.mark.parametrize("expected, wire", [
    ("on_demand", "provisioned"),
    ("provisioned", "on_demand")
])
def test_compare_table_wrong_billing_mode(expected, wire, model, logger):
    description = description_for(model)
    description["BillingModeSummary"]["BillingMode"] = {
        "on_demand": "PAY_PER_REQUEST",
        "provisioned": "PROVISIONED"
    }[wire]
    model.Meta.billing["mode"] = expected
    assert not compare_tables(model, description)
    logger.assert_only_logged(f"Model expects billing mode to be '{expected}' but was '{wire}'")


def test_compare_table_wrong_provisioned_throughput(model, logger):
    description = description_for(model)
    description["ProvisionedThroughput"]["ReadCapacityUnits"] = 200
    description["ProvisionedThroughput"]["WriteCapacityUnits"] = -100
    assert not compare_tables(model, description)
    logger.assert_logged("Model expects 3 read units but was 200")
    logger.assert_logged("Model expects 7 write units but was -100")


@pytest.mark.parametrize("index_type", ["gsis", "lsis"])
def test_compare_table_missing_index(index_type, model, logger):
    index_name = any_index(model, index_type).dynamo_name
    description = description_for(model)
    remove_index_by_name(description, index_name)
    assert not compare_tables(model, description)
    logger.assert_only_logged(f"Table is missing expected index '{index_name}'")


@pytest.mark.parametrize("index_type", ["gsis", "lsis"])
def test_compare_table_wrong_index_key_schema(index_type, model, logger):
    index = any_index(model, index_type)
    description = description_for(model)
    # drop the last entry in the key schema to ensure it's invalid
    index_description = find_index(description, index.dynamo_name)
    index_description["KeySchema"] = index_description["KeySchema"][:-1]
    assert not compare_tables(model, description)
    logger.assert_only_logged(f"KeySchema mismatch for index '{index.dynamo_name}'")


@pytest.mark.parametrize("index_type", ["gsis", "lsis"])
def test_compare_table_wrong_index_projection_type(index_type, model, logger):
    index = any_index(model, index_type)
    description = description_for(model)
    index_description = find_index(description, index.dynamo_name)
    index_description["Projection"]["ProjectionType"] = "UnknownProjectionType"
    assert not compare_tables(model, description)
    logger.assert_logged(f"Projection mismatch for index '{index.dynamo_name}'")
    logger.assert_logged(f"unexpected index ProjectionType 'UnknownProjectionType'", level=logging.INFO)


@pytest.mark.parametrize("index_type", ["gsis", "lsis"])
def test_compare_table_missing_index_projection_attributes(index_type, model, logger):
    index = any_index(model, index_type, require_attributes=True)
    description = description_for(model)
    index_description = find_index(description, index.dynamo_name)
    index_description["Projection"]["NonKeyAttributes"] = []
    # Since an index projecting "ALL" short-circuits the superset check, we need to advertise
    # a different valid but insufficient projection type
    if index_description["Projection"]["ProjectionType"] == "ALL":
        index_description["Projection"]["ProjectionType"] = "INCLUDE"
    assert not compare_tables(model, description)
    logger.assert_only_logged(f"Projection mismatch for index '{index.dynamo_name}'")


@pytest.mark.parametrize("unit_type", ["ReadCapacityUnits", "WriteCapacityUnits"])
def test_compare_table_wrong_gsi_throughput(unit_type, model, logger):
    index = any_index(model, "gsis")
    description = description_for(model)
    # set the capacity units to an impossible value
    index_description = find_index(description, index.dynamo_name)
    index_description["ProvisionedThroughput"][unit_type] = -1
    assert not compare_tables(model, description)
    logger.assert_only_logged(f"ProvisionedThroughput.{unit_type} mismatch for index '{index.dynamo_name}'")


def test_compare_table_missing_attribute(model, logger):
    description = description_for(model)
    attribute = description["AttributeDefinitions"].pop(-1)
    name = attribute["AttributeName"]
    assert not compare_tables(model, description)
    logger.assert_only_logged(f"Table is missing expected attribute '{name}'")


def test_compare_table_wrong_attribute_type(model, logger):
    description = description_for(model)
    attribute = description["AttributeDefinitions"][-1]
    attribute["AttributeType"] = "B"
    name = attribute["AttributeName"]
    assert not compare_tables(model, description)
    logger.assert_only_logged(f"AttributeDefinition mismatch for attribute '{name}'")


def test_compare_table_extra_indexes(basic_model, model):
    description = description_for(basic_model)
    extended = description_for(model)
    description["GlobalSecondaryIndexes"] = extended["GlobalSecondaryIndexes"]
    description["LocalSecondaryIndexes"] = extended["LocalSecondaryIndexes"]
    assert compare_tables(basic_model, description)


@pytest.mark.parametrize("index_type", ["gsis", "lsis"])
def test_compare_table_index_superset(index_type, model, capsys):
    index = any_index(model, index_type)
    description = description_for(model)
    index_description = find_index(description, index.dynamo_name)
    index_description["Projection"]["NonKeyAttributes"].append("AdditionalAttribute")
    with capsys.disabled():
        assert compare_tables(model, description)


def test_compare_table_extra_attribute(basic_model, model):
    description = description_for(basic_model)
    extended = description_for(model)
    description["AttributeDefinitions"].extend(extended["AttributeDefinitions"])
    assert compare_tables(basic_model, description)

# END COMPARE TABLES ============================================================================== END COMPARE TABLES


# OTHER TABLE HELPERS ============================================================================ OTHER TABLE HELPERS


def assert_unordered(obj, other):
    assert ordered(obj) == ordered(other)


def test_create_simple():
    expected = {
        'AttributeDefinitions': [
            {'AttributeName': 'id', 'AttributeType': 'S'}],
        'BillingMode': 'PROVISIONED',
        'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1},
        'TableName': 'Simple'}
    assert_unordered(create_table_request("Simple", SimpleModel), expected)


def test_create_complex():
    expected = {
        'AttributeDefinitions': [
            {'AttributeType': 'S', 'AttributeName': 'date'},
            {'AttributeType': 'S', 'AttributeName': 'email'},
            {'AttributeType': 'S', 'AttributeName': 'joined'},
            {'AttributeType': 'S', 'AttributeName': 'name'}],
        'BillingMode': 'PROVISIONED',
        'GlobalSecondaryIndexes': [{
            'IndexName': 'by_email',
            'KeySchema': [{'KeyType': 'HASH', 'AttributeName': 'email'}],
            'Projection': {'ProjectionType': 'ALL'},
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 4, 'WriteCapacityUnits': 5}}],
        'KeySchema': [{'KeyType': 'HASH', 'AttributeName': 'name'},
                      {'KeyType': 'RANGE', 'AttributeName': 'date'}],
        'LocalSecondaryIndexes': [{
            'IndexName': 'by_joined',
            'KeySchema': [
                {'KeyType': 'HASH', 'AttributeName': 'name'},
                {'KeyType': 'RANGE', 'AttributeName': 'joined'}],
            'Projection': {
                'NonKeyAttributes': ['joined', 'email', 'date', 'name'],
                'ProjectionType': 'INCLUDE'}}],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 3, 'WriteCapacityUnits': 2},
        'TableName': 'CustomTableName'}
    assert_unordered(create_table_request("CustomTableName", ComplexModel), expected)


def test_create_table_no_stream():
    """No StreamSpecification if Model.Meta.stream is None"""
    class Model(BaseModel):
        class Meta:
            stream = None
        id = Column(String, hash_key=True)
    table = create_table_request("Model", Model)
    assert "StreamSpecification" not in table


@pytest.mark.parametrize("include, view_type", [
    (["keys"], "KEYS_ONLY"),
    (["new"], "NEW_IMAGE"),
    (["old"], "OLD_IMAGE"),
    (["new", "old"], "NEW_AND_OLD_IMAGES"),
])
def test_create_table_with_stream(include, view_type):
    """A table that streams only new images"""
    class Model(BaseModel):
        class Meta:
            stream = {"include": include}
        id = Column(String, hash_key=True)
    table = create_table_request("Model", Model)
    assert table["StreamSpecification"] == {"StreamEnabled": True, "StreamViewType": view_type}


@pytest.mark.parametrize("sse_encryption", [True, False])
def test_create_table_with_encryption(sse_encryption):
    """A table that specifies encryption settings"""
    class Model(BaseModel):
        class Meta:
            encryption = {"enabled": sse_encryption}
        id = Column(String, hash_key=True)
    table = create_table_request("Model", Model)
    assert table["SSESpecification"] == {"Enabled": bool(sse_encryption)}


@pytest.mark.parametrize("table_status, gsi_status, expected_status", [
    ("ACTIVE", "ACTIVE", ready),
    ("ACTIVE", None, ready),
    ("ACTIVE", "BUSY", None),
    ("BUSY", "ACTIVE", None),
    ("BUSY", "BUSY", None)
])
def test_simple_status(table_status, gsi_status, expected_status):
    """Status is busy because table isn't ACTIVE, no GSIs"""
    description = {"TableStatus": table_status}
    if gsi_status is not None:
        description["GlobalSecondaryIndexes"] = [{"IndexStatus": gsi_status}]
    assert simple_table_status(description) == expected_status


# END OTHER TABLE HELPERS ==================================================================== END OTHER TABLE HELPERS
