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
)
from bloop.models import BaseModel, Column, GlobalSecondaryIndex
from bloop.session import (
    BATCH_GET_ITEM_CHUNK_SIZE,
    SessionWrapper,
    create_table_request,
    expected_table_description,
    ready,
    sanitize_table_description,
    simple_table_status,
)
from bloop.types import String, Timestamp
from bloop.util import Sentinel, ordered

from ..helpers.models import ComplexModel, ProjectedIndexes, SimpleModel, User


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
            {"AttributeType": "S", "AttributeName": "email"}]}

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

# VALIDATE TABLE ====================================================================================== VALIDATE TABLE


def test_validate_compares_tables(session, dynamodb):
    description = expected_table_description("User", User)
    description["TableStatus"] = "ACTIVE"
    description["GlobalSecondaryIndexes"][0]["IndexStatus"] = "ACTIVE"

    dynamodb.describe_table.return_value = {"Table": description}
    session.validate_table("User", User)
    dynamodb.describe_table.assert_called_once_with(TableName="User")


def test_describe_checks_status(session, dynamodb):
    full = expected_table_description("ProjectedIndexes", ProjectedIndexes)
    full["TableStatus"] = "ACTIVE"
    full["GlobalSecondaryIndexes"][0]["IndexStatus"] = "ACTIVE"

    dynamodb.describe_table.side_effect = [
        {"Table": {"TableStatus": "CREATING"}},
        {"Table": {"TableStatus": "ACTIVE",
                   "GlobalSecondaryIndexes": [
                       {"IndexStatus": "CREATING"}]}},
        {"Table": full}
    ]
    session.describe_table("ProjectedIndexes")
    dynamodb.describe_table.assert_called_with(TableName="ProjectedIndexes")
    assert dynamodb.describe_table.call_count == 3


def test_validate_invalid_table(session, dynamodb, caplog):
    """DynamoDB returns an invalid json document"""
    dynamodb.describe_table.return_value = \
        {"Table": {"TableStatus": "ACTIVE"}}
    with pytest.raises(TableMismatch):
        session.validate_table("Simple", SimpleModel)

    assert "the following attributes are missing for model \"SimpleModel\"" in caplog.text


def test_validate_simple_model(session, dynamodb):
    full = {
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        "TableName": "Simple",
        "TableStatus": "ACTIVE"}
    dynamodb.describe_table.return_value = {"Table": full}
    session.validate_table("Simple", SimpleModel)
    dynamodb.describe_table.assert_called_once_with(
        TableName="Simple")


def test_validate_unspecified_throughput(session, dynamodb, caplog):
    """Model doesn't care what table read/write units are"""
    class MyModel(BaseModel):
        class Meta:
            pass
        id = Column(String, hash_key=True)

    full = {
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 15, "WriteCapacityUnits": 20},
        "TableName": "MyModel",
        "TableStatus": "ACTIVE"}
    dynamodb.describe_table.return_value = {"Table": full}

    assert MyModel.Meta.read_units is None
    assert MyModel.Meta.write_units is None
    caplog.handler.records.clear()
    session.validate_table("MyModel", MyModel)
    assert MyModel.Meta.read_units == 15
    assert MyModel.Meta.write_units == 20

    assert caplog.record_tuples == [
        ("bloop.session", logging.DEBUG,
         "describe_table: table \"MyModel\" was in ACTIVE state after 1 calls"),
        ("bloop.session", logging.DEBUG,
         "MyModel.Meta does not specify read_units, set to 15 from DescribeTable response"),
        ("bloop.session", logging.DEBUG,
         "MyModel.Meta does not specify write_units, set to 20 from DescribeTable response")
    ]


def test_validate_unspecified_gsi_throughput(session, dynamodb, caplog):
    """Model doesn't care what GSI read/write units are"""
    class MyModel(BaseModel):
        class Meta:
            read_units = 1
            write_units = 1
        id = Column(String, hash_key=True)
        other = Column(String)
        by_other = GlobalSecondaryIndex(projection="keys", hash_key=other)

    description = expected_table_description("MyModel", MyModel)
    description["TableStatus"] = "ACTIVE"
    description["GlobalSecondaryIndexes"][0]["IndexStatus"] = "ACTIVE"
    throughput = description["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"]
    throughput["ReadCapacityUnits"] = 15
    throughput["WriteCapacityUnits"] = 20

    dynamodb.describe_table.return_value = {"Table": description}

    assert MyModel.by_other.read_units is None
    assert MyModel.by_other.write_units is None
    caplog.handler.records.clear()
    session.validate_table("MyModel", MyModel)
    assert MyModel.by_other.read_units == 15
    assert MyModel.by_other.write_units == 20

    assert caplog.record_tuples == [
        ("bloop.session", logging.DEBUG,
         "describe_table: table \"MyModel\" was in ACTIVE state after 1 calls"),
        ("bloop.session", logging.DEBUG,
         "MyModel.by_other does not specify read_units, set to 15 from DescribeTable response"),
        ("bloop.session", logging.DEBUG,
         "MyModel.by_other does not specify write_units, set to 20 from DescribeTable response")
    ]


def test_validate_stream_exists(session, dynamodb, caplog):
    """Model expects a stream that exists and matches"""
    class MyModel(BaseModel):
        class Meta:
            read_units = 1
            write_units = 1
            stream = {
                "include": ["keys"]
            }
        id = Column(String, hash_key=True)

    full = {
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "LatestStreamArn": "table/stream_both/stream/2016-08-29T03:30:15.582",
        "LatestStreamLabel": "2016-08-29T03:30:15.582",
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        "StreamSpecification": {
            "StreamEnabled": True,
            "StreamViewType": "KEYS_ONLY"},
        "TableName": "LocalTableName",
        "TableStatus": "ACTIVE"}
    dynamodb.describe_table.return_value = {"Table": full}
    caplog.handler.records.clear()
    session.validate_table("LocalTableName", MyModel)
    assert MyModel.Meta.stream["arn"] == "table/stream_both/stream/2016-08-29T03:30:15.582"

    assert caplog.record_tuples == [
        ("bloop.session", logging.DEBUG,
         "describe_table: table \"LocalTableName\" was in ACTIVE state after 1 calls"),
        ("bloop.session", logging.DEBUG,
         ("Set MyModel.Meta.stream[\"arn\"] to "
          "\"table/stream_both/stream/2016-08-29T03:30:15.582\" from DescribeTable response"))
    ]


def test_validate_stream_wrong_view_type(session, dynamodb):
    """Model expects a stream that doesn't exist"""
    class Model(BaseModel):
        class Meta:
            stream = {
                "include": ["keys"]
            }
        id = Column(String, hash_key=True)

    full = {
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        "StreamSpecification": {
            "StreamEnabled": True,
            "StreamViewType": "NEW_IMAGE"},
        "TableName": "LocalTableName",
        "TableStatus": "ACTIVE"}
    dynamodb.describe_table.return_value = {"Table": full}
    with pytest.raises(TableMismatch):
        session.validate_table("LocalTableName", Model)


def test_validate_stream_missing(session, dynamodb, caplog):
    """Model expects a stream that doesn't exist"""
    class Model(BaseModel):
        class Meta:
            stream = {
                "include": ["keys"]
            }
        id = Column(String, hash_key=True)

    full = {
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        "TableName": "LocalTableName",
        "TableStatus": "ACTIVE"}
    dynamodb.describe_table.return_value = {"Table": full}
    with pytest.raises(TableMismatch):
        session.validate_table("LocalTableName", Model)

    assert "expected and actual table descriptions for model \"Model\" do not match" in caplog.text


def test_validate_stream_unexpected(session, dynamodb):
    """It's ok if the model doesn't expect a stream that exists"""
    class Model(BaseModel):
        class Meta:
            stream = None
        id = Column(String, hash_key=True)

    full = {
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "LatestStreamArn": "table/stream_both/stream/2016-08-29T03:30:15.582",
        "LatestStreamLabel": "2016-08-29T03:30:15.582",
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        "StreamSpecification": {
            "StreamEnabled": True,
            "StreamViewType": "KEYS_ONLY"},
        "TableName": "LocalTableName",
        "TableStatus": "ACTIVE"}
    dynamodb.describe_table.return_value = {"Table": full}
    session.validate_table("LocalTableName", Model)


def test_validate_wrong_table(session, dynamodb):
    """dynamo returns a valid document but it doesn't match"""
    full = expected_table_description("Simple", SimpleModel)
    full["TableStatus"] = "ACTIVE"

    full["TableName"] = "wrong table name"

    dynamodb.describe_table.return_value = {"Table": full}
    with pytest.raises(TableMismatch):
        session.validate_table("Simple", SimpleModel)


def test_validate_raises(session, dynamodb):
    cause = dynamodb.describe_table.side_effect = client_error("FooError")

    with pytest.raises(BloopException) as excinfo:
        session.validate_table("User", User)
    assert excinfo.value.__cause__ is cause


def test_validate_unexpected_index(session, dynamodb):
    """Validation doesn't fail when the backing table has an extra GSI"""
    full = expected_table_description("CustomTableName", ComplexModel)
    full["GlobalSecondaryIndexes"].append({
        "IndexName": "extra_gsi",
        "Projection": {"ProjectionType": "KEYS_ONLY"},
        "KeySchema": [{"KeyType": "HASH", "AttributeName": "date"}],
        "ProvisionedThroughput": {"WriteCapacityUnits": 1, "ReadCapacityUnits": 1},
    })

    full["LocalSecondaryIndexes"].append({
        "IndexName": "extra_lsi",
        "Projection": {"ProjectionType": "KEYS_ONLY"},
        "KeySchema": [{"KeyType": "RANGE", "AttributeName": "date"}],
        "ProvisionedThroughput": {"WriteCapacityUnits": 1, "ReadCapacityUnits": 1}
    })
    dynamodb.describe_table.return_value = {"Table": full}

    full["TableStatus"] = "ACTIVE"
    for gsi in full["GlobalSecondaryIndexes"]:
        gsi["IndexStatus"] = "ACTIVE"
    # Validation passes even though there are extra Indexes and AttributeDefinitions
    session.validate_table("CustomTableName", ComplexModel)


def test_validate_superset_index(session, dynamodb):
    """Validation passes if an Index's projection is a superset of the required projection"""
    description = expected_table_description("ProjectedIndexes", ProjectedIndexes)
    description["TableStatus"] = "ACTIVE"
    description["GlobalSecondaryIndexes"][0]["IndexStatus"] = "ACTIVE"

    # projection is ALL in DynamoDB, not the exact ["both", "gsi_only"] from the model
    description["GlobalSecondaryIndexes"][0]["Projection"] = {"ProjectionType": "ALL"}

    dynamodb.describe_table.return_value = {"Table": description}
    session.validate_table("ProjectedIndexes", ProjectedIndexes)
    dynamodb.describe_table.assert_called_once_with(TableName="ProjectedIndexes")


def test_validate_missing_index(session, dynamodb, caplog):
    """Required GSI is missing"""
    description = expected_table_description("ProjectedIndexes", ProjectedIndexes)
    description["TableStatus"] = "ACTIVE"
    dynamodb.describe_table.return_value = {"Table": description}

    del description["GlobalSecondaryIndexes"]
    with pytest.raises(TableMismatch):
        session.validate_table("ProjectedIndexes", ProjectedIndexes)

    assert "table is missing expected index \"by_gsi\"" in caplog.text


def test_validate_bad_index_projection_type(session, dynamodb, caplog):
    """Required GSI is missing"""
    description = expected_table_description("ProjectedIndexes", ProjectedIndexes)
    description["TableStatus"] = "ACTIVE"
    description["GlobalSecondaryIndexes"][0]["IndexStatus"] = "ACTIVE"
    dynamodb.describe_table.return_value = {"Table": description}

    description["GlobalSecondaryIndexes"][0]["Projection"] = {"ProjectionType": "KEYS_ONLY"}
    with pytest.raises(TableMismatch):
        session.validate_table("ProjectedIndexes", ProjectedIndexes)

    assert "actual projection for index \"by_gsi\" is missing expected columns" in caplog.text


def test_validate_bad_index_key_schema(session, dynamodb, caplog):
    """KeySchema doesn't match"""
    description = expected_table_description("ProjectedIndexes", ProjectedIndexes)
    description["TableStatus"] = "ACTIVE"
    description["GlobalSecondaryIndexes"][0]["IndexStatus"] = "ACTIVE"
    dynamodb.describe_table.return_value = {"Table": description}

    description["GlobalSecondaryIndexes"][0]["KeySchema"] = [{"KeyType": "HASH", "AttributeName": "unknown"}]
    with pytest.raises(TableMismatch):
        session.validate_table("ProjectedIndexes", ProjectedIndexes)

    assert "key schema mismatch for \"by_gsi\"" in caplog.text


def test_validate_bad_index_provisioned_throughput(session, dynamodb, caplog):
    """KeySchema doesn't match"""
    description = expected_table_description("ProjectedIndexes", ProjectedIndexes)
    description["TableStatus"] = "ACTIVE"
    description["GlobalSecondaryIndexes"][0]["IndexStatus"] = "ACTIVE"
    dynamodb.describe_table.return_value = {"Table": description}

    description["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"]["WriteCapacityUnits"] = -2
    with pytest.raises(TableMismatch):
        session.validate_table("ProjectedIndexes", ProjectedIndexes)

    assert "GSI ProvisionedThroughput mismatch" in caplog.text


def test_validate_unknown_projection_type(session, dynamodb, caplog):
    """DynamoDB starts returning a new projection type"""
    description = expected_table_description("ProjectedIndexes", ProjectedIndexes)
    description["TableStatus"] = "ACTIVE"
    description["GlobalSecondaryIndexes"][0]["IndexStatus"] = "ACTIVE"
    dynamodb.describe_table.return_value = {"Table": description}

    description["GlobalSecondaryIndexes"][0]["Projection"]["ProjectionType"] = "NewProjectionType"
    with pytest.raises(TableMismatch):
        session.validate_table("ProjectedIndexes", ProjectedIndexes)

    assert "unknown index projection type \"NewProjectionType\"" in caplog.text


def test_validate_ttl(session, dynamodb):
    class Model(BaseModel):
        class Meta:
            ttl = {"column": "expiry"}
        id = Column(String, hash_key=True)
        expiry = Column(Timestamp, dynamo_name="e!!")
    description = create_table_request("LocalTableName", Model)
    description["TableStatus"] = "ACTIVE"

    dynamodb.describe_table.return_value = {"Table": description}
    dynamodb.describe_time_to_live.return_value = {
        "TimeToLiveDescription": {
            "AttributeName": "e!!",
            "TimeToLiveStatus": "ENABLING",
            "UnexpectedAttributeHere": 13  # this should be stripped out by the sanitize step
        }
    }

    session.validate_table("LocalTableName", Model)
    dynamodb.describe_table.assert_called_once_with(TableName="LocalTableName")
    dynamodb.describe_time_to_live.assert_called_once_with(TableName="LocalTableName")

    assert Model.Meta.ttl["enabled"] == "enabling"


def test_validate_ttl_mismatch(session, dynamodb):
    class Model(BaseModel):
        class Meta:
            ttl = {"column": "expiry"}
        id = Column(String, hash_key=True)
        expiry = Column(Timestamp, dynamo_name="e!!")
    description = create_table_request("LocalTableName", Model)
    description["TableStatus"] = "ACTIVE"

    dynamodb.describe_table.return_value = {"Table": description}
    dynamodb.describe_time_to_live.return_value = {
        "TimeToLiveDescription": {
            "AttributeName": "wrong-name-here",  # <-- someone bound the table's ttl to a different attribute
            "TimeToLiveStatus": "ENABLING",
            "UnexpectedAttributeHere": 13  # this should be stripped out by the sanitize step
        }
    }

    with pytest.raises(TableMismatch):
        session.validate_table("LocalTableName", Model)

    dynamodb.describe_table.assert_called_once_with(TableName="LocalTableName")
    dynamodb.describe_time_to_live.assert_called_once_with(TableName="LocalTableName")

    # status isn't changed because there's no match
    assert Model.Meta.ttl["enabled"] == "disabled"


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


# TABLE HELPERS ======================================================================================== TABLE HELPERS


def assert_unordered(obj, other):
    assert ordered(obj) == ordered(other)


def test_create_simple():
    expected = {
        'AttributeDefinitions': [
            {'AttributeName': 'id', 'AttributeType': 'S'}],
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
            stream = {
                "include": include
            }
        id = Column(String, hash_key=True)

    table = create_table_request("Model", Model)
    assert table["StreamSpecification"] == {
        "StreamEnabled": True,
        "StreamViewType": view_type
    }


def test_expected_description():
    create = create_table_request("LocalTableName", ComplexModel)
    expected = expected_table_description("LocalTableName", ComplexModel)
    assert_unordered(create, expected)


def test_expected_description_with_ttl():
    """when the model has a TTL, the column's dynamo_name is injected into the expected description"""
    class Model(BaseModel):
        class Meta:
            ttl = {"column": "expiry"}
        id = Column(String, hash_key=True)
        expiry = Column(Timestamp, dynamo_name="e!!")
    expected = create_table_request("LocalTableName", Model)
    expected["TimeToLiveDescription"] = {"AttributeName": "e!!"}

    actual = expected_table_description("LocalTableName", Model)
    assert_unordered(expected, actual)


def test_sanitize_drop_empty_lists():
    expected = expected_table_description("LocalTableName", ComplexModel)
    # Start from the same base, but inject an unnecessary NonKeyAttributes
    description = expected_table_description("LocalTableName", ComplexModel)
    index = description["GlobalSecondaryIndexes"][0]
    index["Projection"]["NonKeyAttributes"] = []

    assert_unordered(expected, sanitize_table_description(description))


def test_sanitize_drop_empty_indexes():
    expected = expected_table_description("LocalTableName", SimpleModel)
    # Start from the same base, but inject an unnecessary NonKeyAttributes
    description = expected_table_description("LocalTableName", SimpleModel)
    description["GlobalSecondaryIndexes"] = []

    assert_unordered(expected, sanitize_table_description(description))


def test_sanitize_expected():
    expected = expected_table_description("LocalTableName", User)
    # Add some extra fields
    description = {
        'AttributeDefinitions': [
            {'AttributeType': 'S', 'AttributeName': 'email'},
            {'AttributeType': 'S', 'AttributeName': 'id'}],
        'CreationDateTime': 'EXTRA_FIELD',
        'ItemCount': 'EXTRA_FIELD',
        'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}],
        'GlobalSecondaryIndexes': [{
            'IndexArn': 'EXTRA_FIELD',
            'IndexName': 'by_email',
            'IndexSizeBytes': 'EXTRA_FIELD',
            'IndexStatus': 'EXTRA_FIELD',
            'KeySchema': [{'AttributeName': 'email', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'},
            'ProvisionedThroughput': {
                'NumberOfDecreasesToday': 'EXTRA_FIELD',
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1}}],
        'ProvisionedThroughput': {
            'LastDecreaseDateTime': 'EXTRA_FIELD',
            'LastIncreaseDateTime': 'EXTRA_FIELD',
            'NumberOfDecreasesToday': 'EXTRA_FIELD',
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1},
        'TableArn': 'EXTRA_FIELD',
        'TableName': 'LocalTableName',
        'TableSizeBytes': 'EXTRA_FIELD',
        'TableStatus': 'EXTRA_FIELD'}
    sanitized = sanitize_table_description(description)
    assert_unordered(expected, sanitized)


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


# END TABLE HELPERS ================================================================================ END TABLE HELPERS
