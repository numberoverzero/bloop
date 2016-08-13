import uuid
from unittest.mock import Mock

import botocore.exceptions
import pytest
from bloop.exceptions import (
    AbstractModelException,
    ConstraintViolation,
    TableMismatch,
)
from bloop.models import BaseModel, Column
from bloop.operations.models import BATCH_GET_ITEM_CHUNK_SIZE
from bloop.operations.tables import describe_table, expected_table_description
from bloop.operations import SessionWrapper
from bloop.types import String
from bloop.util import ordered

from ..helpers.models import ComplexModel, SimpleModel, User


@pytest.fixture
def dynamodb_client():
    # No spec since clients are generated dynamically.
    # We could use botocore.client.BaseClient but it's so generic
    # that we don't gain any useful inspections
    return Mock()


@pytest.fixture
def session(dynamodb_client):
    class Session:
        def client(self, name):
            return dynamodb_client
    return SessionWrapper(Session())


def client_error(code):
    error_response = {
        "Error": {
            "Code": code,
            "Message": "FooMessage"}}
    operation_name = "OperationName"
    return botocore.exceptions.ClientError(error_response, operation_name)


def test_batch_get_one_item(session, dynamodb_client):
    """A single call for a single item"""
    user1 = User(id=uuid.uuid4())

    request = {"User": {"Keys": [{"id": {"S": str(user1.id)}}],
                        "ConsistentRead": False}}
    # When batching input with less keys than the batch size, the request
    # will look identical
    expected_request = request
    response = {
        "Responses": {
            "User": [{"id": {"S": str(user1.id)}, "age": {"N": "4"}}]
        },
        "UnprocessedKeys": {}
    }
    # Expected response is a single list of users
    expected_response = {"User": [{"id": {"S": str(user1.id)}, "age": {"N": "4"}}]}

    def handle(RequestItems):
        assert RequestItems == expected_request
        return response
    dynamodb_client.batch_get_item.side_effect = handle

    response = session.load_items(request)
    assert response == expected_response
    dynamodb_client.batch_get_item.assert_called_once_with(RequestItems=expected_request)


def test_batch_get_one_batch(session, dynamodb_client):
    """A single call when the number of requested items is <= batch size"""
    users = [User(id=uuid.uuid4()) for _ in range(BATCH_GET_ITEM_CHUNK_SIZE)]

    # Request to the bloop client
    client_request = {
        "User": {
            "Keys": [
                {"id": {"S": str(user.id)}}
                for user in users
            ],
            "ConsistentRead": False
        }
    }

    boto3_client_response = {
            "Responses": {
                "User": [
                    {"id": {"S": str(user.id)}, "age": {"N": "4"}}
                    for user in users
                ]
            },
            "UnprocessedKeys": {}
        }

    # The response that the bloop client should return
    expected_client_response = boto3_client_response["Responses"]

    dynamodb_client.batch_get_item.return_value = boto3_client_response
    response = session.load_items(client_request)

    dynamodb_client.batch_get_item.assert_called_once_with(RequestItems=client_request)
    assert response == expected_client_response


def test_batch_get_paginated(session, dynamodb_client):
    """Paginate requests to fit within the max batch size"""
    users = [User(id=uuid.uuid4()) for _ in range(BATCH_GET_ITEM_CHUNK_SIZE+1)]
    keys = [
        {"id": {"S": str(user.id)}}
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
                    {"id": {"S": str(user.id)}, "age": {"N": "4"}}
                    for user in users[:BATCH_GET_ITEM_CHUNK_SIZE]
                ]
            },
            "UnprocessedKeys": {}
        },
        # [BATCH_GET_ITEM_CHUNK_SIZE+1, ] items
        {
            "Responses": {
                "User": [
                    {"id": {"S": str(user.id)}, "age": {"N": "4"}}
                    for user in users[BATCH_GET_ITEM_CHUNK_SIZE:]
                ]
            },
            "UnprocessedKeys": {}
        }
    ]

    # The response that the bloop client should return (all items)
    expected_client_response = {
            "User": [
                {"id": {"S": str(user.id)}, "age": {"N": "4"}}
                for user in users
            ]
        }

    dynamodb_client.batch_get_item.side_effect = batched_responses
    response = session.load_items(client_request)

    assert dynamodb_client.batch_get_item.call_count == 2
    assert response == expected_client_response


def test_batch_get_unprocessed(session, dynamodb_client):
    """ Re-request unprocessed keys """
    user1 = User(id=uuid.uuid4())

    request = {
        "User": {
            "Keys": [{"id": {"S": str(user1.id)}}],
            "ConsistentRead": False
        }
    }
    expected_requests = [{
        "User": {
            "Keys": [{"id": {"S": str(user1.id)}}],
            "ConsistentRead": False}
    }, {
        "User": {
            "Keys": [{"id": {"S": str(user1.id)}}],
            "ConsistentRead": False}
    }]

    responses = [{
        "UnprocessedKeys": {
            "User": {
                "Keys": [{"id": {"S": str(user1.id)}}],
                "ConsistentRead": False}}
    }, {
        "Responses": {
            "User": [{"id": {"S": str(user1.id)}, "age": {"N": "4"}}]
        },
        "UnprocessedKeys": {}
    }]

    expected_response = {"User": [{"id": {"S": str(user1.id)}, "age": {"N": "4"}}]}
    calls = 0

    def handle(RequestItems):
        nonlocal calls
        expected = expected_requests[calls]
        response = responses[calls]
        calls += 1
        assert RequestItems == expected
        return response
    dynamodb_client.batch_get_item = handle

    response = session.load_items(request)

    assert calls == 2
    assert response == expected_response


def test_create_table(session, dynamodb_client):
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
        "TableName": "CustomTableName",
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
    dynamodb_client.create_table.side_effect = handle
    session.create_table(ComplexModel)
    assert dynamodb_client.create_table.call_count == 1


def test_create_subclass(session, dynamodb_client):
    base_model = User

    # Shouldn't include base model's columns in create_table call
    class SubModel(base_model):
        id = Column(String, hash_key=True)

    expected = {
        'AttributeDefinitions': [
            {'AttributeName': 'id', 'AttributeType': 'S'}],
        'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1},
        'TableName': 'SubModel'}

    def handle(**table):
        assert ordered(table) == ordered(expected)

    dynamodb_client.create_table.side_effect = handle
    session.create_table(SubModel)
    assert dynamodb_client.create_table.call_count == 1


def test_create_raises_unknown(session, dynamodb_client):
    dynamodb_client.create_table.side_effect = client_error("FooError")

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        session.create_table(User)
    assert excinfo.value.response["Error"]["Code"] == "FooError"
    assert dynamodb_client.create_table.call_count == 1


def test_create_abstract_raises(session, dynamodb_client):
    with pytest.raises(AbstractModelException) as excinfo:
        session.create_table(BaseModel)
    assert excinfo.value.model is BaseModel


def test_create_already_exists(session, dynamodb_client):
    dynamodb_client.create_table.side_effect = client_error("ResourceInUseException")

    session.create_table(User)
    assert dynamodb_client.create_table.call_count == 1


def test_delete_item(session, dynamodb_client):
    request = {"foo": "bar"}
    session.delete_item(request)
    dynamodb_client.delete_item.assert_called_once_with(**request)


def test_delete_item_unknown_error(session, dynamodb_client):
    request = {"foo": "bar"}
    dynamodb_client.delete_item.side_effect = client_error("FooError")

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        session.delete_item(request)
    assert excinfo.value.response["Error"]["Code"] == "FooError"
    dynamodb_client.delete_item.assert_called_once_with(**request)


def test_delete_item_condition_failed(session, dynamodb_client):
    request = {"foo": "bar"}
    dynamodb_client.delete_item.side_effect = client_error("ConditionalCheckFailedException")

    with pytest.raises(ConstraintViolation) as excinfo:
        session.delete_item(request)
    assert excinfo.value.obj == request
    dynamodb_client.delete_item.assert_called_once_with(**request)


def test_save_item(session, dynamodb_client):
    request = {"foo": "bar"}
    session.save_item(request)
    dynamodb_client.update_item.assert_called_once_with(**request)


def test_save_item_unknown_error(session, dynamodb_client):
    request = {"foo": "bar"}
    dynamodb_client.update_item.side_effect = client_error("FooError")

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        session.save_item(request)
    assert excinfo.value.response["Error"]["Code"] == "FooError"
    dynamodb_client.update_item.assert_called_once_with(**request)


def test_save_item_condition_failed(session, dynamodb_client):
    request = {"foo": "bar"}
    dynamodb_client.update_item.side_effect = client_error("ConditionalCheckFailedException")

    with pytest.raises(ConstraintViolation) as excinfo:
        session.save_item(request)
    assert excinfo.value.obj == request
    dynamodb_client.update_item.assert_called_once_with(**request)


def test_describe_table(dynamodb_client):
    dynamodb_client.describe_table.return_value = {"Table": {"test": "value"}}

    assert describe_table(dynamodb_client, ComplexModel) == {"test": "value"}
    dynamodb_client.describe_table.assert_called_once_with(
        TableName=ComplexModel.Meta.table_name)


@pytest.mark.parametrize("response, expected", [
    ({}, (0, 0)),
    ({"Count": -1}, (-1, -1)),
    ({"ScannedCount": -1}, (0, -1)),
    ({"Count": 1, "ScannedCount": 2}, (1, 2))
], ids=str)
def test_query_scan(session, dynamodb_client, response, expected):
    dynamodb_client.query.return_value = response
    dynamodb_client.scan.return_value = response

    expected = {"Count": expected[0], "ScannedCount": expected[1]}
    assert session.query_items({}) == expected
    assert session.scan_items({}) == expected


def test_validate_compares_tables(session, dynamodb_client):
    # Hardcoded to protect against bugs in bloop.client._table_for_model
    description = expected_table_description(User)
    description["TableStatus"] = "ACTIVE"
    description["GlobalSecondaryIndexes"][0]["IndexStatus"] = "ACTIVE"

    dynamodb_client.describe_table.return_value = {"Table": description}
    session.validate_table(User)
    dynamodb_client.describe_table.assert_called_once_with(TableName="User")


def test_validate_checks_status(session, dynamodb_client):
    # Don't care about the value checking, just want to observe retries
    # based on busy tables or indexes
    full = expected_table_description(User)
    full["TableStatus"] = "ACTIVE"
    full["GlobalSecondaryIndexes"][0]["IndexStatus"] = "ACTIVE"

    dynamodb_client.describe_table.side_effect = [
        {"Table": {"TableStatus": "CREATING"}},
        {"Table": {"TableStatus": "ACTIVE",
                   "GlobalSecondaryIndexes": [
                       {"IndexStatus": "CREATING"}]}},
        {"Table": full}
    ]
    session.validate_table(User)
    dynamodb_client.describe_table.assert_called_with(TableName="User")
    assert dynamodb_client.describe_table.call_count == 3


def test_validate_fails(session, dynamodb_client):
    """dynamo returns a json document that doesn't match the expected table"""
    dynamodb_client.describe_table.return_value = \
        {"Table": {"TableStatus": "ACTIVE"}}
    with pytest.raises(TableMismatch) as excinfo:
        session.validate_table(SimpleModel)

    # Exception includes the model that failed
    assert excinfo.value.model is SimpleModel
    # Exception should include the full table description that was expected
    expected = expected_table_description(SimpleModel)
    assert ordered(excinfo.value.expected) == ordered(expected)
    # And the actual table that was returned - the unsanitized description,
    # since the parsing failed
    assert excinfo.value.actual == {"TableStatus": "ACTIVE"}


def test_validate_simple_model(session, dynamodb_client):
    full = {
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        "TableName": "Simple",
        "TableStatus": "ACTIVE"}
    dynamodb_client.describe_table.return_value = {"Table": full}
    session.validate_table(SimpleModel)
    dynamodb_client.describe_table.assert_called_once_with(
        TableName="Simple")


def test_validate_mismatch(session, dynamodb_client):
    """dynamo returns a valid document but it doesn't match"""
    full = expected_table_description(SimpleModel)
    full["TableStatus"] = "ACTIVE"

    full["TableName"] = "wrong table name"

    dynamodb_client.describe_table.return_value = {"Table": full}
    with pytest.raises(TableMismatch) as excinfo:
        session.validate_table(SimpleModel)

    # Exception includes the model that failed
    assert excinfo.value.model is SimpleModel
    # Exception should include the full table description that was expected
    expected = expected_table_description(SimpleModel)
    assert ordered(excinfo.value.expected) == ordered(expected)
    # And the actual table that was returned
    del full["TableStatus"]
    assert excinfo.value.actual == full
