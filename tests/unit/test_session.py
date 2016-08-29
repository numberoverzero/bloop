from unittest.mock import Mock

import botocore.exceptions
import pytest
from bloop.exceptions import (
    BloopException,
    ConstraintViolation,
    TableMismatch,
    UnknownSearchMode,
)
from bloop.models import BaseModel, Column
from bloop.session import (
    BATCH_GET_ITEM_CHUNK_SIZE,
    SessionWrapper,
    create_table_request,
    expected_table_description,
    ready,
    sanitized_table_description,
    simple_table_status,
)
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


# LOAD ITEMS =============================================================================================== LOAD ITEMS


def test_batch_get_raises(session, dynamodb_client):
    cause = dynamodb_client.batch_get_item.side_effect = client_error("FooError")
    request = {"TableName": {"Keys": ["key"], "ConsistentRead": False}}
    with pytest.raises(BloopException) as excinfo:
        session.load_items(request)
    assert excinfo.value.__cause__ is cause


def test_batch_get_one_item(session, dynamodb_client):
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
    dynamodb_client.batch_get_item.side_effect = handle

    response = session.load_items(request)
    assert response == expected_response
    dynamodb_client.batch_get_item.assert_called_once_with(RequestItems=expected_request)


def test_batch_get_one_batch(session, dynamodb_client):
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

    dynamodb_client.batch_get_item.return_value = boto3_client_response
    response = session.load_items(client_request)

    dynamodb_client.batch_get_item.assert_called_once_with(RequestItems=client_request)
    assert response == expected_client_response


def test_batch_get_paginated(session, dynamodb_client):
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

    dynamodb_client.batch_get_item.side_effect = batched_responses
    response = session.load_items(client_request)

    assert dynamodb_client.batch_get_item.call_count == 2
    assert response == expected_client_response


def test_batch_get_unprocessed(session, dynamodb_client):
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
    dynamodb_client.batch_get_item = handle

    response = session.load_items(request)

    assert calls == 2
    assert response == expected_response


# END LOAD ITEMS ======================================================================================= END LOAD ITEMS


# CREATE TABLE =========================================================================================== CREATE TABLE


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
    cause = dynamodb_client.create_table.side_effect = client_error("FooError")

    with pytest.raises(BloopException) as excinfo:
        session.create_table(User)
    assert excinfo.value.__cause__ is cause
    assert dynamodb_client.create_table.call_count == 1


def test_create_already_exists(session, dynamodb_client):
    dynamodb_client.create_table.side_effect = client_error("ResourceInUseException")

    session.create_table(User)
    assert dynamodb_client.create_table.call_count == 1


# END CREATE TABLE =================================================================================== END CREATE TABLE


# DELETE ITEM ============================================================================================= DELETE ITEM


def test_delete_item(session, dynamodb_client):
    request = {"foo": "bar"}
    session.delete_item(request)
    dynamodb_client.delete_item.assert_called_once_with(**request)


def test_delete_item_unknown_error(session, dynamodb_client):
    request = {"foo": "bar"}
    cause = dynamodb_client.delete_item.side_effect = client_error("FooError")

    with pytest.raises(BloopException) as excinfo:
        session.delete_item(request)
    assert excinfo.value.__cause__ is cause
    dynamodb_client.delete_item.assert_called_once_with(**request)


def test_delete_item_condition_failed(session, dynamodb_client):
    request = {"foo": "bar"}
    dynamodb_client.delete_item.side_effect = client_error("ConditionalCheckFailedException")

    with pytest.raises(ConstraintViolation):
        session.delete_item(request)
    dynamodb_client.delete_item.assert_called_once_with(**request)


# END DELETE ITEM ===================================================================================== END DELETE ITEM


# SAVE ITEM ================================================================================================= SAVE ITEM


def test_save_item(session, dynamodb_client):
    request = {"foo": "bar"}
    session.save_item(request)
    dynamodb_client.update_item.assert_called_once_with(**request)


def test_save_item_unknown_error(session, dynamodb_client):
    request = {"foo": "bar"}
    cause = dynamodb_client.update_item.side_effect = client_error("FooError")

    with pytest.raises(BloopException) as excinfo:
        session.save_item(request)
    assert excinfo.value.__cause__ is cause
    dynamodb_client.update_item.assert_called_once_with(**request)


def test_save_item_condition_failed(session, dynamodb_client):
    request = {"foo": "bar"}
    dynamodb_client.update_item.side_effect = client_error("ConditionalCheckFailedException")

    with pytest.raises(ConstraintViolation):
        session.save_item(request)
    dynamodb_client.update_item.assert_called_once_with(**request)


# END SAVE ITEM ========================================================================================= END SAVE ITEM


# QUERY SCAN SEARCH ================================================================================= QUERY SCAN SEARCH


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


def test_query_scan_raise(session, dynamodb_client):
    cause = dynamodb_client.query.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.query_items({})
    assert excinfo.value.__cause__ is cause

    cause = dynamodb_client.scan.side_effect = client_error("FooError")
    with pytest.raises(BloopException) as excinfo:
        session.scan_items({})
    assert excinfo.value.__cause__ is cause


def test_search_unknown(session):
    with pytest.raises(UnknownSearchMode) as excinfo:
        session.search_items(mode="foo", request={})
    assert "foo" in str(excinfo.value)


# VALIDATION HELPERS =============================================================================== VALIDATION HELPERS


def test_validate_compares_tables(session, dynamodb_client):
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


def test_validate_invalid_table(session, dynamodb_client):
    """dynamo returns an invalid json document"""
    dynamodb_client.describe_table.return_value = \
        {"Table": {"TableStatus": "ACTIVE"}}
    with pytest.raises(TableMismatch):
        session.validate_table(SimpleModel)


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


def test_validate_wrong_table(session, dynamodb_client):
    """dynamo returns a valid document but it doesn't match"""
    full = expected_table_description(SimpleModel)
    full["TableStatus"] = "ACTIVE"

    full["TableName"] = "wrong table name"

    dynamodb_client.describe_table.return_value = {"Table": full}
    with pytest.raises(TableMismatch):
        session.validate_table(SimpleModel)


def test_validate_raises(session, dynamodb_client):
    cause = dynamodb_client.describe_table.side_effect = client_error("FooError")

    with pytest.raises(BloopException) as excinfo:
        session.validate_table(User)
    assert excinfo.value.__cause__ is cause


# END VALIDATION HELPERS ======================================================================= END VALIDATION HELPERS


# TABLE HELPERS ========================================================================================= TABLE HELPERS


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
    assert_unordered(create_table_request(SimpleModel), expected)


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
    assert_unordered(create_table_request(ComplexModel), expected)


def test_expected_description():
    # Eventually expected_table_description will probably diverge from create_table
    # This will guard against (or coverage should show) if there's drift
    create = create_table_request(ComplexModel)
    expected = expected_table_description(ComplexModel)
    assert_unordered(create, expected)


def test_create_table_no_stream():
    """No StreamSpecification if Model.Meta.stream is None"""
    class Model(BaseModel):
        class Meta:
            stream = None
        id = Column(String, hash_key=True)
    table = create_table_request(Model)
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

    table = create_table_request(Model)
    assert table["StreamSpecification"] == {
        "StreamEnabled": True,
        "StreamViewType": view_type
    }


def test_expected_stream_no_label():
    """LatestStreamLabel shouldn't be included unless there's one in Model.Meta.Stream"""
    class Model(BaseModel):
        class Meta:
            stream = {
                "include": ["keys"]
            }
        id = Column(String, hash_key=True)
    table = expected_table_description(Model)
    assert "LatestStreamLabel" not in table


def test_expected_stream_with_label():
    """LatestStreamLabel should be included when there's one in Model.Meta.Stream"""
    class Model(BaseModel):
        class Meta:
            stream = {
                "include": ["keys"],
                "label": "2016-08-29T03:26:22.376"
            }
        id = Column(String, hash_key=True)
    table = expected_table_description(Model)
    assert table["LatestStreamLabel"] == "2016-08-29T03:26:22.376"


def test_sanitize_drop_empty_lists():
    expected = expected_table_description(ComplexModel)
    # Start from the same base, but inject an unnecessary NonKeyAttributes
    description = expected_table_description(ComplexModel)
    index = description["GlobalSecondaryIndexes"][0]
    index["Projection"]["NonKeyAttributes"] = []

    assert_unordered(expected, sanitized_table_description(description))


def test_sanitize_drop_empty_indexes():
    expected = expected_table_description(SimpleModel)
    # Start from the same base, but inject an unnecessary NonKeyAttributes
    description = expected_table_description(SimpleModel)
    description["GlobalSecondaryIndexes"] = []

    assert_unordered(expected, sanitized_table_description(description))


def test_sanitize_expected():
    expected = expected_table_description(User)
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
        'TableName': 'User',
        'TableSizeBytes': 'EXTRA_FIELD',
        'TableStatus': 'EXTRA_FIELD'}
    sanitized = sanitized_table_description(description)
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


# END TABLE HELPERS ================================================================================= END TABLE HELPERS
