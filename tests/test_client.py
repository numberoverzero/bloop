import bloop
import bloop.client
import bloop.exceptions
import bloop.util
import boto3
import botocore
import copy
import pytest
import uuid
from unittest.mock import Mock

from test_models import SimpleModel, ComplexModel, User


@pytest.fixture
def client():
    session = Mock(spec=boto3.session.Session)
    return bloop.client.Client(session=session)


def client_error(code):
    error_response = {"Error": {
        "Code": code,
        "Message": "FooMessage"}}
    operation_name = "OperationName"
    return botocore.exceptions.ClientError(error_response, operation_name)


def test_batch_get_one_item(client):
    """ A single call for a single item """
    user1 = User(id=uuid.uuid4())

    request = {"User": {"Keys": [{"id": {"S": str(user1.id)}}],
                        "ConsistentRead": False}}
    # When batching input with less keys than the batch size, the request
    # will look identical
    expected_request = request
    response = {"Responses": {"User": [{"id": {"S": str(user1.id)},
                                        "age": {"N": "4"}}]}}
    # Expected response is a single list of users
    expected_response = {"User": [{"id": {"S": str(user1.id)},
                                   "age": {"N": "4"}}]}

    def handle(RequestItems):
        assert RequestItems == expected_request
        return response
    client.client.batch_get_item.side_effect = handle

    response = client.batch_get_items(request)
    assert response == expected_response
    client.client.batch_get_item.assert_called_once_with(
        RequestItems=expected_request)


def test_batch_get_one_batch(client):
    """ A single call when the number of requested items is <= batch size """
    # Simulate a full batch
    client.batch_size = 2

    user1 = User(id=uuid.uuid4())
    user2 = User(id=uuid.uuid4())

    request = {"User": {"Keys": [{"id": {"S": str(user1.id)}},
                                 {"id": {"S": str(user2.id)}}],
                        "ConsistentRead": False}}
    # When batching input with less keys than the batch size, the request
    # will look identical
    expected_request = request
    response = {"Responses": {"User": [{"id": {"S": str(user1.id)},
                                        "age": {"N": "4"}},
                                       {"id": {"S": str(user2.id)},
                                        "age": {"N": "5"}}]}}
    # Expected response is a single list of users
    expected_response = {"User": [{"id": {"S": str(user1.id)},
                                   "age": {"N": "4"}},
                                  {"id": {"S": str(user2.id)},
                                   "age": {"N": "5"}}]}

    def handle(RequestItems):
        assert RequestItems == expected_request
        return response
    client.client.batch_get_item = handle

    response = client.batch_get_items(request)
    assert response == expected_response


def test_batch_get_paginated(client):
    """ Paginate requests to fit within the max batch size """
    # Minimum batch size so we can force pagination with 2 users
    client.batch_size = 1

    user1 = User(id=uuid.uuid4())
    user2 = User(id=uuid.uuid4())

    request = {"User": {"Keys": [{"id": {"S": str(user1.id)}},
                                 {"id": {"S": str(user2.id)}}],
                        "ConsistentRead": False}}

    expected_requests = [
        {"User": {"Keys": [{"id": {"S": str(user1.id)}}],
                  "ConsistentRead": False}},
        {"User": {"Keys": [{"id": {"S": str(user2.id)}}],
                  "ConsistentRead": False}}
    ]
    responses = [
        {"Responses": {"User": [{"id": {"S": str(user1.id)},
                                 "age": {"N": "4"}}]}},
        {"Responses": {"User": [{"id": {"S": str(user2.id)},
                                 "age": {"N": "5"}}]}}
    ]
    expected_response = {"User": [{"id": {"S": str(user1.id)},
                                   "age": {"N": "4"}},
                                  {"id": {"S": str(user2.id)},
                                   "age": {"N": "5"}}]}
    calls = 0

    def handle(RequestItems):
        nonlocal calls
        expected = expected_requests[calls]
        response = responses[calls]
        calls += 1
        assert RequestItems == expected
        return response
    client.client.batch_get_item = handle

    response = client.batch_get_items(request)

    assert calls == 2
    assert response == expected_response


def test_batch_get_unprocessed(client):
    """ Re-request unprocessed keys """
    user1 = User(id=uuid.uuid4())

    request = {"User": {"Keys": [{"id": {"S": str(user1.id)}}],
                        "ConsistentRead": False}}
    expected_requests = [
        {"User": {"Keys": [{"id": {"S": str(user1.id)}}],
                  "ConsistentRead": False}},
        {"User": {"Keys": [{"id": {"S": str(user1.id)}}],
                  "ConsistentRead": False}}
    ]
    responses = [
        {"UnprocessedKeys": {"User": {"Keys": [{"id": {"S": str(user1.id)}}],
                             "ConsistentRead": False}}},
        {"Responses": {"User": [{"id": {"S": str(user1.id)},
                                 "age": {"N": "4"}}]}}
    ]
    expected_response = {"User": [{"id": {"S": str(user1.id)},
                                   "age": {"N": "4"}}]}
    calls = 0

    def handle(RequestItems):
        nonlocal calls
        expected = expected_requests[calls]
        response = responses[calls]
        calls += 1
        assert RequestItems == expected
        return response
    client.client.batch_get_item = handle

    response = client.batch_get_items(request)

    assert calls == 2
    assert response == expected_response


def test_call_with_retries(client):
    max_tries = 4
    tries = 0

    def backoff(attempts):
        nonlocal tries
        tries += 1
        if attempts == max_tries:
            raise RuntimeError("Failed after {} attempts".format(attempts))
        # Don't sleep at all
        return 0
    client.backoff_func = backoff

    def always_raise_retryable(context):
        context["calls"] += 1
        raise client_error(bloop.client.RETRYABLE_ERRORS[0])

    def raise_twice_retryable(context):
        context["calls"] += 1
        if context["calls"] <= 2:
            raise client_error(bloop.client.RETRYABLE_ERRORS[0])

    def raise_unretryable(context):
        context["calls"] += 1
        raise client_error("FooError")

    def raise_non_botocore(context):
        context["calls"] += 1
        raise ValueError("not botocore error")

    # Try the call 4 times, then raise RuntimeError
    tries, context = 0, {"calls": 0}
    with pytest.raises(RuntimeError):
        client._call_with_retries(always_raise_retryable, context)
    assert tries == 4
    assert context["calls"] == 4

    # Fails on first call, first retry, succeeds third call
    tries, context = 0, {"calls": 0}
    client._call_with_retries(raise_twice_retryable, context)
    assert tries == 2
    assert context["calls"] == 3

    # Fails on first call, no retries
    tries, context = 0, {"calls": 0}
    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        client._call_with_retries(raise_unretryable, context)
    assert tries == 0
    assert context["calls"] == 1
    assert excinfo.value.response["Error"]["Code"] == "FooError"

    # Fails on first call, no retries
    tries, context = 0, {"calls": 0}
    with pytest.raises(ValueError):
        client._call_with_retries(raise_non_botocore, context)
    assert tries == 0
    assert context["calls"] == 1


def test_default_backoff():
    attempts = range(bloop.client.DEFAULT_MAX_ATTEMPTS)
    durations = [(50.0 * (2 ** x)) / 1000.0 for x in attempts]

    for (attempts, expected) in zip(attempts, durations):
        actual = bloop.client._default_backoff_func(attempts)
        assert actual == expected

    with pytest.raises(RuntimeError):
        bloop.client._default_backoff_func(bloop.client.DEFAULT_MAX_ATTEMPTS)


def test_create_table(client):
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

    def create_table(**table):
        assert bloop.util.ordered(table) == bloop.util.ordered(expected)
    client.client.create_table.side_effect = create_table
    client.create_table(ComplexModel)
    assert client.client.create_table.call_count == 1


def test_create_subclass(client):
    base_model = User

    # Shouldn't include base model's columns in create_table call
    class SubModel(base_model):
        id = bloop.Column(bloop.String, hash_key=True)

    expected = {
        'AttributeDefinitions': [
            {'AttributeName': 'id', 'AttributeType': 'S'}],
        'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1},
        'TableName': 'SubModel'}

    def create_table(**table):
        assert bloop.util.ordered(table) == bloop.util.ordered(expected)

    client.client.create_table.side_effect = create_table
    client.create_table(SubModel)
    assert client.client.create_table.call_count == 1


def test_create_raises_unknown(client):
    client.client.create_table.side_effect = client_error("FooError")

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        client.create_table(User)
    assert excinfo.value.response["Error"]["Code"] == "FooError"
    assert client.client.create_table.call_count == 1


def test_create_abstract_raises(client):
    abstract_model = bloop.new_base()
    with pytest.raises(bloop.exceptions.AbstractModelException) as excinfo:
        client.create_table(abstract_model)
    assert excinfo.value.model is abstract_model


def test_create_already_exists(client):
    client.client.create_table.side_effect = \
        client_error("ResourceInUseException")

    client.create_table(User)
    assert client.client.create_table.call_count == 1


def test_delete_item(client):
    request = {"foo": "bar"}
    client.delete_item(request)
    client.client.delete_item.assert_called_once_with(**request)


def test_delete_item_unknown_error(client):
    request = {"foo": "bar"}
    client.client.delete_item.side_effect = client_error("FooError")

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        client.delete_item(request)
    assert excinfo.value.response["Error"]["Code"] == "FooError"
    client.client.delete_item.assert_called_once_with(**request)


def test_delete_item_condition_failed(client):
    request = {"foo": "bar"}
    client.client.delete_item.side_effect = \
        client_error("ConditionalCheckFailedException")

    with pytest.raises(bloop.exceptions.ConstraintViolation) as excinfo:
        client.delete_item(request)
    assert excinfo.value.obj == request
    client.client.delete_item.assert_called_once_with(**request)


def test_update_item(client):
    request = {"foo": "bar"}
    client.update_item(request)
    client.client.update_item.assert_called_once_with(**request)


def test_update_item_unknown_error(client):
    request = {"foo": "bar"}
    client.client.update_item.side_effect = client_error("FooError")

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        client.update_item(request)
    assert excinfo.value.response["Error"]["Code"] == "FooError"
    client.client.update_item.assert_called_once_with(**request)


def test_update_item_condition_failed(client):
    request = {"foo": "bar"}
    client.client.update_item.side_effect = \
        client_error("ConditionalCheckFailedException")

    with pytest.raises(bloop.exceptions.ConstraintViolation) as excinfo:
        client.update_item(request)
    assert excinfo.value.obj == request
    client.client.update_item.assert_called_once_with(**request)


def test_describe_table(client):
    full = {
        "LocalSecondaryIndexes": [
            {"ItemCount": 7,
             "IndexSizeBytes": 8,
             "Projection": {"NonKeyAttributes": ["date", "name",
                                                 "email", "joined"],
                            "ProjectionType": "INCLUDE"},
             "IndexName": "by_joined",
             "KeySchema": [
                 {"KeyType": "HASH", "AttributeName": "name"},
                 {"KeyType": "RANGE", "AttributeName": "joined"}]}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 3,
                                  "WriteCapacityUnits": 2,
                                  "NumberOfDecreasesToday": 4},
        "GlobalSecondaryIndexes": [
            {"IndexArn": "arn:aws:dynamodb:us-west-2:*:*",
             "ItemCount": 7,
             "IndexSizeBytes": 8,
             "Projection": {"ProjectionType": "ALL"},
             "IndexName": "by_email",
             "ProvisionedThroughput": {"ReadCapacityUnits": 4,
                                       "WriteCapacityUnits": 5,
                                       "NumberOfDecreasesToday": 6},
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

    expected = copy.deepcopy(full)
    expected["ProvisionedThroughput"].pop("NumberOfDecreasesToday")
    gsi = expected["GlobalSecondaryIndexes"][0]
    gsi.pop("ItemCount")
    gsi.pop("IndexSizeBytes")
    gsi.pop("IndexArn")
    gsi["ProvisionedThroughput"].pop("NumberOfDecreasesToday")
    lsi = expected["LocalSecondaryIndexes"][0]
    lsi.pop("ItemCount")
    lsi.pop("IndexSizeBytes")

    client.client.describe_table.return_value = {"Table": full}

    assert client.describe_table(ComplexModel) == expected
    client.client.describe_table.assert_called_once_with(
        TableName=ComplexModel.Meta.table_name)


@pytest.mark.parametrize("response, expected", [
    ({}, (0, 0)),
    ({"Count": -1}, (-1, -1)),
    ({"ScannedCount": -1}, (0, -1)),
    ({"Count": 1, "ScannedCount": 2}, (1, 2))
], ids=str)
def test_query_scan(client, response, expected):
    client.client.query.return_value = response
    client.client.scan.return_value = response

    expected = {"Count": expected[0], "ScannedCount": expected[1]}
    assert client.query({}) == expected
    assert client.scan({}) == expected


def test_validate_compares_tables(client):
    # Hardcoded to protect against bugs in bloop.client._table_for_model
    full = {
        "AttributeDefinitions": [
            {"AttributeType": "S", "AttributeName": "id"},
            {"AttributeType": "S", "AttributeName": "email"}],
        "KeySchema": [{"KeyType": "HASH", "AttributeName": "id"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 1,
                                  "WriteCapacityUnits": 1,
                                  "NumberOfDecreasesToday": 4},
        "GlobalSecondaryIndexes": [
            {"ItemCount": 7,
             "IndexSizeBytes": 8,
             "IndexName": "by_email",
             "ProvisionedThroughput": {
                 "NumberOfDecreasesToday": 3,
                 "ReadCapacityUnits": 1,
                 "WriteCapacityUnits": 1},
             "KeySchema": [{"KeyType": "HASH", "AttributeName": "email"}],
             "Projection": {"ProjectionType": "ALL"}}],
        "TableName": "User"}

    client.client.describe_table.return_value = {"Table": full}
    client.validate_table(User)
    client.client.describe_table.assert_called_once_with(TableName="User")


def test_validate_checks_status(client):
    # Don't care about the value checking, just want to observe retries
    # based on busy tables or indexes
    full = bloop.client._table_for_model(User)

    client.client.describe_table.side_effect = [
        {"Table": {"TableStatus": "CREATING"}},
        {"Table": {"GlobalSecondaryIndexes": [{"IndexStatus": "CREATING"}]}},
        {"Table": full}
    ]
    client.validate_table(User)
    client.client.describe_table.assert_called_with(TableName="User")
    assert client.client.describe_table.call_count == 3


def test_validate_fails(client):
    """dynamo returns a json document that doesn't match the expected table"""
    client.client.describe_table.return_value = {"Table": {}}
    with pytest.raises(bloop.exceptions.TableMismatch) as excinfo:
        client.validate_table(ComplexModel)

    # Exception includes the model that failed
    assert excinfo.value.model is ComplexModel
    # Exception should include the full table description that was expected
    ordered = bloop.util.ordered
    expected = bloop.client._table_for_model(ComplexModel)
    assert ordered(excinfo.value.expected) == ordered(expected)
    # And the actual table that was returned
    assert excinfo.value.actual == {}


def test_validate_simple_model(client):
    full = {
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}],
        "TableName": "Simple",
        "ProvisionedThroughput": {"ReadCapacityUnits": 1,
                                  "WriteCapacityUnits": 1}}
    client.client.describe_table.return_value = {"Table": full}
    client.validate_table(SimpleModel)
    client.client.describe_table.assert_called_once_with(TableName="Simple")
