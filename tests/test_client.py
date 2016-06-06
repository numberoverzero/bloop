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

from test_models import ComplexModel, User


@pytest.fixture
def client():
    session = Mock(spec=boto3.session.Session)
    return bloop.client.Client(session=session)


@pytest.fixture
def client_error():
    def _client_error(code):
        error_response = {"Error": {
            "Code": code,
            "Message": "FooMessage"}}
        operation_name = "OperationName"
        return botocore.exceptions.ClientError(error_response, operation_name)
    return _client_error


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
    client.client.batch_get_item.__name__ = "batch_get_item"

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


def test_call_with_retries(client, client_error):
    max_tries = 4
    tries = 0

    def backoff(operation, attempts):
        nonlocal tries
        tries += 1
        if attempts == max_tries:
            raise RuntimeError("Failed {} after {} attempts".format(
                operation, attempts))
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
    operation = "foobar"
    attempts = range(bloop.client.DEFAULT_MAX_ATTEMPTS)
    durations = [(50.0 * (2 ** x)) / 1000.0 for x in attempts]

    for (attempts, expected) in zip(attempts, durations):
        actual = bloop.client._default_backoff_func(operation, attempts)
        assert actual == expected

    with pytest.raises(RuntimeError):
        bloop.client._default_backoff_func(
            operation, bloop.client.DEFAULT_MAX_ATTEMPTS)


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
    called = False

    def create_table(**table):
        nonlocal called
        called = True
        assert bloop.util.ordered(table) == bloop.util.ordered(expected)
    client.client.create_table = create_table
    client.create_table(ComplexModel)
    assert called


def test_create_raises_unknown(client, client_error):
    called = False

    def create_table(**table):
        nonlocal called
        called = True
        raise client_error("FooError")
    client.client.create_table = create_table

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        client.create_table(User)
    assert excinfo.value.response["Error"]["Code"] == "FooError"
    assert called


def test_create_already_exists(client, client_error):
    called = False

    def create_table(**table):
        nonlocal called
        called = True
        raise client_error("ResourceInUseException")
    client.client.create_table = create_table

    client.create_table(User)
    assert called


def test_delete_item(client):
    user_id = uuid.uuid4()
    request = {"Key": {"id": {"S": str(user_id)}},
               "TableName": "User",
               "ExpressionAttributeNames": {"#n0": "id"},
               "ConditionExpression": "(attribute_not_exists(#n0))"}
    called = False

    def delete_item(**item):
        nonlocal called
        called = True
        assert item == request
    client.client.delete_item = delete_item
    client.delete_item(request)
    assert called


def test_delete_item_unknown_error(client, client_error):
    called = False
    user_id = uuid.uuid4()
    request = {"Key": {"id": {"S": str(user_id)}},
               "TableName": "User",
               "ExpressionAttributeNames": {"#n0": "id"},
               "ConditionExpression": "(attribute_not_exists(#n0))"}

    def delete_item(**item):
        nonlocal called
        called = True
        raise client_error("FooError")
    client.client.delete_item = delete_item

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        client.delete_item(request)
    assert excinfo.value.response["Error"]["Code"] == "FooError"
    assert called


def test_delete_item_condition_failed(client, client_error):
    called = False
    user_id = uuid.uuid4()
    request = {"Key": {"id": {"S": str(user_id)}},
               "TableName": "User",
               "ExpressionAttributeNames": {"#n0": "id"},
               "ConditionExpression": "(attribute_not_exists(#n0))"}

    def delete_item(**item):
        nonlocal called
        called = True
        raise client_error("ConditionalCheckFailedException")
    client.client.delete_item = delete_item

    with pytest.raises(bloop.exceptions.ConstraintViolation) as excinfo:
        client.delete_item(request)
    assert excinfo.value.obj == request
    assert called


def test_update_item(client):
    user_id = uuid.uuid4()
    request = {"Key": {"id": {"S": str(user_id)}},
               "TableName": "User",
               "ExpressionAttributeNames": {"#n0": "id"},
               "ConditionExpression": "(attribute_not_exists(#n0))"}
    called = False

    def update_item(**item):
        nonlocal called
        called = True
        assert item == request
    client.client.update_item = update_item
    client.update_item(request)
    assert called


def test_update_item_unknown_error(client, client_error):
    called = False
    user_id = uuid.uuid4()
    request = {"Key": {"id": {"S": str(user_id)}},
               "TableName": "User",
               "ExpressionAttributeNames": {"#n0": "id"},
               "ConditionExpression": "(attribute_not_exists(#n0))"}

    def update_item(**item):
        nonlocal called
        called = True
        assert item == request
        raise client_error("FooError")
    client.client.update_item = update_item

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        client.update_item(request)
    assert excinfo.value.response["Error"]["Code"] == "FooError"
    assert called


def test_update_item_condition_failed(client, client_error):
    called = False
    user_id = uuid.uuid4()
    request = {"Key": {"id": {"S": str(user_id)}},
               "TableName": "User",
               "ExpressionAttributeNames": {"#n0": "id"},
               "ConditionExpression": "(attribute_not_exists(#n0))"}

    def update_item(**item):
        nonlocal called
        called = True
        assert item == request
        raise client_error("ConditionalCheckFailedException")
    client.client.update_item = update_item

    with pytest.raises(bloop.exceptions.ConstraintViolation) as excinfo:
        client.update_item(request)
    assert excinfo.value.obj == request
    assert called


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
    called = False

    def describe_table(TableName):
        nonlocal called
        called = True
        assert TableName == ComplexModel.Meta.table_name
        return {"Table": full}
    client.client.describe_table = describe_table

    actual = client.describe_table(ComplexModel)
    assert actual == expected
    assert called


def test_query_scan(client):
    def call(**request):
        return responses[request["index"]]

    client.client.query = call
    client.client.scan = call

    responses = [
        {},
        {"Count": -1},
        {"ScannedCount": -1},
        {"Count": 1, "ScannedCount": 2}
    ]

    expecteds = [
        {"Count": 0, "ScannedCount": 0},
        {"Count": -1, "ScannedCount": -1},
        {"Count": 0, "ScannedCount": -1},
        {"Count": 1, "ScannedCount": 2},
    ]

    for index, expected in enumerate(expecteds):
        actual = client.query({"index": index})
        assert actual == expected

        actual = client.scan({"index": index})
        assert actual == expected


def test_validate_compares_tables(client):
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

    def describe_table(TableName):
        assert TableName == "User"
        return {"Table": full}
    client.client.describe_table = describe_table
    client.validate_table(User)


def test_validate_checks_status(client):
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
    calls = 0

    def describe_table(TableName):
        nonlocal calls
        calls += 1
        if calls < 2:
            return {"Table": {"TableStatus": "CREATING"}}
        return {"Table": full}

    client.client.describe_table = describe_table
    client.validate_table(User)
    assert calls == 2


def test_validate_checks_index_status(client):
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
    calls = 0

    def describe_table(TableName):
        nonlocal calls
        calls += 1
        if calls < 2:
            return {"Table": {
                "GlobalSecondaryIndexes": [{"IndexStatus": "CREATING"}]}}
        return {"Table": full}

    client.client.describe_table = describe_table
    client.validate_table(User)
    assert calls == 2


def test_validate_fails(client):
    def describe_table(TableName):
        assert TableName == "CustomTableName"
        return {"Table": {}}

    expected = {
        "KeySchema": [
            {"AttributeName": "name", "KeyType": "HASH"},
            {"AttributeName": "date", "KeyType": "RANGE"}],
        "TableName": "CustomTableName",
        "ProvisionedThroughput":
            {"ReadCapacityUnits": 3, "WriteCapacityUnits": 2},
        "LocalSecondaryIndexes": [{
            "KeySchema": [
                {"AttributeName": "name", "KeyType": "HASH"},
                {"AttributeName": "joined", "KeyType": "RANGE"}],
            "Projection": {
                "ProjectionType": "INCLUDE",
                "NonKeyAttributes": ["joined", "email", "name", "date"]},
            "IndexName": "by_joined"}],
        "GlobalSecondaryIndexes": [{
            "KeySchema": [{"AttributeName": "email", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
            "ProvisionedThroughput":
                {"ReadCapacityUnits": 4, "WriteCapacityUnits": 5},
            "IndexName": "by_email"}],
        "AttributeDefinitions": [
            {"AttributeName": "name", "AttributeType": "S"},
            {"AttributeName": "date", "AttributeType": "S"},
            {"AttributeName": "email", "AttributeType": "S"},
            {"AttributeName": "joined", "AttributeType": "S"}]}
    actual = {}
    ordered = bloop.util.ordered
    client.client.describe_table = describe_table
    with pytest.raises(bloop.exceptions.TableMismatch) as excinfo:
        client.validate_table(ComplexModel)
    assert excinfo.value.model is ComplexModel
    assert ordered(excinfo.value.expected) == ordered(expected)
    assert excinfo.value.actual == actual


def test_validate_simple_model(client):
    class SimpleModel(bloop.new_base()):
        id = bloop.Column(bloop.UUID, hash_key=True)
    full = {
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"}],
        "TableName": "SimpleModel",
        "ProvisionedThroughput": {"ReadCapacityUnits": 1,
                                  "WriteCapacityUnits": 1}}

    def describe_table(TableName):
        assert TableName == "SimpleModel"
        return {"Table": full}
    client.client.describe_table = describe_table
    client.validate_table(SimpleModel)
