import bloop.client
import botocore
import copy
import pytest
import uuid


def test_batch_get_one_item(User, client):
    ''' A single call for a single item '''
    user1 = User(id=uuid.uuid4())

    request = {'User': {'Keys': [{'id': {'S': str(user1.id)}}],
                        'ConsistentRead': False}}
    # When batching input with less keys than the batch size, the request
    # will look identical
    expected_request = request
    response = {"Responses": {"User": [{'id': {'S': str(user1.id)},
                                        'age': {'N': '4'}}]}}
    # Expected response is a single list of users
    expected_response = {'User': [{'id': {'S': str(user1.id)},
                                   'age': {'N': '4'}}]}

    def handle(RequestItems):
        assert RequestItems == expected_request
        return response
    client.client.batch_get_item = handle

    response = client.batch_get_items(request)
    assert response == expected_response


def test_batch_get_one_batch(User, client):
    ''' A single call when the number of requested items is <= batch size '''
    # Simulate a full batch
    client.batch_size = 2

    user1 = User(id=uuid.uuid4())
    user2 = User(id=uuid.uuid4())

    request = {'User': {'Keys': [{'id': {'S': str(user1.id)}},
                                 {'id': {'S': str(user2.id)}}],
                        'ConsistentRead': False}}
    # When batching input with less keys than the batch size, the request
    # will look identical
    expected_request = request
    response = {"Responses": {"User": [{'id': {'S': str(user1.id)},
                                        'age': {'N': '4'}},
                                       {'id': {'S': str(user2.id)},
                                        'age': {'N': '5'}}]}}
    # Expected response is a single list of users
    expected_response = {'User': [{'id': {'S': str(user1.id)},
                                   'age': {'N': '4'}},
                                  {'id': {'S': str(user2.id)},
                                   'age': {'N': '5'}}]}

    def handle(RequestItems):
        assert RequestItems == expected_request
        return response
    client.client.batch_get_item = handle

    response = client.batch_get_items(request)
    assert response == expected_response


def test_batch_get_paginated(User, client):
    ''' Paginate requests to fit within the max batch size '''
    # Minimum batch size so we can force pagination with 2 users
    client.batch_size = 1

    user1 = User(id=uuid.uuid4())
    user2 = User(id=uuid.uuid4())

    request = {'User': {'Keys': [{'id': {'S': str(user1.id)}},
                                 {'id': {'S': str(user2.id)}}],
                        'ConsistentRead': False}}

    expected_requests = [
        {'User': {'Keys': [{'id': {'S': str(user1.id)}}],
                  'ConsistentRead': False}},
        {'User': {'Keys': [{'id': {'S': str(user2.id)}}],
                  'ConsistentRead': False}}
    ]
    responses = [
        {"Responses": {"User": [{'id': {'S': str(user1.id)},
                                 'age': {'N': '4'}}]}},
        {"Responses": {"User": [{'id': {'S': str(user2.id)},
                                 'age': {'N': '5'}}]}}
    ]
    expected_response = {'User': [{'id': {'S': str(user1.id)},
                                   'age': {'N': '4'}},
                                  {'id': {'S': str(user2.id)},
                                   'age': {'N': '5'}}]}
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


def test_batch_get_unprocessed(User, client):
    ''' Re-request unprocessed keys '''
    user1 = User(id=uuid.uuid4())

    request = {'User': {'Keys': [{'id': {'S': str(user1.id)}}],
                        'ConsistentRead': False}}
    expected_requests = [
        {'User': {'Keys': [{'id': {'S': str(user1.id)}}],
                  'ConsistentRead': False}},
        {'User': {'Keys': [{'id': {'S': str(user1.id)}}],
                  'ConsistentRead': False}}
    ]
    responses = [
        {"UnprocessedKeys": {'User': {'Keys': [{'id': {'S': str(user1.id)}}],
                             'ConsistentRead': False}}},
        {"Responses": {"User": [{'id': {'S': str(user1.id)},
                                 'age': {'N': '4'}}]}}
    ]
    expected_response = {'User': [{'id': {'S': str(user1.id)},
                                   'age': {'N': '4'}}]}
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


def test_batch_write_one_item(User, client):
    ''' A single call for a single item '''
    user1 = User(id=uuid.uuid4())

    request = {'User': [
        {'PutRequest': {'Item': {'id': {'S': str(user1.id)}}}}]
    }
    # When batching input with less keys than the batch size, the request
    # will look identical
    expected_request = request

    calls = 0

    def handle(RequestItems):
        nonlocal calls
        calls += 1
        assert RequestItems == expected_request
        return {}
    client.client.batch_write_item = handle
    client.batch_write_items(request)
    assert calls == 1


def test_batch_write_one_batch(User, client):
    ''' A single call when the number of requested items is <= batch size '''
    # Simulate a full batch
    client.batch_size = 2

    user1 = User(id=uuid.uuid4())
    user2 = User(id=uuid.uuid4())

    request = {'User': [
        {'PutRequest': {'Item': {'id': {'S': str(user1.id)}}}},
        {'PutRequest': {'Item': {'id': {'S': str(user2.id)}}}}]
    }
    # When batching input with less keys than the batch size, the request
    # will look identical
    expected_request = request

    calls = 0

    def handle(RequestItems):
        nonlocal calls
        calls += 1
        assert RequestItems == expected_request
        return {}
    client.client.batch_write_item = handle

    client.batch_write_items(request)
    assert calls == 1


def test_batch_write_paginated(User, client):
    ''' Paginate requests to fit within the max batch size '''
    # Minimum batch size so we can force pagination with 2 users
    client.batch_size = 1

    user1 = User(id=uuid.uuid4())
    user2 = User(id=uuid.uuid4())

    request = {'User': [
        {'PutRequest': {'Item': {'id': {'S': str(user1.id)}}}},
        {'PutRequest': {'Item': {'id': {'S': str(user2.id)}}}}]
    }
    expected_requests = [
        {'User': [
            {'PutRequest': {'Item': {'id': {'S': str(user1.id)}}}}]},
        {'User': [
            {'PutRequest': {'Item': {'id': {'S': str(user2.id)}}}}]}
    ]
    calls = 0

    def handle(RequestItems):
        nonlocal calls
        expected = expected_requests[calls]
        calls += 1
        assert RequestItems == expected
        return {}
    client.client.batch_write_item = handle

    client.batch_write_items(request)
    assert calls == 2


def test_batch_write_unprocessed(User, client):
    ''' Re-request unprocessed items '''
    user1 = User(id=uuid.uuid4())

    request = {'User': [
        {'PutRequest': {'Item': {'id': {'S': str(user1.id)}}}}]
    }
    expected_requests = [
        {'User': [
            {'PutRequest': {'Item': {'id': {'S': str(user1.id)}}}}]},
        {'User': [
            {'PutRequest': {'Item': {'id': {'S': str(user1.id)}}}}]}
    ]
    responses = [
        {"UnprocessedItems": {'User': [
            {'PutRequest': {'Item': {'id': {'S': str(user1.id)}}}}]}},
        {}
    ]
    calls = 0

    def handle(RequestItems):
        nonlocal calls
        expected = expected_requests[calls]
        response = responses[calls]
        calls += 1
        assert RequestItems == expected
        return response
    client.client.batch_write_item = handle

    client.batch_write_items(request)
    assert calls == 2


def test_create_table(User, client):
    expected = {
        'AttributeDefinitions': [
            {'AttributeName': 'id', 'AttributeType': 'S'},
            {'AttributeName': 'email', 'AttributeType': 'S'}],
        'ProvisionedThroughput': {'ReadCapacityUnits': 1,
                                  'WriteCapacityUnits': 1},
        'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}],
        'TableName': 'User',
        'GlobalSecondaryIndexes': [
            {'Projection': {'ProjectionType': 'ALL'},
             'ProvisionedThroughput': {'ReadCapacityUnits': 1,
                                       'WriteCapacityUnits': 1},
             'IndexName': 'by_email',
             'KeySchema': [{'AttributeName': 'email', 'KeyType': 'HASH'}]}]}

    called = False

    def create_table(**table):
        nonlocal called
        called = True
        assert table == expected
    client.client.create_table = create_table
    client.create_table(User)
    assert called


def test_create_raises_unknown(User, client, client_error):
    called = False

    def create_table(**table):
        nonlocal called
        called = True
        raise client_error('FooError')
    client.client.create_table = create_table

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        client.create_table(User)
    assert excinfo.value.response['Error']['Code'] == 'FooError'
    assert called


def test_create_already_exists(User, client, client_error):
    called = False

    def create_table(**table):
        nonlocal called
        called = True
        raise client_error('ResourceInUseException')
    client.client.create_table = create_table

    client.create_table(User)
    assert called


def test_delete_item(User, client):
    user_id = uuid.uuid4()
    request = {'Key': {'id': {'S': str(user_id)}},
               'TableName': 'User',
               'ExpressionAttributeNames': {'#n0': 'id'},
               'ConditionExpression': '(attribute_not_exists(#n0))'}
    called = False

    def delete_item(**item):
        nonlocal called
        called = True
        assert item == request
    client.client.delete_item = delete_item
    client.delete_item(request)
    assert called


def test_delete_item_unknown_error(User, client, client_error):
    called = False
    user_id = uuid.uuid4()
    request = {'Key': {'id': {'S': str(user_id)}},
               'TableName': 'User',
               'ExpressionAttributeNames': {'#n0': 'id'},
               'ConditionExpression': '(attribute_not_exists(#n0))'}

    def delete_item(**item):
        nonlocal called
        called = True
        raise client_error('FooError')
    client.client.delete_item = delete_item

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        client.delete_item(request)
    assert excinfo.value.response['Error']['Code'] == 'FooError'
    assert called


def test_delete_item_condition_failed(User, client, client_error):
    called = False
    user_id = uuid.uuid4()
    request = {'Key': {'id': {'S': str(user_id)}},
               'TableName': 'User',
               'ExpressionAttributeNames': {'#n0': 'id'},
               'ConditionExpression': '(attribute_not_exists(#n0))'}

    def delete_item(**item):
        nonlocal called
        called = True
        raise client_error('ConditionalCheckFailedException')
    client.client.delete_item = delete_item

    with pytest.raises(bloop.client.ConstraintViolation) as excinfo:
        client.delete_item(request)
    assert excinfo.value.obj == request
    assert called


def test_put_item(User, client):
    user_id = uuid.uuid4()
    request = {'Key': {'id': {'S': str(user_id)}},
               'TableName': 'User',
               'ExpressionAttributeNames': {'#n0': 'id'},
               'ConditionExpression': '(attribute_not_exists(#n0))'}
    called = False

    def put_item(**item):
        nonlocal called
        called = True
        assert item == request
    client.client.put_item = put_item
    client.put_item(request)
    assert called


def test_put_item_unknown_error(User, client, client_error):
    called = False
    user_id = uuid.uuid4()
    request = {'Key': {'id': {'S': str(user_id)}},
               'TableName': 'User',
               'ExpressionAttributeNames': {'#n0': 'id'},
               'ConditionExpression': '(attribute_not_exists(#n0))'}

    def put_item(**item):
        nonlocal called
        called = True
        assert item == request
        raise client_error('FooError')
    client.client.put_item = put_item

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        client.put_item(request)
    assert excinfo.value.response['Error']['Code'] == 'FooError'
    assert called


def test_put_item_condition_failed(User, client, client_error):
    called = False
    user_id = uuid.uuid4()
    request = {'Key': {'id': {'S': str(user_id)}},
               'TableName': 'User',
               'ExpressionAttributeNames': {'#n0': 'id'},
               'ConditionExpression': '(attribute_not_exists(#n0))'}

    def put_item(**item):
        nonlocal called
        called = True
        assert item == request
        raise client_error('ConditionalCheckFailedException')
    client.client.put_item = put_item

    with pytest.raises(bloop.client.ConstraintViolation) as excinfo:
        client.put_item(request)
    assert excinfo.value.obj == request
    assert called


def test_describe_table(User, client):
    full = {
        'AttributeDefinitions': [
            {'AttributeType': 'S', 'AttributeName': 'id'},
            {'AttributeType': 'S', 'AttributeName': 'email'}],
        'KeySchema': [{'KeyType': 'HASH', 'AttributeName': 'id'}],
        'ProvisionedThroughput': {'ReadCapacityUnits': 1,
                                  'WriteCapacityUnits': 1,
                                  'NumberOfDecreasesToday': 4},
        'GlobalSecondaryIndexes': [
            {'ItemCount': 7,
             'IndexSizeBytes': 8,
             'IndexName': 'by_email',
             'ProvisionedThroughput': {
                 'NumberOfDecreasesToday': 3,
                 'ReadCapacityUnits': 1,
                 'WriteCapacityUnits': 1},
             'KeySchema': [{'KeyType': 'HASH', 'AttributeName': 'email'}],
             'Projection': {'ProjectionType': 'ALL'}}],
        'LocalSecondaryIndexes': [
            {'ItemCount': 7,
             'IndexSizeBytes': 8,
             'IndexName': 'by_foo',
             'KeySchema': [{'KeyType': 'RANGE', 'AttributeName': 'foo'}],
             'Projection': {'ProjectionType': 'ALL'}}],
        'TableName': 'User'}

    expected = copy.deepcopy(full)
    expected['ProvisionedThroughput'].pop('NumberOfDecreasesToday')
    gsi = expected['GlobalSecondaryIndexes'][0]
    gsi.pop('ItemCount')
    gsi.pop('IndexSizeBytes')
    gsi['ProvisionedThroughput'].pop('NumberOfDecreasesToday')
    lsi = expected['LocalSecondaryIndexes'][0]
    lsi.pop('ItemCount')
    lsi.pop('IndexSizeBytes')
    called = False

    def describe_table(TableName):
        nonlocal called
        called = True
        assert TableName == User.Meta.table_name
        return {"Table": full}
    client.client.describe_table = describe_table

    actual = client.describe_table(User)
    assert actual == expected
    assert called


def test_query_scan(User, client):
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
        actual = client.query(index=index)
        assert actual == expected

        actual = client.scan(index=index)
        assert actual == expected
