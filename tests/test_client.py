import bloop
import uuid


def test_batch_get_one_item(User, client):
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
    # Simulate a full batch
    bloop.client.MAX_BATCH_SIZE = 2

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
    # Minimum batch size so we can force pagination with 2 users
    bloop.client.MAX_BATCH_SIZE = 1

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
