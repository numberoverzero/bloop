import bloop
import pytest
import uuid


def test_invalid_key(engine, local_bind):
    '''
    filter.key should only accept BeginsWith, Between, and Comparators
    (excluding NE)
    '''
    class Visit(engine.model):
        page = bloop.Column(bloop.String, hash_key=True)
        visitor = bloop.Column(bloop.Integer, range_key=True)
        date = bloop.Column(bloop.DateTime)

        by_date = bloop.GlobalSecondaryIndex(hash_key='date')
    engine.bind()

    q = engine.query(Visit)

    invalid_conditions = [
        # operator.net
        (Visit.page != 'foo'),
        # OR
        ((Visit.page == 'foo') | (Visit.page == 'bar')),
        # NOT
        (~(Visit.page == 'foo')),
        # AttributeNotExists
        (Visit.page.is_(None)),
        # AttributeExists
        (Visit.page.is_not(None)),
        # CONTAINS
        (Visit.page.contains('foo')),
        # IN
        (Visit.page.in_(['foo', 'bar'])),

        # No hash key
        (Visit.date.begins_with('foo')),

        # None
        None,

        # Same column twice in AND
        ((Visit.page.begins_with('f')) & (Visit.page == 'foo')),
        # Too many conditions
        ((Visit.page == 'a') & (Visit.page == 'b') & (Visit.page == 'c')),
        # AND without hash key
        ((Visit.visitor == 0) & (Visit.date == 1))
    ]

    for condition in invalid_conditions:
        with pytest.raises(ValueError):
            q.key(condition)


def test_iterative_key(engine, local_bind):
    ''' iterative .key should AND arguments '''
    class Visit(engine.model):
        page = bloop.Column(bloop.String, hash_key=True)
        visitor = bloop.Column(bloop.Integer, range_key=True)
        date = bloop.Column(bloop.DateTime)

        by_date = bloop.GlobalSecondaryIndex(hash_key='date')
    engine.bind()

    q = engine.query(Visit)

    pcondition = Visit.page == 'foo'
    vcondition = Visit.visitor == 0

    result = q.key(pcondition).key(vcondition).all()

    expected = {'Select': 'ALL_ATTRIBUTES',
                'KeyConditionExpression': '((#n0 = :v1) AND (#n2 = :v3))',
                'TableName': 'Visit',
                'ExpressionAttributeValues': {':v3': {'S': 'foo'},
                                              ':v1': {'N': '0'}},
                'ScanIndexForward': True,
                'ExpressionAttributeNames': {'#n2': 'page', '#n0': 'visitor'},
                'ConsistentRead': False}
    assert result.request == expected


def test_filter(engine, User):
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)
    condition = User.email == 'foo@domain.com'

    result = q.filter(condition).all()
    expected = {'ScanIndexForward': True,
                'ConsistentRead': False,
                'Select': 'ALL_ATTRIBUTES',
                'TableName': 'User',
                'ExpressionAttributeValues': {':v1': {'S': 'foo@domain.com'},
                                              ':v3': {'S': str(user_id)}},
                'ExpressionAttributeNames': {'#n2': 'id', '#n0': 'email'},
                'KeyConditionExpression': '(#n2 = :v3)',
                'FilterExpression': '(#n0 = :v1)'}
    assert result.request == expected


def test_iterative_filter(engine, User):
    ''' iterative .filter should AND arguents '''
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)
    pcondition = User.email == 'foo@domain.com'
    vcondition = User.age == 100

    result = q.filter(pcondition).filter(vcondition).all()
    expected = {'ExpressionAttributeNames': {'#n2': 'email', '#n0': 'age',
                                             '#n4': 'id'},
                'FilterExpression': '((#n0 = :v1) AND (#n2 = :v3))',
                'ConsistentRead': False,
                'TableName': 'User',
                'KeyConditionExpression': '(#n4 = :v5)',
                'Select': 'ALL_ATTRIBUTES',
                'ScanIndexForward': True,
                'ExpressionAttributeValues': {':v5': {'S': str(user_id)},
                                              ':v3': {'S': 'foo@domain.com'},
                                              ':v1': {'N': '100'}}}
    assert result.request == expected


def test_invalid_select(engine, User):
    q = engine.query(User)

    invalid = [
        "all_",
        None,
        set(['foo', User.email]),
        set()
    ]

    for select in invalid:
        with pytest.raises(ValueError):
            q.select(select)


def test_select_count(engine, User):
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)

    result = q.select("count").all()
    expected = {'ConsistentRead': False,
                'ExpressionAttributeValues': {':v1': {'S': str(user_id)}},
                'KeyConditionExpression': '(#n0 = :v1)',
                'TableName': 'User',
                'Select': 'COUNT',
                'ExpressionAttributeNames': {'#n0': 'id'},
                'ScanIndexForward': True}
    assert result.request == expected


def test_select_projected(engine, User):
    # Can't use "projected" on query against table
    user_id = uuid.uuid4()
    mq = engine.query(User).key(User.id == user_id)
    iq = engine.query(User.by_email).key(User.email == "foo@domain.com")

    with pytest.raises(ValueError):
        mq.select("projected")

    # Index query sets Select -> ALL_PROJECTED_ATTRIBUTES
    results = iq.select("projected").all()
    expected = {'ExpressionAttributeNames': {'#n0': 'email'},
                'Select': 'ALL_PROJECTED_ATTRIBUTES',
                'ScanIndexForward': True,
                'TableName': 'User',
                'KeyConditionExpression': '(#n0 = :v1)',
                'IndexName': 'by_email',
                'ExpressionAttributeValues': {':v1': {'S': 'foo@domain.com'}},
                'ConsistentRead': False}
    assert results.request == expected


def test_select_all_invalid_gsi(engine, local_bind):
    '''
    Select all query on GSI without "all" projection
    '''
    class Visit(engine.model):
        page = bloop.Column(bloop.String, hash_key=True)
        visitor = bloop.Column(bloop.Integer)
        date = bloop.Column(bloop.DateTime)

        by_date = bloop.GlobalSecondaryIndex(hash_key='date',
                                             projection=['visitor'])
    engine.bind()

    q = engine.query(Visit.by_date)

    with pytest.raises(ValueError):
        q.select('all')


def test_select_all_gsi(engine, local_bind):
    '''
    Select all query on GSI wit "all" projection
    '''
    class Visit(engine.model):
        page = bloop.Column(bloop.String, hash_key=True)
        visitor = bloop.Column(bloop.Integer, range_key=True)
        date = bloop.Column(bloop.String)

        by_date = bloop.GlobalSecondaryIndex(hash_key='date',
                                             projection='all')
    engine.bind()

    q = engine.query(Visit.by_date).key(Visit.date == "now")
    result = q.select('all').all()
    expected = {'ScanIndexForward': True,
                'IndexName': 'by_date',
                'KeyConditionExpression': '(#n0 = :v1)',
                'ConsistentRead': False,
                'Select': 'ALL_ATTRIBUTES',
                'ExpressionAttributeValues': {':v1': {'S': 'now'}},
                'TableName': 'Visit',
                'ExpressionAttributeNames': {'#n0': 'date'}}
    assert result.request == expected


def test_select_specific(engine, User):
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)

    result = q.select([User.email, User.joined]).all()
    expected = {'Select': 'SPECIFIC_ATTRIBUTES',
                'ExpressionAttributeNames': {'#n0': 'email', '#n1': 'joined',
                                             '#n2': 'id'},
                'ScanIndexForward': True,
                'ExpressionAttributeValues': {':v3': {'S': str(user_id)}},
                'TableName': 'User',
                'KeyConditionExpression': '(#n2 = :v3)',
                'ProjectionExpression': '#n0, #n1',
                'ConsistentRead': False}
    assert result.request == expected


def test_select_specific_gsi_projection(engine, local_bind):
    '''
    When specific attrs are requested on a GSI without all attrs projected,
    validate that the specific attrs are available through the GSI
    '''
    class Visit(engine.model):
        page = bloop.Column(bloop.String, hash_key=True)
        visitor = bloop.Column(bloop.Integer)
        date = bloop.Column(bloop.String)
        not_projected = bloop.Column(bloop.Integer)

        by_date = bloop.GlobalSecondaryIndex(hash_key='date',
                                             projection=['visitor'])
    engine.bind()

    q = engine.query(Visit.by_date).key(Visit.date == 'now')

    # Invalid select because `visitor` isn't projected into the index
    with pytest.raises(ValueError):
        q.select([Visit.not_projected])

    result = q.select([Visit.visitor]).all()
    expected = {'ExpressionAttributeNames': {'#n0': 'visitor', '#n1': 'date'},
                'TableName': 'Visit',
                'IndexName': 'by_date',
                'KeyConditionExpression': '(#n1 = :v2)',
                'ConsistentRead': False,
                'Select': 'SPECIFIC_ATTRIBUTES',
                'ScanIndexForward': True,
                'ExpressionAttributeValues': {':v2': {'S': 'now'}},
                'ProjectionExpression': '#n0'}
    assert result.request == expected


def test_count(engine, User):
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)

    expected = {'TableName': 'User',
                'ConsistentRead': False,
                'KeyConditionExpression': '(#n0 = :v1)',
                'ExpressionAttributeValues': {':v1': {'S': str(user_id)}},
                'ExpressionAttributeNames': {'#n0': 'id'},
                'Select': 'COUNT',
                'ScanIndexForward': True}

    def respond(**request):
        assert request == expected
        item = User(id=user_id, age=5)
        return {
            "Count": 1,
            "ScannedCount": 2,
            "Items": [engine.__dump__(User, item)]
        }
    engine.client.query = respond

    count = q.count()
    assert count == {"count": 1, "scanned_count": 2}


def test_first(engine, User):
    q = engine.scan(User).filter(User.email == "foo@domain.com")
    expected = {'Select': 'ALL_ATTRIBUTES',
                'TableName': 'User',
                'FilterExpression': '(#n0 = :v1)',
                'ExpressionAttributeNames': {'#n0': 'email'},
                'ExpressionAttributeValues': {':v1': {'S': 'foo@domain.com'}}}

    def respond(**request):
        assert request == expected
        item = User(email="foo@domain.com")
        return {
            "Count": 1,
            "ScannedCount": 2,
            "Items": [engine.__dump__(User, item)]
        }
    engine.client.scan = respond

    first = q.first()
    assert first.email == "foo@domain.com"


def test_iter(engine, User):
    q = engine.scan(User).filter(User.email == "foo@domain.com")
    expected = {'Select': 'ALL_ATTRIBUTES',
                'TableName': 'User',
                'FilterExpression': '(#n0 = :v1)',
                'ExpressionAttributeNames': {'#n0': 'email'},
                'ExpressionAttributeValues': {':v1': {'S': 'foo@domain.com'}}}

    def respond(**request):
        assert request == expected
        item = User(email="foo@domain.com")
        return {
            "Count": 1,
            "ScannedCount": 2,
            "Items": [engine.__dump__(User, item)]
        }
    engine.client.scan = respond

    results = list(q)
    assert len(results) == 1
    assert results[0].email == "foo@domain.com"
