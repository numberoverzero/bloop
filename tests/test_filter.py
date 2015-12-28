import bloop
import bloop.tracking
import pytest
import uuid


def test_hash_only_key(engine, local_bind):
    """ key calls for a model with no range key """

    class Visit(engine.model):
        hash = bloop.Column(bloop.String, hash_key=True)
        nonkey = bloop.Column(bloop.Integer)
    engine.bind()
    q = engine.query(Visit)

    valid = [
        # Basic equality
        Visit.hash == "==",
        # And with single argument
        bloop.condition.And(Visit.hash == "And(==)")
    ]

    for condition in valid:
        q.key(condition)

    invalid = [
        None,
        bloop.condition.And(),

        # Non-equality comparisons
        Visit.hash < "<",
        Visit.hash <= "<=",
        Visit.hash > ">",
        Visit.hash >= ">=",
        Visit.hash != "!=",
        # Non-equality in an And
        bloop.condition.And(Visit.hash != "And(!=)"),

        # Non-and groupings
        bloop.condition.Not(Visit.hash == "Not(==)"),
        bloop.condition.Or(Visit.hash == "Or(==)"),

        # Non-comparison operators
        Visit.hash.is_(None),
        Visit.hash.is_not(None),
        Visit.hash.between("between", "between"),
        Visit.hash.in_(["in", "in"]),
        Visit.hash.begins_with("begins_with"),
        Visit.hash.contains("contains"),

        # Path on hash
        Visit.hash[0] == "path[0]",
        Visit.hash["path"] == "path['path']",

        # And with non key condition
        ((Visit.hash == "and") & (Visit.nonkey == "nonkey")),
        # And with multiple hash conditions
        ((Visit.hash == "first") & (Visit.hash == "second")),
        # And with only non key condition
        (Visit.nonkey == "nonkey")
    ]

    for condition in invalid:
        with pytest.raises(ValueError):
            q.key(condition)


def test_hash_range_key(engine, local_bind):
    """key calls for a model with hash and range keys"""

    class Visit(engine.model):
        hash = bloop.Column(bloop.String, hash_key=True)
        range = bloop.Column(bloop.Integer, range_key=True)
        nonkey = bloop.Column(bloop.Integer)
    engine.bind()
    q = engine.query(Visit)

    valid = [
        # Basic equality
        Visit.hash == "==",
        # And with single argument
        bloop.condition.And(Visit.hash == "And(==)"),
        # Hash and valid range
        ((Visit.hash == "hash") & (Visit.range == "range")),

        # Valid range conditions
        ((Visit.hash == "hash") & (Visit.range < "<")),
        ((Visit.hash == "hash") & (Visit.range <= "<=")),
        ((Visit.hash == "hash") & (Visit.range > ">")),
        ((Visit.hash == "hash") & (Visit.range >= ">=")),
        ((Visit.hash == "hash") & (Visit.range.between("range", "between"))),
        ((Visit.hash == "hash") & (Visit.range.begins_with("begins_with"))),
    ]

    for condition in valid:
        q.key(condition)

    invalid = [
        # Range only
        Visit.range == "range",
        bloop.condition.And(Visit.range == "And(range)"),

        # Valid hash, invalid range
        ((Visit.hash == "hash") & (Visit.range != "!= range")),
        ((Visit.hash == "hash") & (~(Visit.range == "~(==range)"))),
        ((Visit.hash == "hash") & (Visit.range.is_(None))),
        ((Visit.hash == "hash") & (Visit.range.is_not(None))),
        ((Visit.hash == "hash") & (Visit.range.contains("range contains"))),
        ((Visit.hash == "hash") & (Visit.range.in_(["range", "in"]))),

        # Valid hash, non key condition
        ((Visit.hash == "hash") & (Visit.nonkey == "nonkey")),
        ((Visit.hash == "hash") &
         (Visit.range == "range") &
         (Visit.nonkey == "nonkey")),

        # Multiple hash conditions
        ((Visit.hash == "hash") & (Visit.hash == "also hash")),

        # Multiple range conditions
        ((Visit.hash == "hash") &
         (Visit.range == "range") &
         (Visit.range == "also range")),
        ((Visit.range == "range") & (Visit.range == "also range")),
    ]

    for condition in invalid:
        with pytest.raises(ValueError):
            q.key(condition)


def test_filter(engine, User):
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)
    condition = User.email == "foo@domain.com"

    result = q.filter(condition).all()
    expected = {"ScanIndexForward": True,
                "ConsistentRead": False,
                "Select": "ALL_ATTRIBUTES",
                "TableName": "User",
                "ExpressionAttributeValues": {":v1": {"S": "foo@domain.com"},
                                              ":v3": {"S": str(user_id)}},
                "ExpressionAttributeNames": {"#n2": "id", "#n0": "email"},
                "KeyConditionExpression": "(#n2 = :v3)",
                "FilterExpression": "(#n0 = :v1)"}
    assert result.request == expected


def test_iterative_filter(engine, User):
    """ iterative .filter should replace arguents """
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)
    pcondition = User.email == "foo@domain.com"
    vcondition = User.age == 100

    result = q.filter(pcondition).filter(vcondition).all()
    expected = {"ExpressionAttributeNames": {"#n0": "age", "#n2": "id"},
                "FilterExpression": "(#n0 = :v1)",
                "ConsistentRead": False,
                "TableName": "User",
                "KeyConditionExpression": "(#n2 = :v3)",
                "Select": "ALL_ATTRIBUTES",
                "ScanIndexForward": True,
                "ExpressionAttributeValues": {":v3": {"S": str(user_id)},
                                              ":v1": {"N": "100"}}}
    assert result.request == expected


def test_invalid_select(engine, User):
    q = engine.query(User)

    invalid = [
        "all_",
        None,
        set(["foo", User.email]),
        set()
    ]

    for select in invalid:
        with pytest.raises(ValueError):
            q.select(select)


def test_select_projected(engine, User):
    # Can't use "projected" on query against table
    user_id = uuid.uuid4()
    mq = engine.query(User).key(User.id == user_id)
    iq = engine.query(User.by_email).key(User.email == "foo@domain.com")

    with pytest.raises(ValueError):
        mq.select("projected")

    # Index query sets Select -> ALL_PROJECTED_ATTRIBUTES
    results = iq.select("projected").all()
    expected = {"ExpressionAttributeNames": {"#n0": "email"},
                "Select": "ALL_PROJECTED_ATTRIBUTES",
                "ScanIndexForward": True,
                "TableName": "User",
                "KeyConditionExpression": "(#n0 = :v1)",
                "IndexName": "by_email",
                "ExpressionAttributeValues": {":v1": {"S": "foo@domain.com"}},
                "ConsistentRead": False}
    assert results.request == expected


def test_select_all_invalid_gsi(engine, local_bind):
    """
    Select all query on GSI without "all" projection
    """
    class Visit(engine.model):
        page = bloop.Column(bloop.String, hash_key=True)
        visitor = bloop.Column(bloop.Integer)
        date = bloop.Column(bloop.DateTime)

        by_date = bloop.GlobalSecondaryIndex(hash_key="date",
                                             projection=["visitor"])
    engine.bind()

    q = engine.query(Visit.by_date)

    with pytest.raises(ValueError):
        q.select("all")


def test_select_strict_lsi(engine, ComplexModel):
    """ Select all/specific on LSI without "all" projection in strict mode """
    q = engine.query(ComplexModel.by_joined)

    with pytest.raises(ValueError):
        q.select("all")

    with pytest.raises(ValueError):
        q.select([ComplexModel.not_projected])


def test_select_all_gsi(engine, ComplexModel):
    """
    Select all query on GSI with "all" projection
    """
    q = engine.query(ComplexModel.by_email).key(ComplexModel.email == "foo")
    result = q.select("all").all()
    expected = {"ScanIndexForward": True,
                "IndexName": "by_email",
                "KeyConditionExpression": "(#n0 = :v1)",
                "ConsistentRead": False,
                "Select": "ALL_ATTRIBUTES",
                "ExpressionAttributeValues": {":v1": {"S": "foo"}},
                "TableName": "CustomTableName",
                "ExpressionAttributeNames": {"#n0": "email"}}
    assert result.request == expected


def test_select_specific(engine, User):
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)

    result = q.select([User.email, User.joined]).all()
    expected = {"Select": "SPECIFIC_ATTRIBUTES",
                "ExpressionAttributeNames": {"#n0": "email", "#n1": "j",
                                             "#n2": "id"},
                "ScanIndexForward": True,
                "ExpressionAttributeValues": {":v3": {"S": str(user_id)}},
                "TableName": "User",
                "KeyConditionExpression": "(#n2 = :v3)",
                "ProjectionExpression": "#n0, #n1",
                "ConsistentRead": False}
    assert result.request == expected


def test_select_specific_gsi_projection(engine, local_bind):
    """
    When specific attrs are requested on a GSI without all attrs projected,
    validate that the specific attrs are available through the GSI
    """
    class Visit(engine.model):
        page = bloop.Column(bloop.String, hash_key=True)
        visitor = bloop.Column(bloop.Integer)
        date = bloop.Column(bloop.String)
        not_projected = bloop.Column(bloop.Integer)

        by_date = bloop.GlobalSecondaryIndex(hash_key="date",
                                             projection=["visitor"])
    engine.bind()

    q = engine.query(Visit.by_date).key(Visit.date == "now")

    # Invalid select because `not_projected` isn't projected into the index
    with pytest.raises(ValueError):
        q.select([Visit.not_projected])

    result = q.select([Visit.visitor]).all()
    expected = {"ExpressionAttributeNames": {"#n0": "visitor", "#n1": "date"},
                "TableName": "Visit",
                "IndexName": "by_date",
                "KeyConditionExpression": "(#n1 = :v2)",
                "ConsistentRead": False,
                "Select": "SPECIFIC_ATTRIBUTES",
                "ScanIndexForward": True,
                "ExpressionAttributeValues": {":v2": {"S": "now"}},
                "ProjectionExpression": "#n0"}
    assert result.request == expected


def test_select_specific_lsi(ComplexModel, engine):
    key_condition = ComplexModel.name == "name"
    key_condition &= (ComplexModel.joined == "now")
    q = engine.query(ComplexModel.by_joined).key(key_condition)

    # Unprojected attributes expect a full load, strict by default
    with pytest.raises(ValueError):
        q.select([ComplexModel.not_projected]).all()
    # Unprojected is fine
    engine.config["strict"] = False
    result = q.select([ComplexModel.not_projected]).all()
    assert set(result.expected) == set(ComplexModel.Meta.columns)

    # All attributes projected
    result = q.select([ComplexModel.email]).all()
    assert set(result.expected).issubset(
        ComplexModel.by_joined.projection_attributes)


def test_count(engine, User):
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)

    expected = {"TableName": "User",
                "ConsistentRead": False,
                "KeyConditionExpression": "(#n0 = :v1)",
                "ExpressionAttributeValues": {":v1": {"S": str(user_id)}},
                "ExpressionAttributeNames": {"#n0": "id"},
                "Select": "COUNT",
                "ScanIndexForward": True}

    def respond(request):
        assert request == expected
        item = User(id=user_id, age=5)
        return {
            "Count": 1,
            "ScannedCount": 2,
            "Items": [engine._dump(User, item)]
        }
    engine.client.query = respond

    count = q.count()
    assert count == {"count": 1, "scanned_count": 2}


def test_first(engine, User):
    q = engine.scan(User).filter(User.email == "foo@domain.com")
    expected = {"ConsistentRead": False,
                "Select": "ALL_ATTRIBUTES",
                "TableName": "User",
                "FilterExpression": "(#n0 = :v1)",
                "ExpressionAttributeNames": {"#n0": "email"},
                "ExpressionAttributeValues": {":v1": {"S": "foo@domain.com"}}}

    def respond(request):
        assert request == expected
        item = User(email="foo@domain.com")
        return {
            "Count": 1,
            "ScannedCount": 2,
            "Items": [engine._dump(User, item)]
        }
    engine.client.scan = respond

    first = q.first()
    assert first.email == "foo@domain.com"


def test_atomic_load(User, atomic, renderer):
    """Queying objects in an atomic context caches the loaded condition"""

    user_id = uuid.uuid4()
    q = atomic.scan(User).filter(User.email == "foo@domain.com")

    def respond(request):
        item = User(id=user_id, email="foo@domain.com")
        return {
            "Count": 1,
            "ScannedCount": 1,
            "Items": [atomic._dump(User, item)]
        }
    atomic.client.scan = respond
    obj = q.first()

    condition = ('((attribute_not_exists(#n0)) AND (#n1 = :v2) '
                 'AND (#n3 = :v4) AND (attribute_not_exists(#n5)) AND '
                 '(attribute_not_exists(#n6)))')
    expected = {
        'ExpressionAttributeValues': {
            ':v4': {'S': str(user_id)},
            ':v2': {'S': 'foo@domain.com'}},
        'ConditionExpression': condition,
        'ExpressionAttributeNames': {
            '#n0': 'age', '#n1': 'email', '#n3': 'id',
            '#n5': 'j', '#n6': 'name'}}

    actual_condition = bloop.tracking.get_snapshot(obj)
    renderer.render(actual_condition, "condition")
    print(renderer.rendered)
    assert expected == renderer.rendered


def test_iter(engine, User):
    q = engine.scan(User).filter(User.email == "foo@domain.com").consistent
    expected = {"ConsistentRead": True,
                "Select": "ALL_ATTRIBUTES",
                "TableName": "User",
                "FilterExpression": "(#n0 = :v1)",
                "ExpressionAttributeNames": {"#n0": "email"},
                "ExpressionAttributeValues": {":v1": {"S": "foo@domain.com"}}}

    def respond(request):
        assert request == expected
        item = User(email="foo@domain.com")
        return {
            "Count": 1,
            "ScannedCount": 2,
            "Items": [engine._dump(User, item)]
        }
    engine.client.scan = respond

    results = list(q)
    assert len(results) == 1
    assert results[0].email == "foo@domain.com"


def test_properties(engine, User):
    """ ascending, descending, consistent """
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)

    # ascending
    result = q.ascending.all()
    expected = {"Select": "ALL_ATTRIBUTES",
                "ExpressionAttributeNames": {"#n0": "id"},
                "ScanIndexForward": True,
                "ExpressionAttributeValues": {":v1": {"S": str(user_id)}},
                "TableName": "User",
                "KeyConditionExpression": "(#n0 = :v1)",
                "ConsistentRead": False}
    assert result.request == expected

    # descending
    result = q.descending.all()
    expected = {"Select": "ALL_ATTRIBUTES",
                "ExpressionAttributeNames": {"#n0": "id"},
                "ScanIndexForward": False,
                "ExpressionAttributeValues": {":v1": {"S": str(user_id)}},
                "TableName": "User",
                "KeyConditionExpression": "(#n0 = :v1)",
                "ConsistentRead": False}
    assert result.request == expected

    # consistent
    result = q.consistent.all()
    expected = {"Select": "ALL_ATTRIBUTES",
                "ExpressionAttributeNames": {"#n0": "id"},
                "ScanIndexForward": True,
                "ExpressionAttributeValues": {":v1": {"S": str(user_id)}},
                "TableName": "User",
                "KeyConditionExpression": "(#n0 = :v1)",
                "ConsistentRead": True}
    assert result.request == expected


def test_query_no_key(User, engine):
    q = engine.query(User)

    with pytest.raises(ValueError):
        q.all()


def test_query_consistent_gsi(User, engine):
    q = engine.query(User.by_email).key(User.email == "foo")

    with pytest.raises(ValueError):
        q.consistent


def test_results_incomplete(User, engine):
    q = engine.query(User).key(User.id == uuid.uuid4())
    calls = 0

    def respond(request):
        nonlocal calls
        calls += 1
        return {"Count": 1, "ScannedCount": 2}
    engine.client.query = respond

    results = q.all()
    assert not results.complete

    with pytest.raises(RuntimeError):
        results.results

    list(results)
    assert results.complete
    assert not results.results

    # Multiple iterations don't re-call the client
    list(results)
    assert calls == 1


def test_first_no_prefetch(User, engine):
    """
    When there's no prefetch and a pagination token is presented,
    .first should return a result from the first page, without being marked
    complete.
    """
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)

    expected = {"TableName": "User",
                "ConsistentRead": False,
                "KeyConditionExpression": "(#n0 = :v1)",
                "ExpressionAttributeValues": {":v1": {"S": str(user_id)}},
                "ExpressionAttributeNames": {"#n0": "id"},
                "Select": "ALL_ATTRIBUTES",
                "ScanIndexForward": True}
    continue_tokens = {None: "first", "first": "second", "second": None}

    def respond(request):
        token = request.pop("ExclusiveStartKey", None)
        assert request == expected
        item = User(id=user_id, name=None)
        return {
            "Count": 1,
            "ScannedCount": 2,
            "Items": [engine._dump(User, item)],
            "LastEvaluatedKey": continue_tokens[token]
        }
    engine.client.query = respond

    results = q.all()

    assert results.first.name is None
    # Second call doesn't fetch
    assert results.first.name is None
    assert not results.complete


def test_first_no_results(User, engine):
    """
    When there's no prefetch and a pagination token is presented,
    .first should return a result from the first page, without being marked
    complete.
    """
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)

    def respond(request):
        return {
            "Count": 1,
            "ScannedCount": 2
        }
    engine.client.query = respond

    results = q.all()

    with pytest.raises(ValueError):
        results.first
    # Subsequent results skip the stepping
    with pytest.raises(ValueError):
        results.first


def test_prefetch_all(User, engine):
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)
    calls = 0
    expected = {"TableName": "User",
                "ConsistentRead": False,
                "KeyConditionExpression": "(#n0 = :v1)",
                "ExpressionAttributeValues": {":v1": {"S": str(user_id)}},
                "ExpressionAttributeNames": {"#n0": "id"},
                "Select": "ALL_ATTRIBUTES",
                "ScanIndexForward": True}
    continue_tokens = {None: "first", "first": "second", "second": None}

    def respond(request):
        nonlocal calls
        calls += 1

        token = request.pop("ExclusiveStartKey", None)
        assert request == expected
        item = User(id=user_id, name=token)

        result = {
            "Count": 1,
            "ScannedCount": 2,
            "Items": [engine._dump(User, item)],
            "LastEvaluatedKey": continue_tokens[token]
        }
        next_token = continue_tokens.get(token, None)
        if next_token:
            result["LastEvaluatedKey"] = next_token
        return result
    engine.client.query = respond

    results = q.all(prefetch="all")

    assert calls == 3
    assert results.count == 3
    assert results.scanned_count == 6


def test_invalid_prefetch(User, engine):
    q = engine.query(User).key(User.id == uuid.uuid4())
    with pytest.raises(ValueError):
        q.all(prefetch=-1)
    with pytest.raises(ValueError):
        q.all(prefetch="none")


def test_prefetch_first(User, engine):
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)
    calls = 0
    expected = {"TableName": "User",
                "ConsistentRead": False,
                "KeyConditionExpression": "(#n0 = :v1)",
                "ExpressionAttributeValues": {":v1": {"S": str(user_id)}},
                "ExpressionAttributeNames": {"#n0": "id"},
                "Select": "ALL_ATTRIBUTES",
                "ScanIndexForward": True}
    continue_tokens = {None: "first", "first": None}

    def respond(request):
        nonlocal calls
        calls += 1

        token = request.pop("ExclusiveStartKey", None)
        assert request == expected
        item = User(id=user_id, name=token)

        result = {
            "Count": 1,
            "ScannedCount": 2,
            "Items": [engine._dump(User, item)],
            "LastEvaluatedKey": continue_tokens[token]
        }
        next_token = continue_tokens.get(token, None)
        if next_token:
            result["LastEvaluatedKey"] = next_token
        return result
    engine.client.query = respond

    results = q.all(prefetch=1)

    # Not iterated, no fetches
    assert calls == 0
    # First call fetches twice, even though a result
    # is in the first response.
    results.first
    assert calls == 2


def test_prefetch_iter(User, engine):
    user_id = uuid.uuid4()
    q = engine.query(User).key(User.id == user_id)
    calls = 0
    expected = {"TableName": "User",
                "ConsistentRead": False,
                "KeyConditionExpression": "(#n0 = :v1)",
                "ExpressionAttributeValues": {":v1": {"S": str(user_id)}},
                "ExpressionAttributeNames": {"#n0": "id"},
                "Select": "ALL_ATTRIBUTES",
                "ScanIndexForward": True}
    continue_tokens = {None: "first", "first": "second", "second": None}

    def respond(request):
        nonlocal calls
        calls += 1

        token = request.pop("ExclusiveStartKey", None)
        assert request == expected
        item = User(id=user_id, name=token)

        result = {
            "Count": 1,
            "ScannedCount": 2,
            "Items": [engine._dump(User, item)],
            "LastEvaluatedKey": continue_tokens[token]
        }
        next_token = continue_tokens.get(token, None)
        if next_token:
            result["LastEvaluatedKey"] = next_token
        return result
    engine.client.query = respond

    results = q.all(prefetch=1)

    # Not iterated, no fetches
    assert calls == 0
    # Exhaust the results
    assert len(list(results)) == 3
    # Only two continue tokens
    assert calls == 3
