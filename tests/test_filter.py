import bloop
import bloop.filter
import pytest

from test_models import SimpleModel, ComplexModel, User

valid_hash_conditions = [
    # condition, index
    (ComplexModel.name == "foo", None),
    (ComplexModel.email == "foo", ComplexModel.by_email),
    (ComplexModel.name == "foo", ComplexModel.by_joined)
]
invalid_hash_conditions = [
    ComplexModel.name <= "foo",
    ComplexModel.name < "foo",
    ComplexModel.name >= "foo",
    ComplexModel.name > "foo",
    ComplexModel.name != "foo",
    ComplexModel.name.begins_with("foo"),
    ComplexModel.name.between("foo", "bar"),
    ComplexModel.name.contains("foo"),
    ComplexModel.name.in_(["foo", "bar"]),
    ComplexModel.name.is_(None),
    ComplexModel.name.is_not(None)
]

valid_range_conditions = [
    ComplexModel.date == "now",
    ComplexModel.date <= "now",
    ComplexModel.date < "now",
    ComplexModel.date >= "now",
    ComplexModel.date > "now",
    ComplexModel.date.begins_with("foo"),
    ComplexModel.date.between("foo", "bar")
]
invalid_range_conditions = [
    # Can't use operator.ne
    ComplexModel.date != "now",
    # ... or contains
    ComplexModel.date.contains("foo"),
    # ... or in
    ComplexModel.date.in_(["foo", "bar"]),
    # ... or attribute (not)exists
    ComplexModel.date.is_(None),
    ComplexModel.date.is_not(None),
    # Not the range key for the model
    ComplexModel.email == "foo"
]


# Provides a gsi and lsi with constrained projections for testing Filter.select validation
class ProjectedIndexes(bloop.new_base()):
    h = bloop.Column(bloop.Integer, hash_key=True)
    r = bloop.Column(bloop.Integer, range_key=True)
    both = bloop.Column(bloop.String)
    neither = bloop.Column(bloop.String)
    gsi_only = bloop.Column(bloop.String)
    lsi_only = bloop.Column(bloop.String)

    by_gsi = bloop.GlobalSecondaryIndex(hash_key="h", projection=["both", "gsi_only"])
    by_lsi = bloop.LocalSecondaryIndex(range_key="r", projection=["both", "lsi_only"])


@pytest.fixture
def query(engine):
    return bloop.filter.Filter(
        engine=engine, mode="query", model=ComplexModel, index=ComplexModel.by_email, strict=False,
        select={ComplexModel.date}, prefetch=3, consistent=True, forward=False, limit=4,
        key=(ComplexModel.name == "foo"), filter=(ComplexModel.email.contains("@")))


@pytest.fixture
def simple_query(engine):
    return bloop.filter.Filter(
        engine=engine, mode="query", model=SimpleModel, index=None,
        strict=False, select="all", key=(SimpleModel.id == "foo"))


@pytest.fixture
def projection_query(engine):
    return bloop.filter.Filter(
        engine=engine, mode="query", model=ProjectedIndexes, index=None, strict=False,
        select="all", prefetch=3, consistent=True, forward=False, limit=4, key=None, filter=None)


def test_copy(query):
    # Use non-defaults so we don't get false positives on missing attributes
    same = query.copy()
    attrs = [
        "engine", "mode", "model", "index", "strict",
        "_prefetch", "_consistent", "_forward", "_limit", "_key", "_filter", "_select"]
    assert all(map(lambda a: (getattr(query, a) == getattr(same, a)), attrs))


def test_key_none(query):
    """None can be used to clear an existing key condition"""
    old_condition = query._key
    assert old_condition is not None  # guard false positives from query fixture changing
    query.key(None)
    assert query._key is None


@pytest.mark.parametrize("condition", [False, 0, bloop.Condition()], ids=repr)
def test_key_falsey(query, condition):
    """Can't use any falsey value to clear conditions, must be exactly None"""
    with pytest.raises(ValueError) as excinfo:
        query.key(condition)
    assert str(excinfo.value) == "Key condition must contain exactly 1 hash condition, at most 1 range condition"


def test_key_wrong_hash(query):
    """Condition is against a hash_key, but not for the index being queried"""
    query.index = ComplexModel.by_email

    with pytest.raises(ValueError) as excinfo:
        query.key(ComplexModel.name == "foo")  # table hash_key, not index hash_key
    assert str(excinfo.value) == "Key condition must contain exactly 1 hash condition, at most 1 range condition"


def test_key_and_one_value(query):
    """If the key condition is an AND, it must have exactly 2 values; even if its sole value is valid on its own"""
    query.index = None
    condition = bloop.condition.And(ComplexModel.name == "foo")
    with pytest.raises(ValueError) as excinfo:
        query.key(condition)
    assert str(excinfo.value) == "Key condition must contain exactly 1 hash condition, at most 1 range condition"


def test_key_and_three_values(query):
    """Redundant values aren't collapsed"""
    query.index = None
    condition = (ComplexModel.name == "foo") & (ComplexModel.date == "now") & (ComplexModel.name == "foo")
    with pytest.raises(ValueError) as excinfo:
        query.key(condition)
    assert str(excinfo.value) == "Key condition must contain exactly 1 hash condition, at most 1 range condition"


@pytest.mark.parametrize("condition", invalid_range_conditions, ids=str)
def test_key_invalid_range_condition(query, condition):
    query.index = None
    # Attach a valid hash_key condition, so that we're only failing on the range_key condition
    condition &= (ComplexModel.name == "foo")
    with pytest.raises(ValueError) as excinfo:
        query.key(condition)
    assert str(excinfo.value) == "Key condition must contain exactly 1 hash condition, at most 1 range condition"


@pytest.mark.parametrize("condition", valid_range_conditions, ids=str)
def test_key_and_valid_range(query, condition):
    query.index = None
    condition &= (ComplexModel.name == "foo")
    query.key(condition)
    assert query._key == condition

    # Test reversed order
    condition.conditions = condition.conditions[::-1]
    query.key(condition)
    assert query._key == condition


@pytest.mark.parametrize("condition", invalid_hash_conditions, ids=str)
def test_key_invalid_hash(query, condition):
    query.index = None
    with pytest.raises(ValueError) as excinfo:
        query.key(condition)
    assert str(excinfo.value) == "Key condition must contain exactly 1 hash condition, at most 1 range condition"


@pytest.mark.parametrize("condition, index", valid_hash_conditions, ids=str)
def test_key_valid_hash(query, condition, index):
    query.index = index
    query.key(condition)
    assert query._key == condition


def test_select_count(query):
    query.select("count")
    assert query._select == "count"


def test_select_all_table(query):
    query.index = None
    query.select("all")
    assert query._select == "all"


def test_select_all_gsi(query):
    """Can't select all on a GSI"""
    query.index = ComplexModel.by_email
    with pytest.raises(ValueError) as excinfo:
        query.select("all")
    assert str(excinfo.value) == "Can't select 'all' on a GSI or strict LSI"


def test_select_all_strict_lsi(query):
    """Can't select all on a strict LSI (would incur additional reads)"""
    query.index = ComplexModel.by_joined
    query.strict = True
    with pytest.raises(ValueError) as excinfo:
        query.select("all")
    assert str(excinfo.value) == "Can't select 'all' on a GSI or strict LSI"


def test_select_all_lsi(query):
    """Even though extra reads are incurred, a non-strict LSI can query all"""
    query.index = ComplexModel.by_joined
    query.strict = False
    query.select("all")
    assert query._select == "all"


def test_select_projected_table(query):
    """Can't select projected on a non-index query"""
    query.index = None
    with pytest.raises(ValueError) as excinfo:
        query.select("projected")
    assert str(excinfo.value) == "Can't query projected attributes without an index"


def test_select_projected_index(query):
    """Any index can select projected"""
    # GSI
    query.index = ComplexModel.by_email
    query.select("projected")
    assert query._select == "projected"

    # LSI
    query.index = ComplexModel.by_joined
    query.strict = False
    query.select("projected")
    assert query._select == "projected"

    # Strict LSI
    query.index = ComplexModel.by_joined
    query.strict = True
    query.select("projected")
    assert query._select == "projected"


def test_select_unknown(query):
    with pytest.raises(ValueError) as excinfo:
        query.select("foobar")
    assert str(excinfo.value) == "Unknown select mode 'foobar'"


def test_select_empty(query):
    """Can't specify an empty set of columns to load"""
    with pytest.raises(ValueError) as excinfo:
        query.select([])
    assert str(excinfo.value) == "Must specify at least one column to load"


def test_select_wrong_model(query):
    """All columns must be part of the model being queried"""
    with pytest.raises(ValueError) as excinfo:
        query.select([User.email])
    assert str(excinfo.value) == "Select must be all, projected, count, or an iterable of columns on the model"


def test_select_non_column(query):
    """All selections must be columns"""
    with pytest.raises(ValueError) as excinfo:
        query.select([ComplexModel.email, ComplexModel.date, object()])
    assert str(excinfo.value) == "Select must be all, projected, count, or an iterable of columns on the model"


def test_select_specific_table(query):
    """Any subset of a table query is valid"""
    selected = [
        ComplexModel.name, ComplexModel.date, ComplexModel.email,
        ComplexModel.joined, ComplexModel.not_projected]
    query.select(selected)
    assert query._select == set(selected)


def test_select_gsi_subset(projection_query):
    """Subset of GSI projection_attributes is valid"""
    projection_query.index = ProjectedIndexes.by_gsi
    # ProjectedIndexes.h is available since it's part of the hash/range key of the index
    selected = [ProjectedIndexes.gsi_only, ProjectedIndexes.both, ProjectedIndexes.h]
    projection_query.select(selected)
    assert projection_query._select == set(selected)


def test_select_gsi_superset(projection_query):
    """Superset of GSI projection_attributes fails, can't outside projection"""
    projection_query.index = ProjectedIndexes.by_gsi
    # ProjectedIndexes.neither isn't projected into the GSI
    selected = [ProjectedIndexes.gsi_only, ProjectedIndexes.neither]
    with pytest.raises(ValueError) as excinfo:
        projection_query.select(selected)
    assert str(excinfo.value) == "Tried to select a superset of the GSI's projected columns"


def test_select_strict_lsi_subset(projection_query):
    """Subset of strict LSI projection_attributes is valid"""
    projection_query.index = ProjectedIndexes.by_lsi
    projection_query.strict = True
    # ProjectedIndexes.h is available since it's part of the hash/range key of the index
    selected = [ProjectedIndexes.lsi_only, ProjectedIndexes.both, ProjectedIndexes.h]
    projection_query.select(selected)
    assert projection_query._select == set(selected)


def test_select_strict_lsi_superset(projection_query):
    """Superset of strict LSI projection_attributes fails, can't outside projection"""
    projection_query.index = ProjectedIndexes.by_lsi
    projection_query.strict = True
    # ProjectedIndexes.neither isn't projected into the LSI
    selected = [ProjectedIndexes.lsi_only, ProjectedIndexes.neither]
    with pytest.raises(ValueError) as excinfo:
        projection_query.select(selected)
    assert str(excinfo.value) == "Tried to select a superset of the LSI's projected columns in strict mode"


def test_select_non_strict_lsi_superset(projection_query):
    """Superset of non-strict LSI projection_attributes is valid, will incur additional reads"""
    projection_query.index = ProjectedIndexes.by_lsi
    projection_query.strict = False
    selected = [ProjectedIndexes.lsi_only, ProjectedIndexes.neither]
    projection_query.select(selected)
    assert projection_query._select == set(selected)


def test_filter_not_condition(query):
    with pytest.raises(ValueError) as excinfo:
        query.filter("should be a condition")
    assert str(excinfo.value) == "Filter must be a condition or None"


def test_filter_none(query):
    """None is allowed, for clearing an existing filter"""
    assert query._filter is not None
    query.filter(None)
    assert query._filter is None


def test_filter_compound(query):
    new_condition = (ComplexModel.not_projected == "foo") & (ComplexModel.name.is_not(None))
    query.filter(new_condition)
    assert query._filter is new_condition


def test_consistent_gsi_raises(query):
    """Can't use consistent queries on a GSI"""
    query.index = ComplexModel.by_email

    with pytest.raises(ValueError) as excinfo:
        query.consistent(True)
    assert str(excinfo.value) == "Can't use ConsistentRead with a GlobalSecondaryIndex"

    with pytest.raises(ValueError):
        query.consistent(False)


def test_consistent(query):
    query.index = None
    new_value = not query._consistent
    query.consistent(new_value)
    assert query._consistent == new_value


def test_forward_scan(query):
    """Can't set forward on a scan"""
    query.mode = "scan"

    with pytest.raises(ValueError) as excinfo:
        query.forward(True)
    assert str(excinfo.value) == "Can't set ScanIndexForward for scan operations, only queries"

    with pytest.raises(ValueError):
        query.forward(False)


def test_forward(query):
    query.mode = "query"
    new_value = not query._forward
    query.forward(new_value)
    assert query._forward == new_value


@pytest.mark.parametrize("limit", [-5, None, object()], ids=str)
def test_illegal_limit(query, limit):
    with pytest.raises(ValueError) as excinfo:
        query.limit(limit)
    assert str(excinfo.value) == "Limit must be a non-negative int"


@pytest.mark.parametrize("limit", [0, 3])
def test_limit(query, limit):
    query.limit(limit)
    assert query._limit == limit


@pytest.mark.parametrize("prefetch", [-5, None, object()], ids=str)
def test_illegal_prefetch(query, prefetch):
    with pytest.raises(ValueError) as excinfo:
        query.prefetch(prefetch)
    assert str(excinfo.value) == "Prefetch must be a non-negative int"


@pytest.mark.parametrize("prefetch", [0, 3])
def test_prefetch(query, prefetch):
    query.prefetch(prefetch)
    assert query._prefetch == prefetch


def test_one_no_results(simple_query, engine):
    """one raises when there are no results"""
    engine.client.query.return_value = {"Count": 0, "ScannedCount": 6, "Items": []}

    with pytest.raises(bloop.exceptions.ConstraintViolation) as excinfo:
        simple_query.one()
    same_prepared_request = simple_query.build()._prepared_request
    same_prepared_request["ExclusiveStartKey"] = None
    assert excinfo.value.args[0] == "Failed to meet required condition during query.one"
    assert excinfo.value.obj == same_prepared_request
    assert engine.client.query.call_count == 1


def test_one_extra_results(simple_query, engine):
    """one raises when there are too many results"""
    engine.client.query.return_value = {
        "Count": 2, "ScannedCount": 6,
        "Items": [{"id": {"S": "first"}}, {"id": {"S": "second"}}]}

    with pytest.raises(bloop.exceptions.ConstraintViolation) as excinfo:
        simple_query.one()
    same_prepared_request = simple_query.build()._prepared_request
    same_prepared_request["ExclusiveStartKey"] = None
    assert excinfo.value.args[0] == "Failed to meet required condition during query.one"
    assert excinfo.value.obj == same_prepared_request
    assert engine.client.query.call_count == 1


def test_one_exact(simple_query, engine):
    """one returns when there is exactly one value in the full query"""
    engine.client.query.return_value = {"Count": 1, "ScannedCount": 6, "Items": [{"id": {"S": "unique"}}]}

    result = simple_query.one()
    assert result.id == "unique"

    assert engine.client.query.call_count == 1


def test_first_no_results(simple_query, engine):
    """first raises when there are no results"""
    engine.client.query.return_value = {"Count": 0, "ScannedCount": 6, "Items": []}

    with pytest.raises(bloop.exceptions.ConstraintViolation) as excinfo:
        simple_query.first()
    same_prepared_request = simple_query.build()._prepared_request
    same_prepared_request["ExclusiveStartKey"] = None
    assert excinfo.value.args[0] == "Failed to meet required condition during query.first"
    assert excinfo.value.obj == same_prepared_request
    assert engine.client.query.call_count == 1


def test_first_extra_results(simple_query, engine):
    """first returns the first result, even when there are multiple values"""
    engine.client.query.return_value = {
        "Count": 2, "ScannedCount": 6,
        "Items": [{"id": {"S": "first"}}, {"id": {"S": "second"}}]}

    result = simple_query.first()
    assert result.id == "first"
    assert engine.client.query.call_count == 1


def test_build_gsi_consistent_read(query):
    """Queries on a GSI can't be consistent"""
    query.index = ComplexModel.by_email
    # Can't use builder function - assume it was provided at __init__
    query._consistent = True
    prepared_request = query.build()._prepared_request
    assert "ConsistentRead" not in prepared_request


def test_build_lsi_consistent_read(query):
    query.index = ComplexModel.by_joined
    # Can't use builder function - assume it was provided at __init__
    query.consistent(False)
    prepared_request = query.build()._prepared_request
    assert prepared_request["ConsistentRead"] is False


def test_build_no_limit(query):
    query.limit(0)
    prepared_request = query.build()._prepared_request
    assert "Limit" not in prepared_request


def test_build_limit(query):
    query.limit(4)
    prepared_request = query.build()._prepared_request
    assert prepared_request["Limit"] == 4


def test_build_scan_forward(query):
    query.mode = "scan"
    # Can't have a key condition on scan
    query.key(None)

    prepared_request = query.build()._prepared_request
    assert "ScanIndexForward" not in prepared_request


def test_build_query_forward(query):
    query.forward(False)
    prepared_request = query.build()._prepared_request
    assert prepared_request["ScanIndexForward"] is False


def test_build_table_name(query):
    """TableName is always set"""
    # On an index...
    query.mode = "query"
    query.index = ComplexModel.by_email
    prepared_request = query.build()._prepared_request
    assert prepared_request["TableName"] == ComplexModel.Meta.table_name

    # On the table...
    query.index = None
    prepared_request = query.build()._prepared_request
    assert prepared_request["TableName"] == ComplexModel.Meta.table_name

    # On scan...
    query.mode = "scan"
    # Can't have a key condition on scan
    query.key(None)

    prepared_request = query.build()._prepared_request
    assert prepared_request["TableName"] == ComplexModel.Meta.table_name


def test_build_no_index_name(query):
    """IndexName isn't set on table queries"""
    query.index = None
    prepared_request = query.build()._prepared_request
    assert "IndexName" not in prepared_request


def test_build_index_name(query):
    query.index = ComplexModel.by_joined
    prepared_request = query.build()._prepared_request
    assert prepared_request["IndexName"] == "by_joined"


def test_build_filter(query):
    condition = ComplexModel.email.contains("@")
    query.filter(condition)
    prepared_request = query.build()._prepared_request
    # Filters are rendered first, so these name and value ids are stable
    assert prepared_request["FilterExpression"] == "(contains(#n0, :v1))"
    assert prepared_request["ExpressionAttributeNames"]["#n0"] == "email"
    assert prepared_request["ExpressionAttributeValues"][":v1"] == {"S": "@"}


def test_build_no_filter(query):
    query.filter(None)
    prepared_request = query.build()._prepared_request
    assert "FilterExpression" not in prepared_request


def test_build_select_columns(query):
    query.select({ComplexModel.date})
    prepared_request = query.build()._prepared_request
    assert prepared_request["Select"] == "SPECIFIC_ATTRIBUTES"
    assert prepared_request["ExpressionAttributeNames"]["#n2"] == "date"
    assert prepared_request["ProjectionExpression"] == "#n2"


def test_build_select_all(query):
    query.index = None
    query.select("all")
    prepared_request = query.build()._prepared_request
    assert prepared_request["Select"] == "ALL_ATTRIBUTES"
    assert "ProjectionExpression" not in prepared_request


def test_build_select_projected(query):
    query.index = ComplexModel.by_email
    query.select("projected")
    prepared_request = query.build()._prepared_request
    assert prepared_request["Select"] == "ALL_PROJECTED_ATTRIBUTES"
    assert "ProjectionExpression" not in prepared_request


def test_build_select_count(query):
    query.select("count")
    prepared_request = query.build()._prepared_request
    assert prepared_request["Select"] == "COUNT"
    assert "ProjectionExpression" not in prepared_request


def test_build_query_no_key(query):
    # Bypass any validation on the key condition
    query.mode = "query"
    query._key = None

    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Query must specify at least a hash key condition"


def test_build_query_key(query):
    query.mode = "query"
    query.index = None
    query.key(ComplexModel.name == "foo")

    prepared_request = query.build()._prepared_request
    assert prepared_request["KeyConditionExpression"] == "(#n3 = :v4)"
    assert prepared_request["ExpressionAttributeNames"]["#n3"] == "name"
    assert prepared_request["ExpressionAttributeValues"][":v4"] == {"S": "foo"}


def test_build_scan_no_key(query):
    query.mode = "scan"
    query.key(None)

    prepared_request = query.build()._prepared_request
    assert "KeyConditionExpression" not in prepared_request


def test_build_scan_key(query):
    # Bypass any validation on the key condition
    query.mode = "scan"
    query._key = ComplexModel.name == "foo"

    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Scan cannot have a key condition"


def test_build_expected_all(query):
    query.index = None
    query.select("all")
    expected_columns = query.build()._expected_columns
    assert expected_columns == ComplexModel.Meta.columns


def test_build_expected_projected(query):
    query.index = ComplexModel.by_email
    query.select("projected")
    expected_columns = query.build()._expected_columns
    assert expected_columns == ComplexModel.by_email.projection_attributes


def test_build_expected_count(query):
    """No expected columns for a count"""
    query.select("count")
    expected_columns = query.build()._expected_columns
    assert expected_columns == set()


def test_build_expected_specific(query):
    selected = {ComplexModel.date, ComplexModel.email}
    # Query on the table so all attributes are available
    query.index = None
    query.select(selected)
    expected_columns = query.build()._expected_columns
    assert expected_columns == selected


def test_iter_reset(query):
    iterator = query.build()
    iterator._state["exhausted"] = True
    iterator._state["count"] = 3
    iterator._state["scanned"] = 4

    iterator.reset()
    assert iterator.count == 0
    assert iterator.scanned == 0
    assert iterator.exhausted is False


def test_iter_no_results(query, engine):
    engine.client.query.return_value = {"Items": [], "Count": 0, "ScannedCount": 5}

    iterator = query.build()
    results = list(iterator)
    assert not results
    assert iterator.exhausted
    assert iterator.count == 0
    assert iterator.scanned == 5

    # After exhaustion, iterator doesn't call dynamo again
    list(iterator)
    assert iterator.count == 0
    assert iterator.scanned == 5
    engine.client.query.assert_called_once_with(iterator._prepared_request)


def test_iter_empty_pages(simple_query, engine):
    """
    prefetch is number of items, not pages.
    Even if prefetch isn't met, items are returned after pages are exhausted
    """
    engine.client.query.side_effect = [
        {"LastEvaluatedKey": "call2", "Items": [], "Count": 0, "ScannedCount": 3},
        {"LastEvaluatedKey": "call3", "Items": [{"id": {"S": "first"}}], "Count": 1, "ScannedCount": 4},
        {"LastEvaluatedKey": "call4", "Items": [{"id": {"S": "second"}}], "Count": 1, "ScannedCount": 5},
        {"Items": [], "Count": 0, "ScannedCount": 6}
    ]
    # Try to prefetch 3, even though there are only 2
    iterator = simple_query.prefetch(3).build()
    iterator_iter = iter(iterator)

    first = next(iterator_iter)
    # Exhausted after the first call, since we asked for at least 3 before items are yielded back.
    assert iterator.exhausted
    assert iterator.count == 2
    assert iterator.scanned == 18  # 3 + 4 + 5 + 6

    second = next(iterator_iter)
    # Make sure this is still coming from the buffer, and count/scanned don't change
    assert iterator.count == 2
    assert iterator.scanned == 18

    # And here we run out of items to fetch
    with pytest.raises(StopIteration):
        next(iterator_iter)

    # Things were loaded properly
    assert first.id == "first"
    assert second.id == "second"

    # Followed 3 continuation tokens
    assert engine.client.query.call_count == 4


def test_iter_prefetch_buffering(simple_query, engine):
    """one page has more results than the prefetch number"""
    items = [{"id": {"S": "first"}}, {"id": {"S": "second"}}, {"id": {"S": "third"}}]
    engine.client.query.side_effect = [
        {"Count": 3, "ScannedCount": 5, "Items": items, "LastEvaluatedKey": "next", },
        {"Count": 0, "ScannedCount": 6, "Items": []}
    ]

    # Try to prefetch 2, even though there are 3
    iterator = simple_query.prefetch(2).build()
    iterator_iter = iter(iterator)

    first = next(iterator_iter)
    # Not exhausted after the first call, since there's a continue token we haven't followed yet
    assert not iterator.exhausted
    assert iterator.count == 3
    assert iterator.scanned == 5

    second = next(iterator_iter)
    third = next(iterator_iter)
    # Make sure those still coming from the buffer, and count/scanned don't change
    assert iterator.count == 3
    assert iterator.scanned == 5

    # Now that we have cleared the buffer, we finally try to follow the
    # continue token and discover that we're out of items.
    with pytest.raises(StopIteration):
        next(iterator_iter)

    assert iterator.count == 3
    assert iterator.scanned == 11  # 5 + 6

    # Things were loaded properly
    assert first.id == "first"
    assert second.id == "second"
    assert third.id == "third"

    # Followed 1 continuation token
    assert engine.client.query.call_count == 2
