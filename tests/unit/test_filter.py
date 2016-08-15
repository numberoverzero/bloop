import uuid
import pytest
from bloop.condition import And, Condition
from bloop.exceptions import ConstraintViolation
from bloop.filter import Filter, expected_columns_for
from bloop.models import BaseModel, Column, LocalSecondaryIndex
from bloop.types import Integer

from ..helpers.models import ComplexModel, ProjectedIndexes, SimpleModel, User


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


@pytest.fixture
def query(engine):
    return Filter(
        engine=engine, mode="query", model=ComplexModel, index=ComplexModel.by_email, strict=False,
        select={ComplexModel.joined}, consistent=True, forward=False,
        limit=4, key=(ComplexModel.email == "foo"), filter=(ComplexModel.not_projected > 3))


@pytest.fixture
def simple_query(engine):
    return Filter(
        engine=engine, mode="query", model=SimpleModel, index=None,
        strict=False, select="all", key=(SimpleModel.id == "foo"))


@pytest.fixture
def projection_query(engine):
    return Filter(
        engine=engine, mode="query", model=ProjectedIndexes, index=None, strict=False,
        select="all", consistent=True, forward=False, limit=4, key=None, filter=None)


def test_copy(query):
    # Use non-defaults so we don't get false positives on missing attributes
    same = query.copy()
    attrs = [
        "engine", "mode", "model", "index", "strict",
        "consistent", "forward", "limit", "key", "filter", "select"]
    assert all(map(lambda a: (getattr(query, a) == getattr(same, a)), attrs))


@pytest.mark.parametrize("condition", [None, False, 0, Condition()], ids=repr)
def test_key_falsey(query, condition):
    query.key = condition

    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Key condition must contain exactly 1 hash condition, at most 1 range condition"


def test_key_wrong_hash(query):
    """Condition is against a hash_key, but not for the index being queried"""
    query.index = ComplexModel.by_email
    query.key = ComplexModel.name == "foo"  # table hash_key, not index hash_key

    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Key condition must contain exactly 1 hash condition, at most 1 range condition"


def test_key_and_one_value(query):
    """If the key condition is an AND, it must have exactly 2 values; even if its sole value is valid on its own"""
    query.index = None
    query.key = And(ComplexModel.name == "foo")

    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Key condition must contain exactly 1 hash condition, at most 1 range condition"


def test_key_and_three_values(query):
    """Redundant values aren't collapsed"""
    query.index = None
    query.condition = (
        (ComplexModel.name == "foo") &
        (ComplexModel.date == "now") &
        (ComplexModel.name == "foo")
    )
    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Key condition must contain exactly 1 hash condition, at most 1 range condition"


@pytest.mark.parametrize("condition", invalid_range_conditions, ids=str)
def test_key_invalid_range_condition(query, condition):
    query.index = None
    # Attach a valid hash_key condition, so that we're only failing on the range_key condition
    query.key = condition & (ComplexModel.name == "foo")

    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Key condition must contain exactly 1 hash condition, at most 1 range condition"


@pytest.mark.parametrize("condition", valid_range_conditions, ids=str)
def test_key_and_valid_range(query, condition):
    query.index = None
    query.key = condition & (ComplexModel.name == "foo")

    query.build()

    # Test reversed order
    query.key.conditions = query.key.conditions[::-1]

    query.build()


@pytest.mark.parametrize("condition", invalid_hash_conditions, ids=str)
def test_key_invalid_hash(query, condition):
    query.index = None
    query.key = condition

    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Key condition must contain exactly 1 hash condition, at most 1 range condition"


@pytest.mark.parametrize("condition, index", valid_hash_conditions, ids=str)
def test_key_valid_hash(query, condition, index):
    query.index = index
    query.key = condition

    query.build()


def test_expected_columns_unknown(query):
    """Directly setting select values to an unexpected key raises"""
    query.select = "foobar"
    with pytest.raises(ValueError) as excinfo:
        expected_columns_for(query.model, query.index, query.select, [])
    assert str(excinfo.value) == "unknown mode foobar"


def test_select_count(query):
    query.select = "count"
    query.build()


def test_select_all_table(query):
    query.index = None
    query.select = "all"
    query.key = ComplexModel.name == "foo"
    query.build()


def test_select_all_gsi(query):
    """
    Can select all on a GSI iff its projection is all.
    Otherwise, it fails (even if the selected attribute are all of them)
    """
    query.index = ComplexModel.by_email
    query.select = "all"

    query.build()

    # use a model with a GSI without "all" projection
    query.model = ProjectedIndexes
    query.index = ProjectedIndexes.by_gsi
    query.select = "all"
    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Can't select 'all' on a GSI or strict LSI"


def test_select_all_strict_lsi(query):
    """Can't select all on a strict LSI without "all" projection (would incur additional reads)"""
    query.index = ComplexModel.by_joined
    query.strict = True
    query.select = "all"
    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Can't select 'all' on a GSI or strict LSI"


def test_select_all_strict_lsi_projection(query, engine):
    """No problem selecting all on this strict LSI because its projection is all"""
    class Model(BaseModel):
        id = Column(Integer, hash_key=True)
        foo = Column(Integer, range_key=True)
        bar = Column(Integer)
        by_lsi = LocalSecondaryIndex(range_key="bar", projection="all")
    engine.bind(Model)

    query.model = Model
    query.index = Model.by_lsi
    query.select = "all"
    query.key = Model.id == 3

    query.build()


def test_select_all_lsi(query):
    """Even though extra reads are incurred, a non-strict LSI can query all"""
    query.index = ComplexModel.by_joined
    query.strict = False
    query.select = "all"
    query.key = ComplexModel.name == uuid.uuid4()

    query.build()


def test_select_projected_table(query):
    """Can't select projected on a non-index query"""
    query.index = None
    query.select = "projected"
    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Can't query projected attributes without an index"


def test_select_projected_index(query):
    """Any index can select projected"""
    # GSI
    query.index = ComplexModel.by_email
    query.select = "projected"
    query.build()

    # LSI
    query.key = ComplexModel.name == "foo"
    query.index = ComplexModel.by_joined
    query.strict = False
    query.select = "projected"
    query.build()

    # Strict LSI
    query.index = ComplexModel.by_joined
    query.strict = True
    query.select = "projected"
    query.build()


def test_select_unknown(query):
    query.select = "foobar"
    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Unknown select mode 'foobar'"


def test_select_empty(query):
    """Can't specify an empty set of columns to load"""
    query.select = []
    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Must specify at least one column to load"


def test_select_wrong_model(query):
    """All columns must be part of the model being queried"""
    query.select = [User.email]
    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Select must be 'all', 'count', 'projected', or a list of column objects to select"


def test_select_non_column(query):
    """All selections must be columns"""
    query.select = [ComplexModel.email, ComplexModel.date, object()]
    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Select must be 'all', 'count', 'projected', or a list of column objects to select"


def test_select_specific_table(query):
    """Any subset of a table query is valid"""
    query.select = [
        ComplexModel.name, ComplexModel.date, ComplexModel.email,
        ComplexModel.joined, ComplexModel.not_projected]

    query.build()


def test_select_gsi_subset(projection_query):
    """Subset of GSI projected_columns is valid"""
    projection_query.key = ProjectedIndexes.h == 4
    projection_query.index = ProjectedIndexes.by_gsi
    # ProjectedIndexes.h is available since it's part of the hash/range key of the index
    projection_query.select = [ProjectedIndexes.gsi_only, ProjectedIndexes.both, ProjectedIndexes.h]

    projection_query.build()


def test_select_gsi_superset(projection_query):
    """Superset of GSI projected_columns fails, can't outside projection"""
    projection_query.key = ProjectedIndexes.h == 4
    projection_query.index = ProjectedIndexes.by_gsi
    # ProjectedIndexes.neither isn't projected into the GSI
    projection_query.select = [ProjectedIndexes.gsi_only, ProjectedIndexes.neither]
    with pytest.raises(ValueError) as excinfo:
        projection_query.build()
    assert str(excinfo.value) == "Tried to select a superset of the GSI's projected columns"


def test_select_strict_lsi_subset(projection_query):
    """Subset of strict LSI projected_columns is valid"""
    projection_query.key = ProjectedIndexes.h == 4
    projection_query.index = ProjectedIndexes.by_lsi
    projection_query.strict = True
    # ProjectedIndexes.h is available since it's part of the hash/range key of the index
    projection_query.select = [ProjectedIndexes.lsi_only, ProjectedIndexes.both, ProjectedIndexes.h]

    projection_query.build()


def test_select_strict_lsi_superset(projection_query):
    """Superset of strict LSI projected_columns fails, can't outside projection"""
    projection_query.key = ProjectedIndexes.h == 4
    projection_query.index = ProjectedIndexes.by_lsi
    projection_query.strict = True
    # ProjectedIndexes.neither isn't projected into the LSI
    projection_query.select = [ProjectedIndexes.lsi_only, ProjectedIndexes.neither]
    with pytest.raises(ValueError) as excinfo:
        projection_query.build()
    assert str(excinfo.value) == "Tried to select a superset of the LSI's projected columns in strict mode"


def test_select_non_strict_lsi_superset(projection_query):
    """Superset of non-strict LSI projected_columns is valid, will incur additional reads"""
    projection_query.key = ProjectedIndexes.h == 4
    projection_query.index = ProjectedIndexes.by_lsi
    projection_query.strict = False
    projection_query.select = [ProjectedIndexes.lsi_only, ProjectedIndexes.neither]

    projection_query.build()


def test_filter_not_condition(query):
    query.filter = "should be a condition"
    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Filter must be a condition or None"


def test_filter_none(query):
    """filter isn't required"""
    query.filter = None
    query.build()


def test_filter_compound(query):
    query.filter = (ComplexModel.not_projected == 1) & (ComplexModel.name.is_not(None))
    query.build()


def test_consistent_gsi_raises(query):
    """Setting consistent on a GSI query has no effect"""
    query.index = ComplexModel.by_email
    query.consistent = True

    assert "ConsistentRead" not in query.build()._request


def test_consistent(query):
    query.index = None
    query.key = ComplexModel.name == "foo"
    query.consistent = True
    assert query.build()._request["ConsistentRead"] is True


def test_forward_scan(query):
    """Can't set forward on a scan"""
    query.mode = "scan"
    query.forward = True

    assert "ScanIndexForward" not in query.build()._request


def test_forward(query):
    query.mode = "query"
    query.forward = True
    assert query.build()._request["ScanIndexForward"] is True


def test_negative_limit(query):
    query.limit = -3
    assert "Limit" not in query.build()._request


@pytest.mark.parametrize("limit", [0, 3])
def test_limit(query, limit):
    query.limit = limit
    query.build()


def test_one_no_results(simple_query, engine, session):
    """one raises when there are no results"""
    session.query_items.return_value = {"Count": 0, "ScannedCount": 6, "Items": []}

    with pytest.raises(ConstraintViolation):
        simple_query.one()
    same_request = simple_query.build()._request
    same_request["ExclusiveStartKey"] = None
    assert session.query_items.call_count == 1


def test_one_extra_results(simple_query, engine, session):
    """one raises when there are too many results"""
    session.query_items.return_value = {
        "Count": 2, "ScannedCount": 6,
        "Items": [{"id": {"S": "first"}}, {"id": {"S": "second"}}]}

    with pytest.raises(ConstraintViolation):
        simple_query.one()
    same_request = simple_query.build()._request
    same_request["ExclusiveStartKey"] = None
    assert session.query_items.call_count == 1


def test_one_exact(simple_query, engine, session):
    """one returns when there is exactly one value in the full query"""
    session.query_items.return_value = {"Count": 1, "ScannedCount": 6, "Items": [{"id": {"S": "unique"}}]}

    result = simple_query.one()
    assert result.id == "unique"

    assert session.query_items.call_count == 1


def test_first_no_results(simple_query, session):
    """first raises when there are no results"""
    session.query_items.return_value = {"Count": 0, "ScannedCount": 6, "Items": []}

    with pytest.raises(ConstraintViolation):
        simple_query.first()
    same_request = simple_query.build()._request
    same_request["ExclusiveStartKey"] = None
    assert session.query_items.call_count == 1


def test_first_extra_results(simple_query, engine, session):
    """first returns the first result, even when there are multiple values"""
    session.query_items.return_value = {
        "Count": 2, "ScannedCount": 6,
        "Items": [{"id": {"S": "first"}}, {"id": {"S": "second"}}]}

    result = simple_query.first()
    assert result.id == "first"
    assert session.query_items.call_count == 1


def test_build_gsi_consistent_read(query):
    """Queries on a GSI can't be consistent"""
    query.index = ComplexModel.by_email
    # Can't use builder function - assume it was provided at __init__
    query._consistent = True
    prepared_request = query.build()._request
    assert "ConsistentRead" not in prepared_request


def test_build_lsi_consistent_read(query):
    query.index = ComplexModel.by_joined
    query.key = ComplexModel.name == "foo"
    query.consistent = False
    prepared_request = query.build()._request
    assert prepared_request["ConsistentRead"] is False


def test_build_no_limit(query):
    query.limit = 0
    prepared_request = query.build()._request
    assert "Limit" not in prepared_request


def test_build_limit(query):
    query.limit = 4
    assert query.build()._limit == 4


def test_build_scan_forward(query):
    query.mode = "scan"
    query.forward = True

    prepared_request = query.build()._request
    assert "ScanIndexForward" not in prepared_request


def test_build_query_forward(query):
    query.forward = False
    prepared_request = query.build()._request
    assert prepared_request["ScanIndexForward"] is False


def test_build_table_name(query):
    """TableName is always set"""
    # On an index...
    query.mode = "query"
    query.index = ComplexModel.by_email
    prepared_request = query.build()._request
    assert prepared_request["TableName"] == ComplexModel.Meta.table_name

    # On the table...
    query.index = None
    query.key = ComplexModel.name == "foo"
    prepared_request = query.build()._request
    assert prepared_request["TableName"] == ComplexModel.Meta.table_name

    # On scan...
    query.mode = "scan"

    prepared_request = query.build()._request
    assert prepared_request["TableName"] == ComplexModel.Meta.table_name


def test_build_no_index_name(query):
    """IndexName isn't set on table queries"""
    query.index = None
    query.key = ComplexModel.name == "foo"
    prepared_request = query.build()._request
    assert "IndexName" not in prepared_request


def test_build_index_name(query):
    query.index = ComplexModel.by_joined
    query.key = ComplexModel.name == "foo"
    prepared_request = query.build()._request
    assert prepared_request["IndexName"] == "by_joined"


def test_build_filter(query):
    query.filter = ComplexModel.email.contains("@")
    prepared_request = query.build()._request
    # Filters are rendered first, so these name and value ids are stable
    assert prepared_request["FilterExpression"] == "(contains(#n0, :v1))"
    assert prepared_request["ExpressionAttributeNames"]["#n0"] == "email"
    assert prepared_request["ExpressionAttributeValues"][":v1"] == {"S": "@"}


def test_build_no_filter(query):
    query.filter = None
    prepared_request = query.build()._request
    assert "FilterExpression" not in prepared_request


def test_build_select_columns(query):
    query.select = {ComplexModel.date}
    prepared_request = query.build()._request
    assert prepared_request["Select"] == "SPECIFIC_ATTRIBUTES"
    assert prepared_request["ExpressionAttributeNames"]["#n2"] == "date"
    assert prepared_request["ProjectionExpression"] == "#n2"


def test_build_select_all(query):
    query.index = None
    query.select = "all"
    query.key = ComplexModel.name == "foo"
    prepared_request = query.build()._request
    assert prepared_request["Select"] == "ALL_ATTRIBUTES"
    assert "ProjectionExpression" not in prepared_request


def test_build_select_projected(query):
    query.index = ComplexModel.by_email
    query.select = "projected"
    prepared_request = query.build()._request
    assert prepared_request["Select"] == "ALL_PROJECTED_ATTRIBUTES"
    assert "ProjectionExpression" not in prepared_request


def test_build_select_count(query):
    query.select = "count"
    prepared_request = query.build()._request
    assert prepared_request["Select"] == "COUNT"
    assert "ProjectionExpression" not in prepared_request


def test_build_query_no_key(query):
    query.mode = "query"
    query.key = None

    with pytest.raises(ValueError) as excinfo:
        query.build()
    assert str(excinfo.value) == "Key condition must contain exactly 1 hash condition, at most 1 range condition"


def test_build_query_key(query):
    query.mode = "query"
    query.index = None
    query.key = ComplexModel.name == "foo"

    prepared_request = query.build()._request
    assert prepared_request["KeyConditionExpression"] == "(#n3 = :v4)"
    assert prepared_request["ExpressionAttributeNames"]["#n3"] == "name"
    assert prepared_request["ExpressionAttributeValues"][":v4"] == {"S": "foo"}


def test_build_scan_no_key(query):
    query.mode = "scan"
    query.key = ComplexModel.name == "omitted"

    prepared_request = query.build()._request
    assert "KeyConditionExpression" not in prepared_request


def test_build_expected_all():
    expected_columns = expected_columns_for(
        model=ComplexModel, index=None, select="all", select_attributes=None)
    assert expected_columns == ComplexModel.Meta.columns


def test_build_expected_projected():
    expected_columns = expected_columns_for(
        model=ComplexModel, index=ComplexModel.by_email, select="projected", select_attributes=None)
    assert expected_columns == ComplexModel.Meta.columns


def test_build_expected_count():
    """No expected columns for a count"""
    expected_columns = expected_columns_for(
        model=ComplexModel, index=None, select="count", select_attributes=None)
    assert expected_columns == set()


def test_build_expected_specific():
    select = {ComplexModel.date, ComplexModel.email}
    expected_columns = expected_columns_for(
        model=ComplexModel, index=None, select="specific", select_attributes=select)
    assert expected_columns == select


def test_iter_reset(query):
    iterator = query.build()
    iterator._state["exhausted"] = True
    iterator._state["count"] = 3
    iterator._state["scanned"] = 4

    iterator.reset()
    assert iterator.count == 0
    assert iterator.scanned == 0
    assert iterator.exhausted is False


def test_iter_no_results(query, engine, session):
    session.query_items.return_value = {"Items": [], "Count": 0, "ScannedCount": 5}

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
    session.query_items.assert_called_once_with(iterator._request)


def test_iter_empty_pages(simple_query, engine, session):
    """
    Automatically follow continue tokens until the buffer isn't empty
    Items are returned after pages are exhausted
    """
    session.query_items.side_effect = [
        {"LastEvaluatedKey": "continue", "Items": [], "Count": 0, "ScannedCount": 3},
        {"Items": [{"id": {"S": "first"}}, {"id": {"S": "second"}}], "Count": 2, "ScannedCount": 4}
    ]
    iterator = simple_query.build()

    first = next(iterator)
    assert not iterator.exhausted
    assert iterator.count == 2
    assert iterator.scanned == 7

    second = next(iterator)
    # No new calls, but now the iterator is exhausted
    assert iterator.exhausted

    # And here we run out of items to fetch
    with pytest.raises(StopIteration):
        next(iterator)

    # Things were loaded properly
    assert first.id == "first"
    assert second.id == "second"

    # Followed 3 continuation tokens
    assert session.query_items.call_count == 2
