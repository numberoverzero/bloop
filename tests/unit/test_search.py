import collections
import functools

import pytest

from bloop.conditions import (
    AndCondition,
    BeginsWithCondition,
    BetweenCondition,
    ComparisonCondition,
    Condition,
    ContainsCondition,
    InCondition,
    NotCondition,
    OrCondition,
    comparison_aliases,
)
from bloop.exceptions import ConstraintViolation, InvalidSearch
from bloop.models import (
    BaseModel,
    Column,
    GlobalSecondaryIndex,
    LocalSecondaryIndex,
)
from bloop.search import (
    PreparedSearch,
    QueryIterator,
    ScanIterator,
    Search,
    SearchIterator,
    SearchModelIterator,
    printable_query,
    search_repr,
    validate_filter_condition,
    validate_key_condition,
    validate_search_projection,
)
from bloop.types import Integer
from bloop.util import Sentinel

from ..helpers.models import ComplexModel, User


proceed = Sentinel("proceed")


all_conditions = {
    AndCondition, BeginsWithCondition, BetweenCondition,
    ComparisonCondition, Condition, ContainsCondition, InCondition, NotCondition, OrCondition}
meta_conditions = {AndCondition, OrCondition, NotCondition}
range_conditions = {BeginsWithCondition, BetweenCondition, ComparisonCondition}
# Needed with an include != since all other comparisons are valid
bad_range_conditions = all_conditions - {BeginsWithCondition, BetweenCondition}


def model_for(
        has_model_range=False, has_index=False, has_index_range=False,
        index_type="gsi", index_projection="all", strict=True):
    """Not all permutations are possible.  Impossible selections will always self-correct.

    For instance, has_model_range=False, has_index=True, index_type="gsi" can't happen.
    Instead, the model won't have an index."""
    model_range_ = None
    index_hash_ = None
    index_range_ = None
    by_index_ = None

    if has_model_range:
        model_range_ = Column(Integer, range_key=True)
    # Sets up index_hash, index_range, by_index
    if has_index:
        if index_type == "gsi":
            index_hash_ = Column(Integer)
            if has_index_range:
                index_range_ = Column(Integer)
            by_index_ = GlobalSecondaryIndex(
                projection=index_projection,
                hash_key="index_hash",
                range_key="index_range" if has_index_range else None)
        elif index_type == "lsi" and has_model_range and has_index_range:
            index_range_ = Column(Integer)
            by_index_ = LocalSecondaryIndex(
                projection=index_projection,
                range_key="index_range",
                strict=strict
            )

    class TestModel(BaseModel):
        # Included in an "all" projection, not "keys"
        not_projected = Column(Integer)
        # Included in "include" projections
        inc = Column(Integer)

        model_hash = Column(Integer, hash_key=True)
        model_range = model_range_
        index_hash = index_hash_
        index_range = index_range_
        by_index = by_index_

    return TestModel, by_index_


# LSIs always require a model range key, and index range key.
lsi_for = functools.partial(model_for, has_model_range=True, has_index=True, has_index_range=True, index_type="lsi")
gsi_for = functools.partial(model_for, has_index=True, index_type="gsi")

# permutations with a range key
range_permutations = [
    # Model - hash and range
    model_for(has_model_range=True, has_index=False),

    # LSIs care about strictness
    lsi_for(index_projection="all"),
    lsi_for(index_projection="all", strict=False),
    lsi_for(index_projection="keys"),
    lsi_for(index_projection="keys", strict=False),
    lsi_for(index_projection=["inc"]),
    lsi_for(index_projection=["inc"], strict=False),

    # GSIs with index range key; with and without model range key
    gsi_for(has_model_range=False, has_index_range=True, index_projection="all"),
    gsi_for(has_model_range=False, has_index_range=True, index_projection="keys"),
    gsi_for(has_model_range=False, has_index_range=True, index_projection=["inc"]),
    gsi_for(has_model_range=True, has_index_range=True, index_projection="all"),
    gsi_for(has_model_range=True, has_index_range=True, index_projection="keys"),
    gsi_for(has_model_range=True, has_index_range=True, index_projection=["inc"]),
]

all_permutations = [
    # Model - hash key only
    model_for(has_model_range=False, has_index=False),

    # LSIs included above, always require a model range key and index range key

    # GSIs without an index range key; with and without model range key
    gsi_for(has_model_range=False, has_index_range=False, index_projection="all"),
    gsi_for(has_model_range=False, has_index_range=False, index_projection="keys"),
    gsi_for(has_model_range=False, has_index_range=False, index_projection=["inc"]),
    gsi_for(has_model_range=True, has_index_range=False, index_projection="all"),
    gsi_for(has_model_range=True, has_index_range=False, index_projection="keys"),
    gsi_for(has_model_range=True, has_index_range=False, index_projection=["inc"]),
] + range_permutations


def comparisons_for(include=None, exclude=None):
    """Returns lambdas that take column, so we can permute condition X model/index"""
    value = "value"
    include = include or []
    exclude = exclude or []
    if include:
        check = lambda op: op in include
    elif exclude:
        check = lambda op: op not in exclude
    else:
        check = lambda op: True

    condition_lambdas = []

    for operation in comparison_aliases:
        if check(operation):
            condition_lambdas.append(
                functools.partial(ComparisonCondition, operation=operation, value=value)
            )
    return condition_lambdas


def conditions_for(classes, include=None, exclude=None):
    """Returns lambdas that take column"""
    value = "value"
    values = ["0", "1", "2"]
    condition_lambdas = []
    if BeginsWithCondition in classes:
        condition_lambdas.append(lambda column: BeginsWithCondition(column, value))
    if BetweenCondition in classes:
        condition_lambdas.append(lambda column: BetweenCondition(column, values[0], values[1]))
    if ComparisonCondition in classes:
        condition_lambdas.extend(comparisons_for(include=include, exclude=exclude))
    if Condition in classes:
        condition_lambdas.append(lambda column: Condition())
    if ContainsCondition in classes:
        condition_lambdas.append(lambda column: ContainsCondition(column, value))
    if InCondition in classes:
        condition_lambdas.append(lambda column: InCondition(column, values))

    # Meta Conditions
    if AndCondition in classes:
        condition_lambdas.append(lambda column: AndCondition(column == value, column != value))
    if OrCondition in classes:
        condition_lambdas.append(lambda column: OrCondition(column == value, column != value))
    if NotCondition in classes:
        condition_lambdas.append(lambda column: NotCondition(column == value))

    return condition_lambdas


def next_non_zero_index(chain, start=0):
    """Return the next index >= start of a non-zero value. -1 on failure to find"""
    non_zeros = filter(lambda x: x, chain[start:])
    value = next(non_zeros, None)
    return chain.index(value, start) if value else -1


def calls_for_current_steps(chain, current_steps):
    """The number of dynamodb calls that are required to iterate the given chain in the given number of steps.

    Here, steps is the number of values returned from next(iterator).  In the table below, the first 3 next() calls
    are all served by the first response from Dynamo, but the 4th next() requires two more calls (second call is empty)
    For example, the chain [3, 0, 1, 4] has the following table:

        +-------+---+---+---+---+---+---+---+---+---+---+
        | steps | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |
        +-------+---+---+---+---+---+---+---+---+---+---+
        | calls | 1 | 1 | 1 | 1 | 3 | 4 | 4 | 4 | 4 | 4 |
        +-------+---+---+---+---+---+---+---+---+---+---+
    """
    required_steps = 0
    call_count = 0
    for call_count, page in enumerate(chain):
        required_steps += page
        if required_steps >= current_steps:
            break
    return call_count + 1


def response(count=1, terminate=False, item=Sentinel("item"), items=None):
    """This fills in the required response structure from a single query/scan call:

    Count, ScannedCount
        required fields but not important.
    LastEvaluatedKey
        tells the iterator if there are more pages available.  When it's not falsey,
        it should be fed directly back into the next request's "ExclusiveStartKey".
    Item
        is the number of items in this page.  By passing 0, 1, or 2 we can verify that the buffer is
        fully drained before the next page is loaded.  It also lets us verify that the while loop will
        follow LastEvaluatedKeys until it hits a non-empty page.
    """
    items = items or [item] * count
    return {
        "Count": count,
        "ScannedCount": count * 3,
        "Items": items,
        "LastEvaluatedKey": None if terminate else proceed
    }


def build_responses(chain, items=None):
    """This expands a compact integer description of a set of pages into the appropriate response structure.

    For example: [0, 2, 1] expands into (0 results, proceed) -> (2 results, proceed) -> (1 result, stop).
    We'll also use those integers in the verifier to compare number of results and number of calls.
    """
    items = items or []
    responses = []
    for count in chain[:-1]:
        response_items = None
        if items:
            response_items, items = items[:count], items[count:]
        responses.append(response(count=count, items=response_items))
    responses.append(response(count=chain[-1], items=items, terminate=True))
    return responses


def extract_request(mock_method):
    """Pull out the request object from the last call to the mocked method

    req = extract_request(session.search_items)
    """
    args, kwargs = mock_method.call_args
    *_, request = args
    return request


@pytest.fixture
def simple_iter(engine, session):
    def _simple_iter(cls=SearchIterator, model=User, index=None):
        kwargs = {
            "engine": engine,
            "session": session,
            "model": model,
            "index": index,
            "request": {
                "Select": "SPECIFIC_ATTRIBUTES"
            },
            "projected": set()
        }
        if issubclass(cls, SearchModelIterator):
            kwargs.pop("session")
        elif cls is SearchIterator:
            kwargs.pop("engine")

        return cls(**kwargs)
    return _simple_iter


@pytest.fixture
def valid_search(engine):
    search = Search(
        engine=engine, model=ComplexModel, index=None, key=ComplexModel.name == "foo",
        filter=None, projection="all", consistent=True, forward=False)
    search.mode = "query"
    return search


# VALIDATION TESTS =================================================================================== VALIDATION TESTS

@pytest.mark.parametrize("model, index", all_permutations)
def test_single_hash_key_success(model, index):
    """Single key condition: equality comparison on hash key"""
    query_on = index or model.Meta
    key = query_on.hash_key == "value"
    validate_key_condition(model, index, key)


@pytest.mark.parametrize("model, index", all_permutations)
@pytest.mark.parametrize("key_lambda", conditions_for(all_conditions - {AndCondition}, exclude=["=="]))
def test_single_key_failure(model, index, key_lambda):
    """No other single key condition (except AND) will succeed"""
    # Get the correct hash key so only the condition type is wrong
    hash_key_column = (index or model.Meta).hash_key
    key = key_lambda(column=hash_key_column)

    with pytest.raises(InvalidSearch):
        validate_key_condition(model, index, key)


@pytest.mark.parametrize("model, index", range_permutations)
@pytest.mark.parametrize("count", [0, 1, 3])
def test_and_not_two(model, index, count):
    """AND on hash+range fails if there aren't exactly 2 key conditions"""
    hash_key_column = (index or model.Meta).hash_key
    key = AndCondition(*[hash_key_column == "value"] * count)
    with pytest.raises(InvalidSearch):
        validate_key_condition(model, index, key)


@pytest.mark.parametrize("model, index", range_permutations)
@pytest.mark.parametrize("key_name", ["hash_key", "range_key"])
def test_and_both_same_key(model, index, key_name):
    """AND with 2 conditions, but both conditions are on the same key"""
    key_column = getattr(index or model.Meta, key_name)
    condition = key_column == "value"
    key = AndCondition(condition, condition)
    with pytest.raises(InvalidSearch):
        validate_key_condition(model, index, key)


@pytest.mark.parametrize("model, index", range_permutations)
@pytest.mark.parametrize("range_condition_lambda", conditions_for(range_conditions, exclude=["!="]))
def test_hash_and_range_key_success(model, index, range_condition_lambda):
    """AND(hash, range) + AND(range, hash) for valid hash and range key conditions"""
    hash_condition = (index or model.Meta).hash_key == "value"
    range_condition = range_condition_lambda(column=(index or model.Meta).range_key)
    validate_key_condition(model, index, AndCondition(hash_condition, range_condition))
    validate_key_condition(model, index, AndCondition(range_condition, hash_condition))


@pytest.mark.parametrize("model, index", range_permutations)
@pytest.mark.parametrize("range_condition_lambda", conditions_for(range_conditions, exclude=["!="]))
def test_and_bad_hash_key(model, index, range_condition_lambda):
    """AND with valid range key condition but bad hash key condition"""
    hash_condition = (index or model.Meta).hash_key.between("bad", "condition")
    range_condition = range_condition_lambda(column=(index or model.Meta).range_key)

    with pytest.raises(InvalidSearch):
        validate_key_condition(model, index, AndCondition(hash_condition, range_condition))
    with pytest.raises(InvalidSearch):
        validate_key_condition(model, index, AndCondition(range_condition, hash_condition))


@pytest.mark.parametrize("model, index", range_permutations)
@pytest.mark.parametrize("range_condition_lambda", conditions_for(bad_range_conditions, include=["!="]))
def test_and_bad_range_key(model, index, range_condition_lambda):
    """AND with valid hash range condition but bad hash key condition"""
    hash_condition = (index or model.Meta).hash_key == "value"
    range_condition = range_condition_lambda(column=(index or model.Meta).range_key)

    with pytest.raises(InvalidSearch):
        validate_key_condition(model, index, AndCondition(hash_condition, range_condition))
    with pytest.raises(InvalidSearch):
        validate_key_condition(model, index, AndCondition(range_condition, hash_condition))


@pytest.mark.parametrize("model, index", all_permutations)
def test_search_projection_is_required(model, index):
    """Test a missing projection, and an empty list of column names"""
    with pytest.raises(InvalidSearch):
        validate_search_projection(model, index, projection=None)
    with pytest.raises(InvalidSearch):
        validate_search_projection(model, index, projection=list())


@pytest.mark.parametrize("model, index", all_permutations)
def test_search_projection_is_count(model, index):
    assert validate_search_projection(model, index, projection="count") is None


@pytest.mark.parametrize("model, index", all_permutations)
def test_search_projection_all(model, index):
    projected = validate_search_projection(model, index, projection="all")
    expected = (index or model.Meta).projection["included"]
    assert projected == expected


@pytest.mark.parametrize("model, index", all_permutations)
def test_search_projection_unknown_column(model, index):
    with pytest.raises(InvalidSearch):
        validate_search_projection(model, index, projection=["model_hash", "unknown"])
    with pytest.raises(InvalidSearch):
        validate_search_projection(model, index, projection=["model_hash", None])


def test_search_projection_converts_strings():
    """This doesn't need the full matrix of model/index combinations.

    Simply checks that the user can pass strings and get columns"""
    model, index = model_for()
    projection = ["model_hash"]
    expected = [model.model_hash]
    assert validate_search_projection(model, index, projection) == expected


@pytest.mark.parametrize("model, index", all_permutations)
def test_search_projection_includes_non_projected_column(model, index):
    """Specific column names exist.

    Table, non-strict LSI, and indexes that project all columns will succeed; the rest fail."""
    should_succeed = False
    # Table searches always include all columns
    if index is None:
        should_succeed = True
    elif isinstance(index, LocalSecondaryIndex) and index.projection["strict"] is False:
        should_succeed = True
    elif index.projection["mode"] == "all":
        should_succeed = True

    projection = [model.model_hash, model.not_projected]

    if should_succeed:
        projected = validate_search_projection(model, index, projection=projection)
        assert projected == [model.model_hash, model.not_projected]

    else:
        with pytest.raises(InvalidSearch):
            validate_search_projection(model, index, projection=projection)


@pytest.mark.parametrize("model, index", all_permutations)
def test_search_projection_unknown_string(model, index):
    """Don't confuse a string for an iterable list.
    Users can be explicit with list("string") if their column names are all single-characters
    """

    with pytest.raises(InvalidSearch):
        validate_search_projection(model, index, projection="keys")


def test_validate_no_filter():
    """Filter can be None"""
    validate_filter_condition(None, ComplexModel.Meta.columns, set())


def test_validate_filter_not_available():
    """The condition uses a column that's not available"""
    with pytest.raises(InvalidSearch):
        validate_filter_condition(ComplexModel.date == "now", set(), set())


def test_validate_filter_blacklisted():
    """The condition uses a column that's on the blacklist"""
    with pytest.raises(InvalidSearch):
        validate_filter_condition(ComplexModel.date == "now", ComplexModel.Meta.columns, {ComplexModel.date})


def test_validate_filter_success():
    """All of the columns in the condition are available and not blacklisted"""
    condition = (
        (ComplexModel.date >= "now") &
        (ComplexModel.email.contains("@")) &
        ((ComplexModel.joined.is_(None)) | (ComplexModel.name.in_(["foo", "bar"])))
    )
    validate_filter_condition(condition, ComplexModel.Meta.columns, set())

# END VALIDATION TESTS =========================================================================== END VALIDATION TESTS


# PREPARE TESTS ========================================================================================= PREPARE TESTS


@pytest.mark.parametrize("mode, cls", [("query", QueryIterator), ("scan", ScanIterator)])
def test_prepare_iterator_cls(valid_search, engine, mode, cls):
    valid_search.mode = mode
    prepared = valid_search.prepare()

    assert prepared.engine is engine
    assert prepared.mode == mode
    assert prepared._iterator_cls is cls


def test_prepare_unknown_mode(valid_search):
    valid_search.mode = "foo"
    with pytest.raises(InvalidSearch):
        valid_search.prepare()


def test_prepare_model(valid_search):
    prepared = valid_search.prepare()
    assert prepared.model is valid_search.model
    assert prepared.index is valid_search.index
    assert prepared.consistent == valid_search.consistent


def test_prepare_key_for_scan(valid_search):
    """Key isn't validated or set for a scan"""
    valid_search.mode = "scan"
    # Key condition isn't even on the right model
    valid_search.key = User.joined < "now"
    prepared = valid_search.prepare()
    assert prepared.key is None


def test_prepare_key_bad_condition(valid_search):
    valid_search.key = ComplexModel.email <= "foobar"
    with pytest.raises(InvalidSearch):
        valid_search.prepare()


def test_prepare_key_good_condition(valid_search):
    valid_search.key = ComplexModel.name == "bar"
    prepared = valid_search.prepare()
    assert prepared.key is valid_search.key


def test_prepare_count_projection(valid_search):
    valid_search.projection = "count"
    prepared = valid_search.prepare()
    assert prepared._projected_columns is None
    assert prepared._projection_mode == "count"


def test_prepare_specific_projection(valid_search):
    # Even "all" is performed through specific
    valid_search.projection = "all"
    valid_search.index = ComplexModel.by_joined
    valid_search.strict = True

    prepared = valid_search.prepare()
    assert prepared._projected_columns == ComplexModel.by_joined.projection["included"]
    assert prepared._projection_mode == "specific"


def test_prepare_no_filter(valid_search):
    valid_search.filter = None
    prepared = valid_search.prepare()
    assert prepared.filter is None


def test_prepare_valid_filter(valid_search):
    condition = ComplexModel.email == "now"
    valid_search.filter = condition
    prepared = valid_search.prepare()
    assert prepared.filter is condition


def test_prepare_invalid_filter(valid_search):
    # Can't include a key column in a query filter
    condition = ComplexModel.name > "hello"
    valid_search.filter = condition

    with pytest.raises(InvalidSearch):
        valid_search.prepare()


def test_prepare_constraints(valid_search):
    valid_search.forward = False
    valid_search.parallel = (1, 5)
    prepared = valid_search.prepare()
    assert prepared.forward is False
    assert prepared.parallel == (1, 5)


@pytest.mark.parametrize("mode, cls", [("query", QueryIterator), ("scan", ScanIterator)])
def test_prepare_iter(valid_search, mode, cls):
    valid_search.mode = mode
    prepared = valid_search.prepare()
    it = iter(prepared)
    assert isinstance(it, cls)


@pytest.mark.parametrize("mode, include", [("scan", False), ("query", True)])
def test_prepare_request_forward(valid_search, mode, include):
    valid_search.mode = mode
    prepared = valid_search.prepare()
    assert ("ScanIndexForward" in prepared._request) is include


@pytest.mark.parametrize("index, consistent", [(ComplexModel.by_joined, True), (ComplexModel.by_email, False)])
def test_prepare_request_consistent(valid_search, index, consistent):
    valid_search.index = index
    # So we don't have to fix the key condition
    valid_search.mode = "scan"
    prepared = valid_search.prepare()
    assert ("ConsistentRead" in prepared._request) is consistent


def test_prepare_request_count(valid_search):
    """count has Select=COUNT and no entry for ProjectionExpression"""
    valid_search.projection = "count"
    prepared = valid_search.prepare()
    assert prepared._request["Select"] == "COUNT"
    assert "ProjectionExpression" not in prepared._request["Select"]


@pytest.mark.parametrize("index", [ComplexModel.by_joined, ComplexModel.by_email])
def test_prepare_request_specific(valid_search, index):
    valid_search.index = index
    valid_search.projection = {ComplexModel.email}
    # So we don't have to fix the key condition
    valid_search.mode = "scan"

    prepared = valid_search.prepare()
    assert prepared._request["Select"] == "SPECIFIC_ATTRIBUTES"
    assert prepared._request["ProjectionExpression"] == "#n0"


@pytest.mark.parametrize("mode", ["query", "scan"])
@pytest.mark.parametrize("parallel", [False, (2, 5)])
def test_prepare_request_parallel(valid_search, mode, parallel):
    valid_search.mode = mode
    valid_search.parallel = parallel
    prepared = valid_search.prepare()
    if parallel and (mode == "scan"):
        actual = prepared._request["Segments"], prepared._request["TotalSegments"]
        assert actual == parallel
    else:
        assert "Segments" not in prepared._request
        assert "TotalSegments" not in prepared._request


# END PREPARE TESTS ================================================================================= END PREPARE TESTS


@pytest.mark.parametrize("query_on, expected", [
    (User.Meta, User),
    (User.by_email, User.by_email)
])
def test_printable_query(query_on, expected):
    """Unpacks Model.Meta into Model, Index into Index for consistent attribute lookup"""
    assert printable_query(query_on) is expected


def test_search_repr():
    cls = type("Class", tuple(), {})
    model = type("Model", tuple(), {})
    index = type("Index", tuple(), {"name": "by_gsi"})()

    for has_model, has_index, expected in [
        (None, None, "<Class[None]>"),
        (None, True, "<Class[None.by_gsi]>"),
        (True, None, "<Class[Model]>"),
        (True, True, "<Class[Model.by_gsi]>"),
    ]:
        assert search_repr(cls, has_model and model, has_index and index) == expected


def test_reprs(simple_iter):
    assert repr(Search(model=User, index=None)) == "<Search[User]>"
    prepared_search = PreparedSearch()
    prepared_search.model = None
    prepared_search.index = User.by_email
    assert repr(prepared_search) == "<PreparedSearch[None.by_email]>"

    assert repr(simple_iter(SearchIterator, User, None)) == "<SearchIterator[User]>"
    assert repr(simple_iter(SearchModelIterator, User, User.by_email)) == "<SearchModelIterator[User.by_email]>"
    assert repr(simple_iter(QueryIterator, None, None)) == "<QueryIterator[None]>"
    assert repr(simple_iter(ScanIterator, None, User.by_email)) == "<ScanIterator[None.by_email]>"


# ITERATOR TESTS ======================================================================================= ITERATOR TESTS


def test_iterator_returns_self(simple_iter):
    iterator = simple_iter()
    assert iterator is iter(iterator)


def test_iterator_reset(simple_iter, session):
    """reset clears buffer, count, scanned, exhausted"""
    iterator = simple_iter()
    iterator.request["Select"] = "COUNT"

    def reset_state():
        # This helper is necessary because the .count and .scanned properties will
        # exhaust the search pagination during a Select=COUNT query,
        # so we must reset the mock responses each time
        session.search_items.side_effect = build_responses([0, 1, 1], items=["a", "b"])
        iterator.reset()

    # Pretend we've stepped the iterator a few times
    iterator._count = 9
    iterator._scanned = 12
    iterator.buffer.append("obj")
    iterator._exhausted = True

    # Ready to go again, buffer empty and counters reset
    reset_state()
    assert iterator.count == 2
    reset_state()
    assert iterator.scanned == 6
    assert len(iterator.buffer) == 0
    assert iterator.exhausted


@pytest.mark.parametrize("buffer_size", [0, 1])
@pytest.mark.parametrize("has_tokens", [False, True])
def test_iterator_exhausted(simple_iter, buffer_size, has_tokens):
    """Only exhausted when the iterator has run out of continuation tokens and the buffer is empty."""
    iterator = simple_iter()
    iterator.buffer = collections.deque([True] * buffer_size)
    iterator._exhausted = not has_tokens
    should_be_exhausted = not buffer_size and not has_tokens
    assert iterator.exhausted == should_be_exhausted


def test_next_states(simple_iter, session):
    """This monster tests all of the buffer management in SearchIterator.__next__"""

    # Here are the possible boundaries for pagination:
    #
    #                  # calls results
    # 0 Results        # --+-- ---+---
    #   [           ]  #   1      0
    #   [     |     ]  #   2      0
    # 1 Result         # --+-- ---+---
    #   [     ✔     ]  #   1      1
    #   [     |  ✔  ]  #   2      1
    #   [ ✔   |     ]  #   2      1
    # N Results        # --+-- ---+---
    #   [    ✔ ✔    ]  #   1      N
    #   [     | ✔ ✔ ]  #   2      N
    #   [  ✔  |  ✔  ]  #   2      N
    #   [ ✔ ✔ |     ]  #   2      N
    # Additional calls are repetitions of the above
    chains = [
        [0], [0, 0],
        [1], [0, 1], [1, 0],
        [2], [0, 2], [1, 1], [2, 0]
    ]

    def verify_iterator(chain):
        """For a given sequence of result page sizes, verify that __next__ steps forward as expected.

        1) Build a new iterator for the given chain
        2) Load the responses into the mock session
        3) Advance the iterator until it raises StopIteration.  Each step, make sure the iterator is only
           calling dynamodb when the buffer is empty, and that it follows continue tokens on empty pages.
        """
        iterator = simple_iter()
        # VERY IMPORTANT!  Without the reset, calls from
        # the previous chain will count against this chain.
        session.search_items.reset_mock()
        session.search_items.side_effect = build_responses(chain)

        current_steps = 0
        while next(iterator, None):
            current_steps += 1
            expected_call_count = calls_for_current_steps(chain, current_steps)
            assert session.search_items.call_count == expected_call_count
        assert iterator.exhausted

    # Kick it all off
    for chain in chains:
        verify_iterator(chain)


@pytest.mark.parametrize("chain", [[1], [0, 1], [1, 0], [2, 0]])
def test_first_success(simple_iter, session, chain):
    iterator = simple_iter()
    item_count = sum(chain)
    session.search_items.side_effect = build_responses(chain, items=list(range(item_count)))

    first = iterator.first()

    assert first == 0
    assert session.search_items.call_count == next_non_zero_index(chain) + 1


@pytest.mark.parametrize("chain", [[0], [0, 0]])
def test_first_failure(simple_iter, session, chain):
    iterator = simple_iter()
    session.search_items.side_effect = build_responses(chain)

    with pytest.raises(ConstraintViolation):
        iterator.first()
    assert session.search_items.call_count == len(chain)


@pytest.mark.parametrize("chain", [[1], [0, 1], [1, 0]])
def test_one_success(simple_iter, session, chain):
    iterator = simple_iter()
    one = Sentinel("one")
    session.search_items.side_effect = build_responses(chain, items=[one])

    assert iterator.one() is one
    # SearchIterator.one should advance exactly twice, every time
    expected_calls = calls_for_current_steps(chain, 2)
    assert session.search_items.call_count == expected_calls


@pytest.mark.parametrize("chain", [[0], [0, 0], [2], [2, 0], [1, 1], [0, 2]])
def test_one_failure(simple_iter, session, chain):
    iterator = simple_iter()
    session.search_items.side_effect = build_responses(chain)

    with pytest.raises(ConstraintViolation):
        iterator.one()
    # SearchIterator.one should advance exactly twice, every time
    expected_calls = calls_for_current_steps(chain, 2)
    assert session.search_items.call_count == expected_calls
    assert iterator.count == sum(chain)
    assert iterator.scanned == 3 * sum(chain)


@pytest.mark.parametrize("cls", [ScanIterator, QueryIterator])
def test_model_iterator_unpacks(simple_iter, session, cls):
    iterator = simple_iter(cls=cls)
    iterator.projected = {User.name, User.joined}

    attrs = {"name": {"S": "numberoverzero"}}
    session.search_items.return_value = response(terminate=True, count=1, item=attrs)

    obj = iterator.first()

    assert obj.name == "numberoverzero"
    assert obj.joined is None
    for attr in ["id", "age", "email"]:
        assert not hasattr(obj, attr)


@pytest.mark.parametrize("chain", [[0], [0, 0], [2], [2, 0], [1, 1], [0, 2]])
def test_all_resets(simple_iter, session, chain):
    """calls to .all() will always re-execute the search, then return all results at once"""
    iterator = simple_iter()
    responses = build_responses(chain)
    session.search_items.side_effect = responses

    results = iterator.all()
    assert len(results) == sum(chain)
    assert session.search_items.call_count == len(chain)

    # clear mock state, ensure there are 2 new calls
    # https://stackoverflow.com/a/25826320
    session.search_items.reset_mock()
    session.search_items.side_effect = responses

    results = iterator.all()
    assert len(results) == sum(chain)
    assert session.search_items.call_count == len(chain)


def test_empty_token(simple_iter):
    """When no results have been fetched the token is empty"""
    iterator = simple_iter()
    token = iterator.token
    assert token == {"ExclusiveStartKey": None}


def test_token_last_yielded(simple_iter, session):
    """When any results have been yielded, the token should always be the last one"""
    items = [
        {"id": {"S": "first"}},
        {"id": {"S": "second"}},
    ]
    session.search_items.side_effect = build_responses([1, 1], items=items)
    iterator = simple_iter()

    assert iterator.token == {"ExclusiveStartKey": None}

    next(iterator)
    assert iterator.token == {"ExclusiveStartKey": items[0]}

    next(iterator)
    assert iterator.token == {"ExclusiveStartKey": items[1]}

    # advance with no result; token still points at last yielded
    next(iterator, None)
    assert iterator.token == {"ExclusiveStartKey": items[1]}


def test_move_to_beginning(simple_iter, session):
    """Moving to None starts at the beginning of the search again"""
    items = [
        {"id": {"S": "first"}},
        {"id": {"S": "second"}},
    ]
    session.search_items.side_effect = build_responses([1, 1], items=items)
    iterator = simple_iter()

    empty = iterator.token
    next(iterator)

    # ensure we're not still at the beginning; something has been yielded
    assert iterator.token != empty

    # we don't send the ESK from the first yielded value because the iterator was reset
    iterator.move_to(empty)
    next(iterator)
    *_, request = session.search_items.call_args
    assert "ExclusiveStartKey" not in request


def test_move_to_token(simple_iter, session):
    """Moving to a token sends that ESK in the next request"""
    items = [
        {"id": {"S": "first"}},
        {"id": {"S": "second"}},
    ]
    session.search_items.side_effect = build_responses([1, 1], items=items)
    iterator = simple_iter()
    same = simple_iter()

    # advance once and create a token
    next(iterator)
    token = iterator.token

    # move and advance; this should update .request to include ESK
    same.move_to(token)
    next(same)
    request = extract_request(session.search_items)
    assert request["ExclusiveStartKey"] == items[0]

    # since this is the second advance the new token should be 2
    assert same.token == {"ExclusiveStartKey": items[1]}


# END ITERATOR TESTS =============================================================================== END ITERATOR TESTS
