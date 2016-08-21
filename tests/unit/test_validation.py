import functools

import pytest
from bloop.conditions import (
    And,
    AttributeExists,
    BeginsWith,
    Between,
    Comparison,
    Condition,
    Contains,
    In,
    Not,
    Or,
    comparison_aliases,
)
from bloop.exceptions import (
    InvalidFilterCondition,
    InvalidKeyCondition,
    InvalidProjection,
)
from bloop.models import (
    BaseModel,
    Column,
    GlobalSecondaryIndex,
    LocalSecondaryIndex,
)
from bloop.types import Integer
from bloop.validation import (
    validate_filter_condition,
    validate_key_condition,
    validate_search_projection,
)

from ..helpers.models import ComplexModel


all_conditions = {
    And, AttributeExists, BeginsWith, Between,
    Comparison, Condition, Contains, In, Not, Or}
meta_conditions = {And, Or, Not}
range_conditions = {BeginsWith, Between, Comparison}
# Needed with an include != since all other comparisons are valid
bad_range_conditions = all_conditions - {BeginsWith, Between}


def model_for(has_model_range=False, has_index=False, has_index_range=False,
              index_type="gsi", index_projection="all"):
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
                range_key="index_range"
            )

    class TestModel(BaseModel):
        # Included in an "all" projection, not "keys"
        not_projected = Column(Integer)

        model_hash = Column(Integer, hash_key=True)
        model_range = model_range_
        index_hash = index_hash_
        index_range = index_range_
        by_index = by_index_

    return TestModel, by_index_

# permutations with a range key
range_permutations = [
    # Model - hash and range
    model_for(has_model_range=True, has_index=False),

    # LSIs always require a model range key and index range key.
    model_for(has_model_range=True, has_index=True, has_index_range=True, index_type="lsi", index_projection="all"),
    model_for(has_model_range=True, has_index=True, has_index_range=True, index_type="lsi", index_projection="keys"),

    # GSIs with index range key; with and without model range key
    model_for(has_model_range=False, has_index=True, has_index_range=True, index_type="gsi", index_projection="all"),
    model_for(has_model_range=False, has_index=True, has_index_range=True, index_type="gsi", index_projection="keys"),
    model_for(has_model_range=True, has_index=True, has_index_range=True, index_type="gsi", index_projection="all"),
    model_for(has_model_range=True, has_index=True, has_index_range=True, index_type="gsi", index_projection="keys"),
]

all_permutations = [
    # Model - hash key only
    model_for(has_model_range=False, has_index=False),

    # LSIs included above, always require a model range key and index range key

    # GSIs without an index range key; with and without model range key
    model_for(has_model_range=False, has_index=True, has_index_range=False, index_type="gsi", index_projection="all"),
    model_for(has_model_range=False, has_index=True, has_index_range=False, index_type="gsi", index_projection="keys"),
    model_for(has_model_range=True, has_index=True, has_index_range=False, index_type="gsi", index_projection="all"),
    model_for(has_model_range=True, has_index=True, has_index_range=False, index_type="gsi", index_projection="keys"),
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

    for operator in comparison_aliases:
        if check(operator):
            condition_lambdas.append(
                functools.partial(Comparison, operator=operator, value=value)
            )
    return condition_lambdas


def conditions_for(classes, include=None, exclude=None):
    """Returns lambdas that take column"""
    value = "value"
    values = ["0", "1", "2"]
    condition_lambdas = []
    if AttributeExists in classes:
        condition_lambdas.append(lambda column: AttributeExists(column, negate=False))
        condition_lambdas.append(lambda column: AttributeExists(column, negate=True))
    if BeginsWith in classes:
        condition_lambdas.append(lambda column: BeginsWith(column, value))
    if Between in classes:
        condition_lambdas.append(lambda column: Between(column, values[0], values[1]))
    if Comparison in classes:
        condition_lambdas.extend(comparisons_for(include=include, exclude=exclude))
    if Condition in classes:
        condition_lambdas.append(lambda column: Condition())
    if Contains in classes:
        condition_lambdas.append(lambda column: Contains(column, value))
    if In in classes:
        condition_lambdas.append(lambda column: In(column, values))

    # Meta Conditions
    if And in classes:
        condition_lambdas.append(lambda column: And(column == value, column != value))
    if Or in classes:
        condition_lambdas.append(lambda column: Or(column == value, column != value))
    if Not in classes:
        condition_lambdas.append(lambda column: Not(column == value))

    return condition_lambdas


@pytest.mark.parametrize("model, index", all_permutations)
def test_single_hash_key_success(model, index):
    """Single key condition: equality comparison on hash key"""
    query_on = index or model.Meta
    key = query_on.hash_key == "value"
    validate_key_condition(model, index, key)


@pytest.mark.parametrize("model, index", all_permutations)
@pytest.mark.parametrize("key_lambda", conditions_for(all_conditions - {And}, exclude=["=="]))
def test_single_key_failure(model, index, key_lambda):
    """No other single key condition (except AND) will succeed"""
    # Get the correct hash key so only the condition type is wrong
    hash_key_column = (index or model.Meta).hash_key
    key = key_lambda(column=hash_key_column)

    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, key)


@pytest.mark.parametrize("model, index", range_permutations)
@pytest.mark.parametrize("count", [0, 1, 3])
def test_and_not_two(model, index, count):
    """AND on hash+range fails if there aren't exactly 2 key conditions"""
    hash_key_column = (index or model.Meta).hash_key
    key = And(*[hash_key_column == "value"] * count)
    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, key)


@pytest.mark.parametrize("model, index", range_permutations)
@pytest.mark.parametrize("key_name", ["hash_key", "range_key"])
def test_and_both_same_key(model, index, key_name):
    """AND with 2 conditions, but both conditions are on the same key"""
    key_column = getattr(index or model.Meta, key_name)
    condition = key_column == "value"
    key = And(condition, condition)
    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, key)


@pytest.mark.parametrize("model, index", range_permutations)
@pytest.mark.parametrize("range_condition_lambda", conditions_for(range_conditions, exclude=["!="]))
def test_hash_and_range_key_success(model, index, range_condition_lambda):
    """AND(hash, range) + AND(range, hash) for valid hash and range key conditions"""
    hash_condition = (index or model.Meta).hash_key == "value"
    range_condition = range_condition_lambda(column=(index or model.Meta).range_key)
    validate_key_condition(model, index, And(hash_condition, range_condition))
    validate_key_condition(model, index, And(range_condition, hash_condition))


@pytest.mark.parametrize("model, index", range_permutations)
@pytest.mark.parametrize("range_condition_lambda", conditions_for(range_conditions, exclude=["!="]))
def test_and_bad_hash_key(model, index, range_condition_lambda):
    """AND with valid range key condition but bad hash key condition"""
    hash_condition = (index or model.Meta).hash_key.between("bad", "condition")
    range_condition = range_condition_lambda(column=(index or model.Meta).range_key)

    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, And(hash_condition, range_condition))
    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, And(range_condition, hash_condition))


@pytest.mark.parametrize("model, index", range_permutations)
@pytest.mark.parametrize("range_condition_lambda", conditions_for(bad_range_conditions, include=["!="]))
def test_and_bad_range_key(model, index, range_condition_lambda):
    """AND with valid hash range condition but bad hash key condition"""
    hash_condition = (index or model.Meta).hash_key == "value"
    range_condition = range_condition_lambda(column=(index or model.Meta).range_key)

    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, And(hash_condition, range_condition))
    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, And(range_condition, hash_condition))


@pytest.mark.parametrize("model, index", all_permutations)
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("empty_projection", [None, list()])
def test_search_projection_is_required(model, index, strict, empty_projection):
    """Test a missing projection, and an empty list of column names"""
    with pytest.raises(InvalidProjection):
        validate_search_projection(model, index, strict, projection=empty_projection)


@pytest.mark.parametrize("model, index", all_permutations)
@pytest.mark.parametrize("strict", [False, True])
def test_search_projection_is_count(model, index, strict):
    assert validate_search_projection(model, index, strict, projection="count") is None


@pytest.mark.parametrize("model, index", all_permutations)
@pytest.mark.parametrize("strict", [False, True])
def test_search_projection_all(model, index, strict):
    projected = validate_search_projection(model, index, strict, projection="all")

    if not index:
        # Model searches don't care about strict
        expected = model.Meta.columns
    else:
        # GSI all doesn't care about strict
        if isinstance(index, GlobalSecondaryIndex):
            expected = index.projected_columns
        else:
            # LSI strict is only the index projection
            if strict:
                expected = index.projected_columns
            # Grab it all
            else:
                expected = model.Meta.columns

    assert projected == expected


@pytest.mark.parametrize("model, index", all_permutations)
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("bad_column", ["unknown", None])
def test_search_projection_unknown_column(model, index, strict, bad_column):
    with pytest.raises(InvalidProjection):
        validate_search_projection(model, index, strict, projection=["model_hash", bad_column])


@pytest.mark.parametrize("model, index", all_permutations)
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("as_strings", [False, True])
def test_search_projection_includes_non_projected_column(model, index, strict, as_strings):
    """Specific column names exist.

    Table, non-strict LSI, and indexes that project all columns will succeed; the rest fail."""
    should_succeed = False
    # Table searches always include all columns
    if index is None:
        should_succeed = True
    elif isinstance(index, LocalSecondaryIndex) and strict is False:
        should_succeed = True
    elif index.projection == "all":
        should_succeed = True

    if as_strings:
        projection = ["model_hash", "not_projected"]
    else:
        projection = [model.model_hash, model.not_projected]

    if should_succeed:
        projected = validate_search_projection(
            model, index, strict, projection=projection)
        assert projected == [model.model_hash, model.not_projected]

    else:
        with pytest.raises(InvalidProjection):
            validate_search_projection(
                model, index, strict, projection=projection)


def test_validate_no_filter():
    """Filter can be None"""
    validate_filter_condition(None, ComplexModel.Meta.columns, set())


def test_validate_filter_not_available():
    """The condition uses a column that's not available"""
    with pytest.raises(InvalidFilterCondition):
        validate_filter_condition(ComplexModel.date == "now", set(), set())


def test_validate_filter_blacklisted():
    """The condition uses a column that's on the blacklist"""
    with pytest.raises(InvalidFilterCondition):
        validate_filter_condition(ComplexModel.date == "now", ComplexModel.Meta.columns, {ComplexModel.date})


def test_validate_filter_success():
    """All of the columns in the condition are available and not blacklisted"""
    condition = (
        (ComplexModel.date >= "now") &
        (ComplexModel.email.contains("@")) &
        ((ComplexModel.joined.is_(None)) | (ComplexModel.name.in_(["foo", "bar"])))
    )
    validate_filter_condition(condition, ComplexModel.Meta.columns, set())
