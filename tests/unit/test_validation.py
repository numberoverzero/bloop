import functools
import pytest

from bloop.condition import (
    And, AttributeExists,
    BeginsWith, Between,
    Comparison, Condition,
    Contains, In, Not, Or,
    comparison_aliases
)
from bloop.exceptions import InvalidKeyCondition
from bloop.models import BaseModel, Column, GlobalSecondaryIndex
from bloop.types import Integer
from bloop.validation import validate_key_condition

all_conditions = {
    And, AttributeExists, BeginsWith, Between,
    Comparison, Condition, Contains, In, Not, Or}
meta_conditions = {And, Or, Not}
range_conditions = {BeginsWith, Between, Comparison}
# Needed with an include != since all other comparisons are valid
bad_range_conditions = all_conditions - {BeginsWith, Between}


def model_for(has_model_range=False, has_index=False, has_index_range=False):
    class Model(BaseModel):
        model_hash = Column(Integer, hash_key=True)
        if has_model_range:
            model_range = Column(Integer, range_key=True)
        if has_index:
            index_hash = Column(Integer)
            if has_index_range:
                index_range = Column(Integer)
            by_gsi = GlobalSecondaryIndex(
                projection="all",
                hash_key="index_hash",
                range_key=("index_range" if has_index_range else False))
    return Model, (Model.by_gsi if has_index else None)

all_permutations = [
    # Model hash only
    (False, False, False),
    # Model hash + range
    (True, False, False),
    # Index hash only
    (False, True, False),
    # Index hash + range
    (False, True, True),
]

hash_and_range_permutations = [
    # Model
    (True, False, False),
    # Index
    (False, True, True)
]


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


@pytest.mark.parametrize("permutation", all_permutations)
def test_single_hash_key_success(permutation):
    """Single key condition: equality comparison on hash key"""
    model, index = model_for(*permutation)
    query_on = index or model.Meta
    key = query_on.hash_key == "value"
    validate_key_condition(model, index, key)


@pytest.mark.parametrize("permutation", all_permutations)
@pytest.mark.parametrize("key_lambda", conditions_for(all_conditions - {And}, exclude=["=="]))
def test_single_key_failure(permutation, key_lambda):
    """No other single key condition (except AND) will succeed"""
    model, index = model_for(*permutation)
    # Get the correct hash key so only the condition type is wrong
    hash_key_column = (index or model.Meta).hash_key
    key = key_lambda(column=hash_key_column)

    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, key)


@pytest.mark.parametrize("permutation", hash_and_range_permutations)
@pytest.mark.parametrize("count", [0, 1, 3])
def test_and_not_two(permutation, count):
    """AND on hash+range fails if there aren't exactly 2 key conditions"""
    model, index = model_for(*permutation)
    hash_key_column = (index or model.Meta).hash_key
    key = And(*[hash_key_column == "value"] * count)
    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, key)


@pytest.mark.parametrize("permutation", hash_and_range_permutations)
@pytest.mark.parametrize("key_name", ["hash_key", "range_key"])
def test_and_both_same_key(permutation, key_name):
    """AND with 2 conditions, but both conditions are on the same key"""
    model, index = model_for(*permutation)
    key_column = getattr(index or model.Meta, key_name)
    condition = key_column == "value"
    key = And(condition, condition)
    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, key)


@pytest.mark.parametrize("permutation", hash_and_range_permutations)
@pytest.mark.parametrize("range_condition_lambda", conditions_for(range_conditions, exclude=["!="]))
def test_hash_and_range_key_success(permutation, range_condition_lambda):
    """AND(hash, range) + AND(range, hash) for valid hash and range key conditions"""
    model, index = model_for(*permutation)
    hash_condition = (index or model.Meta).hash_key == "value"
    range_condition = range_condition_lambda(column=(index or model.Meta).range_key)
    validate_key_condition(model, index, And(hash_condition, range_condition))
    validate_key_condition(model, index, And(range_condition, hash_condition))


@pytest.mark.parametrize("permutation", hash_and_range_permutations)
@pytest.mark.parametrize("range_condition_lambda", conditions_for(range_conditions, exclude=["!="]))
def test_and_bad_hash_key(permutation, range_condition_lambda):
    """AND with valid range key condition but bad hash key condition"""
    model, index = model_for(*permutation)
    hash_condition = (index or model.Meta).hash_key.between("bad", "condition")
    range_condition = range_condition_lambda(column=(index or model.Meta).range_key)

    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, And(hash_condition, range_condition))
    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, And(range_condition, hash_condition))


@pytest.mark.parametrize("permutation", hash_and_range_permutations)
@pytest.mark.parametrize("range_condition_lambda", conditions_for(bad_range_conditions, include=["!="]))
def test_and_bad_range_key(permutation, range_condition_lambda):
    """AND with valid hash range condition but bad hash key condition"""
    model, index = model_for(*permutation)
    hash_condition = (index or model.Meta).hash_key == "value"
    range_condition = range_condition_lambda(column=(index or model.Meta).range_key)

    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, And(hash_condition, range_condition))
    with pytest.raises(InvalidKeyCondition):
        validate_key_condition(model, index, And(range_condition, hash_condition))
