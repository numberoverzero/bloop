from bloop.column import Column
from bloop.condition import AttributeExists, BeginsWith, Between, Contains, In
from bloop.types import Integer

import operator as op
import pytest

operations = [op.ne, op.eq, op.lt, op.le, op.gt, op.ge]
operation_ids = ["!=", "==", "<", "<=", ">", ">="]


def test_equals_alias_exists():
    """
    == and != should map to attribute_not_exists and attribute_exists
    when compared to None
    """
    column = Column(Integer)

    condition = column.is_(None)
    assert isinstance(condition, AttributeExists)
    assert condition.column is column
    assert condition.negate is True

    condition = column.is_not(None)
    assert isinstance(condition, AttributeExists)
    assert condition.column is column
    assert condition.negate is False


@pytest.mark.parametrize("operation", operations, ids=operation_ids)
def test_comparison(operation):
    column = Column(Integer)
    value = object()

    condition = operation(column, value)
    assert condition.comparator is operation
    assert condition.column is column
    assert condition.value is value


def test_between():
    lower, upper = object(), object()
    column = Column(Integer)
    condition = column.between(lower, upper)

    assert isinstance(condition, Between)
    assert condition.column is column
    assert condition.lower is lower
    assert condition.upper is upper


def test_in():
    values = [object() for _ in range(3)]
    column = Column(Integer)
    condition = column.in_(values)

    assert isinstance(condition, In)
    assert condition.column is column
    assert condition.values == values


def test_begins_with():
    value = object
    column = Column(Integer)
    condition = column.begins_with(value)

    assert isinstance(condition, BeginsWith)
    assert condition.column is column
    assert condition.value == value


def test_contains():
    value = object
    column = Column(Integer)
    condition = column.contains(value)

    assert isinstance(condition, Contains)
    assert condition.column is column
    assert condition.value == value


def test_dynamo_name():
    """ Returns model name unless dynamo name is specified """
    column = Column(Integer)
    # Normally set when a class is defined
    column.model_name = "foo"
    assert column.dynamo_name == "foo"

    column = Column(Integer, name="foo")
    column.model_name = "bar"
    assert column.dynamo_name == "foo"


def test_column_path():
    """ Paths can be iteratively built up, with strings or ints as keys """
    column = Column(Integer)

    comparison = column["foo"]
    assert comparison.path == ["foo"]

    comparison = column["f"]["o"]["o"]["b"]["a"]["r"]
    assert comparison.path == list("foobar")

    with pytest.raises(ValueError):
        column[object()]
