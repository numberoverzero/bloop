import operator

import pytest
from bloop.condition import AttributeExists, BeginsWith, Between, Contains, In
from bloop.models import Column
from bloop.types import Integer

from ..helpers.models import User


operations = [
    (operator.ne, "!="),
    (operator.eq, "=="),
    (operator.lt, "<"),
    (operator.le, "<="),
    (operator.gt, ">"),
    (operator.ge, ">=")
]


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


@pytest.mark.parametrize("op_func, op_name", operations, ids=repr)
def test_comparison(op_func, op_name):
    column = Column(Integer)
    value = object()

    condition = op_func(column, value)
    assert condition.comparator == op_name
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


def test_repr():
    column = Column(Integer, name="f")
    column.model = User
    column.model_name = "foo"
    assert repr(column) == "<Column[User.foo]>"

    column.hash_key = True
    assert repr(column) == "<Column[User.foo=hash]>"

    column.hash_key = False
    column.range_key = True
    assert repr(column) == "<Column[User.foo=range]>"


def test_repr_path():
    column = Column(Integer, name="f")
    column.model = User
    column.model_name = "foo"

    assert repr(column[3]["foo"]["bar"][2][1]) == "<Column[User.foo[3].foo.bar[2][1]]>"

    column.hash_key = True
    assert repr(column[3]["foo"]["bar"][2][1]) == "<Column[User.foo[3].foo.bar[2][1]=hash]>"
