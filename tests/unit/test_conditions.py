import operator

import pytest
from bloop.conditions import (
    And,
    AttributeExists,
    BeginsWith,
    Between,
    Comparison,
    Condition,
    ConditionRenderer,
    Contains,
    In,
    Not,
    Or,
    get_marked,
    get_snapshot,
    iter_columns,
    object_deleted,
    object_loaded,
    object_saved,
    render
)
from bloop.exceptions import InvalidComparisonOperator
from bloop.models import BaseModel, Column
from bloop.types import Integer

from ..helpers.models import (
    ComplexModel,
    Document,
    DocumentType,
    User,
    conditions,
)

operations = [
    (operator.ne, "!="),
    (operator.eq, "=="),
    (operator.lt, "<"),
    (operator.le, "<="),
    (operator.gt, ">"),
    (operator.ge, ">=")
]

# Columns are sorted by model name
empty_user_condition = (
    User.age.is_(None) &
    User.email.is_(None) &
    User.id.is_(None) &
    User.joined.is_(None) &
    User.name.is_(None)
)


def test_duplicate_name_refs(engine):
    """ name refs are re-used for the same name """
    renderer = ConditionRenderer(engine)
    assert renderer.name_ref(User.age) == renderer.name_ref(User.age) == "#n0"


def test_no_refs(engine):
    """
    when name/value refs are missing, ExpressionAttributeNames/Values
    aren't populated """
    condition = And()
    expected = {}
    assert render(engine, condition=condition) == expected


@pytest.mark.parametrize("op", [operator.and_, operator.or_])
@pytest.mark.parametrize("empty_cls", [Condition, Or, And])
def test_basic_simplification(op, empty_cls):
    condition = Comparison(User.name, "==", "foo")
    same = op(condition, empty_cls())
    assert same is condition


@pytest.mark.parametrize("cls", [Condition, Or, And])
def test_negate_empty_conditions(cls):
    empty = cls()
    assert ~empty is empty


@pytest.mark.parametrize("cls, op", [(And, operator.and_), (Or, operator.or_)])
@pytest.mark.parametrize("empty_cls", [Condition, Or, And])
def test_shortcut_multi_appends(cls, op, empty_cls):
    # And() & None -> same And
    # Or() & None -> same Or
    obj = cls()
    same = op(obj, empty_cls())
    assert same is obj


def test_double_negate():
    condition = Comparison(User.name, "==", "foo")
    assert ~~condition is condition


def test_condition_ops():
    age, name = (User.age >= 3), (User.name == "foo")

    assert age & name == And(age, name)
    assert age | name == Or(age, name)
    assert ~age == Not(age)


def test_condition_len():
    age, name = (User.age >= 3), (User.name == "foo")
    and_condition = age & name
    or_condition = And(age, name, age)
    not_condition = ~age

    assert len(or_condition) == 3
    assert len(and_condition) == 2
    assert len(age) == len(name) == len(not_condition) == 1


def test_multi_shortcut():
    """ And or Or with single conditions render as their sole condition """
    age = User.age >= 3
    condition = And(age)
    assert condition.conditions == [age]

    condition = Or(age)
    assert condition.conditions == [age]


def test_multi_chains_flatten():
    """
    ((condition & condition) & condition) flattens the AND into one condition
    """
    age = User.age >= 3
    name = User.name == "foo"
    email = User.email != "bar"

    # Flatten left -> right:  (a & b) & c -> a & b & c
    and_condition = Condition()
    or_condition = Condition()
    for c in [age, name, email]:
        and_condition &= c
        or_condition |= c
    assert and_condition == And(age, name, email)
    assert or_condition == Or(age, name, email)

    # Flatten right -> left:  a & (b & c) -> a & b & c
    and_condition = Condition()
    or_condition = Condition()
    for c in [age, name, email]:
        and_condition = c & and_condition
        or_condition = c | or_condition
    assert and_condition == And(email, name, age)
    assert or_condition == Or(email, name, age)


def test_not(engine):
    age = ~(User.age >= 3)
    condition = And(age)
    expected = {
        "ConditionExpression": "(NOT (#n0 >= :v1))",
        "ExpressionAttributeNames": {"#n0": "age"},
        "ExpressionAttributeValues": {":v1": {"N": "3"}}}
    assert render(engine, condition=condition) == expected


def test_invalid_comparator():
    with pytest.raises(InvalidComparisonOperator):
        Comparison(User.age, "not-a-comparator", 5)


def test_attribute_exists(engine):
    condition = User.age.is_not(None)
    expected = {
        "ConditionExpression": "(attribute_exists(#n0))",
        "ExpressionAttributeNames": {"#n0": "age"}}
    assert render(engine, condition=condition) == expected


def test_attribute_not_exists(engine):
    condition = User.age.is_(None)
    expected = {
        "ConditionExpression": "(attribute_not_exists(#n0))",
        "ExpressionAttributeNames": {"#n0": "age"}}
    assert render(engine, condition=condition) == expected


def test_begins_with(engine):
    condition = User.name.begins_with("foo")
    expected = {
        "ConditionExpression": "(begins_with(#n0, :v1))",
        "ExpressionAttributeNames": {"#n0": "name"},
        "ExpressionAttributeValues": {":v1": {"S": "foo"}}}
    assert render(engine, condition=condition) == expected


def test_contains(engine):
    condition = User.name.contains("foo")
    expected = {
        "ConditionExpression": "(contains(#n0, :v1))",
        "ExpressionAttributeNames": {"#n0": "name"},
        "ExpressionAttributeValues": {":v1": {"S": "foo"}}}
    assert render(engine, condition=condition) == expected


def test_between(engine):
    condition = User.name.between("bar", "foo")
    expected = {
        "ConditionExpression": "(#n0 BETWEEN :v1 AND :v2)",
        "ExpressionAttributeNames": {"#n0": "name"},
        "ExpressionAttributeValues": {":v1": {"S": "bar"}, ":v2": {"S": "foo"}}}
    assert render(engine, condition=condition) == expected


def test_in(engine):
    condition = User.name.in_(["bar", "foo"])
    expected = {
        "ConditionExpression": "(#n0 IN (:v1, :v2))",
        "ExpressionAttributeNames": {"#n0": "name"},
        "ExpressionAttributeValues": {":v1": {"S": "bar"}, ":v2": {"S": "foo"}}}
    assert render(engine, condition=condition) == expected


def test_base_condition(engine):
    """ (Condition() OP condition) is condition """
    base = Condition()
    other = User.email == "foo"

    assert (base & other) is other
    assert (base | other) is other
    assert (~base) is base
    assert len(base) == 0

    assert base.render(object()) is None

    assert not render(engine, condition=base)


def test_render_path(engine):
    """ A path should be rendered as #column.#field.#field """
    renderer = ConditionRenderer(engine)
    path = "foo bar baz".split()
    renderer.name_ref(User.email, path=path)
    expected = {'ExpressionAttributeNames': {'#n0': 'email', '#n3': 'baz', '#n2': 'bar', '#n1': 'foo'}}
    assert renderer.rendered == expected


def test_path_comparator(engine):
    """ Render paths for operations, comparisons, and multi-conditions """
    rating = Document.data["Rating"] > 0.5
    no_body = Document.data["Description"]["Body"].is_(None)
    stock = Document.data["Stock"].in_([1, 2, 3])
    condition = (rating & no_body) | stock

    expected = {
        'ConditionExpression': (
            '(((#n0.#n1 > :v2) AND (attribute_not_exists(#n0.#n3.#n4))) OR (#n0.#n5 IN (:v6, :v7, :v8)))'),
        'ExpressionAttributeValues': {
            ':v2': {'N': '0.5'}, ':v6': {'N': '1'}, ':v7': {'N': '2'}, ':v8': {'N': '3'}},
        'ExpressionAttributeNames': {
            '#n0': 'data', '#n1': 'Rating', '#n3': 'Description', '#n4': 'Body', '#n5': 'Stock'}}
    assert render(engine, condition=condition) == expected


def test_name_ref_with_path(engine):
    """ Columns with custom names with literal periods render correctly """
    class Model(BaseModel):
        id = Column(Integer, hash_key=True, name='this.is.id')
        data = Column(DocumentType)
    engine.bind(base=Model)

    no_id = Model.id.is_(None)
    path_condition = Model.data["Rating"] >= 2
    condition = no_id & path_condition

    expected = {
        'ConditionExpression': '((attribute_not_exists(#n0)) AND (#n1.#n2 >= :v3))',
        'ExpressionAttributeNames': {'#n0': 'this.is.id', '#n1': 'data', '#n2': 'Rating'},
        'ExpressionAttributeValues': {':v3': {'N': '2'}}}
    assert render(engine, condition=condition) == expected


def test_list_path(engine):
    """ render list indexes correctly """
    condition = Document.numbers[1] >= 3
    expected = {
        'ConditionExpression': '(#n0[1] >= :v1)',
        'ExpressionAttributeNames': {'#n0': 'numbers'},
        'ExpressionAttributeValues': {':v1': {'N': '3'}}}
    assert render(engine, condition=condition) == expected


# If we parametrize conditions x conditions, the test count explode into a
# useless number, so we only parametrize one. This should still make isolating
# failures easier, from O(len(conditions*conditions)) when neither
# is parametrized to O(len(conditions))
@pytest.mark.parametrize("condition", conditions, ids=str)
def test_equality(condition):
    for other in conditions:
        if condition is other:
            assert condition == other
        else:
            assert condition != other


def test_complex_iter_columns():
    """Includes cycles, empty conditions, Not, MultiConditions"""

    first_comp = ComplexModel.name == "foo"
    second_comp = ComplexModel.date == "bar"
    third_comp = ComplexModel.email == "baz"

    negate = Not(third_comp)
    empty = Condition()

    both = first_comp & second_comp
    either = Or(negate, empty)

    # cycle = (
    #   (1 & 2) &
    #   (~ | _) &
    #   cycle
    # )
    cycle = And(both, either)
    cycle.conditions.append(cycle)

    expected = {
        ComplexModel.name,
        ComplexModel.date,
        ComplexModel.email
    }
    assert set(iter_columns(cycle)) == expected


def test_condition_repr():
    assert repr(Condition()) == "<empty condition>"


def test_and_repr():
    empty = Condition()

    assert repr(And(empty)) == "({} &)".format(repr(empty))
    assert repr(And(empty, empty)) == "({0} & {0})".format(repr(empty))


def test_or_repr():
    empty = Condition()

    assert repr(Or(empty)) == "({} |)".format(repr(empty))
    assert repr(Or(empty, empty)) == "({0} | {0})".format(repr(empty))


def test_not_repr():
    empty = Condition()

    assert repr(Not(empty)) == "(~{})".format(repr(empty))


def test_comparison_repr():
    operators = ["==", "!=", "<", ">", "<=", ">="]
    value = "foo"
    column = User.age

    for op in operators:
        assert repr(Comparison(column, op, value)) == "(User.age {} 'foo')".format(op)


def test_attribute_exists_repr():
    column = User.age

    assert repr(AttributeExists(column, False)) == "(exists User.age)"
    assert repr(AttributeExists(column, True)) == "(not_exists User.age)"


def test_begins_with_repr():
    value = "foo"
    column = User.age

    assert repr(BeginsWith(column, value)) == "(User.age begins with 'foo')"


def test_contains_repr():
    value = "foo"
    column = User.age

    assert repr(Contains(column, value)) == "(User.age contains 'foo')"


def test_between_repr():
    lower = "3"
    higher = 3
    column = User.age

    assert repr(Between(column, lower, higher)) == "(User.age between ['3', 3])"


def test_in_repr():
    values = ["foo", 3]
    column = User.age

    assert repr(In(column, values)) == "(User.age in ['foo', 3])"


def test_path_repr():
    column = User.age
    path = ["foo", 3, "bar", "baz", 2, 1]

    condition = Comparison(column, ">", 0, path=path)

    assert repr(condition) == "(User.age.foo[3].bar.baz[2][1] > 0)"


# TRACKING SIGNALS ================================================================================== TRACKING SIGNALS

def test_on_deleted(engine):
    """When an object is deleted, the snapshot expects all columns to be empty"""
    user = User(age=3, name="foo")
    object_deleted.send(engine, obj=user)
    assert get_snapshot(user) == empty_user_condition

    # It doesn't matter if the object had non-empty values saved from a previous sync
    object_saved.send(engine, obj=user)
    assert get_snapshot(user) == (
        User.age.is_({"N": "3"}) &
        User.name.is_({"S": "foo"})
    )

    # The deleted signal still clears everything
    object_deleted.send(engine, obj=user)
    assert get_snapshot(user) == empty_user_condition

    # But the current values aren't replaced
    assert user.age == 3
    assert user.name == "foo"


def test_on_loaded_partial(engine):
    """When an object is loaded, the state after loading is snapshotted for future atomic calls"""
    # Creating an instance doesn't snapshot anything
    user = User(age=3, name="foo")
    assert get_snapshot(user) == empty_user_condition

    # Pretend the user was just loaded.  Because only
    # age and name are marked, they will be the only
    # columns included in the snapshot.  A normal load
    # would set the other values to None, and the
    # snapshot would expect those.
    object_loaded.send(engine, obj=user)

    # Values are stored dumped.  Since the dumped flag isn't checked as
    # part of equality testing, we can simply construct the dumped
    # representations to compare.
    assert get_snapshot(user) == (
        User.age.is_({"N": "3"}) &
        User.name.is_({"S": "foo"})
    )


def test_on_loaded_full(engine):
    """Same as the partial test, but with explicit Nones to simulate a real engine.load"""
    user = User(age=3, email=None, id=None, joined=None, name="foo")
    object_loaded.send(engine, obj=user)
    assert get_snapshot(user) == (
        User.age.is_({"N": "3"}) &
        User.email.is_(None) &
        User.id.is_(None) &
        User.joined.is_(None) &
        User.name.is_({"S": "foo"})
    )


def test_on_modified():
    """When an object's values are set or deleted, those columns are marked for tracking"""

    # Creating an instance doesn't mark anything
    user = User()
    assert get_marked(user) == set()

    user.id = "foo"
    assert get_marked(user) == {User.id}

    # Deleting the value does not clear it from the set of marked columns
    del user.id
    assert get_marked(user) == {User.id}

    # Even when the delete fails, the column is marked.
    # We're tracking intention, not state change.
    with pytest.raises(AttributeError):
        del user.age
    assert get_marked(user) == {User.id, User.age}


def test_on_saved(engine):
    """Saving is equivalent to loading w.r.t. tracking.

    The state after saving is snapshotted for future atomic operations."""
    user = User(name="foo", age=3)
    object_saved.send(engine, obj=user)

    # Since "name" and "age" were the only marked columns saved to DynamoDB,
    # they are the only columns that must match for an atomic save.  The
    # state of the other columns wasn't specified, so it's not safe to
    # assume the intended value (missing vs empty)
    assert get_snapshot(user) == (
        User.age.is_({"N": "3"}) &
        User.name.is_({"S": "foo"})
    )


# END TRACKING SIGNALS ========================================================================== END TRACKING SIGNALS


# COMPARISON MIXIN ================================================================================== COMPARISON MIXIN


def test_column_equals_alias_exists():
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
def test_column_comparison(op_func, op_name):
    column = Column(Integer)
    value = object()

    condition = op_func(column, value)
    assert condition.comparator == op_name
    assert condition.column is column
    assert condition.value is value


def test_column_between():
    lower, upper = object(), object()
    column = Column(Integer)
    condition = column.between(lower, upper)

    assert isinstance(condition, Between)
    assert condition.column is column
    assert condition.lower is lower
    assert condition.upper is upper


def test_column_in():
    values = [object() for _ in range(3)]
    column = Column(Integer)
    condition = column.in_(values)

    assert isinstance(condition, In)
    assert condition.column is column
    assert condition.values == values


def test_column_begins_with():
    value = object
    column = Column(Integer)
    condition = column.begins_with(value)

    assert isinstance(condition, BeginsWith)
    assert condition.column is column
    assert condition.value == value


def test_column_contains():
    value = object
    column = Column(Integer)
    condition = column.contains(value)

    assert isinstance(condition, Contains)
    assert condition.column is column
    assert condition.value == value


# END COMPARISON MIXIN ========================================================================== END COMPARISON MIXIN
