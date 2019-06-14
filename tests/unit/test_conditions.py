import logging
import operator

import pytest

from bloop import actions
from bloop.conditions import (
    AndCondition,
    BaseCondition,
    BeginsWithCondition,
    BetweenCondition,
    ComparisonCondition,
    ComparisonMixin,
    Condition,
    ConditionRenderer,
    ContainsCondition,
    InCondition,
    InvalidCondition,
    NotCondition,
    OrCondition,
    Proxy,
    Reference,
    ReferenceTracker,
    global_tracking,
    iter_columns,
    iter_conditions,
    printable_name,
    render,
)
from bloop.models import BaseModel, Column
from bloop.signals import object_deleted, object_loaded, object_saved
from bloop.types import Binary, Boolean, Integer, List, Map, Set, String

from ..helpers.models import Document, User, VectorModel


class MockColumn(Column):
    """model, name, dynamo_name, __repr__"""
    def __init__(self, name):
        super().__init__(String(), dynamo_name="d_" + name)
        self._name = name

        # Mock model so this can render as M.name
        self.model = type("M", tuple(), {})


c = MockColumn("c")
d = MockColumn("d")


def condition_for(operation, column=None):
    return conditions_for(operation, column=column)[0]


def conditions_for(*operations, column=None):
    column = column or MockColumn("c")
    value = 0
    values = [1, 2]
    conditions = []
    if None in operations:
        conditions.append(Condition())
    if "and" in operations:
        left = ComparisonCondition("==", column, value)
        right = ComparisonCondition("!=", column, value)
        conditions.append(AndCondition(left, right))
    if "or" in operations:
        left = ComparisonCondition("==", column, value)
        right = ComparisonCondition("!=", column, value)
        conditions.append(OrCondition(left, right))
    if "not" in operations:
        inner = ComparisonCondition("==", column, value)
        conditions.append(NotCondition(inner))
    if "begins_with" in operations:
        conditions.append(BeginsWithCondition(column, value))
    if "between" in operations:
        conditions.append(BetweenCondition(column, *values))
    if "contains" in operations:
        conditions.append(ContainsCondition(column, value))
    if "in" in operations:
        conditions.append(InCondition(column, values))
    for operation in ("<", "<=", ">", ">=", "!=", "=="):
        if operation in operations:
            conditions.append(ComparisonCondition(operation, column, value))
    return conditions


def non_meta_conditions(column=None):
    return conditions_for(
        "begins_with", "between", "contains", "in",
        ">", "<", ">=", "<=", "==", "!=",
        column=column
    )


def meta_conditions(column=None):
    return conditions_for("and", "or", "not", column=column)


def empty_conditions():
    return [Condition(), AndCondition(), OrCondition(), NotCondition(Condition())]


@pytest.fixture
def reference_tracker(engine):
    return ReferenceTracker(engine)


@pytest.fixture
def renderer(engine):
    return ConditionRenderer(engine)


# TRACKING SIGNALS ================================================================================== TRACKING SIGNALS


# Columns are sorted by model name
empty_user_condition = (
    User.age.is_(None) &
    User.email.is_(None) &
    User.id.is_(None) &
    User.joined.is_(None) &
    User.name.is_(None)
)


def test_on_deleted(engine):
    """When an object is deleted, the snapshot expects all columns to be empty"""
    user = User(age=3, name="foo")
    object_deleted.send(engine, engine=engine, obj=user)
    assert global_tracking.get_snapshot(user) == empty_user_condition

    # It doesn't matter if the object had non-empty values saved from a previous sync
    object_saved.send(engine, engine=engine, obj=user)
    assert global_tracking.get_snapshot(user) == (
        User.age.is_({"N": "3"}) &
        User.name.is_({"S": "foo"})
    )

    # The deleted signal still clears everything
    object_deleted.send(engine, engine=engine, obj=user)
    assert global_tracking.get_snapshot(user) == empty_user_condition

    # But the current values aren't replaced
    assert user.age == 3
    assert user.name == "foo"


def test_on_loaded_partial(engine):
    """When an object is loaded, the state after loading is snapshotted for future atomic calls"""
    # Creating an instance doesn't snapshot anything
    user = User(age=3, name="foo")
    assert global_tracking.get_snapshot(user) == empty_user_condition

    # Pretend the user was just loaded.  Because only
    # age and name are marked, they will be the only
    # columns included in the snapshot.  A normal load
    # would set the other values to None, and the
    # snapshot would expect those.
    object_loaded.send(engine, engine=engine, obj=user)

    # Values are stored dumped.  Since the dumped flag isn't checked as
    # part of equality testing, we can simply construct the dumped
    # representations to compare.
    assert global_tracking.get_snapshot(user) == (
        User.age.is_({"N": "3"}) &
        User.name.is_({"S": "foo"})
    )


def test_on_loaded_full(engine):
    """Same as the partial test, but with explicit Nones to simulate a real engine.load"""
    user = User(age=3, email=None, id=None, joined=None, name="foo")
    object_loaded.send(engine, engine=engine, obj=user)
    assert global_tracking.get_snapshot(user) == (
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
    assert global_tracking.get_marked(user) == set()

    user.id = "foo"
    assert global_tracking.get_marked(user) == {User.id}

    # Deleting the value does not clear it from the set of marked columns
    del user.id
    assert global_tracking.get_marked(user) == {User.id}

    # Even when the delete fails, the column is marked.
    # We're tracking intention, not state change.
    with pytest.raises(AttributeError):
        del user.age
    assert global_tracking.get_marked(user) == {User.id, User.age}


def test_on_saved(engine):
    """Saving is equivalent to loading w.r.t. tracking.

    The state after saving is snapshotted for future atomic operations."""
    user = User(name="foo", age=3)
    object_saved.send(engine, engine=engine, obj=user)

    # Since "name" and "age" were the only marked columns saved to DynamoDB,
    # they are the only columns that must match for an atomic save.  The
    # state of the other columns wasn't specified, so it's not safe to
    # assume the intended value (missing vs empty)
    assert global_tracking.get_snapshot(user) == (
        User.age.is_({"N": "3"}) &
        User.name.is_({"S": "foo"})
    )


# END TRACKING SIGNALS ========================================================================== END TRACKING SIGNALS


# REFERENCE TRACKER ================================================================================ REFERENCE TRACKER


def test_ref_index_always_increments(reference_tracker):
    """Don't risk forgetting to increment it - ALWAYS increment after getting."""
    assert reference_tracker.next_index == 0
    assert reference_tracker.next_index == 1


def test_ref_same_name(reference_tracker):
    """Don't create two references for the same name string"""
    name = "foo"
    expected_ref = "#n0"

    ref = reference_tracker._name_ref(name)
    same_ref = reference_tracker._name_ref(name)

    assert ref == same_ref == expected_ref
    assert reference_tracker.attr_names[ref] == name
    assert reference_tracker.name_attr_index[name] == ref
    assert reference_tracker.counts[ref] == 2


def test_ref_path_empty(reference_tracker):
    """Path reference without a path (column only) is just a name ref"""
    column = MockColumn("column")
    expected_name = "d_column"
    expected_ref = "#n0"

    ref = reference_tracker._path_ref(column)

    assert ref == expected_ref
    assert reference_tracker.attr_names[ref] == expected_name
    assert reference_tracker.name_attr_index[expected_name] == ref
    assert reference_tracker.counts[ref] == 1


def test_ref_path_complex(reference_tracker):
    """Path reference with integer and string indexes.  Strings include duplicates and literal periods."""
    column = MockColumn("column")["foo"][3][4]["repeat"]["has.period"]["repeat"]
    expected_ref = "#n0.#n1[3][4].#n2.#n3.#n2"
    expected_names = {
        "#n0": "d_column",
        "#n1": "foo",
        "#n2": "repeat",
        "#n3": "has.period"
    }

    ref = reference_tracker._path_ref(column)

    assert ref == expected_ref
    assert reference_tracker.attr_names == expected_names


def test_ref_path_reuse(reference_tracker):
    """paths are re-used, even across columns"""
    first = MockColumn("first")[3]["foo"]
    second = MockColumn("second")[3]["foo"]

    expected_first = "#n0[3].#n1"
    expected_second = "#n2[3].#n1"
    expected_names = {
        "#n0": "d_first",
        "#n1": "foo",
        "#n2": "d_second"
    }

    first_ref = reference_tracker._path_ref(first)
    second_ref = reference_tracker._path_ref(second)
    assert first_ref == expected_first
    assert second_ref == expected_second
    assert reference_tracker.attr_names == expected_names


def test_ref_path_periods(reference_tracker):
    """Path segments with periods aren't de-duped with each individual segment"""
    column = MockColumn("column")["foo"]["foo.bar"]["bar"]
    expected_ref = "#n0.#n1.#n2.#n3"
    expected_names = {
        "#n0": "d_column",
        "#n1": "foo",
        "#n2": "foo.bar",
        "#n3": "bar",
    }

    ref = reference_tracker._path_ref(column)

    assert ref == expected_ref
    assert reference_tracker.attr_names == expected_names


def test_ref_value(reference_tracker):
    """no path, value not dumped"""
    column = User.age
    value = 3
    expected_ref = ":v0"
    expected_value = {"N": "3"}
    expected_values = {":v0": expected_value}

    ref, value = reference_tracker._value_ref(column, value)

    assert ref == expected_ref
    assert value == expected_value
    assert reference_tracker.attr_values == expected_values


def test_ref_value_path(reference_tracker):
    """has path, value not dumped"""
    column = Document.data["Description"]["Body"]
    value = "value"
    expected_ref = ":v0"
    expected_value = {"S": value}
    expected_values = {":v0": expected_value}

    ref, value = reference_tracker._value_ref(column, value)

    assert ref == expected_ref
    assert value == expected_value
    assert reference_tracker.attr_values == expected_values


def test_ref_value_dumped(reference_tracker):
    """no path, value already dumped"""
    column = Document.id
    # This shouldn't be dumped, so we use an impossible value for the type
    dumped_value = object()
    expected_ref = ":v0"
    expected_values = {":v0": dumped_value}

    ref, value = reference_tracker._value_ref(column, dumped_value, dumped=True)

    assert ref == expected_ref
    assert value == dumped_value
    assert reference_tracker.attr_values == expected_values


def test_ref_value_dumped_path(reference_tracker):
    """has path, value already dumped"""
    column = Document.data["Description"]
    # Description's typedef is Map, wich can't dump an object
    # This shouldn't be dumped, so we use an impossible value for the type
    dumped_value = object()
    expected_ref = ":v0"
    expected_values = {":v0": dumped_value}

    ref, value = reference_tracker._value_ref(column, dumped_value, dumped=True)

    assert ref == expected_ref
    assert value == dumped_value
    assert reference_tracker.attr_values == expected_values


def test_ref_any_name(reference_tracker):
    """Render a reference to the column name (and path) when there's no value"""
    column = Document.data["Description"]["Body"]
    expected_ref = Reference(name="#n0.#n1.#n2", type="name", value=None)
    expected_names = {
        "#n0": "data",
        "#n1": "Description",
        "#n2": "Body"
    }

    ref = reference_tracker.any_ref(column=column)

    assert ref == expected_ref
    assert reference_tracker.attr_names == expected_names


def test_ref_any_value_is_column(reference_tracker):
    """Render a reference to a value that is also a column"""
    column = Document.id["Description"]["Rating"]

    # value has its own path
    value = Document.data["Description"]["Body"]

    expected_ref = Reference(name="#n0.#n1.#n2", type="name", value=None)
    expected_names = {
        "#n0": "data",
        "#n1": "Description",
        "#n2": "Body"
    }

    ref = reference_tracker.any_ref(column=column, value=value)

    assert ref == expected_ref
    assert reference_tracker.attr_names == expected_names


def test_ref_any_value_not_column(reference_tracker):
    """Render a reference to a regular value"""
    column = Document.id
    value = 3
    expected_value = {"N": "3"}
    expected_ref = Reference(name=":v0", type="value", value=expected_value)
    expected_values = {":v0": expected_value}

    ref = reference_tracker.any_ref(column=column, value=value)

    assert ref == expected_ref
    assert reference_tracker.attr_values == expected_values


def test_ref_pop_none(reference_tracker):
    """pop_refs without args doesn't pop any refs"""
    # Add a name and value ref so we can make sure nothing is cleared
    name = reference_tracker.any_ref(column=Document.id).name
    value = reference_tracker.any_ref(column=Document.id, value=3).name

    reference_tracker.pop_refs()

    assert name in reference_tracker.attr_names
    assert value in reference_tracker.attr_values


def test_ref_pop_unknown(reference_tracker):
    """Popping an unknown ref doesn't do anything"""
    # Add a name and value ref so we can make sure nothing is cleared
    name = reference_tracker.any_ref(column=Document.id).name
    value = reference_tracker.any_ref(column=Document.id, value=3).name

    unknown_name_ref = Reference(name="foo", type="value", value=None)
    unknown_value_ref = Reference(name="bar", type="name", value=None)
    reference_tracker.pop_refs(unknown_name_ref, unknown_value_ref)

    assert name in reference_tracker.attr_names
    assert value in reference_tracker.attr_values


def test_ref_pop_name(reference_tracker):
    """References aren't removed until they're popped as many times as they're used"""
    name_ref = reference_tracker.any_ref(column=Document.id)
    same_name_ref = reference_tracker.any_ref(column=Document.id)
    assert reference_tracker.counts[name_ref.name] == 2

    # Still in attr_names, name_attr_index
    reference_tracker.pop_refs(same_name_ref)
    assert reference_tracker.counts[name_ref.name] == 1
    assert reference_tracker.attr_names[name_ref.name] == "id"
    assert reference_tracker.name_attr_index["id"] == name_ref.name

    # Not in attr_names, name_attr_index
    reference_tracker.pop_refs(same_name_ref)
    assert reference_tracker.counts[name_ref.name] == 0
    assert name_ref.name not in reference_tracker.attr_names
    assert "id" not in reference_tracker.name_attr_index

    # Count doesn't go below 0
    reference_tracker.pop_refs(name_ref)
    assert reference_tracker.counts[name_ref.name] == 0


def test_ref_pop_value(reference_tracker):
    """Same pop test, for values"""
    value_ref = reference_tracker.any_ref(column=Document.id, value=3)
    # Have to fake this out a bit, because there's no de-duping for values
    # This test exists to guard incorrect pop behavior, in case values are
    # ever de-duped.
    reference_tracker.counts[value_ref.name] += 1
    assert reference_tracker.counts[value_ref.name] == 2

    # Still in attr_names, name_attr_index
    reference_tracker.pop_refs(value_ref)
    assert reference_tracker.counts[value_ref.name] == 1
    assert reference_tracker.attr_values[value_ref.name] == {"N": "3"}

    # Not in attr_names, name_attr_index
    reference_tracker.pop_refs(value_ref)
    assert reference_tracker.counts[value_ref.name] == 0
    assert value_ref.name not in reference_tracker.attr_values

    # Count doesn't go below 0
    reference_tracker.pop_refs(value_ref)
    assert reference_tracker.counts[value_ref.name] == 0


# END REFERENCE TRACKER ======================================================================== END REFERENCE TRACKER


# RENDERER ================================================================================================== RENDERER


def test_render_missing_object(engine):
    """Can't render atomic or update without an object"""
    with pytest.raises(InvalidCondition):
        render(engine, update=True)
    with pytest.raises(InvalidCondition):
        render(engine, atomic=True)


@pytest.mark.parametrize("kwarg_name, expression_key", [
    ("filter", "FilterExpression"),
    ("key", "KeyConditionExpression"),
    ("condition", "ConditionExpression"),
])
def test_render_condition_only(kwarg_name, expression_key, engine, caplog):
    """Only renders the given condition"""
    condition = (User.email == "@") & (User.name.is_(None))
    rendered = render(engine, **{kwarg_name: condition})
    assert rendered == {
        "ExpressionAttributeNames": {"#n0": "email", "#n2": "name"},
        "ExpressionAttributeValues": {":v1": {"S": "@"}},
        expression_key: "((#n0 = :v1) AND (attribute_not_exists(#n2)))"
    }

    assert caplog.record_tuples == [
        ("bloop.conditions", logging.DEBUG, "popping last usage of Reference(name=':v3', type='value', value=None)"),
        ("bloop.conditions", logging.DEBUG, "rendering \"==\" as attribute_not_exists"),
    ]


def test_render_projection_only(engine):
    columns = [User.id, User.email, User.id, User.age]
    rendered = render(engine, projection=columns)
    assert rendered == {
        "ExpressionAttributeNames": {"#n0": "id", "#n1": "email", "#n2": "age"},
        "ProjectionExpression": "#n0, #n1, #n2",
    }


def test_render_atomic_only_new(engine):
    """Atomic condition on a new object only -> all attribute_not_exists"""
    rendered = render(engine, obj=User(), atomic=True)
    assert rendered == {
        "ExpressionAttributeNames": {"#n0": "age", "#n2": "email", "#n4": "id", "#n6": "j", "#n8": "name"},
        "ConditionExpression": (
            "((attribute_not_exists(#n0)) AND (attribute_not_exists(#n2)) AND"
            " (attribute_not_exists(#n4)) AND (attribute_not_exists(#n6)) AND"
            " (attribute_not_exists(#n8)))"
        )
    }


def test_render_atomic_only_partial(engine):
    """Atomic condition on an object already partially synced"""
    user = User(id="user_id", age=3, email=None)
    # Sync gives us an atomic condition
    object_saved.send(engine, engine=engine, obj=user)

    # Unlike a new save, this one has no expectation about the values of "joined" or "name"
    rendered = render(engine, obj=user, atomic=True)

    assert rendered == {
        "ExpressionAttributeNames": {"#n0": "age", "#n2": "email", "#n4": "id"},
        "ExpressionAttributeValues": {":v1": {"N": "3"}, ":v5": {"S": "user_id"}},
        "ConditionExpression": "((#n0 = :v1) AND (attribute_not_exists(#n2)) AND (#n4 = :v5))"
    }


def test_render_atomic_and_condition(engine):
    """Atomic condition and condition are ANDed together (condition first)"""
    user = User(id="user_id", age=3, email=None)
    # Sync gives us an atomic condition
    object_saved.send(engine, engine=engine, obj=user)

    # Value ref isn't re-used
    condition = User.email.contains("@")

    rendered = render(engine, obj=user, condition=condition, atomic=True)

    assert rendered == {
        "ExpressionAttributeNames": {"#n0": "email", "#n2": "age", "#n5": "id"},
        "ExpressionAttributeValues": {":v1": {"S": "@"}, ":v3": {"N": "3"}, ":v6": {"S": "user_id"}},
        "ConditionExpression": "((contains(#n0, :v1)) AND (#n2 = :v3) AND (attribute_not_exists(#n0)) AND (#n5 = :v6))"
    }


def test_render_update_only(engine):
    user = User(email="@", age=3)
    rendered = render(engine, obj=user, update=True)
    assert rendered == {
        "ExpressionAttributeNames": {"#n0": "age", "#n2": "email"},
        "ExpressionAttributeValues": {":v1": {"N": "3"}, ":v3": {"S": "@"}},
        "UpdateExpression": "SET #n0=:v1, #n2=:v3",
    }


def test_render_complex(engine):
    """Render a filter condition, key condition, projection, condition, atomic and update"""
    user = User(id="uid", age=3, email=None)
    # Sync gives us an atomic condition on id, age, email (sorted)
    object_saved.send(engine, engine=engine, obj=user)

    filter_condition = User.email.contains("@")
    key_condition = User.age == 4
    # projection isn't sorted by name
    projection = [User.name, User.id]

    condition = User.age <= User.id

    # SET name, REMOVE age
    # (in addition to REMOVE email, from email=None)
    user.name = "bill"
    del user.age

    rendered = render(engine, obj=user,
                      filter=filter_condition, projection=projection, key=key_condition,
                      atomic=True, condition=condition, update=True)

    # Render order: filter, projection, key, (condition & atomic), update
    assert rendered == {
        "ExpressionAttributeNames": {"#n0": "email", "#n2": "name", "#n3": "id", "#n4": "age"},
        "ExpressionAttributeValues": {
            ":v1": {"S": "@"},
            ":v5": {"N": "4"},
            ":v6": {"N": "3"},
            ":v8": {"S": "uid"},
            ":v11": {"S": "bill"}
        },

        "FilterExpression": "(contains(#n0, :v1))",
        "ProjectionExpression": "#n2, #n3",
        "KeyConditionExpression": "(#n4 = :v5)",
        "ConditionExpression": "((#n4 <= #n3) AND (#n4 = :v6) AND (attribute_not_exists(#n0)) AND (#n3 = :v8))",
        "UpdateExpression": "REMOVE #n4, #n0 SET #n2=:v11",
    }


@pytest.mark.parametrize("func_name, expression_key", [
    ("condition_expression", "ConditionExpression"),
    ("filter_expression", "FilterExpression"),
    ("key_expression", "KeyConditionExpression"),
])
def test_render_simple_conditions(func_name, expression_key, renderer):
    """condition, filter, key expression rendering simply defers to the condition"""
    condition = User.name.between("foo", User.age)
    render = getattr(renderer, func_name)
    render(condition)

    assert renderer.output == {
        "ExpressionAttributeNames": {"#n0": "name", "#n2": "age"},
        "ExpressionAttributeValues": {":v1": {"S": "foo"}},
        expression_key: "(#n0 BETWEEN :v1 AND #n2)"
    }


def test_render_projection_dedupes_names(renderer):
    """Duplicate columns are filtered when rendering the projection expression"""
    columns = [User.id, User.email, User.id, User.age]
    renderer.projection_expression(columns)

    assert renderer.output == {
        "ExpressionAttributeNames": {"#n0": "id", "#n1": "email", "#n2": "age"},
        "ProjectionExpression": "#n0, #n1, #n2",
    }


def test_render_update_no_changes(renderer):
    """When there aren't any marked *non-key* columns on an object, there's no update expression"""
    user = User(id="user_id")
    renderer.update_expression(user)
    assert not renderer.output


def test_render_update_set_only(renderer):
    """Only updates are where values were set (none of the values were None or rendered as None)"""
    user = User(email="@", age=3)
    renderer.update_expression(user)
    assert renderer.output == {
        "ExpressionAttributeNames": {"#n0": "age", "#n2": "email"},
        "ExpressionAttributeValues": {":v1": {"N": "3"}, ":v3": {"S": "@"}},
        "UpdateExpression": "SET #n0=:v1, #n2=:v3",
    }


def test_render_update_remove_only(renderer):
    """Only updates were del'd values, values set to None, or values that render as None"""
    document = Document()
    # Renders as None
    document.data = dict()
    # Deleted, even though it wasn't set
    with pytest.raises(AttributeError):
        del document.numbers
    # Explicit None
    document.value = None

    renderer.update_expression(document)
    assert renderer.output == {
        "ExpressionAttributeNames": {"#n0": "data", "#n2": "numbers", "#n4": "value"},
        "UpdateExpression": "REMOVE #n0, #n2, #n4",
    }


def test_render_actions(renderer):
    obj = VectorModel()
    obj.name = "test_obj"

    # ADD only supports number and set types
    obj.some_int = actions.add(2)
    # DELETE only supports set types
    obj.set_str = actions.delete(["d", "e"])
    # REMOVE supports all types
    obj.list_str = actions.remove("remove value is not used")
    # SET supports all types
    obj.some_bytes = actions.set(b"hello")

    renderer.update_expression(obj)
    assert renderer.output == {
        "ExpressionAttributeNames": {"#n0": "list_str", "#n2": "set_str", "#n4": "some_bytes", "#n6": "some_int"},
        "ExpressionAttributeValues": {":v3": {"SS": ["d", "e"]}, ":v5": {"B": "aGVsbG8="}, ":v7": {"N": "2"}},
        "UpdateExpression": "ADD #n6 :v7 DELETE #n2 :v3 REMOVE #n0 SET #n4=:v5"
    }


def test_render_update_set_and_remove(renderer):
    """Some values set, some values removed"""
    document = Document()
    # Renders as None -> removed
    document.data = dict()
    # Deleted, even though it wasn't set
    with pytest.raises(AttributeError):
        del document.numbers
    # Both set
    document.value = 3
    document.another_value = 4

    renderer.update_expression(document)
    # Ordering is alphabetical by model name: another_value, data, numbers, value
    # REMOVE statements will cause a skip in index (because value renders empty and pops the ref)
    assert renderer.output == {
        "ExpressionAttributeNames": {"#n0": "another_value", "#n2": "data", "#n4": "numbers", "#n6": "value"},
        "ExpressionAttributeValues": {":v1": {"N": "4"}, ":v7": {"N": "3"}},
        "UpdateExpression": "REMOVE #n2, #n4 SET #n0=:v1, #n6=:v7",
    }


# END RENDERER ========================================================================================== END RENDERER


# CONDITIONS ============================================================================================== CONDITIONS


def test_abstract_base(renderer):
    """BaseCondition requires 4 methods for subclasses"""
    condition = BaseCondition(None)
    with pytest.raises(NotImplementedError):
        len(condition)
    with pytest.raises(NotImplementedError):
        repr(condition)
    with pytest.raises(NotImplementedError):
        condition.render(renderer)


def test_empty_condition():
    assert Condition().operation is None


@pytest.mark.parametrize("condition", empty_conditions())
def test_len_empty(condition):
    assert len(condition) == 0


def test_iter_empty():
    condition = Condition()
    assert set(iter_conditions(condition)) == {condition}
    assert next(iter_columns(condition), None) is None


def test_render_empty(renderer):
    condition = Condition()
    condition.render(renderer)
    assert not renderer.output


@pytest.mark.parametrize("condition", non_meta_conditions())
def test_len_non_empty(condition):
    assert len(condition) == 1


@pytest.mark.parametrize("condition", non_meta_conditions())
def test_len_non_meta(condition):
    """Non-meta conditions *must* have exactly 1 condition"""
    assert len(condition) == 1


@pytest.mark.parametrize("condition", meta_conditions())
def test_len_meta(condition):
    if condition.operation == "not":
        assert len(condition) == 1
    else:
        assert len(condition) == 2


def test_len_cyclic():
    """Cyclic conditions count the cyclic reference"""
    # Here's the structure to create:
    #   root
    #  /    \
    # a      b
    #      /   \
    #     c   root
    root = AndCondition()
    a = ComparisonCondition("<", MockColumn("a"), 3)
    b = OrCondition()
    c = ComparisonCondition(">", MockColumn("c"), 3)
    root.values.extend([a, b])
    b.values.extend([c, root])

    assert len(root) == 4


def test_len_unpack_not():
    """Even though not(not(x)) -> x shouldn't exist, its length should be the inner length"""
    lt, gt = conditions_for("<", ">")
    outer = NotCondition(lt)
    condition = NotCondition(outer)
    assert len(condition) == len(outer) == 1

    # Swap inner for an AND with length 2
    and_ = AndCondition(lt, gt)
    outer.values[0] = and_
    assert len(condition) == len(outer) == len(and_) == 2


@pytest.mark.parametrize("condition", conditions_for(
    "begins_with", "between", "contains", "in",
    ">", "<", ">=", "<=", "==", "!=",
    "and", "or"))
def test_invert_wraps(condition):
    """everything but not and () are wrapped in a not"""
    wrapped = ~condition
    assert wrapped.operation == "not"
    assert wrapped.values[0] is condition


def test_invert_empty():
    """~() -> ()"""
    empty = Condition()
    assert (~empty) is empty


def test_invert_simplifies():
    """~~x -> x"""
    condition = ComparisonCondition(">", MockColumn("c"), 3)
    assert (~~condition) is condition


def test_invert_empty_not():
    """~not() -> ()"""
    condition = condition_for("not")
    assert (~condition).operation == condition.values[0].operation


# CONDITIONS AND/IAND ============================================================================ CONDITIONS AND/IAND


@pytest.mark.parametrize("empty", empty_conditions())
def test_and_empty_conditions(empty):
    """When conditions are falsey (literal empty or meta with no inner value), simplify instead of nesting:
    ()_1 & ()_2 -> ()_1
    x & () -> x
    () & x -> x
    """
    also_empty = Condition()
    not_empty = condition_for(">")

    assert (empty & not_empty) is not_empty
    assert (not_empty & empty) is not_empty
    assert (empty & also_empty) is empty
    assert (also_empty & empty) is also_empty


def test_and_both_and():
    """(a & b) & (c & d) -> (a & b & c & d)"""
    a, b, c, d = [condition_for(">") for _ in range(4)]
    left = AndCondition(a, b)
    right = AndCondition(c, d)

    assert (left & right).operation == "and"

    assert (left & right).values == [a, b, c, d]
    assert (right & left).values == [c, d, a, b]


@pytest.mark.parametrize("other", non_meta_conditions())
def test_and_simplifies(other):
    """When only one condition is an and, the other is put in a new and, in the correct place
    (a & b) & (c > 2) -> (a & b & (c > 2))
    (a > 2) & (b & c) -> ((a > 2) & b & c)
    """
    a, b, = [condition_for(">"), condition_for("<")]
    and_condition = AndCondition(a, b)

    assert (and_condition & other).operation == "and"

    assert (and_condition & other).values == [a, b, other]
    assert (other & and_condition).values == [other, a, b]


def test_and_basic():
    a = condition_for(">")
    b = condition_for("<")

    assert (a & b).operation == "and"
    assert (a & b).values == [a, b]
    assert (b & a).values == [b, a]


@pytest.mark.parametrize("empty", empty_conditions())
def test_iand_empty_conditions(empty):
    """Similar to and, empty values don't change the non-empty values.  LHS always wins if both empty."""
    also_empty = Condition()
    not_empty = condition_for(">")

    # None of the following modify the object

    original_empty = empty
    empty &= also_empty
    assert empty is original_empty

    original_also_empty = also_empty
    also_empty &= empty
    assert also_empty is original_also_empty

    original_not_empty = not_empty
    not_empty &= empty
    assert not_empty is original_not_empty

    # The only modifying __iand__
    empty &= not_empty
    assert empty is not_empty


def test_iand_both_and():
    """other's conditions are appended to self's conditions"""
    a, b, c, d = [condition_for(">") for _ in range(4)]
    left = AndCondition(a, b)
    right = AndCondition(c, d)

    original_left = left
    left &= right
    assert left is original_left
    assert left.values == [a, b, c, d]
    assert right.values == [c, d]


@pytest.mark.parametrize("other", non_meta_conditions())
def test_iand_simplifies(other):
    """Similar to and, other value is pushed into the and (on LHS) or front of a new and (on RHS)"""
    a, b, = [condition_for(">"), condition_for("<")]
    and_condition = AndCondition(a, b)

    original_other = other
    other &= and_condition
    assert other is not original_other
    assert other.values == [original_other, a, b]

    original_and_condition = and_condition
    and_condition &= original_other
    assert and_condition is original_and_condition
    assert and_condition.values == [a, b, original_other]


def test_iand_basic():
    a = condition_for(">")
    b = condition_for("<")

    original_a = a
    original_b = b

    a &= original_b
    assert a is not original_a
    assert a.operation == "and"
    assert a.values == [original_a, original_b]

    b &= original_a
    assert b is not original_b
    assert b.operation == "and"
    assert b.values == [original_b, original_a]


# CONDITIONS OR/IOR ================================================================================ CONDITIONS OR/IOR


@pytest.mark.parametrize("empty", empty_conditions())
def test_or_empty_conditions(empty):
    """When conditions are falsey (literal empty or meta with no inner value), simplify instead of nesting:
    ()_1 | ()_2 -> ()_1
    x | () -> x
    () | x -> x
    """
    also_empty = Condition()
    not_empty = condition_for(">")

    assert (empty | not_empty) is not_empty
    assert (not_empty | empty) is not_empty
    assert (empty | also_empty) is empty
    assert (also_empty | empty) is also_empty


def test_or_both_or():
    """(a | b) | (c | d) -> (a | b | c | d)"""
    a, b, c, d = [condition_for(">") for _ in range(4)]
    left = OrCondition(a, b)
    right = OrCondition(c, d)

    assert (left | right).operation == "or"

    assert (left | right).values == [a, b, c, d]
    assert (right | left).values == [c, d, a, b]


@pytest.mark.parametrize("other", non_meta_conditions())
def test_or_simplifies(other):
    """When only one condition is an or, the other is put in a new or, in the correct place
    (a | b) | (c > 2) -> (a | b | (c > 2))
    (a > 2) | (b | c) -> ((a > 2) | b | c)
    """
    a, b, = [condition_for(">"), condition_for("<")]
    or_condition = OrCondition(a, b)

    assert (or_condition | other).operation == "or"

    assert (or_condition | other).values == [a, b, other]
    assert (other | or_condition).values == [other, a, b]


def test_or_basic():
    a = condition_for(">")
    b = condition_for("<")

    assert (a | b).operation == "or"
    assert (a | b).values == [a, b]
    assert (b | a).values == [b, a]


@pytest.mark.parametrize("empty", empty_conditions())
def test_ior_empty_conditions(empty):
    """Similar to or, empty values don't change the non-empty values.  LHS always wins if both empty."""
    also_empty = Condition()
    not_empty = condition_for(">")

    # None of the following modify the object

    original_empty = empty
    empty |= also_empty
    assert empty is original_empty

    original_also_empty = also_empty
    also_empty |= empty
    assert also_empty is original_also_empty

    original_not_empty = not_empty
    not_empty |= empty
    assert not_empty is original_not_empty

    # The only modifying __ior__
    empty |= not_empty
    assert empty is not_empty


def test_ior_both_or():
    """other's conditions are appended to self's conditions"""
    a, b, c, d = [condition_for(">") for _ in range(4)]
    left = OrCondition(a, b)
    right = OrCondition(c, d)

    original_left = left
    left |= right
    assert left is original_left
    assert left.values == [a, b, c, d]
    assert right.values == [c, d]


@pytest.mark.parametrize("other", non_meta_conditions())
def test_ior_simplifies(other):
    """Similar to or, other value is pushed into the or (on LHS) or front of a new or (on RHS)"""
    a, b, = [condition_for(">"), condition_for("<")]
    or_condition = OrCondition(a, b)

    original_other = other
    other |= or_condition
    assert other is not original_other
    assert other.values == [original_other, a, b]

    original_or_condition = or_condition
    or_condition |= original_other
    assert or_condition is original_or_condition
    assert or_condition.values == [a, b, original_other]


def test_ior_basic():
    a = condition_for(">")
    b = condition_for("<")

    original_a = a
    original_b = b

    a |= original_b
    assert a is not original_a
    assert a.operation == "or"
    assert a.values == [original_a, original_b]

    b |= original_a
    assert b is not original_b
    assert b.operation == "or"
    assert b.values == [original_b, original_a]


# CONDITIONS REPR ==================================================================================== CONDITIONS REPR


@pytest.mark.parametrize("condition, expected", [
    # and
    (AndCondition(), "( & )"),
    (AndCondition("foo"), "('foo' &)"),
    (AndCondition("a", "b", "c"), "('a' & 'b' & 'c')"),

    # or
    (OrCondition(), "( | )"),
    (OrCondition("foo"), "('foo' |)"),
    (OrCondition("a", "b", "c"), "('a' | 'b' | 'c')"),

    # not
    (NotCondition("a"), "(~'a')"),

    # comparisons
    (ComparisonCondition("<", column=c, value=3), "(M.c < 3)"),
    (ComparisonCondition(">", column=c, value=3), "(M.c > 3)"),
    (ComparisonCondition("<=", column=c, value=3), "(M.c <= 3)"),
    (ComparisonCondition(">=", column=c, value=3), "(M.c >= 3)"),
    (ComparisonCondition("==", column=c, value=3), "(M.c == 3)"),
    (ComparisonCondition("!=", column=c, value=3), "(M.c != 3)"),

    # begins_with, contains
    (BeginsWithCondition(column=c, value=2), "begins_with(M.c, 2)"),
    (ContainsCondition(column=c, value=2), "contains(M.c, 2)"),

    # between
    (BetweenCondition(column=c, lower=2, upper=3), "(M.c between [2, 3])"),

    # in
    (InCondition(column=c, values=[]), "(M.c in [])"),
    (InCondition(column=c, values=[2, 3]), "(M.c in [2, 3])"),
    (InCondition(column=c, values=[MockColumn("d"), 3]), "(M.c in [<MockColumn[M.d]>, 3])"),

    # empty
    (Condition(), "()")
])
def test_repr(condition, expected):
    assert repr(condition) == expected


# CONDITIONS EQUALITY ============================================================================ CONDITIONS EQUALITY


def test_eq_empty():
    empty = Condition()
    assert empty == empty

    also_empty = Condition()
    assert empty is not also_empty
    assert empty == also_empty


def test_eq_wrong_type():
    """AttributeError returns False"""
    assert not (Condition() == object())


@pytest.mark.parametrize("other", [
    BaseCondition("op", values=list("xy"), column=c["wrong"]["path"]),
    BaseCondition("??", values=list("xy"), column=c["foo"]["bar"]),
    BaseCondition("op", values=list("xy"), column=None),
    # Need to attach a path to the wrong proxy object
    BaseCondition("op", values=list("xy"), column=Proxy(obj=None, path=["foo", "bar"])),
    BaseCondition("op", values=list("xyz"), column=c["foo"]["bar"]),
    BaseCondition("op", values=list("yx"), column=c["foo"]["bar"]),
])
def test_eq_one_wrong_field(other):
    """All four of operation, value, column, and path must match"""
    self = BaseCondition("op", values=list("xy"), column=c["foo"]["bar"])
    assert not (self == other)


@pytest.mark.parametrize("other", [
    BaseCondition("op", values=[c]),
    BaseCondition("op", values=["x"]),
    BaseCondition("op", values=[c, c]),
    BaseCondition("op", values=["x", "x"]),
    BaseCondition("op", values=["x", c]),
    BaseCondition("op", values=[d, "x"]),
])
def test_eq_values_mismatch(other):
    condition = BaseCondition("op", values=[c, "x"])
    assert not (condition == other)


# CONDITIONS RENDER ================================================================================ CONDITIONS RENDER


@pytest.mark.parametrize("condition, as_str, expected_names, expected_values", [
    # Comparison - all operations
    (User.age == 3, "(#n0 = :v1)", {"#n0": "age"}, {":v1": {"N": "3"}}),
    (User.age != 3, "(#n0 <> :v1)", {"#n0": "age"}, {":v1": {"N": "3"}}),
    (User.age < 3, "(#n0 < :v1)", {"#n0": "age"}, {":v1": {"N": "3"}}),
    (User.age > 3, "(#n0 > :v1)", {"#n0": "age"}, {":v1": {"N": "3"}}),
    (User.age <= 3, "(#n0 <= :v1)", {"#n0": "age"}, {":v1": {"N": "3"}}),
    (User.age >= 3, "(#n0 >= :v1)", {"#n0": "age"}, {":v1": {"N": "3"}}),

    # Comparison - against None -> attribute_* functions
    (User.age.is_(None), "(attribute_not_exists(#n0))", {"#n0": "age"}, None),
    (User.age.is_not(None), "(attribute_exists(#n0))", {"#n0": "age"}, None),

    # Comparison - against things that become None -> attribute_* functions
    (Document.data == dict(), "(attribute_not_exists(#n0))", {"#n0": "data"}, None),
    (Document.data != dict(), "(attribute_exists(#n0))", {"#n0": "data"}, None),

    # Comparison - against another Column
    (User.name == User.email, "(#n0 = #n1)", {"#n0": "name", "#n1": "email"}, None),

    # BeginsWith - against value, Column
    (User.name.begins_with("foo"), "(begins_with(#n0, :v1))", {"#n0": "name"}, {":v1": {"S": "foo"}}),
    (User.name.begins_with(User.email), "(begins_with(#n0, #n1))", {"#n0": "name", "#n1": "email"}, None),

    # Between - against value, Column
    (User.age.between(3, 4), "(#n0 BETWEEN :v1 AND :v2)", {"#n0": "age"}, {":v1": {"N": "3"}, ":v2": {"N": "4"}}),
    (User.age.between(3, User.age), "(#n0 BETWEEN :v1 AND #n0)", {"#n0": "age"}, {":v1": {"N": "3"}}),
    (User.age.between(User.age, 4), "(#n0 BETWEEN #n0 AND :v1)", {"#n0": "age"}, {":v1": {"N": "4"}}),

    # Contains - against value, Column
    (User.name.contains("foo"), "(contains(#n0, :v1))", {"#n0": "name"}, {":v1": {"S": "foo"}}),
    (User.name.contains(User.email), "(contains(#n0, #n1))", {"#n0": "name", "#n1": "email"}, None),

    # In - mixed values, Column
    (User.age.in_(3, User.age, 4), "(#n1 IN (:v0, #n1, :v2))", {"#n1": "age"}, {":v0": {"N": "3"}, ":v2": {"N": "4"}})
])
def test_render_valid_condition(condition, as_str, expected_names, expected_values, renderer):
    assert condition.render(renderer) == as_str

    if expected_names:
        assert renderer.output["ExpressionAttributeNames"] == expected_names
    else:
        assert "ExpressionAttributeNames" not in renderer.output

    if expected_values:
        assert renderer.output["ExpressionAttributeValues"] == expected_values
    else:
        assert "ExpressionAttributeValues" not in renderer.output


@pytest.mark.parametrize("condition", [
    # Value is None
    User.age < None,
    User.age > None,
    User.age <= None,
    User.age >= None,
    User.email.begins_with(None),
    # At least one None
    User.age.between(3, None),
    User.age.between(None, 4),
    User.age.between(None, None),
    User.email.contains(None),
    # No values
    User.age.in_(),
    # At least one None
    User.age.in_(None, 4),
    User.age.in_(3, None),
    User.age.in_(None, None),
    # Not literal None, but becomes None
    Document.nested_numbers.contains([]),

    # Empty meta conditions
    AndCondition(),
    OrCondition()
])
def test_render_invalid_condition(condition, renderer):
    """After a condition fails to render, all of its name and value refs should be popped."""
    with pytest.raises(InvalidCondition):
        condition.render(renderer)
    assert not renderer.output


def test_render_nested_meta_condition(renderer):
    """Test meta conditions AND, OR, NOT"""
    has_name = User.name.is_not(None)
    is_foo = User.name == "foo"
    is_3 = User.age != 3
    is_email_address = User.email.contains("@")

    # There's no ref with '1' because the first equality condition (is_not) renders a value ref, and then pops it.
    expected = "(((attribute_exists(#n0)) AND (#n0 = :v2)) OR (NOT (#n3 <> :v4)) OR (contains(#n5, :v6)))"
    expected_names = {"#n0": "name", "#n3": "age", "#n5": "email"}
    expected_values = {":v2": {"S": "foo"}, ":v4": {"N": "3"}, ":v6": {"S": "@"}}

    condition = (has_name & is_foo) | (~is_3) | is_email_address
    assert condition.render(renderer) == expected
    assert renderer.output == {
        "ExpressionAttributeNames": expected_names,
        "ExpressionAttributeValues": expected_values
    }


@pytest.mark.parametrize("condition_cls", [AndCondition, OrCondition])
def test_render_and_or_simplify(condition_cls, renderer):
    """When AND/OR have exactly one condition, they only render that condition (without an AND/OR)"""
    inner = User.age < 3
    condition = condition_cls(inner)
    expected = "(#n0 < :v1)"

    assert condition.render(renderer) == expected
    assert renderer.output == {
        "ExpressionAttributeNames": {"#n0": "age"},
        "ExpressionAttributeValues": {":v1": {"N": "3"}}
    }


# END CONDITIONS ====================================================================================== END CONDITIONS


# COMPARISON MIXIN ================================================================================== COMPARISON MIXIN


def test_mixin_repr():
    assert repr(ComparisonMixin()) == "<ComparisonMixin>"


def test_mixin_path():
    mixin = ComparisonMixin()
    proxy = mixin["some_attribute"][3]
    assert isinstance(proxy, Proxy)
    assert proxy._obj is mixin
    assert proxy._path == ["some_attribute", 3]


@pytest.mark.parametrize("op, expected", [
    (operator.eq, "=="),
    (operator.ne, "!="),
    (operator.lt, "<"),
    (operator.gt, ">"),
    (operator.le, "<="),
    (operator.ge, ">="),
])
def test_mixin_magic_comparisons(op, expected):
    """==, !=, <, >, <=, >= create condition objects with the corresponding operation"""
    condition = op(c, 3)
    assert condition.operation == expected
    assert condition.column is c
    assert condition.values == [3]


def test_mixin_begins_with():
    condition = c.begins_with(3)
    assert condition.operation == "begins_with"
    assert condition.column is c
    assert condition.values == [3]


def test_mixin_between():
    condition = c.between(3, 4)
    assert condition.operation == "between"
    assert condition.column is c
    assert condition.values == [3, 4]


def test_mixin_contains():
    condition = c.contains(3)
    assert condition.operation == "contains"
    assert condition.column is c
    assert condition.values == [3]


def test_mixin_in_():
    condition = c.in_(3, 4)
    assert condition.operation == "in"
    assert condition.column is c
    assert condition.values == [3, 4]


def test_mixin_is_():
    condition = c.is_(3)
    assert condition.operation == "=="
    assert condition.column is c
    assert condition.values == [3]

    condition = c.is_not(3)
    assert condition.operation == "!="
    assert condition.column is c
    assert condition.values == [3]


@pytest.mark.parametrize("op, typedefs, args", [
    (
        "begins_with",
        [
            Integer(), List(String), Map(s=String), Boolean(),
            Set(Integer), Set(Binary), Set(String)
        ],
        ("one-arg",)
    ),
    (
        "contains",
        [
            Integer(), Boolean(), Map(s=String)
        ],
        ("one-arg",)
    ),
    (
        "between",
        [
            Set(String), Set(Binary), Set(String),
            List(String), Map(s=String), Boolean()
        ],
        ("first-arg", "second-arg")
    )
])
def test_unsupported_mixin_function_conditions(op, typedefs, args):
    class Model(BaseModel):
        id = Column(Integer, hash_key=True)
    for typedef in typedefs:
        column = Column(typedef, dynamo_name="d")
        column.model = Model
        column._name = "c"
        with pytest.raises(InvalidCondition):
            getattr(column, op)(*args)
            column.begins_with(object())


@pytest.mark.parametrize("typedef", [
    Set(Integer), Set(Binary), Set(String),
    List(String), Map(s=String), Boolean()
])
@pytest.mark.parametrize("op", [
    operator.lt,
    operator.gt,
    operator.le,
    operator.ge
])
def test_unsupported_mixin_comparison_conditions(op, typedef):
    class Model(BaseModel):
        id = Column(Integer, hash_key=True)
    column = Column(typedef, dynamo_name="d")
    column.model = Model
    column._name = "c"
    with pytest.raises(InvalidCondition):
        op(column, "value")


def test_printable_column_no_path():
    """Model.column"""
    assert printable_name(User.email) == "email"


def test_printable_column_mixed_path():
    """Model.column[3].foo[1]"""
    assert printable_name(User.id, path=[3, "foo", "bar", 0, 1]) == "id[3].foo.bar[0][1]"


def test_printable_column_included_path():
    """Path is part of the 'column' that's provided"""
    assert printable_name(User.id[3]["foo"]["bar"][0][1]) == "id[3].foo.bar[0][1]"


def test_printable_column_both_paths():
    """When both paths are provided, the explicit path wins"""
    assert printable_name(User.id["not used"], path=[3, "foo", "bar", 0, 1]) == "id[3].foo.bar[0][1]"


# END COMPARISON MIXIN ========================================================================== END COMPARISON MIXIN


# PROXY ======================================================================================================== PROXY


def test_proxy_delegates_getattr():
    sentinel = object()
    column = MockColumn("col")
    column.attribute = sentinel
    proxy = column["some"]["path"]
    assert proxy.attribute is sentinel


def test_proxy_masks_protected_path_attr():
    """If a proxied object has a _path or _obj attribute, it's not returned through the proxy"""
    sentinel = object()
    column = MockColumn("col")
    column._obj = sentinel
    column._path = sentinel
    proxy = column["some"]["path"]
    assert proxy._obj is not column._obj
    assert proxy._path is not column._path


def test_proxy_repr():
    column = MockColumn("col")
    proxy = column["some"][2]["path"]
    assert repr(proxy) == "<Proxy[M.col.some[2].path]>"

# END PROXY ================================================================================================ END PROXY


# ITERATORS ================================================================================================ ITERATORS

@pytest.mark.parametrize("condition", non_meta_conditions())
def test_iter_conditions_non_meta(condition):
    """These conditions aren't and/or/not, so they can't yield any inner conditions"""
    assert set(iter_conditions(condition)) == {condition}


@pytest.mark.parametrize("condition", meta_conditions())
def test_iter_conditions_non_cyclic_meta(condition):
    """Yield the inner conditions for each of these meta conditions"""
    expected = condition.values
    actual = list(iter_conditions(condition))
    assert actual == expected


def test_iter_conditions_cyclic():
    """Cyclic conditions can be iterated safely"""
    # Here's the structure to create:
    #   root
    #  /    \
    # a      b
    #      /   \
    #     c   root
    root = AndCondition()
    a = ComparisonCondition("<", MockColumn("a"), 3)
    b = OrCondition()
    c = ComparisonCondition(">", MockColumn("c"), 3)
    root.values.extend([a, b])
    b.values.extend([c, root])

    expected = {root, a, b, c}
    actual = set(iter_conditions(root))
    assert actual == expected


def test_iter_columns_path():
    path = Document.data["Description"]["Heading"]
    assert set(iter_columns(path.begins_with("hello, world"))) == {Document.data}


@pytest.mark.parametrize("condition", [*non_meta_conditions(column=User.age), *meta_conditions(column=User.age)])
def test_iter_columns_single(condition):
    assert set(iter_columns(condition)) == {User.age}


@pytest.mark.parametrize("condition", empty_conditions())
def test_iter_columns_empty(condition):
    assert not set(iter_columns(condition))


def test_iter_columns_nested():
    """Nested AND, OR, NOT are unpacked"""
    a = User.age == 3
    b = User.name == "foo"
    c = User.email.in_(User.age, User.id, "bar")

    # Here's the structure to create:
    #    __root__
    #   /   |    \
    #  a  branch  \
    #       |    leaf
    #       b    /  \
    #           c  root

    branch = ~b
    root = a & branch
    leaf = c | root
    root.values.append(leaf)

    assert set(iter_columns(root)) == {User.age, User.name, User.email, User.id}


# END ITERATORS ======================================================================================== END ITERATORS
