import pytest
from bloop.conditions import (
    NewComparisonMixin,
    NewCondition,
    iter_conditions
)
from bloop.exceptions import InvalidComparisonOperator


class MockColumn(NewComparisonMixin):
    """Has a repr for testing condition reprs"""
    def __init__(self, name):
        self.name = name
        super().__init__()

    def __repr__(self):
        return self.name
column = MockColumn


def conditions_for(*operations):
    return [NewCondition(operation) for operation in operations]


def non_meta_conditions():
    return conditions_for(
        "begins_with", "between", "contains", "in",
        ">", "<", ">=", "<=", "==", "!="
    )


def meta_conditions():
    return conditions_for("and", "or", "not")


def empty_conditions():
    return [NewCondition.empty(), *meta_conditions()]


# NEW CONDITION ======================================================================================== NEW CONDITION


def test_unknown_operator():
    with pytest.raises(InvalidComparisonOperator):
        NewCondition(operation="unknown")


def test_none_operator_allowed():
    assert NewCondition.empty().operation is None


@pytest.mark.parametrize("condition", [
    NewCondition.empty(),
    *meta_conditions()
])
def test_len_empty(condition):
    assert len(condition) == 0


@pytest.mark.parametrize("condition", non_meta_conditions())
def test_len_non_empty(condition):
    assert len(condition) == 1


@pytest.mark.parametrize("condition", non_meta_conditions())
def test_iter_non_meta(condition):
    """These conditions aren't and/or/not, so they can't yield any inner conditions"""
    assert next(iter_conditions(condition), None) is None


@pytest.mark.parametrize("condition", meta_conditions())
def test_iter_non_cyclic_meta(condition):
    """Yield the single inner condition for each of these meta conditions"""
    inner = NewCondition("==")
    condition.values.append(inner)

    expected = [inner]
    actual = list(iter_conditions(condition))
    assert actual == expected


def test_iter_cyclic():
    """Cyclic conditions can be iterated safely"""
    # Here's the structure to create:
    #   root
    #  /    \
    # a      b
    #      /   \
    #     c   root
    root = NewCondition("and")
    a = NewCondition("<")
    b = NewCondition("or")
    c = NewCondition(">")
    root.values.extend([a, b])
    b.values.extend([c, root])

    expected = {root, a, b, c}
    actual = set(iter_conditions(root))
    assert actual == expected


@pytest.mark.parametrize("condition", non_meta_conditions())
def test_len_non_meta(condition):
    """Non-meta conditions *must* have exactly 1 condition"""
    assert len(condition) == 1


@pytest.mark.parametrize("condition", meta_conditions())
def test_len_meta(condition):
    """Meta conditions can have 0, 1, or n conditions"""
    assert len(condition) == 0

    # add a single inner condition
    condition.values.append(NewCondition(">"))
    assert len(condition) == 1

    condition.values.extend(NewCondition("<") for _ in range(30))
    if condition.operation == "not":
        assert len(condition) == 1
    else:
        assert len(condition) == 31


def test_len_cyclic():
    """Cyclic conditions count the cyclic reference"""
    # Here's the structure to create:
    #   root
    #  /    \
    # a      b
    #      /   \
    #     c   root
    root = NewCondition("and")
    a = NewCondition("<")
    b = NewCondition("or")
    c = NewCondition(">")
    root.values.extend([a, b])
    b.values.extend([c, root])

    assert len(root) == 4


def test_len_unpack_not():
    """Even though not(not(x)) -> x shouldn't exist, its length should be 1"""
    condition = NewCondition("not")
    outer = NewCondition("not")
    inner = NewCondition("begins_with")
    condition.values.append(outer)
    outer.values.append(inner)
    assert len(outer) == 1


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
    empty = NewCondition.empty()
    assert (~empty) is empty


def test_invert_simplifies():
    """~~x -> x"""
    condition = NewCondition(">")
    assert (~~condition) is condition


def test_invert_empty_not():
    """~not() -> ()"""
    condition = NewCondition("not")
    assert (~condition).operation is None


# NEW CONDITION AND/IAND ====================================================================== NEW CONDITION AND/IAND


@pytest.mark.parametrize("empty", empty_conditions())
def test_and_empty_conditions(empty):
    """When conditions are falsey (literal empty or meta with no inner value), simplify instead of nesting:
    ()_1 & ()_2 -> ()_1
    x & () -> x
    () & x -> x
    """
    also_empty = NewCondition.empty()
    not_empty = NewCondition(">")

    assert (empty & not_empty) is not_empty
    assert (not_empty & empty) is not_empty
    assert (empty & also_empty) is empty
    assert (also_empty & empty) is also_empty


def test_and_both_and():
    """(a & b) & (c & d) -> (a & b & c & d)"""
    a, b, c, d = [NewCondition(">") for _ in range(4)]
    left = NewCondition("and", [a, b])
    right = NewCondition("and", [c, d])

    assert (left & right).operation == "and"

    assert (left & right).values == [a, b, c, d]
    assert (right & left).values == [c, d, a, b]


@pytest.mark.parametrize("other", non_meta_conditions())
def test_and_simplifies(other):
    """When only one condition is an and, the other is put in a new and, in the correct place
    (a & b) & (c > 2) -> (a & b & (c > 2))
    (a > 2) & (b & c) -> ((a > 2) & b & c)
    """
    a, b, = [NewCondition(">"), NewCondition("<")]
    and_condition = NewCondition("and", [a, b])

    assert (and_condition & other).operation == "and"

    assert (and_condition & other).values == [a, b, other]
    assert (other & and_condition).values == [other, a, b]


def test_and_basic():
    a = NewCondition(">")
    b = NewCondition("<")

    assert (a & b).operation == "and"
    assert (a & b).values == [a, b]
    assert (b & a).values == [b, a]


@pytest.mark.parametrize("empty", empty_conditions())
def test_iand_empty_conditions(empty):
    """Similar to and, empty values don't change the non-empty values.  LHS always wins if both empty."""
    also_empty = NewCondition.empty()
    not_empty = NewCondition(">")

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
    a, b, c, d = [NewCondition(">") for _ in range(4)]
    left = NewCondition("and", [a, b])
    right = NewCondition("and", [c, d])

    original_left = left
    left &= right
    assert left is original_left
    assert left.values == [a, b, c, d]
    assert right.values == [c, d]


@pytest.mark.parametrize("other", non_meta_conditions())
def test_iand_simplifies(other):
    """Similar to and, other value is pushed into the and (on LHS) or front of a new and (on RHS)"""
    a, b, = [NewCondition(">"), NewCondition("<")]
    and_condition = NewCondition("and", [a, b])

    original_other = other
    other &= and_condition
    assert other is not original_other
    assert other.values == [original_other, a, b]

    original_and_condition = and_condition
    and_condition &= original_other
    assert and_condition is original_and_condition
    assert and_condition.values == [a, b, original_other]


def test_iand_basic():
    a = NewCondition(">")
    b = NewCondition("<")

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


# NEW CONDITION OR/IOR ========================================================================== NEW CONDITION OR/IOR


@pytest.mark.parametrize("empty", empty_conditions())
def test_or_empty_conditions(empty):
    """When conditions are falsey (literal empty or meta with no inner value), simplify instead of nesting:
    ()_1 | ()_2 -> ()_1
    x | () -> x
    () | x -> x
    """
    also_empty = NewCondition.empty()
    not_empty = NewCondition(">")

    assert (empty | not_empty) is not_empty
    assert (not_empty | empty) is not_empty
    assert (empty | also_empty) is empty
    assert (also_empty | empty) is also_empty


def test_or_both_or():
    """(a | b) | (c | d) -> (a | b | c | d)"""
    a, b, c, d = [NewCondition(">") for _ in range(4)]
    left = NewCondition("or", [a, b])
    right = NewCondition("or", [c, d])

    assert (left | right).operation == "or"

    assert (left | right).values == [a, b, c, d]
    assert (right | left).values == [c, d, a, b]


@pytest.mark.parametrize("other", non_meta_conditions())
def test_or_simplifies(other):
    """When only one condition is an or, the other is put in a new or, in the correct place
    (a | b) | (c > 2) -> (a | b | (c > 2))
    (a > 2) | (b | c) -> ((a > 2) | b | c)
    """
    a, b, = [NewCondition(">"), NewCondition("<")]
    or_condition = NewCondition("or", [a, b])

    assert (or_condition | other).operation == "or"

    assert (or_condition | other).values == [a, b, other]
    assert (other | or_condition).values == [other, a, b]


def test_or_basic():
    a = NewCondition(">")
    b = NewCondition("<")

    assert (a | b).operation == "or"
    assert (a | b).values == [a, b]
    assert (b | a).values == [b, a]


@pytest.mark.parametrize("empty", empty_conditions())
def test_ior_empty_conditions(empty):
    """Similar to or, empty values don't change the non-empty values.  LHS always wins if both empty."""
    also_empty = NewCondition.empty()
    not_empty = NewCondition(">")

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
    a, b, c, d = [NewCondition(">") for _ in range(4)]
    left = NewCondition("or", [a, b])
    right = NewCondition("or", [c, d])

    original_left = left
    left |= right
    assert left is original_left
    assert left.values == [a, b, c, d]
    assert right.values == [c, d]


@pytest.mark.parametrize("other", non_meta_conditions())
def test_ior_simplifies(other):
    """Similar to or, other value is pushed into the or (on LHS) or front of a new or (on RHS)"""
    a, b, = [NewCondition(">"), NewCondition("<")]
    or_condition = NewCondition("or", [a, b])

    original_other = other
    other |= or_condition
    assert other is not original_other
    assert other.values == [original_other, a, b]

    original_or_condition = or_condition
    or_condition |= original_other
    assert or_condition is original_or_condition
    assert or_condition.values == [a, b, original_other]


def test_ior_basic():
    a = NewCondition(">")
    b = NewCondition("<")

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

# END NEW CONDITION ================================================================================ END NEW CONDITION
