# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ \
#   Expressions.SpecifyingConditions.html#ConditionExpressionReference.Syntax

from .exceptions import InvalidComparisonOperator
from .util import printable_column_name

__all__ = [
    "And", "AttributeExists", "BeginsWith", "Between", "Comparison",
    "Condition", "Contains", "In", "Not", "Or"]

comparison_aliases = {
    "==": "=",
    "!=": "<>",
    "<": "<",
    ">": ">",
    "<=": "<=",
    ">=": ">=",
}


def iter_columns(condition):
    """Yield all columns in the condition; handles nesting and cycles"""
    # Track visited to avoid circular conditions.
    # Who's using a circular condition?!
    conditions = {condition}
    visited = set()
    while conditions:
        condition = conditions.pop()
        if condition in visited:
            continue
        visited.add(condition)
        if isinstance(condition, _MultiCondition):
            conditions.update(condition.conditions)
        elif isinstance(condition, Not):
            conditions.add(condition.condition)
        elif isinstance(condition, Condition):
            continue
        else:  # AttributeExists, BeginsWith, Between, Comparison, Contains, In
            yield condition.column


class _BaseCondition:
    dumped = False

    def __and__(self, other):
        # And with empty condition is the original condition
        if other:
            return And(self, other)
        return self
    __iand__ = __and__

    def __or__(self, other):
        if other:
            return Or(self, other)
        return self
    __ior__ = __or__

    def __invert__(self):
        if self:
            return Not(self)
        return self
    __neg__ = __invert__

    def __len__(self):
        return 1


class Condition(_BaseCondition):
    """Empty condition for iteratively building up conditions.

    Example:
        Constructing an AND condition with 3 sub-conditions::

            condition = Condition()
            for value in [1, 2, 3]:
                condition &= Model.field == value

    """
    def __and__(self, other):
        return other
    __iand__ = __and__

    def __or__(self, other):
        return other
    __ior__ = __or__

    def __invert__(self):
        return self
    __neg__ = __invert__

    def __len__(self):
        return 0

    def __repr__(self):
        return "<empty condition>"

    def __eq__(self, other):
        return isinstance(other, Condition)
    __hash__ = _BaseCondition.__hash__

    def render(self, renderer):
        return None


class _MultiCondition(_BaseCondition):
    name = None
    uname = None

    def __init__(self, *conditions):
        self.conditions = list(conditions)

    def __repr__(self):
        joiner = " | " if self.uname == "OR" else " & "
        conditions = joiner.join(repr(c) for c in self.conditions)
        # Renders as "((condition) | )" to indicate a single-value multi
        if len(self.conditions) == 1:
            return "({} {})".format(conditions, joiner.strip())
        return "({})".format(conditions)

    def __len__(self):
        return sum(map(len, self.conditions))

    def __eq__(self, other):
        if not isinstance(other, _MultiCondition):
            return False
        if self.uname != other.uname:
            return False
        if len(self.conditions) != len(other.conditions):
            return False
        for mine, theirs in zip(self.conditions, other.conditions):
            if mine != theirs:
                return False
        return True
    __hash__ = _BaseCondition.__hash__

    def render(self, renderer):
        if len(self.conditions) == 1:
            return self.conditions[0].render(renderer)
        rendered_conditions = (c.render(renderer) for c in self.conditions)
        conjunction = " {} ".format(self.uname)
        return "(" + conjunction.join(rendered_conditions) + ")"


class And(_MultiCondition):
    name = "And"
    uname = "AND"

    def __and__(self, other):
        if other:
            self.conditions.append(other)
        return self
    __iand__ = __and__


class Or(_MultiCondition):
    name = "Or"
    uname = "OR"

    def __or__(self, other):
        if other:
            self.conditions.append(other)
        return self
    __ior__ = __or__


class Not(_BaseCondition):
    def __init__(self, condition):
        self.condition = condition

    def __repr__(self):
        return "(~{!r})".format(self.condition)

    def __len__(self):
        return len(self.condition)

    def __eq__(self, other):
        if not isinstance(other, Not):
            return False
        return self.condition == other.condition
    __hash__ = _BaseCondition.__hash__

    def __invert__(self):
        return self.condition

    def render(self, renderer):
        return "(NOT {})".format(self.condition.render(renderer))


class Comparison(_BaseCondition):

    def __init__(self, column, operator, value, path=None):
        if operator not in comparison_aliases:
            raise InvalidComparisonOperator(
                "{!r} is not a valid Comparison operator.".format(operator))
        self.column = column
        self.comparator = operator
        self.value = value
        self.path = path

    def __repr__(self):
        return "({} {} {!r})".format(
            printable_column_name(self.column, self.path),
            self.comparator,
            self.value)

    def __eq__(self, other):
        if not isinstance(other, Comparison):
            return False
        # Special-case because we can't use == on a column
        if self.column is not other.column:
            return False
        for attr in ["comparator", "value", "path"]:
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True
    __hash__ = _BaseCondition.__hash__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        vref = renderer.value_ref(self.column, self.value,
                                  dumped=self.dumped, path=self.path)
        # TODO special handling for == and != when value dumps to None
        comparator = comparison_aliases[self.comparator]
        return "({} {} {})".format(nref, comparator, vref)


class AttributeExists(_BaseCondition):
    def __init__(self, column, negate, path=None):
        self.column = column
        self.negate = negate
        self.path = path

    def __repr__(self):
        return "({}exists {})".format(
            "not_" if self.negate else "",
            printable_column_name(self.column, self.path))

    def __eq__(self, other):
        if not isinstance(other, AttributeExists):
            return False
        # Special-case because we can't use == on a column
        if self.column is not other.column:
            return False
        for attr in ["negate", "path"]:
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True
    __hash__ = _BaseCondition.__hash__

    def render(self, renderer):
        name = "attribute_not_exists" if self.negate else "attribute_exists"
        nref = renderer.name_ref(self.column, path=self.path)
        return "({}({}))".format(name, nref)


class BeginsWith(_BaseCondition):
    def __init__(self, column, value, path=None):
        self.column = column
        self.value = value
        self.path = path

    def __repr__(self):
        return "({} begins with {!r})".format(
            printable_column_name(self.column, self.path),
            self.value)

    def __eq__(self, other):
        if not isinstance(other, BeginsWith):
            return False
        # Special-case because we can't use == on a column
        if self.column is not other.column:
            return False
        for attr in ["value", "path"]:
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True
    __hash__ = _BaseCondition.__hash__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        vref = renderer.value_ref(self.column, self.value,
                                  dumped=self.dumped, path=self.path)
        return "(begins_with({}, {}))".format(nref, vref)


class Contains(_BaseCondition):
    def __init__(self, column, value, path=None):
        self.column = column
        self.value = value
        self.path = path

    def __repr__(self):
        return "({} contains {!r})".format(
            printable_column_name(self.column, self.path),
            self.value)

    def __eq__(self, other):
        if not isinstance(other, Contains):
            return False
        # Special-case because we can't use == on a column
        if self.column is not other.column:
            return False
        for attr in ["value", "path"]:
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True
    __hash__ = _BaseCondition.__hash__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        vref = renderer.value_ref(self.column, self.value,
                                  dumped=self.dumped, path=self.path)
        return "(contains({}, {}))".format(nref, vref)


class Between(_BaseCondition):
    def __init__(self, column, lower, upper, path=None):
        self.column = column
        self.lower = lower
        self.upper = upper
        self.path = path

    def __repr__(self):
        return "({} between [{!r}, {!r}])".format(
            printable_column_name(self.column, self.path),
            self.lower, self.upper)

    def __eq__(self, other):
        if not isinstance(other, Between):
            return False
        # Special-case because we can't use == on a column
        if self.column is not other.column:
            return False
        for attr in ["lower", "upper", "path"]:
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True
    __hash__ = _BaseCondition.__hash__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        vref_lower = renderer.value_ref(self.column, self.lower,
                                        dumped=self.dumped, path=self.path)
        vref_upper = renderer.value_ref(self.column, self.upper,
                                        dumped=self.dumped, path=self.path)
        return "({} BETWEEN {} AND {})".format(
            nref, vref_lower, vref_upper)


class In(_BaseCondition):
    def __init__(self, column, values, path=None):
        self.column = column
        self.values = values
        self.path = path

    def __repr__(self):
        return "({} in {!r})".format(
            printable_column_name(self.column, self.path),
            self.values)

    def __eq__(self, other):
        if not isinstance(other, In):
            return False
        # Special-case because we can't use == on a column
        if self.column is not other.column:
            return False
        for attr in ["values", "path"]:
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True
    __hash__ = _BaseCondition.__hash__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        values = []
        for value in self.values:
            rendered_value = renderer.value_ref(
                self.column, value, dumped=self.dumped, path=self.path)
            values.append(rendered_value)
        values = ", ".join(values)
        return "({} IN ({}))".format(nref, values)
