# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ \
#   Expressions.SpecifyingConditions.html#ConditionExpressionReference.Syntax

from .exceptions import InvalidComparisonOperator
from .util import WeakDefaultDictionary, printable_column_name, signal


__all__ = [
    "And", "AttributeExists", "BeginsWith", "Between", "Comparison",
    "Condition", "Contains", "In", "Not", "Or", "render"]

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
        if other:
            # This lets And handle folding if other is also an And
            return And(self) & other
        return self

    __iand__ = __and__

    def __or__(self, other):
        if other:
            # This lets Or handle folding if other is also an Or
            return Or(self) | other
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
            if isinstance(other, And):
                return And(*self.conditions, *other.conditions)
            return And(*self.conditions, other)
        return self

    __iand__ = __and__


class Or(_MultiCondition):
    name = "Or"
    uname = "OR"

    def __or__(self, other):
        if other:
            if isinstance(other, Or):
                return Or(*self.conditions, *other.conditions)
            return Or(*self.conditions, other)
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


# Tracks the state of instances of models:
# 1) Are any columns marked for including in an update?
# 2) Latest snapshot for atomic operations
_obj_tracking = WeakDefaultDictionary(lambda: {"marked": set(), "snapshot": None})


# Watched signals
object_loaded = signal("object_loaded")
object_saved = signal("object_saved")
object_deleted = signal("object_deleted")
object_modified = signal("object_modified")

# Ensure signals aren't connected twice
__signals_connected = False
if not __signals_connected:  # pragma: no branch
    __signals_connected = True

    @object_deleted.connect
    def on_object_deleted(_, obj, **kwargs):
        clear(obj)

    @object_loaded.connect
    def on_object_loaded(engine, obj, **kwargs):
        sync(obj, engine)

    @object_modified.connect
    def on_object_modified(_, obj, column, **kwargs):
        mark(obj, column)

    @object_saved.connect
    def on_object_saved(engine, obj, **kwargs):
        sync(obj, engine)


def clear(obj):
    """Store a snapshot of an entirely empty object.

    Usually called after deleting an object.
    """
    snapshot = Condition()
    for column in sorted(obj.Meta.columns, key=lambda col: col.dynamo_name):
        snapshot &= column.is_(None)
    _obj_tracking[obj]["snapshot"] = snapshot


def mark(obj, column):
    """
    Mark a column for a given object as being modified in any way.
    Any marked columns will be pushed (possibly as DELETES) in
    future UpdateItem calls that include the object.
    """
    _obj_tracking[obj]["marked"].add(column)


def sync(obj, engine):
    """Mark the object as having been persisted at least once.

    Store the latest snapshot of all marked values."""
    snapshot = Condition()
    # Only expect values (or lack of a value) for columns that have been explicitly set
    for column in sorted(_obj_tracking[obj]["marked"], key=lambda col: col.dynamo_name):
        value = getattr(obj, column.model_name, None)
        value = engine._dump(column.typedef, value)
        condition = column == value
        # The renderer shouldn't try to dump the value again.
        # We're dumping immediately in case the value is mutable,
        # such as a set or (many) custom data types.
        condition.dumped = True
        snapshot &= condition
    _obj_tracking[obj]["snapshot"] = snapshot


def get_snapshot(obj):
    # Cached value
    condition = _obj_tracking[obj]["snapshot"]
    if condition is not None:
        return condition

    # If the object has never been synced, create and cache
    # a condition that expects every column to be empty
    clear(obj)
    return _obj_tracking[obj]["snapshot"]


def get_marked(obj):
    """Returns the set of marked columns for an object"""
    return set(_obj_tracking[obj]["marked"])


def render(engine, filter=None, select=None, key=None, atomic=None, condition=None, update=None):
    renderer = ConditionRenderer(engine)
    if filter is not None:
        renderer.filter_expression(filter)
    if select is not None:
        renderer.projection_expression(select)
    if key is not None:
        renderer.key_expression(key)
    if condition and atomic:
        condition_expression = condition & get_snapshot(atomic)
    elif atomic:
        condition_expression = get_snapshot(atomic)
    elif condition:
        condition_expression = condition
    else:
        condition_expression = None
    renderer.condition_expression(condition_expression)
    renderer.update_expression(update)
    return renderer.rendered


class ConditionRenderer:
    def __init__(self, engine):
        self.engine = engine
        self.expressions = {}
        self.attr_values = {}
        self.attr_names = {}
        # Reverse index names so we can re-use ExpressionAttributeNames.
        # We don't do the same for ExpressionAttributeValues since they are
        # dicts of {"TYPE": "VALUE"} and would take more space and time to use
        # as keys, as well as less frequently being re-used than names.
        self.name_attr_index = {}
        self.__ref_index = 0

    def _name_ref(self, name):
        # Small optimization to request size for duplicate name refs
        existing_ref = self.name_attr_index.get(name, None)
        if existing_ref:
            return existing_ref

        ref = "#n{}".format(self.__ref_index)
        self.__ref_index += 1
        self.attr_names[ref] = name
        self.name_attr_index[name] = ref
        return ref

    def name_ref(self, column, path=None):
        pieces = [column.dynamo_name]
        pieces.extend(path or [])
        str_pieces = []
        for piece in pieces:
            # List indexes are attached to last path item directly
            if isinstance(piece, int):
                str_pieces[-1] += "[{}]".format(piece)
            # Path keys are attached with a "."
            else:
                str_pieces.append(self._name_ref(piece))
        return ".".join(str_pieces)

    def value_ref(self, column, value, *, dumped=False, path=None):
        """
        Dumped controls whether the value is already in a dynamo format (True),
        or needs to be dumped through the engine (False).
        """
        ref = ":v{}".format(self.__ref_index)
        self.__ref_index += 1

        if not dumped:
            typedef = column.typedef
            for segment in (path or []):
                typedef = typedef[segment]
            value = self.engine._dump(typedef, value)

        self.attr_values[ref] = value
        return ref

    def condition_expression(self, condition):
        if not condition:
            return
        self.expressions["ConditionExpression"] = condition.render(self)

    def filter_expression(self, condition):
        self.expressions["FilterExpression"] = condition.render(self)

    def key_expression(self, condition):
        self.expressions["KeyConditionExpression"] = condition.render(self)

    def projection_expression(self, columns):
        self.expressions["ProjectionExpression"] = ", ".join(map(self.name_ref, columns))

    def update_expression(self, obj):
        if obj is None:
            return
        updates = {
            "set": [],
            "remove": []}
        for column in sorted(
                # Don't include key columns in an UpdateExpression
                filter(lambda c: c not in obj.Meta.keys, get_marked(obj)),
                key=lambda c: c.dynamo_name):
            nref = self.name_ref(column)
            value = getattr(obj, column.model_name, None)
            value = self.engine._dump(column.typedef, value)

            if value is not None:
                vref = self.value_ref(column, value, dumped=True)
                updates["set"].append("{}={}".format(nref, vref))
            else:
                updates["remove"].append(nref)

        expression = ""
        if updates["set"]:
            expression += "SET " + ", ".join(updates["set"])
        if updates["remove"]:
            expression += " REMOVE " + ", ".join(updates["remove"])
        if expression:
            self.expressions["UpdateExpression"] = expression.strip()

    @property
    def rendered(self):
        expressions = {k: v for (k, v) in self.expressions.items() if v is not None}
        if self.attr_names:
            expressions["ExpressionAttributeNames"] = self.attr_names
        if self.attr_values:
            expressions["ExpressionAttributeValues"] = self.attr_values
        return expressions
