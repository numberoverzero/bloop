# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ \
#   Expressions.SpecifyingConditions.html#ConditionExpressionReference.Syntax
from typing import Optional, List

from .exceptions import InvalidComparisonOperator, InvalidCondition
from .util import WeakDefaultDictionary, printable_column_name, signal


__all__ = ["Condition", "object_deleted", "object_loaded", "object_modified", "object_saved", "render"]


comparison_aliases = {
    "==": "=",
    "!=": "<>",
    "<": "<",
    ">": ">",
    "<=": "<=",
    ">=": ">=",
}
comparisons = list(comparison_aliases.keys())

allowed_operations = {
    *comparisons,
    "and",
    "begins_with",
    "between",
    "contains",
    "in",
    "not",
    "or",
    None
}


# CONDITION TRACKING ============================================================================== CONDITION TRACKING


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
        _obj_tracking[obj].pop("snapshot", None)

    @object_loaded.connect
    def on_object_loaded(engine, obj, **kwargs):
        sync(obj, engine)

    @object_modified.connect
    def on_object_modified(_, obj, column, **kwargs):
        # Mark a column for a given object as being modified in any way.
        # Any marked columns will be pushed (possibly as DELETES) in
        # future UpdateItem calls that include the object.
        _obj_tracking[obj]["marked"].add(column)

    @object_saved.connect
    def on_object_saved(engine, obj, **kwargs):
        sync(obj, engine)


def sync(obj, engine):
    """Mark the object as having been persisted at least once.

    Store the latest snapshot of all marked values."""
    snapshot = NewBaseCondition.empty()
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
    snapshot = _obj_tracking[obj].get("snapshot", None)
    if snapshot is not None:
        return snapshot

    # If the object has never been synced, create and cache
    # a condition that expects every column to be empty
    snapshot = NewBaseCondition.empty()
    for column in sorted(obj.Meta.columns, key=lambda col: col.dynamo_name):
        snapshot &= column.is_(None)
    _obj_tracking[obj]["snapshot"] = snapshot
    return snapshot


def get_marked(obj):
    """Returns the set of marked columns for an object"""
    return set(_obj_tracking[obj]["marked"])


# END CONDITION TRACKING ====================================================================== END CONDITION TRACKING


class NewBaseCondition:
    def __init__(self, operation, *, column=None, path=None, values=None):
        self.operation = operation
        self.column = column
        self.values = list(values or [])
        self.path = list(path or [])
        if operation not in allowed_operations:
            raise InvalidComparisonOperator("Unknown operation {!r}".format(operation))

    @classmethod
    def empty(cls):
        return NewBaseCondition(None)

    def __len__(self):
        if not self.operation:
            return 0
        elif self.operation in ("and", "or"):
            return sum(1 for _ in iter_conditions(self))
        elif self.operation == "not":
            # Guard against a not without a value
            return bool(self.values) and len(self.values[0])
        else:
            return 1

    def __invert__(self):
        if not self.operation:
            return self
        if self.operation == "not":
            if not self.values:
                return NewBaseCondition.empty()
            # Cancel the negation
            return self.values[0]
        # return not(self)
        return NewBaseCondition("not", values=[self])

    __neg__ = __invert__

    def __and__(self, other):
        # ()_1 & ()_2 -> ()_1
        # or
        # (a > 2) & () -> (a > 2)
        if not other:
            return self
        # () & (b < 3) -> (b < 3)
        elif not self:
            return other
        # (a & b) & (c & d) -> (a & b & c & d)
        elif self.operation == other.operation == "and":
            return NewBaseCondition("and", values=self.values + other.values)
        # (a & b) & (c > 2) -> (a & b & (c > 2))
        elif self.operation == "and":
            return NewBaseCondition("and", values=self.values + [other])
        # (a > 2) & (b & c) -> ((a > 2) & b & c)
        elif other.operation == "and":
            return NewBaseCondition("and", values=[self] + other.values)
        # (a > 2) & (b < 3) -> ((a > 2) & (b < 3))
        else:
            return NewBaseCondition("and", values=[self, other])

    def __iand__(self, other):
        # x &= () -> x
        if not other:
            return self
        # () &= x -> x
        elif not self:
            return other
        # (a & b) &= (c & d) -> (a & b & c & d)
        elif self.operation == "and" and other.operation == "and":
            self.values.extend(other.values)
            return self
        # (a & b) &= (c > 2) -> (a & b & (c > 2))
        elif self.operation == "and":
            self.values.append(other)
            return self
        # (a > 2) &= (c & d) -> ((a > 2) & c & d)
        elif other.operation == "and":
            return NewBaseCondition("and", values=[self] + other.values)
        # (a > 2) &= (b < 3) -> ((a > 2) & (b < 3))
        else:
            return NewBaseCondition("and", values=[self, other])

    def __or__(self, other):
        # ()_1 | ()_2 -> ()_1
        # or
        # (a > 2) | () -> (a > 2)
        if not other:
            return self
        # () | (b < 3) -> (b < 3)
        elif not self:
            return other
        # (a | b) | (c | d) -> (a | b | c | d)
        elif self.operation == other.operation == "or":
            return NewBaseCondition("or", values=self.values + other.values)
        # (a | b) | (c > 2) -> (a | b | (c > 2))
        elif self.operation == "or":
            return NewBaseCondition("or", values=self.values + [other])
        # (a > 2) | (b | c) -> ((a > 2) | b | c)
        elif other.operation == "or":
            return NewBaseCondition("or", values=[self] + other.values)
        # (a > 2) | (b < 3) -> ((a > 2) | (b < 3))
        else:
            return NewBaseCondition("or", values=[self, other])

    def __ior__(self, other):
        # x |= () -> x
        if not other:
            return self
        # () |= x -> x
        elif not self:
            return other
        # (a | b) |= (c | d) -> (a | b | c | d)
        elif self.operation == "or" and other.operation == "or":
            self.values.extend(other.values)
            return self
        # (a | b) |= (c > 2) -> (a | b | (c > 2))
        elif self.operation == "or":
            self.values.append(other)
            return self
        # (a > 2) |= (c | d) -> ((a > 2) | c | d)
        elif other.operation == "or":
            return NewBaseCondition("or", values=[self] + other.values)
        # (a > 2) |= (b < 3) -> ((a > 2) | (b < 3))
        else:
            return NewBaseCondition("or", values=[self, other])

    def __repr__(self):
        if self.operation in ("and", "or"):
            joiner = " | " if self.operation == "or" else " & "
            if not self.values:
                return "({})".format(joiner)
            elif len(self.values) == 1:
                return "({!r} {})".format(self.values[0], joiner.strip())
            else:
                return "({})".format(joiner.join(repr(c) for c in self.values))
        elif self.operation == "not":
            if not self.values:
                return "(~)"
            else:
                return "(~{!r})".format(self.values[0])
        elif self.operation in comparisons:
            return "({!r} {} {!r})".format(self.column, self.operation, self.values[0])
        elif self.operation in ["begins_with", "contains"]:
            return "{}({!r}, {!r})".format(self.operation, self.column, self.values[0])
        elif self.operation == "between":
            if not self.values:
                return "({!r} between [,])".format(self.column)
            elif len(self.values) == 1:
                return "({!r} between [{!r},])".format(self.column, self.values[0])
            else:
                return "({!r} between [{!r}, {!r}])".format(self.column, self.values[0], self.values[1])
        elif self.operation == "in":
            return "({!r} in {!r})".format(self.column, self.values)
        elif self.operation is None:
            return "()"
        else:
            raise InvalidComparisonOperator("Unknown operation {!r}".format(self.operation))

    def __eq__(self, other):
        if self is other:
            return True
        try:
            if (
                    (self.operation != other.operation) or
                    (self.column is not other.column) or
                    (self.path != other.path)):
                return False
        except AttributeError:
            return False
        # Can't use a straight list == list because
        # values could contain columns, which will break equality.
        # Can't use 'is' either, since it won't work for non-column
        # objects.
        if len(self.values) != len(other.values):
            return False
        for s, o in zip(self.values, other.values):
            # Both NewComparisonMixin, use `is`
            if isinstance(s, NewComparisonMixin) and isinstance(o, NewComparisonMixin):
                if s is not o:
                    return False
            # This would mean only one was a NewComparisonMixin
            elif isinstance(s, NewComparisonMixin) or isinstance(o, NewComparisonMixin):
                return False
            # Neither are NewComparisonMixin, use `==`
            elif s != o:  # pragma: no branch
                return False
        return True

    __hash__ = object.__hash__


class AndCondition(NewBaseCondition):
    def __init__(self, values=None):
        super().__init__("and", values=values)


class OrCondition(NewBaseCondition):
    def __init__(self, values=None):
        super().__init__("or", values=values)


class NotCondition(NewBaseCondition):
    def __init__(self, value=None):
        values = [value] if value is not None else []
        super().__init__("not", values=values)


class ComparisonCondition(NewBaseCondition):
    def __init__(self, operation, column, value, path=None):
        super().__init__(operation=operation, column=column, values=[value], path=path)


class BeginsWithCondition(NewBaseCondition):
    def __init__(self, column, value, path=None):
        super().__init__("begins_with", column=column, values=[value], path=path)


class BetweenCondition(NewBaseCondition):
    def __init__(self, column, lower, upper, path=None):
        super().__init__("between", column=column, values=[lower, upper], path=path)


class ContainsCondition(NewBaseCondition):
    def __init__(self, column, value, path=None):
        super().__init__("contains", column=column, values=[value], path=path)


class InCondition(NewBaseCondition):
    def __init__(self, column, values, path=None):
        super().__init__("in", column=column, values=values, path=path)


class NewComparisonMixin:
    def __init__(self, *args, proxied=None, path=None, **kwargs):
        self.__path = path or []
        self.__proxied = self if proxied is None else proxied
        super().__init__(*args, **kwargs)

    def __repr__(self):
        if type(self.__proxied) is NewComparisonMixin:
            return "<ComparisonMixin>"
        return repr(self.__proxied)

    def __getattr__(self, item):
        if self.__proxied is self:
            raise AttributeError
        return getattr(self.__proxied, item)

    def __getitem__(self, path):
        return NewComparisonMixin(proxied=self.__proxied, path=self.__path + [path])

    def __eq__(self, value):
        return ComparisonCondition(operation="==", column=self.__proxied, value=value, path=self.__path)

    def __ne__(self, value):
        return ComparisonCondition(operation="!=", column=self.__proxied, value=value, path=self.__path)

    def __lt__(self, value):
        return ComparisonCondition(operation="<", column=self.__proxied, value=value, path=self.__path)

    def __gt__(self, value):
        return ComparisonCondition(operation=">", column=self.__proxied, value=value, path=self.__path)

    def __le__(self, value):
        return ComparisonCondition(operation="<=", column=self.__proxied, value=value, path=self.__path)

    def __ge__(self, value):
        return ComparisonCondition(operation=">=", column=self.__proxied, value=value, path=self.__path)

    def begins_with(self, value):
        return BeginsWithCondition(column=self.__proxied, value=value, path=self.__path)

    def between(self, lower, upper):
        return BetweenCondition(column=self.__proxied, lower=lower, upper=upper, path=self.__path)

    def contains(self, value):
        return ContainsCondition(column=self.__proxied, value=value, path=self.__path)

    def in_(self, *values):
        return InCondition(column=self.__proxied, values=values, path=self.__path)

    is_ = __eq__

    is_not = __ne__


def path_for(value: NewComparisonMixin) -> Optional[List[str]]:
    return value.__path


def iter_conditions(condition: NewBaseCondition):
    """Yield all conditions WITHIN the given condition.

    If there are no conditions within this condition (any condition besides and, or, not; or any of those without
    an inner value) then this will not yield those conditions."""
    conditions = set()
    visited = set()
    # Has to be split out, since we don't want to visit the root (for cyclic conditions)
    # but we don't want to yield it (if it's non-cyclic) because this only yields inner conditions
    if condition.operation in {"and", "or"}:
        conditions.update(condition.values)
    if condition.operation == "not":
        conditions.add(condition.values[0])
    while conditions:
        condition = conditions.pop()
        if condition in visited:
            continue
        visited.add(condition)
        yield condition
        if condition.operation in {"and", "or", "not"}:
            conditions.update(condition.values)


def render(engine, filter=None, projection=None, key=None, atomic=None, condition=None, update=None):
    renderer = ConditionRenderer(engine)
    if filter is not None:
        renderer.filter_expression(filter)
    if projection is not None:
        renderer.projection_expression(projection)
    if key is not None:
        renderer.key_expression(key)
    condition = condition or NewBaseCondition.empty()
    if atomic:
        condition &= get_snapshot(atomic)
    if condition:
        renderer.condition_expression(condition)
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

    def any_ref(self, column=None, path=None, value=None):
        """Name ref if value is None.  Value ref if value is provided; value may be a column with path"""
        raise NotImplemented

    def pop_refs(self, *ref):
        """Decrement the usage of each ref by 1.

        If the count is 0, pop the ref from attr_names or attr_values"""
        raise NotImplemented

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


# TODO ========================================================================================================== TODO


def render_condition(condition: NewBaseCondition, renderer: ConditionRenderer) -> Optional[str]:
    if condition.operation in {"and", "or"}:
        render_func = render_and_or
    elif condition.operation == "not":
        render_func = render_not
    elif condition in comparisons:
        render_func = render_comparison
    elif condition.operation == "begins_with":
        render_func = render_begins_with
    elif condition.operation == "between":
        render_func = render_between
    elif condition.operation == "contains":
        render_func = render_contains
    elif condition.operation == "in":
        render_func = render_in
    elif condition.operation is None:
        render_func = lambda c, r: None
    else:
        raise InvalidComparisonOperator("Unknown operation {!r}".format(condition.operation))
    return render_func(condition, renderer)


def render_and_or(condition: NewBaseCondition, renderer: ConditionRenderer) -> Optional[str]:
    rendered_conditions = [render_condition(c, renderer) for c in condition.values]
    if not rendered_conditions:
        raise InvalidCondition("Invalid Condition: <{!r}> does not contain any Conditions.".format(condition))
    if len(rendered_conditions) == 1:
        return rendered_conditions[0]
    # " AND " " OR "
    joiner = " {} ".format(condition.operation.upper())
    return "({})".format(joiner.join(rendered_conditions))


def render_not(condition: NewBaseCondition, renderer: ConditionRenderer) -> Optional[str]:
    if not condition.values:
        raise InvalidCondition("Invalid Condition: 'not' condition does not contain an inner Condition.")
    rendered_condition = render_condition(condition.values[0], renderer)
    if rendered_condition is None:
        raise InvalidCondition("Invalid Condition: 'not' condition does not contain an inner Condition.")
    return "(NOT {})".format(rendered_condition)


def render_comparison(condition: NewBaseCondition, renderer: ConditionRenderer) -> Optional[str]:
    if not condition.column or not condition.values:
        raise InvalidCondition("Comparison <{!r}> is missing column or value.".format(condition))
    # (column name > value)
    # (#n1 = :v2)
    # (#n0 >= #n1)
    column_ref = renderer.any_ref(column=condition.column, path=condition.path)
    value_ref, value = renderer.any_ref(column=condition.column, path=condition.path, value=condition.values[0])
    # Could be attribute_exists/attribute_not_exists
    if value is None and condition.operation in ("==", "!="):
        # Won't be sending this value, so pop it from the renderer
        renderer.pop_refs(value_ref)
        if condition.operation == "==":
            return "(attribute_not_exists({}))".format(column_ref)
        else:
            return "(attribute_exists({}))".format(column_ref)
    # No other comparison can be against None
    elif value is None:
        # Try to revert the renderer to a valid state
        renderer.pop_refs(column_ref, value_ref)
        raise InvalidCondition("Comparison <{!r}> is against the value None.".format(condition))
    else:
        return "({} {} {})".format(column_ref, comparison_aliases[condition.operation], value_ref)


def render_begins_with(condition: NewBaseCondition, renderer: ConditionRenderer) -> Optional[str]:
    if not condition.column or not condition.values:
        raise InvalidCondition("Condition <{!r}> is missing column or value.".format(condition))
    column_ref = renderer.any_ref(column=condition.column, path=condition.path)
    value_ref, value = renderer.any_ref(column=condition.column, path=condition.path, value=condition.values[0])
    if value is None:
        # Try to revert the renderer to a valid state
        renderer.pop_refs(column_ref, value_ref)
        raise InvalidCondition("Condition <{!r}> is against the value None.".format(condition))
    return "(begins_with({}, {}))".format(column_ref, value_ref)


def render_between(condition: NewBaseCondition, renderer: ConditionRenderer) -> Optional[str]:
    if not condition.column or not condition.values:
        raise InvalidCondition("Condition <{!r}> is missing column or value".format(condition))
    column_ref = renderer.any_ref(column=condition.column, path=condition.path)
    lower_ref, lower_value = renderer.any_ref(
        column=condition.column, path=condition.path, value=condition.values[0])
    upper_ref, upper_value = renderer.any_ref(
        column=condition.column, path=condition.path, value=condition.values[1])
    if lower_value is None or upper_value is None:
        # Try to revert the renderer to a valid state
        renderer.pop_refs(column_ref, lower_ref, upper_ref)
        raise InvalidCondition("Condition <{!r}> includes the value None".format(condition))
    return "({} BETWEEN {} AND {})".format(column_ref, lower_ref, upper_ref)


def render_contains(condition: NewBaseCondition, renderer: ConditionRenderer) -> Optional[str]:
    if not condition.column or not condition.values:
        raise InvalidCondition("Condition <{!r}> is missing column or value.".format(condition))
    column_ref = renderer.any_ref(column=condition.column, path=condition.path)
    value_ref, value = renderer.any_ref(column=condition.column, path=condition.path, value=condition.values[0])
    if value is None:
        # Try to revert the renderer to a valid state
        renderer.pop_refs(column_ref, value_ref)
        raise InvalidCondition("Condition <{!r}> is against the value None.".format(condition))
    return "(contains({}, {}))".format(column_ref, value_ref)


def render_in(condition: NewBaseCondition, renderer: ConditionRenderer) -> Optional[str]:
    if not condition.column or not condition.values:
        raise InvalidCondition("Condition <{!r}> is missing column or value".format(condition))
    value_refs = []
    for value in condition.values:
        value_ref, value = renderer.any_ref(column=condition.column, path=condition.path, value=value)
        value_refs.append(value_ref)
        if value is None:
            renderer.pop_refs(*value_refs)
            raise InvalidCondition("Condition <{!r}> includes the value None.".format(condition))
    column_ref = renderer.any_ref(column=condition.column, path=condition.path)
    return "({} IN ({}))".format(column_ref, ", ".join(value_refs))


# TODO Rewrite to use iter_conditions for condition refactor
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


# TODO REMOVE BELOW HERE ====================================================================== TODO REMOVE BELOW HERE


class BaseCondition:
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


class Condition(BaseCondition):
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
    __hash__ = BaseCondition.__hash__

    def render(self, renderer):
        return None


class _MultiCondition(BaseCondition):
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
    __hash__ = BaseCondition.__hash__

    def render(self, renderer):
        if len(self.conditions) == 1:
            return self.conditions[0].render(renderer)
        rendered_conditions = (c.render(renderer) for c in self.conditions)
        # (foo AND bar AND baz)
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


class Not(BaseCondition):
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
    __hash__ = BaseCondition.__hash__

    def __invert__(self):
        return self.condition

    def render(self, renderer):
        return "(NOT {})".format(self.condition.render(renderer))


class Comparison(BaseCondition):

    def __init__(self, column, operator, value, path=None):
        if operator not in comparison_aliases:
            raise InvalidComparisonOperator(
                "{!r} is not a valid Comparison operator.".format(operator))
        self.column = column
        self.comparator = operator
        self.value = value
        self.path = path

    def __repr__(self):
        return "({}.{} {} {!r})".format(
            self.column.model.__name__, printable_column_name(self.column, self.path),
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
    __hash__ = BaseCondition.__hash__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        vref = renderer.value_ref(self.column, self.value,
                                  dumped=self.dumped, path=self.path)
        # TODO special handling for == and != when value dumps to None
        comparator = comparison_aliases[self.comparator]
        return "({} {} {})".format(nref, comparator, vref)


class AttributeExists(BaseCondition):
    def __init__(self, column, negate, path=None):
        self.column = column
        self.negate = negate
        self.path = path

    def __repr__(self):
        return "({}exists {}.{})".format(
            "not_" if self.negate else "",
            self.column.model.__name__, printable_column_name(self.column, self.path))

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
    __hash__ = BaseCondition.__hash__

    def render(self, renderer):
        name = "attribute_not_exists" if self.negate else "attribute_exists"
        nref = renderer.name_ref(self.column, path=self.path)
        return "({}({}))".format(name, nref)


class BeginsWith(BaseCondition):
    def __init__(self, column, value, path=None):
        self.column = column
        self.value = value
        self.path = path

    def __repr__(self):
        return "({}.{} begins with {!r})".format(
            self.column.model.__name__, printable_column_name(self.column, self.path),
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
    __hash__ = BaseCondition.__hash__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        vref = renderer.value_ref(self.column, self.value,
                                  dumped=self.dumped, path=self.path)
        return "(begins_with({}, {}))".format(nref, vref)


class Contains(BaseCondition):
    def __init__(self, column, value, path=None):
        self.column = column
        self.value = value
        self.path = path

    def __repr__(self):
        return "({}.{} contains {!r})".format(
            self.column.model.__name__, printable_column_name(self.column, self.path),
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
    __hash__ = BaseCondition.__hash__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        vref = renderer.value_ref(self.column, self.value,
                                  dumped=self.dumped, path=self.path)
        return "(contains({}, {}))".format(nref, vref)


class Between(BaseCondition):
    def __init__(self, column, lower, upper, path=None):
        self.column = column
        self.lower = lower
        self.upper = upper
        self.path = path

    def __repr__(self):
        return "({}.{} between [{!r}, {!r}])".format(
            self.column.model.__name__, printable_column_name(self.column, self.path),
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
    __hash__ = BaseCondition.__hash__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        vref_lower = renderer.value_ref(self.column, self.lower,
                                        dumped=self.dumped, path=self.path)
        vref_upper = renderer.value_ref(self.column, self.upper,
                                        dumped=self.dumped, path=self.path)
        return "({} BETWEEN {} AND {})".format(
            nref, vref_lower, vref_upper)


class In(BaseCondition):
    def __init__(self, column, values, path=None):
        self.column = column
        self.values = values
        self.path = path

    def __repr__(self):
        return "({}.{} in {!r})".format(
            self.column.model.__name__, printable_column_name(self.column, self.path),
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
    __hash__ = BaseCondition.__hash__

    def render(self, renderer):
        nref = renderer.name_ref(self.column, path=self.path)
        values = []
        for value in self.values:
            rendered_value = renderer.value_ref(
                self.column, value, dumped=self.dumped, path=self.path)
            values.append(rendered_value)
        values = ", ".join(values)
        return "({} IN ({}))".format(nref, values)


class ComparisonMixin:
    def __init__(self, *, path=None, obj=None, **kwargs):
        self.path = path or []
        # By default the object points to itself; subclasses and recursive
        # structures (for instance, __getitem__) can specify the original
        # object to maintain constant time access to the underlying object.
        self.__obj = obj or self
        super().__init__(**kwargs)

    def __hash__(self):
        # With single inheritance this looks stupid, but as a Mixin this
        # ensures we kick hashing back to the other base class so things
        # don't get fucked up, like `set()`.

        # While the docs recommend using `__hash__ = some_parent.__hash__`,
        # that won't work here - we don't know the parent when the mixin is
        # defined.
        # https://docs.python.org/3.1/reference/datamodel.html#object.__hash__
        return super().__hash__()

    def __eq__(self, value):
        # Special case - None should use function attribute_not_exists
        if value is None:
            return AttributeExists(self.__obj, negate=True, path=self.path)
        return Comparison(self.__obj, "==", value, path=self.path)
    is_ = __eq__

    def __ne__(self, value):
        # Special case - None should use function attribute_exists
        if value is None:
            return AttributeExists(self.__obj, negate=False, path=self.path)
        return Comparison(self.__obj, "!=", value, path=self.path)
    is_not = __ne__

    def __lt__(self, value):
        return Comparison(self.__obj, "<", value, path=self.path)

    def __gt__(self, value):
        return Comparison(self.__obj, ">", value, path=self.path)

    def __le__(self, value):
        return Comparison(self.__obj, "<=", value, path=self.path)

    def __ge__(self, value):
        return Comparison(self.__obj, ">=", value, path=self.path)

    def between(self, lower, upper):
        """ lower <= column.value <= upper """
        return Between(self.__obj, lower, upper, path=self.path)

    def in_(self, values):
        """ column.value in [3, 4, 5] """
        return In(self.__obj, values, path=self.path)

    def begins_with(self, value):
        return BeginsWith(self.__obj, value, path=self.path)

    def contains(self, value):
        return Contains(self.__obj, value, path=self.path)

    def __getitem__(self, path):
        return ComparisonMixin(obj=self.__obj, path=self.path + [path])

    def __repr__(self):
        return self.__obj.__repr__(path=self.path)
