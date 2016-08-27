# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ \
#   Expressions.SpecifyingConditions.html#ConditionExpressionReference.Syntax

from .exceptions import InvalidCondition
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
    snapshot = _obj_tracking[obj].get("snapshot", None)
    if snapshot is not None:
        return snapshot

    # If the object has never been synced, create and cache
    # a condition that expects every column to be empty
    snapshot = Condition()
    for column in sorted(obj.Meta.columns, key=lambda col: col.dynamo_name):
        snapshot &= column.is_(None)
    _obj_tracking[obj]["snapshot"] = snapshot
    return snapshot


def get_marked(obj):
    """Returns the set of marked columns for an object"""
    return set(_obj_tracking[obj]["marked"])


# END CONDITION TRACKING ====================================================================== END CONDITION TRACKING


# RENDERING ================================================================================================ RENDERING


# TODO refactor
def render(engine, filter=None, projection=None, key=None, atomic=None, condition=None, update=None):
    renderer = ConditionRenderer(engine)
    if filter is not None:
        renderer.filter_expression(filter)
    if projection is not None:
        renderer.projection_expression(projection)
    if key is not None:
        renderer.key_expression(key)
    condition = condition or Condition()
    if atomic:
        condition &= get_snapshot(atomic)
    if condition:
        renderer.condition_expression(condition)
    renderer.update_expression(update)
    return renderer.rendered


# TODO refactor
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
        raise NotImplementedError

    def pop_refs(self, *ref):
        """Decrement the usage of each ref by 1.

        If the count is 0, pop the ref from attr_names or attr_values"""
        raise NotImplementedError

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


# END RENDERING ======================================================================================== END RENDERING

# CONDITIONS ============================================================================================== CONDITIONS


class BaseCondition:
    def __init__(self, operation, *, column=None, path=None, values=None, dumped=False):
        self.operation = operation
        self.column = column
        self.values = list(values or [])
        self.path = list(path or [])
        self.dumped = dumped

    __hash__ = object.__hash__

    def __len__(self):
        raise NotImplementedError

    def __repr__(self):
        raise NotImplementedError

    def iter_columns(self):
        raise NotImplementedError

    def render(self, renderer: ConditionRenderer):
        raise NotImplementedError

    def __invert__(self):
        if self.operation is None:
            return self
        if self.operation == "not":
            if not self.values:
                return Condition()
            # Cancel the negation
            return self.values[0]
        # return not(self)
        return NotCondition(value=self)

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
            return AndCondition(*self.values, *other.values)
        # (a & b) & (c > 2) -> (a & b & (c > 2))
        elif self.operation == "and":
            return AndCondition(*self.values, other)
        # (a > 2) & (b & c) -> ((a > 2) & b & c)
        elif other.operation == "and":
            return AndCondition(self, *other.values)
        # (a > 2) & (b < 3) -> ((a > 2) & (b < 3))
        else:
            return AndCondition(self, other)

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
            return AndCondition(self, *other.values)
        # (a > 2) &= (b < 3) -> ((a > 2) & (b < 3))
        else:
            return AndCondition(self, other)

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
            return OrCondition(*self.values, *other.values)
        # (a | b) | (c > 2) -> (a | b | (c > 2))
        elif self.operation == "or":
            return OrCondition(*self.values, other)
        # (a > 2) | (b | c) -> ((a > 2) | b | c)
        elif other.operation == "or":
            return OrCondition(self, *other.values)
        # (a > 2) | (b < 3) -> ((a > 2) | (b < 3))
        else:
            return OrCondition(self, other)

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
            return OrCondition(self, *other.values)
        # (a > 2) |= (b < 3) -> ((a > 2) | (b < 3))
        else:
            return OrCondition(self, other)

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
            # Both ComparisonMixin, use `is`
            if isinstance(s, ComparisonMixin) and isinstance(o, ComparisonMixin):
                if s is not o:
                    return False
            # This would mean only one was a ComparisonMixin
            elif isinstance(s, ComparisonMixin) or isinstance(o, ComparisonMixin):
                return False
            # Neither are ComparisonMixin, use `==`
            elif s != o:  # pragma: no branch
                return False
        return True


class Condition(BaseCondition):
    def __init__(self):
        super().__init__(operation=None)

    def __len__(self):
        return 0

    def __repr__(self):
        return "()"

    def iter_columns(self):
        raise StopIteration

    def render(self, renderer: ConditionRenderer):
        raise InvalidCondition("Condition is not renderable")


class AndCondition(BaseCondition):
    def __init__(self, *values):
        super().__init__("and", values=values)

    def __len__(self):
        return sum(1 for _ in iter_conditions(self))

    def __repr__(self):
        joiner = " & "
        if not self.values:
            return "({})".format(joiner)
        elif len(self.values) == 1:
            return "({!r} {})".format(self.values[0], joiner.strip())
        else:
            return "({})".format(joiner.join(repr(c) for c in self.values))

    def iter_columns(self):
        for condition in self.values:
            for column in condition.iter_columns():
                yield column

    def render(self, renderer: ConditionRenderer):
        rendered_conditions = [c.render(renderer) for c in self.values]
        if not rendered_conditions:
            raise InvalidCondition("Invalid Condition: <{!r}> does not contain any Conditions.".format(self))
        if len(rendered_conditions) == 1:
            return rendered_conditions[0]
        return "({})".format(" AND ".join(rendered_conditions))


class OrCondition(BaseCondition):
    def __init__(self, *values):
        super().__init__("or", values=values)

    def __len__(self):
        return sum(1 for _ in iter_conditions(self))

    def __repr__(self):
        joiner = " | "
        if not self.values:
            return "({})".format(joiner)
        elif len(self.values) == 1:
            return "({!r} {})".format(self.values[0], joiner.strip())
        else:
            return "({})".format(joiner.join(repr(c) for c in self.values))

    def iter_columns(self):
        for condition in self.values:
            for column in condition.iter_columns():
                yield column

    def render(self, renderer: ConditionRenderer):
        rendered_conditions = [c.render(renderer) for c in self.values]
        if not rendered_conditions:
            raise InvalidCondition("Invalid Condition: <{!r}> does not contain any Conditions.".format(self))
        if len(rendered_conditions) == 1:
            return rendered_conditions[0]
        return "({})".format(" AND ".join(rendered_conditions))


class NotCondition(BaseCondition):
    def __init__(self, value):
        super().__init__("not", values=[value])

    def __len__(self):
        return len(self.values[0])

    def __repr__(self):
        return "(~{!r})".format(self.values[0])

    def iter_columns(self):
        for column in self.values[0].iter_columns():
            yield column

    def render(self, renderer: ConditionRenderer):
        rendered_condition = self.values[0].render(renderer)
        if rendered_condition is None:
            raise InvalidCondition("Invalid Condition: 'not' condition does not contain an inner Condition.")
        return "(NOT {})".format(rendered_condition)


class ComparisonCondition(BaseCondition):
    def __init__(self, operation, column, value, path=None):
        super().__init__(operation=operation, column=column, values=[value], path=path)

    def __len__(self):
        return 1

    def __repr__(self):
        return "({}.{} {} {!r})".format(
            self.column.model.__name__, printable_column_name(self.column, self.path),
            self.operation, self.values[0])

    def iter_columns(self):
        yield self.column
        if isinstance(self.values[0], ComparisonMixin):
            yield self.values[0]

    def render(self, renderer: ConditionRenderer):
        column_ref = renderer.any_ref(column=self.column, path=self.path)
        value_ref, value = renderer.any_ref(column=self.column, path=self.path, value=self.values[0])
        # Could be attribute_exists/attribute_not_exists
        if value is None and self.operation in ("==", "!="):
            # Won't be sending this value, so pop it from the renderer
            renderer.pop_refs(value_ref)
            if self.operation == "==":
                return "(attribute_not_exists({}))".format(column_ref)
            else:
                return "(attribute_exists({}))".format(column_ref)
        # No other comparison can be against None
        elif value is None:
            # Try to revert the renderer to a valid state
            renderer.pop_refs(column_ref, value_ref)
            raise InvalidCondition("Comparison <{!r}> is against the value None.".format(self))
        else:
            return "({} {} {})".format(column_ref, comparison_aliases[self.operation], value_ref)


class BeginsWithCondition(BaseCondition):
    def __init__(self, column, value, path=None):
        super().__init__("begins_with", column=column, values=[value], path=path)

    def __len__(self):
        return 1

    def __repr__(self):
        return "begins_with({}.{}, {!r})".format(
            self.column.model.__name__, printable_column_name(self.column, self.path),
            self.values[0])

    def iter_columns(self):
        yield self.column
        if isinstance(self.values[0], ComparisonMixin):
            yield self.values[0]

    def render(self, renderer: ConditionRenderer):
        column_ref = renderer.any_ref(column=self.column, path=self.path)
        value_ref, value = renderer.any_ref(column=self.column, path=self.path, value=self.values[0])
        if value is None:
            # Try to revert the renderer to a valid state
            renderer.pop_refs(column_ref, value_ref)
            raise InvalidCondition("Condition <{!r}> is against the value None.".format(self))
        return "(begins_with({}, {}))".format(column_ref, value_ref)


class BetweenCondition(BaseCondition):
    def __init__(self, column, lower, upper, path=None):
        super().__init__("between", column=column, values=[lower, upper], path=path)

    def __len__(self):
        return 1

    def __repr__(self):
        return "({}.{} between [{!r}, {!r}])".format(
            self.column.model.__name__, printable_column_name(self.column, self.path),
            self.values[0], self.values[1])

    def iter_columns(self):
        yield self.column
        for value in self.values:
            if isinstance(value, ComparisonMixin):
                yield value

    def render(self, renderer: ConditionRenderer):
        column_ref = renderer.any_ref(column=self.column, path=self.path)
        lower_ref, lower_value = renderer.any_ref(column=self.column, path=self.path, value=self.values[0])
        upper_ref, upper_value = renderer.any_ref(column=self.column, path=self.path, value=self.values[1])
        if lower_value is None or upper_value is None:
            # Try to revert the renderer to a valid state
            renderer.pop_refs(column_ref, lower_ref, upper_ref)
            raise InvalidCondition("Condition <{!r}> includes the value None.".format(self))
        return "({} BETWEEN {} AND {})".format(column_ref, lower_ref, upper_ref)


class ContainsCondition(BaseCondition):
    def __init__(self, column, value, path=None):
        super().__init__("contains", column=column, values=[value], path=path)

    def __len__(self):
        return 1

    def __repr__(self):
        return "contains({}.{}, {!r})".format(
            self.column.model.__name__, printable_column_name(self.column, self.path),
            self.values[0])

    def iter_columns(self):
        yield self.column
        if isinstance(self.values[0], ComparisonMixin):
            yield self.values[0]

    def render(self, renderer: ConditionRenderer):
        column_ref = renderer.any_ref(column=self.column, path=self.path)
        value_ref, value = renderer.any_ref(column=self.column, path=self.path, value=self.values[0])
        if value is None:
            # Try to revert the renderer to a valid state
            renderer.pop_refs(column_ref, value_ref)
            raise InvalidCondition("Condition <{!r}> is against the value None.".format(self))
        return "(contains({}, {}))".format(column_ref, value_ref)


class InCondition(BaseCondition):
    def __init__(self, column, values, path=None):
        super().__init__("in", column=column, values=values, path=path)

    def __len__(self):
        return 1

    def __repr__(self):
        return "({}.{} in {!r})".format(
            self.column.model.__name__, printable_column_name(self.column, self.path),
            self.values)

    def iter_columns(self):
        yield self.column
        for value in self.values:
            if isinstance(value, ComparisonMixin):
                yield value

    def render(self, renderer: ConditionRenderer):
        if not self.values:
            raise InvalidCondition("Condition <{!r}> is missing values.".format(self))
        value_refs = []
        for value in self.values:
            value_ref, value = renderer.any_ref(column=self.column, path=self.path, value=value)
            value_refs.append(value_ref)
            if value is None:
                renderer.pop_refs(*value_refs)
                raise InvalidCondition("Condition <{!r}> includes the value None.".format(self))
        column_ref = renderer.any_ref(column=self.column, path=self.path)
        return "({} IN ({}))".format(column_ref, ", ".join(value_refs))


# END CONDITIONS ====================================================================================== END CONDITIONS


class ComparisonMixin:
    def __init__(self, *args, proxied=None, path=None, **kwargs):
        self._path = path or []
        self._proxied = self if proxied is None else proxied
        super().__init__(*args, **kwargs)

    def _repr_with_path(self, path):
        return "<ComparisonMixin>"

    def __repr__(self):
        return self._proxied._repr_with_path(self._path)

    def __getattr__(self, item):
        if self._proxied is self:
            raise AttributeError
        return getattr(self._proxied, item)

    def __getitem__(self, path):
        return ComparisonMixin(proxied=self._proxied, path=self._path + [path])

    def __eq__(self, value):
        return ComparisonCondition(operation="==", column=self._proxied, value=value, path=self._path)

    def __ne__(self, value):
        return ComparisonCondition(operation="!=", column=self._proxied, value=value, path=self._path)

    def __lt__(self, value):
        return ComparisonCondition(operation="<", column=self._proxied, value=value, path=self._path)

    def __gt__(self, value):
        return ComparisonCondition(operation=">", column=self._proxied, value=value, path=self._path)

    def __le__(self, value):
        return ComparisonCondition(operation="<=", column=self._proxied, value=value, path=self._path)

    def __ge__(self, value):
        return ComparisonCondition(operation=">=", column=self._proxied, value=value, path=self._path)

    def begins_with(self, value):
        return BeginsWithCondition(column=self._proxied, value=value, path=self._path)

    def between(self, lower, upper):
        return BetweenCondition(column=self._proxied, lower=lower, upper=upper, path=self._path)

    def contains(self, value):
        return ContainsCondition(column=self._proxied, value=value, path=self._path)

    def in_(self, *values):
        return InCondition(column=self._proxied, values=values, path=self._path)

    is_ = __eq__

    is_not = __ne__


def iter_conditions(condition: BaseCondition):
    """Yield all conditions WITHIN the given condition.

    If there are no conditions within this condition (any condition besides and, or, not; or any of those without
    an inner value) then this will not yield those conditions."""
    conditions = list()
    visited = set()
    # Has to be split out, since we don't want to visit the root (for cyclic conditions)
    # but we don't want to yield it (if it's non-cyclic) because this only yields inner conditions
    if condition.operation in {"and", "or"}:
        conditions.extend(reversed(condition.values))
    if condition.operation == "not":
        conditions.append(condition.values[0])
    while conditions:
        condition = conditions.pop()
        if condition in visited:
            continue
        visited.add(condition)
        yield condition
        if condition.operation in {"and", "or", "not"}:
            conditions.extend(reversed(condition.values))
