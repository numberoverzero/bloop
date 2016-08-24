# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ \
#   Expressions.SpecifyingConditions.html#ConditionExpressionReference.Syntax

from .exceptions import InvalidComparisonOperator
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


class NewCondition:
    def __init__(self, *values, operation=None, column=None, path=None):
        self.operation = operation
        self.column = column
        self.values = list(values)
        self.path = path or []

    def __len__(self):
        if not self.operation:
            return 0
        elif self.operation in ("and", "or"):
            return sum(map(len, self.values))
        elif self.operation == "not":
            # Guard against not without a value
            return bool(self.values) and len(self.values[0])
        else:
            return 1

    def __invert__(self):
        if self.operation == "not":
            if not self.values:
                return NewCondition()
            return self.values[0]
        return NewCondition(self.values[0], operation="not")

    __neg__ = __invert__

    def __and__(self, other):
        # ()_1 & ()_2 -> ()_1
        if not (self or other):
            return self

        # (a > 2) & () -> (a > 2)
        elif not other:
            return self
        # () & (b < 3) -> (b < 3)
        elif not self:
            return other

        # (a & b) & (c & d) -> (a & b & c & d)
        elif self.operation == other.operation == "and":
            return NewCondition(*self.values, *other.values, operation="and")
        # (a & b) & (c > 2) -> (a & b & (c > 2))
        elif self.operation == "and":
            return NewCondition(other, *self.values, operation="and")
        # (a > 2) & (b & c) -> ((a > 2) & b & c)
        elif other.operation == "and":
            return NewCondition(self, *other.values, operation="and")
        # (a > 2) & (b < 3) -> ((a > 2) & (b < 3))
        else:
            return NewCondition(self, other, operation="and")

    def __iand__(self, other):
        # x &= () -> x
        if not other:
            return self
        # (a & b) &= (c & d) -> (a & b & c & d)
        elif self.operation == "and" and other.operation == "and":
            self.values.extend(other.values)
            return self
        # (a & b) &= (c > 2) -> (a & b & (c > 2))
        elif self.operation == "and":
            self.values.append(other)
            return self
        # (a > 2) &= (b < 3) -> ((a > 2) & (b < 3))
        else:
            return NewCondition(self, other, operation="and")

    def __or__(self, other):
        # ()_1 | ()_2 -> ()_1
        if not (self or other):
            return self

        # (a > 2) | () -> (a > 2)
        elif not other:
            return self
        # () | (b < 3) -> (b < 3)
        elif not self:
            return other

        # (a | b) | (c | d) -> (a | b | c | d)
        elif self.operation == other.operation == "or":
            return NewCondition(*self.values, *other.values, operation="or")
        # (a | b) | (c > 2) -> (a | b | (c > 2))
        elif self.operation == "or":
            return NewCondition(other, *self.values, operation="or")
        # (a > 2) | (b | c) -> ((a > 2) | b | c)
        elif other.operation == "or":
            return NewCondition(self, *other.values, operation="or")
        # (a > 2) | (b < 3) -> ((a > 2) | (b < 3))
        else:
            return NewCondition(self, other, operation="or")

    def __ior__(self, other):
        # x |= () -> x
        if not other:
            return self
        # (a | b) |= (c | d) -> (a | b | c | d)
        elif self.operation == "or" and other.operation == "or":
            self.values.extend(other.values)
            return self
        # (a | b) |= (c > 2) -> (a | b | (c > 2))
        elif self.operation == "or":
            self.values.append(other)
            return self
        # (a > 2) |= (b < 3) -> ((a > 2) | (b < 3))
        else:
            return NewCondition(self, other, operation="or")

    def __repr__(self):
        if self.operation in ("and", "or"):
            joiner = " | " if self.operation == "or" else " & "
            if not self.values:
                return "({})".format(joiner)
            elif len(self.values) == 1:
                return "({!r} {})".format(self.values[0], joiner.strip())
            else:
                "({})".format(joiner.join(repr(c) for c in self.values))
        elif self.operation == "not":
            if not self.values:
                return "(~)"
            else:
                return "(~{!r})".format(self.values[0])
        elif self.column is None:
            return "()"
        elif self.operation in ["<", "<=", ">=", ">", "==", "!="]:
            return "({!r} {} {!r})".format(self.column, self.operation, self.values[0])
        elif self.operation in ["attribute_exists", "attribute_not_exists"]:
            return "{}({!r})".format(self.operation, self.column)
        elif self.operation in ["begins_with", "contains"]:
            return "{}({!r}, {!r})".format(self.operation, self.column, self.values[0])
        elif self.operation == "between":
            if not self.values:
                return "({!r} between [,]".format(self.column)
            elif len(self.values) == 1:
                return "({!r} between [{!r},]".format(self.column, self.values[0])
            else:
                return "({!r} between [{!r}, {!r}])".format(self.column, self.values[0], self.values[1])
        elif self.operation == "in":
            return "({!r} in {!r})".format(self.column, self.values)
        else:
            raise ValueError("Unknown operation {!r}".format(self.operation))

    def __eq__(self, other):
        if (
                (self.operation != other.operation) or
                (self.column is not other.column) or
                (self.path != other.path)):
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
            if s != o:
                return False

    __hash__ = object.__hash__


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


class NewComparisonMixin:
    def __init__(self, proxied=None, path=None):
        self.__path = path or []
        self.__proxied = self if proxied is None else proxied

    def __hash__(self):
        return super().__hash__()

    def __repr__(self):
        if self.__proxied is self:
            return "<ComparisonMixin>"
        return repr(self.__proxied)

    def __getattr__(self, item):
        if self.__proxied is self:
            raise AttributeError
        return getattr(self.__proxied, item)

    def __getitem__(self, path):
        return NewComparisonMixin(proxied=self.__proxied, path=self.__path + [path])

    def __eq__(self, value):
        if value is None:
            return NewCondition(operation="attribute_not_exists", column=self.__proxied, path=self.__path)
        return NewCondition(value, operation="==", column=self.__proxied, path=self.__path)

    def __ne__(self, value):
        if value is None:
            return NewCondition(operation="attribute_exists", column=self.__proxied, path=self.__path)
        return NewCondition(value, operation="!=", column=self.__proxied, path=self.__path)

    def __lt__(self, value):
        return NewCondition(value, operation="<", column=self.__proxied, path=self.__path)

    def __gt__(self, value):
        return NewCondition(value, operation=">", column=self.__proxied, path=self.__path)

    def __le__(self, value):
        return NewCondition(value, operation="<=", column=self.__proxied, path=self.__path)

    def __ge__(self, value):
        return NewCondition(value, operation=">=", column=self.__proxied, path=self.__path)

    def begins_with(self, value):
        return NewCondition(value, operation="begins_with", column=self.__proxied, path=self.__path)

    def between(self, lower, upper):
        return NewCondition(lower, upper, operation="between", column=self.__proxied, path=self.__path)

    def contains(self, value):
        return NewCondition(value, operation="contains", column=self.__proxied, path=self.__path)

    def in_(self, values):
        return NewCondition(*values, operation="in", column=self.__proxied, path=self.__path)

    is_ = __eq__

    is_not = __ne__
