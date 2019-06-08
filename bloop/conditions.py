# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ \
#   Expressions.SpecifyingConditions.html#ConditionExpressionReference.Syntax
import collections
import logging
import weakref
from typing import Any, Set

from .actions import ActionType, unwrap, wrap
from .exceptions import InvalidCondition
from .signals import (
    object_deleted,
    object_loaded,
    object_modified,
    object_saved,
)
from .util import missing


__all__ = ["BaseCondition", "ComparisonMixin", "Condition", "iter_columns", "render"]


comparison_aliases = {
    "==": "=",
    "!=": "<>",
    "<": "<",
    ">": ">",
    "<=": "<=",
    ">=": ">=",
}
comparisons = list(comparison_aliases.keys())
logger = logging.getLogger("bloop.conditions")


# CONDITION TRACKING ============================================================================== CONDITION TRACKING


class ObjectTracking(weakref.WeakKeyDictionary):
    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError:
            r = self[key] = {"marked": set(), "snapshot": None}
            return r

    def get_snapshot(self, obj):
        """Return the latest snapshot of the object.

        If none exists, creates a snapshot that expects all values to be None.
        """
        snapshot = self[obj].get("snapshot")
        if snapshot is not None:
            return snapshot

        # If the object has never been synced, create and cache
        # a condition that expects every column to be empty
        snapshot = Condition()
        for column in sorted(obj.Meta.columns, key=lambda col: col.dynamo_name):
            snapshot &= column.is_(None)
        self[obj]["snapshot"] = snapshot
        return snapshot

    def set_snapshot(self, obj, snapshot) -> None:
        """Set the object snapshot eg. after saving or deleting"""
        self[obj]["snapshot"] = snapshot

    def get_marked(self, obj) -> Set:
        """Return the set of columns considered 'dirty' for this object."""
        return self[obj]["marked"]

    def sync(self, obj, engine) -> None:
        """
        Mark the object as having been persisted at least once.

        Store the latest snapshot of all marked values.
        """
        snapshot = Condition()
        # Only expect values (or lack of a value) for columns that have been explicitly set
        for column in sorted(global_tracking.get_marked(obj), key=lambda col: col.dynamo_name):
            value = getattr(obj, column.name, None)
            # noinspection PyProtectedMember
            value = engine._dump(column.typedef, value)
            condition = column == value
            # The renderer shouldn't try to dump the value again.
            # We're dumping immediately in case the value is mutable,
            # such as a set or (many) custom data types.
            condition.dumped = True
            snapshot &= condition
        self.set_snapshot(obj, snapshot)


# Tracks the state of instances of models:
# 1) Are any columns marked for including in an update?
# 2) Latest snapshot for atomic operations
global_tracking = ObjectTracking()


@object_deleted.connect
def on_object_deleted(_, *, obj, **__):
    global_tracking.set_snapshot(obj, None)


@object_loaded.connect
def on_object_loaded(_, *, engine, obj, **__):
    global_tracking.sync(obj, engine)


@object_modified.connect
def on_object_modified(_, *, obj, column, **__):
    # Mark a column for a given object as being modified in any way.
    # Any marked columns will be pushed (possibly as DELETE) in
    # future UpdateItem calls that include the object.
    global_tracking.get_marked(obj).add(column)


@object_saved.connect
def on_object_saved(_, *, engine, obj, **__):
    global_tracking.sync(obj, engine)


# END CONDITION TRACKING ====================================================================== END CONDITION TRACKING


# RENDERING ================================================================================================ RENDERING


Reference = collections.namedtuple("Reference", ["name", "type", "value"])


def is_empty(ref):
    """True if ref is a value ref with None value"""
    return ref.type == "value" and unwrap(ref.value) is None


class ReferenceTracker:
    """De-dupes reference names for the same path segments and generates unique placeholders for all
    names, paths, and values.  The reference tracker can also forget references if, for example, a value fails to
    render but the rest of the condition should be left intact.  This is primarily used when a value is unexpectedly
    dumped as None, or an expression uses another column as a value.

    :param engine: Used to dump column values for value refs.
    :type engine: :class:`~bloop.engine.Engine`
    """
    def __init__(self, engine):
        self.__next_index = 0
        self.counts = collections.defaultdict(lambda: 0)
        self.attr_values = {}
        self.attr_names = {}
        # Index ref -> attr name for de-duplication
        self.name_attr_index = {}
        self.engine = engine

    @property
    def next_index(self):
        """Prevent the ref index from *ever* decreasing and causing a collision."""
        current = self.__next_index
        self.__next_index += 1
        return current

    def _name_ref(self, name):
        # Small optimization to request size for duplicate name refs
        ref = self.name_attr_index.get(name, None)
        if ref:
            self.counts[ref] += 1
            return ref

        ref = "#n{}".format(self.next_index)
        self.attr_names[ref] = name
        self.name_attr_index[name] = ref
        self.counts[ref] += 1
        return ref

    def _path_ref(self, column: "ComparisonMixin"):
        pieces = [column.dynamo_name]
        pieces.extend(path_of(column))
        str_pieces = []
        for piece in pieces:
            # List indexes are attached to last path item directly
            if isinstance(piece, int):
                str_pieces[-1] += "[{}]".format(piece)
            # Path keys are attached with a "."
            else:
                str_pieces.append(self._name_ref(piece))
        return ".".join(str_pieces)

    def _value_ref(self, column, value, *, dumped=False, inner=False):
        """inner=True uses column.typedef.inner_type instead of column.typedef"""
        ref = ":v{}".format(self.next_index)

        if not dumped:
            typedef = column.typedef
            for segment in path_of(column):
                typedef = typedef[segment]
            if inner:
                typedef = typedef.inner_typedef
            # noinspection PyProtectedMember
            value = self.engine._dump(typedef, value)

        # The raw value needs to be stored in attr_values, but the Action information needs
        # to be passed back for the renderer to decide whether this is a set/remove/add/delete
        self.attr_values[ref] = unwrap(value)
        self.counts[ref] += 1
        return ref, value

    def any_ref(self, *, column, value=missing, dumped=False, inner=False) -> Reference:
        # noinspection PyUnresolvedReferences
        """Returns a NamedTuple of (name, type, value) for any type of reference.

        .. code-block:: python

            # Name ref
            >>> tracker.any_ref(column=User.email)
            Reference(name='email', type='name', value=None)

            # Value ref
            >>> tracker.any_ref(column=User.email, value='user@domain')
            Reference(name='email', type='value', value={'S': 'user@domain'})

            # Passed as value ref, but value is another column
            >>> tracker.any_ref(column=User.email, value=User.other_column)
            Reference(name='other_column', type='name', value=None)

        :param column: The column to reference.  If ``value`` is None, this will render a name ref for this column.
        :type column: :class:`~bloop.conditions.ComparisonMixin`
        :param value: *(Optional)* If provided, this is likely a value ref.  If ``value`` is also a column,
            this will render a name ref for that column (not the ``column`` parameter).
        :param bool dumped:  *(Optional)* True if the value has already been dumped and should not be dumped
            through the column's typedef again.  Commonly used with atomic conditions (which store the object's dumped
            representation).  Default is False.
        :param bool inner: *(Optional)* True if this is a value ref and it should be dumped through a collection's
            inner type, and not the collection type itself.  Default is False.
        :return: A name or value reference
        :rtype: :class:`bloop.conditions.Reference`
        """
        # Can't use None since it's a legal value for comparisons (attribute_not_exists)
        if value is missing:
            # Simple path ref to the column.
            name = self._path_ref(column=column)
            ref_type = "name"
            value = None
        elif isinstance(value, ComparisonMixin):
            # value is also a column!  Also a path ref.
            name = self._path_ref(column=value)
            ref_type = "name"
            value = None
        else:
            # Simple value ref.
            name, value = self._value_ref(column=column, value=value, dumped=dumped, inner=inner)
            ref_type = "value"
        return Reference(name=name, type=ref_type, value=value)

    def pop_refs(self, *refs):
        """Decrement the usage of each ref by 1.

        If this was the last use of a ref, remove it from attr_names or attr_values.
        """
        for ref in refs:
            name = ref.name
            count = self.counts[name]
            # Not tracking this ref
            if count < 1:
                continue
            # Someone else is using this ref
            elif count > 1:
                self.counts[name] -= 1
            # Last reference
            else:
                logger.debug("popping last usage of {}".format(ref))
                self.counts[name] -= 1
                if ref.type == "value":
                    del self.attr_values[name]
                else:
                    # Clean up both name indexes
                    path_segment = self.attr_names[name]
                    del self.attr_names[name]
                    del self.name_attr_index[path_segment]


def render(engine, obj=None, filter=None, projection=None, key=None, atomic=None, condition=None, update=None):
    renderer = ConditionRenderer(engine)
    renderer.render(
        obj=obj, condition=condition,
        atomic=atomic, update=update,
        filter=filter, projection=projection, key=key,
    )
    return renderer.output


class ConditionRenderer:
    # noinspection PyUnresolvedReferences
    """Renders collections of :class:`~bloop.conditions.BaseCondition` into DynamoDB's wire format for expressions,
    including:

    * ``"ConditionExpression"`` -- used in conditional operations
    * ``"FilterExpression"`` -- used in queries and scans to ignore results that don't match the filter
    * ``"KeyConditionExpressions"`` -- used to describe a query's hash (and range) key(s)
    * ``"ProjectionExpression"`` -- used to include a subset of possible columns in the results of a query or scan
    * ``"UpdateExpression"`` -- used to save objects

    Normally, you will only need to call :func:`~bloop.conditions.ConditionRenderer.render` to handle any combination
    of conditions.  You can also call each individual ``render_*`` function to control how multiple conditions of
    each type are applied.

    You can collect the rendered condition at any time through :attr:`~bloop.conditions.ConditionRenderer.rendered`.

    .. code-block:: python

        >>> renderer.render(obj=user, atomic=True)
        >>> renderer.output
        {'ConditionExpression': '((#n0 = :v1) AND (attribute_not_exists(#n2)) AND (#n4 = :v5))',
         'ExpressionAttributeNames': {'#n0': 'age', '#n2': 'email', '#n4': 'id'},
         'ExpressionAttributeValues': {':v1': {'N': '3'}, ':v5': {'S': 'some-user-id'}}}


    :param engine: Used to dump values in conditions into the appropriate wire format.
    :type engine: :class:`~bloop.engine.Engine`
    """
    def __init__(self, engine):
        self.refs = ReferenceTracker(engine)
        self.engine = engine
        self.expressions = {}

    def render(self, obj=None, condition=None, atomic=False, update=False, filter=None, projection=None, key=None):
        """Main entry point for rendering multiple expressions.  All parameters are optional, except obj when
        atomic or update are True.

        :param obj: *(Optional)* An object to render an atomic condition or update expression for.  Required if
            update or atomic are true.  Default is False.
        :param condition: *(Optional)* Rendered as a "ConditionExpression" for a conditional operation.
            If atomic is True, the two are rendered in an AND condition.  Default is None.
        :type condition: :class:`~bloop.conditions.BaseCondition`
        :param bool atomic: *(Optional)*  True if an atomic condition should be created for ``obj`` and rendered as
            a "ConditionExpression".  Default is False.
        :param bool update: *(Optional)*  True if an "UpdateExpression" should be rendered for ``obj``.
            Default is False.
        :param filter: *(Optional)* A filter condition for a query or scan, rendered as a "FilterExpression".
            Default is None.
        :type filter: :class:`~bloop.conditions.BaseCondition`
        :param projection: *(Optional)* A set of Columns to include in a query or scan, rendered as a
            "ProjectionExpression".  Default is None.
        :type projection: set :class:`~bloop.models.Column`
        :param key: *(Optional)* A key condition for queries, rendered as a "KeyConditionExpression".  Default is None.
        :type key: :class:`~bloop.conditions.BaseCondition`
        """
        if (atomic or update) and not obj:
            raise InvalidCondition("An object is required to render atomic conditions or updates without an object.")

        if filter:
            self.filter_expression(filter)

        if projection:
            self.projection_expression(projection)

        if key:
            self.key_expression(key)

        # Condition requires a bit of work, because either one can be empty/false
        condition = (condition or Condition()) & (global_tracking.get_snapshot(obj) if atomic else Condition())
        if condition:
            self.condition_expression(condition)

        if update:
            self.update_expression(obj)

    def condition_expression(self, condition):
        self.expressions["ConditionExpression"] = condition.render(self)

    def filter_expression(self, condition):
        self.expressions["FilterExpression"] = condition.render(self)

    def key_expression(self, condition):
        self.expressions["KeyConditionExpression"] = condition.render(self)

    def projection_expression(self, columns):
        included = set()
        ref_names = []
        for column in columns:
            if column in included:
                continue
            included.add(column)
            ref = self.refs.any_ref(column=column)
            ref_names.append(ref.name)
        self.expressions["ProjectionExpression"] = ", ".join(ref_names)

    def update_expression(self, obj):
        updates = {
            ActionType.Add: [],
            ActionType.Delete: [],
            ActionType.Remove: [],
            ActionType.Set: [],
        }
        for column in sorted(
                # Don't include key columns in an UpdateExpression
                filter(lambda c: c not in obj.Meta.keys, global_tracking.get_marked(obj)),
                key=lambda c: c.dynamo_name):
            name_ref = self.refs.any_ref(column=column)
            value_ref = self.refs.any_ref(column=column, value=getattr(obj, column.name, None))
            update_type = wrap(value_ref.value).type
            # Can't set to an empty value
            if is_empty(value_ref) or update_type is ActionType.Remove:
                self.refs.pop_refs(value_ref)
                updates[ActionType.Remove].append((name_ref, None))
                continue
            update_type = wrap(value_ref.value).type
            updates[update_type].append((name_ref, value_ref))

        expressions = []
        for update_type, refs in updates.items():
            if not refs:
                continue
            k = update_type.wire_key.upper()
            r = update_type.render
            expressions.append(f"{k} " + ", ".join(r(*ref) for ref in refs))
        if expressions:
            self.expressions["UpdateExpression"] = " ".join(e.strip() for e in expressions)

    @property
    def output(self):
        """The wire format for all conditions that have been rendered.
        A new :class:`~bloop.conditions.ConditionRenderer` should be used for each operation."""
        expressions = {k: v for (k, v) in self.expressions.items() if v is not None}
        if self.refs.attr_names:
            expressions["ExpressionAttributeNames"] = self.refs.attr_names
        if self.refs.attr_values:
            expressions["ExpressionAttributeValues"] = self.refs.attr_values
        return expressions


# END RENDERING ======================================================================================== END RENDERING


# CONDITIONS ============================================================================================== CONDITIONS


class BaseCondition:
    def __init__(self, operation, *, column=None, values=None, dumped=False):
        self.operation = operation
        self.column = column
        self.values = list(values or [])
        self.dumped = dumped

    __hash__ = object.__hash__

    def __len__(self):
        raise NotImplementedError

    def __repr__(self):
        raise NotImplementedError

    def render(self, renderer):
        raise NotImplementedError

    def __invert__(self):
        if self.operation is None:
            return self
        if self.operation == "not":
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
        if not isinstance(other, BaseCondition):
            return False
        if self.operation != other.operation:
            return False
        if isinstance(self.column, ComparisonMixin) != isinstance(other.column, ComparisonMixin):
            return False
        # If one isn't None, neither is None
        if self.column is not None:
            if proxied(self.column) is not proxied(other.column):
                return False
            if path_of(self.column) != path_of(other.column):
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
    """An empty condition.

    .. code-block:: python

        combined = Condition()

        for each_condition in get_conditions_list():
            combined &= each_condition

        if not combined:
            print("Conditions list only had empty conditions, or no conditions")

    Useful for iteratively building complex conditions, you can concatenate multiple conditions
    together without finding an initial condition in a possibly-empty list.

    An empty condition is equivalent to omitting a condition:

    .. code-block:: python

        engine.save(some_user)
        engine.save(some_user, condition=Condition())

    """
    def __init__(self):
        super().__init__(operation=None)

    def __len__(self):
        return 0

    def __repr__(self):
        return "()"

    def render(self, renderer):
        """Empty conditions don't render anything."""
        pass


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

    def render(self, renderer):
        if not self.values:
            raise InvalidCondition("Invalid Condition: <{!r}> does not contain any Conditions.".format(self))
        rendered_conditions = [c.render(renderer) for c in self.values]
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

    def render(self, renderer):
        if not self.values:
            raise InvalidCondition("Invalid Condition: <{!r}> does not contain any Conditions.".format(self))
        rendered_conditions = [c.render(renderer) for c in self.values]
        if len(rendered_conditions) == 1:
            return rendered_conditions[0]
        return "({})".format(" OR ".join(rendered_conditions))


class NotCondition(BaseCondition):
    def __init__(self, value):
        super().__init__("not", values=[value])

    def __len__(self):
        return len(self.values[0])

    def __repr__(self):
        return "(~{!r})".format(self.values[0])

    def render(self, renderer):
        rendered_condition = self.values[0].render(renderer)
        return "(NOT {})".format(rendered_condition)


class ComparisonCondition(BaseCondition):
    def __init__(self, operation, column, value):
        super().__init__(operation=operation, column=column, values=[value])

    def __len__(self):
        return 1

    def __repr__(self):
        return "({}.{} {} {!r})".format(
            self.column.model.__name__, printable_name(self.column),
            self.operation, self.values[0])

    def render(self, renderer):
        column_ref = renderer.refs.any_ref(
            column=self.column, dumped=self.dumped)
        value_ref = renderer.refs.any_ref(
            column=self.column, dumped=self.dumped, value=self.values[0])

        # #n0 >= :v1
        # Comparison against another column, or comparison against non-None value
        if (value_ref.type == "name") or (value_ref.value is not None):
            return "({} {} {})".format(column_ref.name, comparison_aliases[self.operation], value_ref.name)

        # attribute_exists(#n0), attribute_not_exists(#n1)
        # This is a value ref for ==, != against None
        if self.operation in ("==", "!="):
            renderer.refs.pop_refs(value_ref)
            function = "attribute_not_exists" if self.operation == "==" else "attribute_exists"
            logger.debug("rendering \"{}\" as {}".format(self.operation, function))
            return "({}({}))".format(function, column_ref.name)

        # #n0 <= None
        # This doesn't work; comparisons besides ==, != can't have a None value ref
        renderer.refs.pop_refs(column_ref, value_ref)
        raise InvalidCondition("Comparison <{!r}> is against the value None.".format(self))


class BeginsWithCondition(BaseCondition):
    def __init__(self, column, value):
        super().__init__("begins_with", column=column, values=[value])

    def __len__(self):
        return 1

    def __repr__(self):
        return "begins_with({}.{}, {!r})".format(
            self.column.model.__name__, printable_name(self.column),
            self.values[0])

    def render(self, renderer):
        column_ref = renderer.refs.any_ref(
            column=self.column, dumped=self.dumped)
        value_ref = renderer.refs.any_ref(
            column=self.column, dumped=self.dumped, value=self.values[0])
        if is_empty(value_ref):
            # Try to revert the renderer to a valid state
            renderer.refs.pop_refs(column_ref, value_ref)
            raise InvalidCondition("Condition <{!r}> is against the value None.".format(self))
        return "(begins_with({}, {}))".format(column_ref.name, value_ref.name)


class BetweenCondition(BaseCondition):
    def __init__(self, column, lower, upper):
        super().__init__("between", column=column, values=[lower, upper])

    def __len__(self):
        return 1

    def __repr__(self):
        return "({}.{} between [{!r}, {!r}])".format(
            self.column.model.__name__, printable_name(self.column),
            self.values[0], self.values[1])

    def render(self, renderer):
        column_ref = renderer.refs.any_ref(
            column=self.column, dumped=self.dumped)
        lower_ref = renderer.refs.any_ref(
            column=self.column, dumped=self.dumped, value=self.values[0])
        upper_ref = renderer.refs.any_ref(
            column=self.column, dumped=self.dumped, value=self.values[1])
        if is_empty(lower_ref) or is_empty(upper_ref):
            # Try to revert the renderer to a valid state
            renderer.refs.pop_refs(column_ref, lower_ref, upper_ref)
            raise InvalidCondition("Condition <{!r}> includes the value None.".format(self))
        return "({} BETWEEN {} AND {})".format(column_ref.name, lower_ref.name, upper_ref.name)


class ContainsCondition(BaseCondition):
    def __init__(self, column, value):
        super().__init__("contains", column=column, values=[value])

    def __len__(self):
        return 1

    def __repr__(self):
        return "contains({}.{}, {!r})".format(
            self.column.model.__name__, printable_name(self.column),
            self.values[0])

    def render(self, renderer):
        column_ref = renderer.refs.any_ref(
            column=self.column, dumped=self.dumped)
        value_ref = renderer.refs.any_ref(
            column=self.column, dumped=self.dumped, value=self.values[0], inner=True)
        if is_empty(value_ref):
            # Try to revert the renderer to a valid state
            renderer.refs.pop_refs(column_ref, value_ref)
            raise InvalidCondition("Condition <{!r}> is against the value None.".format(self))
        return "(contains({}, {}))".format(column_ref.name, value_ref.name)


class InCondition(BaseCondition):
    def __init__(self, column, values):
        super().__init__("in", column=column, values=values)

    def __len__(self):
        return 1

    def __repr__(self):
        return "({}.{} in {!r})".format(
            self.column.model.__name__, printable_name(self.column),
            self.values)

    def render(self, renderer):
        if not self.values:
            raise InvalidCondition("Condition <{!r}> is missing values.".format(self))
        value_refs = []
        for value in self.values:
            value_ref = renderer.refs.any_ref(
                column=self.column, dumped=self.dumped, value=value)
            value_refs.append(value_ref)
            if is_empty(value_ref):
                renderer.refs.pop_refs(*value_refs)
                raise InvalidCondition("Condition <{!r}> includes the value None.".format(self))
        column_ref = renderer.refs.any_ref(
            column=self.column, dumped=self.dumped)
        return "({} IN ({}))".format(column_ref.name, ", ".join(ref.name for ref in value_refs))


# END CONDITIONS ====================================================================================== END CONDITIONS


class ComparisonMixin:
    dynamo_name: str
    model: Any
    name: str
    typedef: Any

    def __repr__(self):
        return "<ComparisonMixin>"

    def __getitem__(self, path):
        return Proxy(self, [path])

    def __eq__(self, value):
        check_support(self, "==")
        return ComparisonCondition(operation="==", column=self, value=value)

    def __ne__(self, value):
        check_support(self, "!=")
        return ComparisonCondition(operation="!=", column=self, value=value)

    def __lt__(self, value):
        check_support(self, "<")
        return ComparisonCondition(operation="<", column=self, value=value)

    def __gt__(self, value):
        check_support(self, ">")
        return ComparisonCondition(operation=">", column=self, value=value)

    def __le__(self, value):
        check_support(self, "<=")
        return ComparisonCondition(operation="<=", column=self, value=value)

    def __ge__(self, value):
        check_support(self, ">=")
        return ComparisonCondition(operation=">=", column=self, value=value)

    def begins_with(self, value):
        check_support(self, "begins_with")
        return BeginsWithCondition(column=self, value=value)

    def between(self, lower, upper):
        check_support(self, "between")
        return BetweenCondition(column=self, lower=lower, upper=upper)

    def contains(self, value):
        check_support(self, "contains")
        return ContainsCondition(column=self, value=value)

    def in_(self, *values):
        check_support(self, "in")
        return InCondition(column=self, values=values)

    is_ = __eq__

    is_not = __ne__


def check_support(column: ComparisonMixin, operation):
    typedef = column.typedef
    for segment in path_of(column):
        typedef = typedef[segment]
    if not typedef.supports_operation(operation):
        tpl = "Backing type {!r} for {}.{} does not support condition {!r}."
        raise InvalidCondition(tpl.format(
            column.typedef.backing_type,
            column.model.__name__,
            printable_name(column),
            operation
        ))


class Proxy(ComparisonMixin):
    def __init__(self, obj, path):
        self._obj = obj
        self._path = path
        super().__init__()

    def __getattr__(self, item):
        return getattr(self._obj, item)

    def __getitem__(self, item):
        return Proxy(self._obj, self._path + [item])

    def __repr__(self):
        # "<Proxy[File.metadata[3].foo.bar[0]]>"
        name = self._obj.model.__name__
        path = printable_name(self._obj, self._path)
        return "<Proxy[{}.{}]>".format(name, path)


def printable_name(column, path=None):
    """Provided for debug output when rendering conditions.

    User.name[3]["foo"][0]["bar"] -> name[3].foo[0].bar
    """
    pieces = [column.name]
    path = path or path_of(column)
    for segment in path:
        if isinstance(segment, str):
            pieces.append(segment)
        else:
            pieces[-1] += "[{}]".format(segment)
    return ".".join(pieces)


def path_of(obj):
    if isinstance(obj, Proxy):
        # noinspection PyProtectedMember
        return obj._path
    return []


# noinspection PyProtectedMember
def proxied(obj):
    if isinstance(obj, Proxy):
        return obj._obj
    return obj


def iter_conditions(condition):
    """Yield all conditions within the given condition.

    If the root condition is and/or/not, it is not yielded (unless a cyclic reference to it is found)."""
    conditions = list()
    visited = set()
    # Has to be split out, since we don't want to visit the root (for cyclic conditions)
    # but we don't want to yield it (if it's non-cyclic) because this only yields inner conditions
    if condition.operation in {"and", "or"}:
        conditions.extend(reversed(condition.values))
    elif condition.operation == "not":
        conditions.append(condition.values[0])
    else:
        conditions.append(condition)
    while conditions:
        condition = conditions.pop()
        if condition in visited:
            continue
        visited.add(condition)
        yield condition
        if condition.operation in {"and", "or", "not"}:
            conditions.extend(reversed(condition.values))


def iter_columns(condition):
    """
    Yield all columns in the condition or its inner conditions.

    Unwraps proxies when the condition's column (or any of its values) include paths.
    """
    # Like iter_conditions, this can't live in each condition without going possibly infinite on the
    # recursion, or passing the visited set through every call.  That makes the signature ugly, so we
    # take care of it here.  Luckily, it's pretty easy to leverage iter_conditions and just unpack the
    # actual columns.
    visited = set()
    for condition in iter_conditions(condition):
        if condition.operation in ("and", "or", "not"):
            continue
        # Non-meta conditions always have a column, and each of values has the potential to be a column.
        # Comparison will only have a list of len 1, but it's simpler to just iterate values and check each

        # unwrap proxies created for paths
        column = proxied(condition.column)

        # special case for None
        # this could also have skipped on isinstance(condition, Condition)
        # but this is slightly more flexible for users to create their own None-sentinel Conditions
        if column is None:
            continue
        if column not in visited:
            visited.add(column)
            yield column
            for value in condition.values:
                if isinstance(value, ComparisonMixin):
                    if value not in visited:
                        visited.add(value)
                        yield value
