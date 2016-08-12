import collections
import functools
import operator

from .condition import And, BeginsWith, Between, Comparison, _BaseCondition
from .exceptions import ConstraintViolation
from .expressions import render
from .index import GlobalSecondaryIndex, LocalSecondaryIndex
from .tracking import sync


__all__ = ["Filter", "FilterIterator"]

SELECT_MODES = {
    "all": "ALL_ATTRIBUTES",
    "count": "COUNT",
    "projected": "ALL_PROJECTED_ATTRIBUTES",
    "specific": "SPECIFIC_ATTRIBUTES"
}

INVALID_SELECT = ValueError("Select must be 'all', 'count', 'projected', or a list of column objects to select")
INVALID_FILTER = ValueError("Filter must be a condition or None")


def expected_columns_for(model, index, select, select_attributes):
    if select == "all":
        return model.Meta.columns
    elif select == "projected":
        return index.projection_attributes
    elif select == "count":
        return set()
    elif select == "specific":
        return select_attributes
    else:
        raise ValueError("unknown mode {}".format(select))


def validate_select_for(model, index, strict, select):
    if isinstance(select, str):
        if select == "all":
            # Table query, all is fine
            if index is None:
                return select, None
            # LSIs are allowed when queries aren't strict
            if isinstance(index, LocalSecondaryIndex) and not strict:
                return select, None
            # GSIs and strict LSIs can load all attributes if they project all
            elif index.projection == "ALL":
                return select, None
            # Out of luck
            else:
                raise ValueError("Can't select 'all' on a GSI or strict LSI")
        elif select == "count":
            return select, None
        elif select == "projected":
            # Table queries don't have projected attributes
            if index is None:
                raise ValueError("Can't query projected attributes without an index")
            # projected is valid for any index
            return select, None
        # Unknown select mode
        else:
            raise ValueError("Unknown select mode '{}'".format(select))

    # Since it's not a string, we're in specific column territory.
    select = set(select)

    # Can't specify no columns
    if not select:
        raise ValueError("Must specify at least one column to load")

    # Make sure the iterable is only of columns on this model
    if not all((s in model.Meta.columns) for s in select):
        raise INVALID_SELECT

    # Table query, any subset of 'all' is valid
    if index is None:
        return "specific", select
    elif isinstance(index, GlobalSecondaryIndex):
        # Selected columns must be a subset of projection_attributes
        if select <= index.projection_attributes:
            return "specific", select
        raise ValueError("Tried to select a superset of the GSI's projected columns")

    # LSI
    else:
        # Unlike a GSI, the LSI can load a superset of the projection, and DynamoDB will happily do this.
        # Unfortunately, it will also incur an additional read per row.  Strict mode checks the cardinality of the
        # requested columns against the LSI's projection_attributes, just like a GSI.

        # When strict mode is disabled, however, any selection is valid (just like a table query)
        if not strict:
            return "specific", select
        # Strict mode - selected columns must be a subset of projection_attributes
        if select <= index.projection_attributes:
            return "specific", select
        raise ValueError("Tried to select a superset of the LSI's projected columns in strict mode")


def validate_hash_key_condition(condition, hash_column):
    return (
        # 1) Comparison
        isinstance(condition, Comparison) and
        # 2) ==
        condition.comparator is operator.eq and
        # 3) hash_column
        condition.column is hash_column)


def validate_range_key_condition(condition, range_column):
    # Valid comparators are EQ | LE | LT | GE | GT -- not NE
    is_comparison = isinstance(condition, Comparison) and (condition.comparator is not operator.ne)
    # ... or begins_with, or between
    is_special_condition = isinstance(condition, (BeginsWith, Between))
    return (is_comparison or is_special_condition) and condition.column is range_column


def validate_key_for(model, index, key):
    hash_column = (index or model.Meta).hash_key
    range_column = (index or model.Meta).range_key
    if isinstance(key, And) and len(key) == 2:
        # Instead of some cleverness, brute force validate the two allowed permutations.

        # 1) AND(hash_condition, range_condition)
        if (validate_hash_key_condition(key.conditions[0], hash_column) and
                validate_range_key_condition(key.conditions[1], range_column)):
            return key
        # 2) AND(range_condition, hash_condition)
        if (validate_hash_key_condition(key.conditions[1], hash_column) and
                validate_range_key_condition(key.conditions[0], range_column)):
            return key
        raise ValueError("Key condition must contain exactly 1 hash condition, at most 1 range condition")

    # Looking at a single condition (or at least, not an AND)
    if validate_hash_key_condition(key, hash_column):
        return key
    raise ValueError("Key condition must contain exactly 1 hash condition, at most 1 range condition")


class Filter:
    def __init__(
            self, *, engine, mode, model, index, strict, select, select_attributes=None, prefetch=0,
            consistent=False, forward=True, limit=None, key=None, filter=None):
        self.engine = engine
        self.mode = mode
        self.model = model
        self.index = index
        self.strict = strict

        self.select = select
        self.prefetch = prefetch
        self.consistent = consistent
        self.forward = forward
        self.limit = limit or 0

        self.key = key
        self.filter = filter

    def copy(self):
        """Convenience method for building specific queries off of a shared base query."""
        return Filter(
            engine=self.engine, mode=self.mode, model=self.model, index=self.index, strict=self.strict,
            select=self.select, prefetch=self.prefetch,
            consistent=self.consistent, forward=self.forward,
            limit=self.limit, key=self.key, filter=self.filter)

    def one(self):
        """Returns the single item that matches the scan/query.

        If there is not exactly one matching result, raises ConstraintViolation.
        """
        f = self.copy()
        f.prefetch = 0
        it = f.build()
        first = next(it, None)
        second = next(it, None)

        # No results, or too many results
        if (first is None) or (second is not None):
            raise ConstraintViolation(self.mode + ".one", it._request)
        return first

    def first(self):
        """Returns the first item that matches the scan/query.

        If there is not at least one matching result, raises ConstraintViolation.
        """
        f = self.copy()
        f.prefetch = 0
        it = f.build()
        value = next(it, None)
        # No results
        if value is None:
            raise ConstraintViolation(self.mode + ".first", it._request)
        return value

    def build(self):
        """Return a FilterIterator which can be iterated (and reset) to execute the query/scan.

        Usage:

            iterator = engine.query(...).build()

            # No work done yet
            iterator.count  # 0
            iterator.scanned_count  # 0

            # Execute the full query
            for _ in iterator:
                pass
            iterator.scanned_count  # > 0

            # Reset the query for re-execution, possibly returning different results
            iterator.reset()
            iterator.scanned_count  # 0
        """
        prepared_request = {
            "ConsistentRead": bool(self.consistent),
            "Limit": int(self.limit),
            "ScanIndexForward": bool(self.forward),
            "TableName": self.model.Meta.table_name,
        }
        # Only set IndexName if this is a query on an index
        if self.index:
            prepared_request["IndexName"] = self.index.dynamo_name
            # Can't perform consistent reads on a GSI
            if isinstance(self.index, GlobalSecondaryIndex):
                del prepared_request["ConsistentRead"]

        # Only send useful limits (omit 0 for no limit)
        if self.limit < 1:
            del prepared_request["Limit"]

        # Scans are always forward
        if self.mode == "scan":
            del prepared_request["ScanIndexForward"]

        select_mode, select_columns = validate_select_for(self.model, self.index, self.strict, self.select)
        prepared_request["Select"] = SELECT_MODES[select_mode]

        # Query MUST have a key condition
        if self.mode == "query":
            key = validate_key_for(self.model, self.index, self.key)
        else:
            key = None

        # Filter can be a condition or None
        if not isinstance(self.filter, (type(None), _BaseCondition)):
            raise INVALID_FILTER

        # Render filter, select, key
        rendered = render(self.engine, filter=self.filter, select=select_columns, key=key)
        prepared_request.update(rendered)

        # Compute the expected columns for this filter
        expected_columns = expected_columns_for(self.model, self.index, select_mode, select_columns)
        unpack = functools.partial(unpack_obj, engine=self.engine, model=self.model, expected=expected_columns)
        call = getattr(self.engine.client, self.mode)
        return FilterIterator(call=call, unpack=unpack, request=prepared_request, prefetch=int(self.prefetch))


def unpack_obj(*, engine, model, attrs, expected):
    # Create an instance to load into
    obj = model.Meta.init()
    # Apply updates from attrs, only inserting expected columns, and sync the new object's tracking
    engine._update(obj, attrs, expected)
    sync(obj, engine)
    return obj


class FilterIterator:
    def __init__(self, *, call, unpack, request, prefetch):
        self._call = call
        self._unpack = unpack
        self._request = request
        self._prefetch = max(prefetch, 1)

        self._buffer = collections.deque()
        self._state = {"count": 0, "scanned": 0, "exhausted": False}

    @property
    def count(self):
        return self._state["count"]

    @property
    def scanned(self):
        return self._state["scanned"]

    @property
    def exhausted(self):
        return self._state["exhausted"]

    def reset(self):
        self._state = {"count": 0, "scanned": 0, "exhausted": False}
        self._request.pop("ExclusiveStartKey", None)

    def __iter__(self):
        return self

    def __next__(self):
        # still have buffered elements available
        if len(self._buffer) > 0:
            return self._buffer.popleft()

        # Refill the buffer until we hit the limit, or Dynamo stops giving us continue tokens.
        while (not self.exhausted) and len(self._buffer) < self._prefetch:
            response = self._call(self._request)
            continuation_token = response.get("LastEvaluatedKey", None)
            self._request["ExclusiveStartKey"] = continuation_token

            self._state["exhausted"] = continuation_token is None
            self._state["count"] += response["Count"]
            self._state["scanned"] += response["ScannedCount"]

            # Each item is a dict of attributes
            for attrs in response.get("Items", []):
                self._buffer.append(self._unpack(attrs=attrs))

        # Clear the first element of a full buffer, or a remaining element after exhaustion
        if self._buffer:
            return self._buffer.popleft()

        # The filter must be exhausted, otherwise the while would have continued.
        # The buffer must be empty, otherwise we would have popped above.
        raise StopIteration
