import collections
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
INVALID_CONSISTENT = ValueError("Can't use ConsistentRead with a GlobalSecondaryIndex")
INVALID_FORWARD = ValueError("Can't set ScanIndexForward for scan operations, only queries")
INVALID_LIMIT = ValueError("Limit must be a non-negative int")
INVALID_PREFETCH = ValueError("Prefetch must be a non-negative int")


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
    # Allow None so that Filter.key(None) clears the condition (intermediate queries, base queries...)
    if key is None:
        return key
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
            consistent=False, forward=True, limit=0, key=None, filter=None):
        self.engine = engine
        self.mode = mode
        self.model = model
        self.index = index
        self.strict = strict

        self._select = select
        self._select_attributes = select_attributes
        self._prefetch = prefetch
        self._consistent = consistent
        self._forward = forward
        self._limit = limit

        self._key = key
        self._filter = filter

    def copy(self):
        """Convenience method for building specific queries off of a shared base query."""
        return Filter(
            engine=self.engine, mode=self.mode, model=self.model, index=self.index, strict=self.strict,
            select=self._select, select_attributes=self._select_attributes, prefetch=self._prefetch,
            consistent=self._consistent, forward=self._forward,
            limit=self._limit, key=self._key, filter=self._filter)

    def key(self, value):
        """Key conditions are only required for queries; there is no key condition for a scan.

        A key condition must include at least an equality (==) condition on the hash key of the index or model.
        A range condition on the index or model is allowed, and can be one of <,<=,>,>=,==, Between, BeginsWith
        There can be at most 1 hash condition and 1 range condition.
        They must be specified together.
        """
        self._key = validate_key_for(self.model, self.index, value)
        return self

    def select(self, value):
        """Which columns to load in the query/scan.

        When searching against a model, this can be "all" or a subset of the model's columns.
        When searching against a GSI, this can be "projected" or a subset of the GSI's projected columns.
        When searching against a LSI, this can be "all" (if not using strict queries), "projected", or:
            1) a subset of the LSI's projected columns (if using strict queries)
            2) a subset of the model's columns (if not using strict queries)
        For any search, "count" is allowed.

        Note that against an LSI, "all" or a superset of the projected columns of the LSI will incur an additional
        read against the table to fetch the non-indexed columns.
        """
        self._select, self._select_attributes = validate_select_for(self.model, self.index, self.strict, value)
        return self

    def filter(self, value):
        """Filtering is done server-side; as such, there may be empty pages to iterate before an instance is returned.

        DynamoDB applies the Limit before the FilterExpression.
            If a table with 10 rows only matches the filter on the 6th row, and a scan has a limit of 5,
            the scan will return no results.
        """
        # None is allowed for clearing a FilterExpression
        if not isinstance(value, (type(None), _BaseCondition)):
            raise INVALID_FILTER
        self._filter = value
        return self

    def consistent(self, value):
        """Whether ConsistentReads should be used.  Note that ConsistentReads cannot be used against a GSI"""
        if isinstance(self.index, GlobalSecondaryIndex):
            raise INVALID_CONSISTENT
        self._consistent = bool(value)
        return self

    def forward(self, value):
        """Whether a query is performed forwards or backwards.  Note that scans cannot be performed in reverse"""
        if self.mode == "scan":
            raise INVALID_FORWARD
        self._forward = bool(value)
        return self

    def limit(self, value):
        """From DynamoDB: The maximum number of items to evaluate (not necessarily the number of matching items).

        Limit should be used when you want to return NO MORE THAN a given value, with the intention that there may not
        be any results at all.  To instead limit the number of items returned, stop iterating the results of .build()
        after the desired number of results.

        For example, to iterate over the rows that meet a given FilterExpression from the first 30 rows:
            for result in engine.query(...).limit(30).filter(...).build():
                # process item

        However, to get the first 30 results that meet a given FilterExpression:
            results = engine.query(...).filter(...).build()
            for i, result in enumerate(results):
                if i == 30:
                    break
                # process item

        """
        try:
            if int(value) < 0:
                raise INVALID_LIMIT
        except (TypeError, ValueError):
            raise INVALID_LIMIT
        self._limit = int(value)
        return self

    def prefetch(self, value):
        """Number of items (not pages) to preload.

        Because a FilterExpression can cause the returned pages of a query to be empty, multiple calls may be made just
        to return one item.  This value is how many items should be loaded each time the iterator advances
        (with an unknown number of continue calls to yield the next item)
        """

        try:
            if int(value) < 0:
                raise INVALID_PREFETCH
        except (TypeError, ValueError):
            raise INVALID_PREFETCH
        self._prefetch = int(value)
        return self

    def one(self):
        """Returns the single item that matches the scan/query.

        If there is not exactly one matching result, raises ConstraintViolation.
        """
        filter = self.copy().prefetch(0).build()
        first = next(filter, None)
        second = next(filter, None)

        # No results, or too many results
        if (first is None) or (second is not None):
            raise ConstraintViolation(filter._mode + ".one", filter._prepared_request)
        return first

    def first(self):
        """Returns the first item that matches the scan/query.

        If there is not at least one matching result, raises ConstraintViolation.
        """
        filter = self.copy().prefetch(0).build()
        value = next(filter, None)
        # No results
        if value is None:
            raise ConstraintViolation(filter._mode + ".first", filter._prepared_request)
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
            "ConsistentRead": self._consistent,
            "Limit": self._limit,
            "ScanIndexForward": self._forward,
            "TableName": self.model.Meta.table_name,
        }
        # Only set IndexName if this is a query on an index
        if self.index:
            prepared_request["IndexName"] = self.index.dynamo_name
            # Can't perform consistent reads on a GSI
            if isinstance(self.index, GlobalSecondaryIndex):
                del prepared_request["ConsistentRead"]

        # Only send useful limits (omit 0 for no limit)
        if self._limit < 1:
            del prepared_request["Limit"]

        # Scans are always forward
        if self.mode == "scan":
            del prepared_request["ScanIndexForward"]

        prepared_request["Select"] = SELECT_MODES[self._select]

        # Query MUST have a key condition
        if self.mode == "query" and self._key is None:
                raise ValueError("Query must specify at least a hash key condition")
        # Scan MUST NOT have a key condition
        elif self.mode == "scan" and self._key is not None:
            raise ValueError("Scan cannot have a key condition")

        # Render filter, select, key
        rendered = render(self.engine, filter=self._filter, select=self._select_attributes, key=self._key)
        prepared_request.update(rendered)

        # Compute the expected columns for this filter
        expected_columns = expected_columns_for(self.model, self.index, self._select, self._select_attributes)
        return FilterIterator(
            engine=self.engine, model=self.model, prepared_request=prepared_request,
            expected_columns=expected_columns, mode=self.mode, prefetch=self._prefetch)


class FilterIterator:
    def __init__(self, *, engine, model, prepared_request, expected_columns, mode, prefetch):
        self._engine = engine
        self._model = model
        self._prepared_request = prepared_request
        self._expected_columns = expected_columns
        self._mode = mode
        self._prefetch = max(prefetch, 1)
        self._buffer = collections.deque()

        # Cache boto3 client call for performance
        self._call = getattr(engine.client, mode)

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
        self._prepared_request.pop("ExclusiveStartKey", None)

    def __iter__(self):
        return self

    def __next__(self):
        # Still have buffered elements available
        if len(self._buffer) > 0:
            return self._buffer.popleft()

        # Refill the buffer until we hit the limit, or Dynamo stops giving us continue tokens.
        while (not self.exhausted) and (len(self._buffer) < self._prefetch):
            response = self._call(self._prepared_request)
            continuation_token = response.get("LastEvaluatedKey", None)
            self._prepared_request["ExclusiveStartKey"] = continuation_token

            self._state["exhausted"] = continuation_token is None
            self._state["count"] += response["Count"]
            self._state["scanned"] += response["ScannedCount"]

            # Each item is a dict of attributes
            for attrs in response.get("Items", []):
                # Create an instance to load into
                obj = self._model.Meta.init()
                # Apply updates from attrs, only inserting expected columns, and sync the new object's tracking
                self._engine._update(obj, attrs, self._expected_columns)
                sync(obj, self._engine)
                self._buffer.append(obj)

        # Clear the first element of a full buffer, or a remaining element after exhaustion
        if self._buffer:
            return self._buffer.popleft()

        # The filter must be exhausted, otherwise the while would have continued.
        # The buffer must be empty, otherwise we would have popped above.
        raise StopIteration
