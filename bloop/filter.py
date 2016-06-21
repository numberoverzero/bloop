import bloop.column
import bloop.condition
import bloop.exceptions
import bloop.index
import bloop.tracking
import operator

__all__ = ["Query", "Scan"]

SELECT_MODES = {
    "all": "ALL_ATTRIBUTES",
    "projected": "ALL_PROJECTED_ATTRIBUTES",
    "count": "COUNT"
}

INVALID_SELECT = ValueError("Select must be 'all', 'projected', 'count', or a list of column objects to select")
INVALID_FILTER = ValueError("Filter must be a condition or None")
INVALID_CONSISTENT = ValueError("Can't use ConsistentRead with a GlobalSecondaryIndex")
INVALID_FORWARD = ValueError("Can't set ScanIndexForward for scan operations, only queries")
INVALID_LIMIT = ValueError("Limit must be a non-negative int")
INVALID_PREFETCH = ValueError("Prefetch must be a non-negative int")


def consume(iter):
    for _ in iter:
        pass


def validate_hash_key_condition(condition):
    # 1 Must be comparison
    if (isinstance(condition, bloop.condition.Comparison) and
            # 2 Must be EQ comparator
            (condition.comparator is operator.eq) and
            # 3 Must not have a path component
            (not condition.path)):
        return True
    raise ValueError("KeyCondition must be EQ, without any document paths")


def validate_range_key_condition(condition):
    if isinstance(condition, (bloop.condition.BeginsWith,
                              bloop.condition.Between)):
        return True
    elif isinstance(condition, bloop.condition.Comparison):
        # Valid comparators are EG | LE | LT | GE | GT -- not NE
        if condition.comparator is not operator.ne:
            return True
    raise ValueError("Invalid KeyCondition {}".format(condition))


def validate_key(key_condition, hash_column, range_column):
    # 0. Must specify at least a hash condition
    if not key_condition:
        raise ValueError("At least one key condition (hash) is required")

    # 1. Comparison condition, single column
    if isinstance(key_condition, bloop.condition.Comparison):
        # 1.1 Comparison, EQ, no path
        validate_hash_key_condition(key_condition)
        # 1.2 Must be a condition on hash_column
        if key_condition.column is not hash_column:
            raise ValueError("KeyCondition must compare against hash column")
    # 2. AND is valid for hash, range combinations
    elif isinstance(key_condition, bloop.condition.And):
        key_columns = {hash_column}
        if range_column is not None:
            key_columns.add(range_column)

        # 2.1 `and` can specify at most as many conditions as there are
        #     key columns (1 or 2)
        if len(key_condition.conditions) > len(key_columns):
            msg = "Only {} key conditions allowed but {} provided".format(
                len(key_columns), len(key_condition))
            raise ValueError(msg)

        has_hash_condition = False
        for subcondition in key_condition.conditions:
            if not hasattr(subcondition, "column"):
                raise ValueError(
                    "Condition can't be made up of And/Or/Not conditions")
            if subcondition.column is hash_column:
                # 2.2 No more than one condition against the hash column
                if has_hash_condition:
                    raise ValueError(
                        "Must specify a condition on the hash key")
                validate_hash_key_condition(subcondition)
                has_hash_condition = True
            elif subcondition.column is range_column:
                # 2.3 Range conditions can be <,<=,>,>=,==, Between, BeginsWith
                validate_range_key_condition(subcondition)
            else:
                # 2.4 Conditions must be against hash or range columns
                msg = "Non-key condition {} passed as KeyCondition"
                raise ValueError(msg.format(subcondition))

        # 2.4 At least one condition against the hash column
        if not has_hash_condition:
            raise ValueError("Must specify a condition on the hash key")
    # 3. Must be EQ or AND
    else:
        raise ValueError("KeyCondition must be EQ or AND")


def validate_prefetch(prefetch):
    try:
        if int(prefetch) < 0:
            raise INVALID_PREFETCH
    except (TypeError, ValueError):
        raise INVALID_PREFETCH
    return int(prefetch)


def validate_select(select):
    if select.lower() in {"all", "projected", "count"}:
        return select
    are_columns = all(map(lambda s: isinstance(s, bloop.column.Column), select))
    if len(select) > 1 and are_columns:
        return select
    raise INVALID_SELECT


def validate_limit(limit):
    try:
        if int(limit) < 0:
            raise INVALID_LIMIT
    except (TypeError, ValueError):
        raise INVALID_LIMIT
    return int(limit)


def is_select_exact(index, engine):
    """
    Returns True if :
    1) The filter is on a GSI, or an LSI and the engine is strict
    2) The index projection is not ALL
    """
    is_gsi = isinstance(index, bloop.index.GlobalSecondaryIndex)
    is_lsi = isinstance(index, bloop.index.LocalSecondaryIndex)
    strict = engine.config["strict"]
    requires_exact = is_gsi or (is_lsi and strict)
    is_exact = (not requires_exact) or (index.projection == "ALL")
    return is_exact

# ====================================================================================================================
# ABOVE: old helpers to remove
# HERE: new helpers


def expected_columns_for(model, index, select):
    if isinstance(select, str):
        if select == "all":
            return model.Meta.columns
        elif select == "projected":
            return index.projection_attributes
        # "count"
        else:
            return set()
    # Otherwise this is a list of specific attributes.
    # It's not a problem if the query returns more attributes than we select, since
    # engine._update will only load the expected columns from the result.
    else:
        return select


def validate_select_for(model, index, strict, select):
    if isinstance(select, str):
        if select == "count":
            return select
        elif select == "all":
            # Table query, all is fine
            if index is None:
                return select
            # LSIs are allowed, but only when queries aren't strict
            if isinstance(index, bloop.index.LocalSecondaryIndex) and not strict:
                return select
            # GSIs and strict LSIs can't load all attributes
            else:
                raise ValueError("Can't select 'all' on a GSI or strict LSI")
        elif select == "projected":
            # Table queries don't have projected attributes
            if index is None:
                raise ValueError("Can't query projected attributes without an index")
            # projected is valid for any index
            return select
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
        raise ValueError("Select must be all, projected, count, or an iterable of columns on the model")
    # Table query, any subset of 'all' is valid
    if index is None:
        return select
    elif isinstance(index, bloop.index.GlobalSecondaryIndex):
        # Selected columns must be a subset of projection_attributes
        if select <= index.projection_attributes:
            return select
        raise ValueError("Tried to select a superset of the GSI's projected columns")
    # LSI
    else:
        # Unlike a GSI, the LSI can load a superset of the projection, and DynamoDB will happily do this.
        # Unfortunately, it will also incur an additional read per row.  Strict mode checks the cardinality of the
        # requested columns against the LSI's projection_attributes, just like a GSI.

        # When strict mode is disabled, however, any selection is valid (just like a table query)
        if not strict:
            return select
        # Strict mode - selected columns must be a subset of projection_attributes
        if select <= index.projection_attributes:
            return select
        raise ValueError("Tried to select a superset of the LSI's projected columns in strict mode")


# ====================================================================================================================


class Filter:
    def __init__(
            self, *, engine, mode, model, index, strict, select, prefetch=0,
            consistent=False, forward=True, limit=0, key=None, filter=None):
        self.engine = engine
        self.mode = mode
        self.model = model
        self.index = index
        self.strict = strict

        self._select = select
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
            select=self._select, prefetch=self._prefetch, consistent=self._consistent, forward=self._forward,
            limit=self._limit, key=self._key, filter=self._filter)

    def key(self, value):
        """Key conditions are only required for queries; there is no key condition for a scan.

        A key condition must include at least an equality (==) condition on the hash key of the index or model.
        A range condition on the index or model is allowed, and can be one of <,<=,>,>=,==, Between, BeginsWith
        There can be at most 1 hash condition and 1 range condition.
        They must be specified together.
        """
        # TODO refactor key validation
        # validate_key(self.model, self.index, value)
        self._key = value
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
        self._select = validate_select_for(self.model, self.index, self.strict, value)
        return self

    def filter(self, value):
        """Filtering is done server-side; as such, there may be empty pages to iterate before an instance is returned.

        DynamoDB applies the Limit before the FilterExpression.
            If a table with 10 rows only matches the filter on the 6th row, and a scan has a limit of 5,
            the scan will return no results.
        """
        # None is allowed for clearing a FilterExpression
        if not isinstance(value, (type(None), bloop.condition._BaseCondition)):
            raise INVALID_FILTER
        self._filter = value
        return self

    def consistent(self, value):
        """Whether ConsistentReads should be used.  Note that ConsistentReads cannot be used against a GSI"""
        if isinstance(self.index, bloop.index.GlobalSecondaryIndex):
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
        f = self.copy().prefetch(0).build()
        iterator = iter(f)
        constraint_not_met = bloop.exceptions.ConstraintViolation(f.mode + ".one", f.prepared_request)
        try:
            first = next(iterator)
        except StopIteration:
            raise constraint_not_met

        # Try to load another item.  If there is only one result, this should
        # raise StopIteration.  Finding another element means there wasn't a
        # unique result for the query, and we should fail.
        try:
            next(iterator)
        except StopIteration:
            return first
        else:
            raise constraint_not_met

    def first(self):
        """Returns the first item that matches the scan/query.

        If there is not at least one matching result, raises ConstraintViolation.
        """
        f = self.copy().prefetch(0).build()
        iterator = iter(f)
        try:
            return next(iterator)
        except StopIteration:
            raise bloop.exceptions.ConstraintViolation(f.mode + ".first", f.prepared_request)

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
        renderer = bloop.condition.ConditionRenderer(self.engine)
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
            if isinstance(self.index, bloop.index.GlobalSecondaryIndex):
                del prepared_request["ConsistentRead"]

        # Only send useful limits (omit 0 for no limit)
        if self._limit < 1:
            del prepared_request["Limit"]

        # Scans are always forward
        if self.mode == "scan":
            del prepared_request["ScanIndexForward"]

        # FilterExpression
        if self._filter:
            renderer.render(self._filter, mode="filter")

        # Select, ProjectionExpression
        if self._select not in {"all", "projected", "count"}:
            renderer.projection(self._select)
            prepared_request["Select"] = "SPECIFIC_ATTRIBUTES"
        else:
            prepared_request["Select"] = SELECT_MODES[self._select]

        # KeyExpression
        if self.mode == "query":
            # Query MUST have a key condition
            if self._key is None:
                raise ValueError("Query must specify at least a hash key condition")
            renderer.render(self._key, mode="key")
        # Scan MUST NOT have a key condition
        elif self._key is not None:
            raise ValueError("Scan cannot have a key condition")

        # Apply rendered expressions: KeyExpression, FilterExpression, ProjectionExpression
        prepared_request.update(renderer.rendered)
        # Compute the expected columns for this filter
        expected_columns = expected_columns_for(self.model, self.index, self._select)
        return FilterIterator(
            engine=self.engine, model=self.model, prepared_request=prepared_request,
            expected_columns=expected_columns, mode=self.mode, prefetch=self._prefetch)


class FilterIterator:
        def __init__(self, *, engine, model, prepared_request, expected_columns, mode, prefetch):
            self.engine = engine
            self.model = model
            self.prepared_request = prepared_request
            self.expected_columns = expected_columns
            self.mode = mode
            self.prefetch = prefetch

            self._count = {"Count": 0, "ScannedCount": 0}

        def __iter__(self):
            # TODO implement __iter__
            pass

        @property
        def count(self):
            return self._count["Count"]

        @property
        def scanned_count(self):
            return self._count["ScannedCount"]

        def reset(self):
            self._count["Count"] = self._count["ScannedCount"] = 0
            self.prepared_request.pop("ExclusiveStartKey", None)


# ====================================================================================================================

class _Filter(object):
    """Base class for Scan and Query."""
    # Scan -> "scan", Query -> "query"
    filter_type = None

    def __init__(self, engine, *, model=None, index=None):
        self.engine = engine
        self.model = model
        self.index = index

        self._key_condition = bloop.condition.Condition()
        self._filter_condition = bloop.condition.Condition()
        if self.index:
            self._select = "projected"
        else:
            self._select = "all"
        self._forward = True
        self._consistent = engine.config["consistent"]

        self._select_columns = []

    def _copy(self):
        cls = self.__class__
        other = cls(engine=self.engine, model=self.model, index=self.index)

        for attr in ["_filter_condition", "_key_condition",
                     "_select", "_forward", "_consistent"]:
            setattr(other, attr, getattr(self, attr))

        other._select_columns = list(self._select_columns)
        other._key_condition = self._key_condition
        return other

    def _expected(self):
        """
        Return a list of Columns that are expected for the current options.
        """
        if self._select == "all":
            return self.model.Meta.columns
        elif self._select == "projected":
            return self.index.projection_attributes
        # specific
        else:
            # If more are requested than a LSI supports, all will be loaded.
            # In all other cases, just the selected columns will be.
            if isinstance(self.index, bloop.index.LocalSecondaryIndex):
                selected = set(self._select_columns)
                available = self.index.projection_attributes
                if not selected.issubset(available):
                    return self.model.Meta.columns
            return self._select_columns

    def _generate_request(self, renderer):
        request = {
            "TableName": self.model.Meta.table_name,
            "Select": SELECT_MODES[self._select],
            "ConsistentRead": self._consistent
        }
        if self.index:
            request["IndexName"] = self.index.dynamo_name
        if self._filter_condition:
            renderer.render(self._filter_condition, mode="filter")
        if self._select == "specific":
            renderer.projection(self._select_columns)
        request.update(renderer.rendered)
        return request

    def all(self, prefetch=0):
        """Creates the FilterResult that will lazy load the results of the
        scan/query.

        Usage:
            Building a query iteratively::

                base_query = engine.query(Model).key(id="foo")
                query = base_query.consistent.ascending

                # Iterate results directly, discarding query metadata
                for result in query:
                    ...

                # Save reference to FilterResult instance
                results = query.all()
                for result in results:
                    ...
                results.count
                results.scanned_count

        """
        # dynamo.client.query or dynamo.client.scan
        call = getattr(self.engine.client, self.filter_type)
        renderer = bloop.condition.ConditionRenderer(self.engine)
        request = self._generate_request(renderer)

        expected = self._expected()
        return FilterResult(prefetch, call, request, self.engine,
                            self.model, expected)

    @property
    def ascending(self):
        other = self._copy()
        other._forward = True
        return other

    @property
    def consistent(self):
        if isinstance(self.index, bloop.index.GlobalSecondaryIndex):
            raise ValueError(
                "Cannot use ConsistentRead with a GlobalSecondaryIndex")
        other = self._copy()
        other._consistent = True
        return other

    def count(self):
        other = self._copy()
        other._select = "count"
        other._select_columns.clear()
        # Force fetch all
        result = other.all(prefetch="all")
        return {
            "count": result.count,
            "scanned_count": result.scanned_count
        }

    @property
    def descending(self):
        other = self._copy()
        other._forward = False
        return other

    def filter(self, condition):
        other = self._copy()
        other._filter_condition = condition
        return other

    def first(self):
        """ Returns the first result that matches the filter. """
        result = self.all(prefetch=0)
        return result.first

    def key(self, condition):
        obj = self.index or self.model.Meta
        hash_column = obj.hash_key
        range_column = obj.range_key

        validate_key(condition, hash_column, range_column)

        other = self._copy()
        other._key_condition = condition
        return other

    def select(self, columns):
        """
        columns must be "all", "projected", or a list of `bloop.Column` objects
        """
        select = validate_select(columns)
        # False for non-index queries.
        # True if we need to query exactly, but the index's projection
        # doesn't support fetching all attributes.  Invalid to select all,
        # possibly valid to select specific.
        is_exact = is_select_exact(self.index, self.engine)

        if select == "projected":
            if not self.index:
                raise ValueError("Cannot select 'projected' attributes"
                                 " without an index")
            other = self._copy()
            other._select = select
            other._select_columns.clear()
            return other

        elif select == "all":
            if not is_exact:
                raise ValueError("Cannot select 'all' attributes from a GSI"
                                 " (or an LSI in strict mode) unless the"
                                 " index's projection is 'ALL'")
            other = self._copy()
            other._select = select
            other._select_columns.clear()
            return other

        # select is a list of model names, use "specific"
        else:
            if not is_exact:
                missing_attrs = set(select) - self.index.projection_attributes
                if missing_attrs:
                    msg = ("Index projection is missing the following expected"
                           " attributes, and is either a GSI or an LSI and"
                           " strict mode is enabled: {}").format(missing_attrs)
                    raise ValueError(msg)
            other = self._copy()
            other._select = "specific"
            other._select_columns = select
            return other

    def __iter__(self):
        return iter(self.all())


class Query(_Filter):
    filter_type = "query"

    def _generate_request(self, renderer):
        request = super()._generate_request(renderer)
        request["ScanIndexForward"] = self._forward

        if not self._key_condition:
            raise ValueError("Must specify at least a hash key condition")
        renderer.render(self._key_condition, mode="key")
        request.update(renderer.rendered)
        return request


class Scan(_Filter):
    filter_type = "scan"


class FilterResult(object):
    """
    Result from a scan or query.  Usually lazy loaded, iterate to execute.

    Uses engine.prefetch to control call batching
    """
    def __init__(self, prefetch, call, request, engine, model, expected):
        self._call = call
        self._prefetch = validate_prefetch(prefetch)
        self.request = request
        self.engine = engine
        self.model = model
        self.expected = expected

        self.count = 0
        self.scanned_count = 0
        self._results = []
        self._continue = None
        self._complete = False

        # Kick off the full execution
        if prefetch == "all":
            consume(self)

    @property
    def complete(self):
        return self._complete

    @property
    def first(self):
        # Advance until we have some results, or we exhaust the query
        step = iter(self)
        while not self._results and not self.complete:
            try:
                next(step)
            except StopIteration:
                # The step above exhausted the results, nothing left
                break

        if not self._results:
            raise ValueError("No results found.")
        return self._results[0]

    @property
    def results(self):
        if not self.complete:
            raise RuntimeError("Can't access results until request exhausted")
        return self._results

    def __iter__(self):
        # Already finished, iterate existing list
        if self.complete:
            return iter(self.results)
        # Fully exhaust the filter before returning an iterator
        elif self._prefetch == "all":
            # Give self._continue a chance to be not None
            consume(self._step())
            while self._continue:
                consume(self._step())
            self._complete = True
            return iter(self.results)
        # Lazy load, prefetching as necessary
        else:
            return self._prefetch_iter()

    def _prefetch_iter(self):
        """
        Separate function because the `yield` statement would turn __iter__
        into a generator when we want to return existing iterators in some
        cases.
        """
        while not self.complete:
            prefetch = self._prefetch

            objs = list(self._step())
            while self._continue and prefetch:
                prefetch -= 1
                # Doesn't need the same catch on StopIteration as in `first`
                # since self._continue would be set on the above _step call
                objs.extend(self._step())
            for obj in objs:
                    yield obj

            # Don't set complete until we've
            # yielded all objects from this step
            if not self._continue:
                self._complete = True

    def _step(self):
        """ Single call, advancing ExclusiveStartKey if necessary. """
        if self._continue:
            self.request["ExclusiveStartKey"] = self._continue
        response = self._call(self.request)
        self._continue = response.get("LastEvaluatedKey", None)

        self.count += response["Count"]
        self.scanned_count += response["ScannedCount"]

        results = response.get("Items", [])
        for result in results:
            obj = self.engine._instance(self.model)
            self.engine._update(obj, result, self.expected)
            bloop.tracking.sync(obj, self.engine)

            self._results.append(obj)
            yield obj
