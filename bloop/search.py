import collections

from .conditions import BaseCondition, iter_columns, render
from .exceptions import ConstraintViolation, InvalidSearch
from .models import Column, GlobalSecondaryIndex, unpack_from_dynamodb
from .signals import object_loaded


__all__ = ["ScanIterator", "Search", "QueryIterator"]


def printable_query(query_on):
    # Model.Meta -> Model
    if getattr(query_on, "__name__", "") == "Meta":
        return query_on.model
    # Index -> Index
    return query_on


def search_repr(cls, model, index):
    if model is not None:
        if index is not None:
            return "<{}[{}.{}]>".format(cls.__name__, model.__name__, index.name)
        else:
            return "<{}[{}]>".format(cls.__name__, model.__name__)
    else:
        if index is not None:
            return "<{}[None.{}]>".format(cls.__name__, index.name)
        else:
            return "<{}[None]>".format(cls.__name__)


def validate_search_mode(mode):
    if mode not in {"query", "scan"}:
        raise InvalidSearch("{!r} is not a valid search mode.".format(mode))


def validate_key_condition(model, index, key):
    # Model will always be provided, but Index has priority
    query_on = index or model.Meta

    # (model or index).hash_key == value
    # Valid for both (hash,) and (hash, range)
    if check_hash_key(query_on, key):
        return

    # Failed.  Without a range key, the check above is the only valid key condition.
    if query_on.range_key is None:
        fail_bad_hash(query_on)

    # If the model or index has a range key, the condition can
    # still be (hash key condition AND range key condition)

    if not isinstance(key, BaseCondition) or key.operation != "and":
        # Too many options to fit into a useful error message.
        fail_bad_range(query_on)

    # This intentionally disallows an AND with just one hash key condition.
    # Otherwise we get into unpacking arbitrarily nested conditions.
    if len(key) != 2:
        fail_bad_range(query_on)
    first_key, second_key = key.values

    # Only two options left -- just try both.
    if check_hash_key(query_on, first_key) and check_range_key(query_on, second_key):
        return
    if check_range_key(query_on, first_key) and check_hash_key(query_on, second_key):
        return

    # Nothing else is valid.
    fail_bad_range(query_on)


def validate_search_projection(model, index, projection):
    if not projection:
        raise InvalidSearch("The projection must be 'count', 'all', or a list of Columns to include.")
    if projection == "count":
        return None

    if projection == "all":
        return (index or model.Meta).projection["included"]
    elif isinstance(projection, str):
        raise InvalidSearch("The projection must be 'count', 'all', or a list of Columns to include.")

    # Keep original around for error messages
    original_projection = projection

    # name -> Column
    if all(isinstance(p, str) for p in projection):
        by_name = model.Meta.columns_by_name
        # This could be a list comprehension, but then the
        # user gets a KeyError when they passed a list.  So,
        # do each individually and throw a useful exception.
        converted_projection = []
        for p in projection:
            try:
                converted_projection.append(by_name[p])
            except KeyError:
                raise InvalidSearch("{!r} is not a column of {!r}.".format(p, model))
        projection = converted_projection

    # Could have been str/Column mix, or just not Columns.
    if not all(isinstance(p, Column) for p in projection):
        raise InvalidSearch(
            "{!r} is not valid: it must be only Columns or only their model names.".format(original_projection))

    # Can the full available columns support this projection?
    if set(projection) <= (index or model.Meta).projection["available"]:
        return projection

    raise InvalidSearch(
        "{!r} includes columns that are not available for {!r}.".format(
            original_projection, printable_query(index or model.Meta)))


def validate_filter_condition(condition, available_columns, column_blacklist):
    if condition is None:
        return

    for column in iter_columns(condition):
        # All of the columns in the condition must be in the available columns
        if column not in available_columns:
            raise InvalidSearch(
                "{!r} is not available for the projection.".format(column))
        # If this is a query, the condition can't contain the hash or range keys.
        # Those are passed in as the column_blacklist.
        if column in column_blacklist:
            raise InvalidSearch("{!r} can not be included in the filter condition.".format(column))


def check_hash_key(query_on, key):
    """Only allows == against query_on.hash_key"""
    return (
        isinstance(key, BaseCondition) and
        (key.operation == "==") and
        (key.column is query_on.hash_key)
    )


def check_range_key(query_on, key):
    """BeginsWith, Between, or any Comparison except '!=' against query_on.range_key"""
    return (
        isinstance(key, BaseCondition) and
        key.operation in ("begins_with", "between", "<", ">", "<=", ">=", "==") and
        key.column is query_on.range_key
    )


def fail_bad_hash(query_on):
    msg = "The key condition for a Query on {!r} must be `{}.{} == value`."
    raise InvalidSearch(msg.format(
        printable_query(query_on), query_on.model.__name__, query_on.hash_key.name))


def fail_bad_range(query_on):
    msg = "Invalid key condition for a Query on {!r}."
    raise InvalidSearch(msg.format(printable_query(query_on)))


class Search:
    """A user-created search object.

    Used to prepare a :class:`~bloop.search.PreparedSearch` which build search iterators.

    :param str mode: Search type, either "query" or "scan".
    :param engine: :class:`~bloop.engine.Engine` to unpack models with.
    :param model: :class:`~bloop.models.BaseModel` being searched.
    :param index: :class:`~bloop.models.Index` to search, or None.
    :param key: *(Query only)* Key condition.  This must include an equality against the hash key,
        and optionally one of a restricted set of conditions on the range key.
    :param filter: Filter condition.  Only matching objects will be included in the results.
    :param projection: "all", "count", a list of column names, or a list of :class:`~bloop.models.Column`.
        When projection is "count", you must advance the iterator to retrieve the count.
    :param bool consistent: Use `strongly consistent reads`__ if True.  Not applicable to GSIs.  Default is False.
    :param bool forward: *(Query only)* Use ascending or descending order.  Default is True (ascending).
    :param tuple parallel: *(Scan only)* A tuple of (Segment, TotalSegments) for this portion of a `parallel scan`__.
            Default is None.

    __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
    __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#QueryAndScanParallelScan
    """

    def __init__(
            self, mode=None, engine=None, model=None, index=None, key=None, filter=None,
            projection=None, consistent=False, forward=True, parallel=None):
        self.mode = mode
        self.engine = engine
        self.model = model
        self.index = index
        self.key = key
        self.filter = filter
        self.projection = projection
        self.consistent = consistent
        self.forward = forward
        self.parallel = parallel

    def __repr__(self):
        return search_repr(self.__class__, self.model, self.index)

    def prepare(self):
        """Constructs a :class:`~bloop.search.PreparedSearch`."""
        p = PreparedSearch()
        p.prepare(
            engine=self.engine,
            mode=self.mode,
            model=self.model,
            index=self.index,
            key=self.key,
            filter=self.filter,
            projection=self.projection,
            consistent=self.consistent,
            forward=self.forward,
            parallel=self.parallel
        )
        return p


class PreparedSearch:
    """Mutable search object.

     Creates :class:`~bloop.search.SearchModelIterator` objects which can be
     used to iterate the results of a query or search multiple times.
     """
    def __init__(self):
        self.engine = None
        self.mode = None
        self._iterator_cls = None

        self.model = None
        self.index = None
        self.consistent = None

        self.key = None

        self._projected_columns = None
        self._projection_mode = None

        self.filter = None

        self.forward = None
        self.parallel = None

        self._request = None

    def prepare(
            self, engine=None, mode=None, model=None, index=None, key=None,
            filter=None, projection=None, consistent=None, forward=None, parallel=None):
        """Validates the search parameters and builds the base request dict for each Query/Scan call."""

        self.prepare_iterator_cls(engine, mode)
        self.prepare_model(model, index, consistent)
        self.prepare_key(key)
        self.prepare_projection(projection)
        self.prepare_filter(filter)
        self.prepare_constraints(forward, parallel)

        self.prepare_request()

    def prepare_iterator_cls(self, engine, mode):
        self.engine = engine
        self.mode = mode
        validate_search_mode(mode)
        self._iterator_cls = ScanIterator if mode == "scan" else QueryIterator

    def prepare_model(self, model, index, consistent):
        self.model = model
        self.index = index
        self.consistent = consistent

    def prepare_key(self, key):
        if self.mode != "query":
            return
        self.key = key
        validate_key_condition(self.model, self.index, self.key)

    def prepare_projection(self, projection):
        self._projected_columns = validate_search_projection(self.model, self.index, projection)

        if self._projected_columns is None:
            self._projection_mode = "count"
        else:
            # Everything else is specific, even "all" on a non-strict LSI.
            # A table could have columns than this model doesn't cares about;
            # don't load those when they'll be discarded immediately.
            self._projection_mode = "specific"

    def prepare_filter(self, filter):
        self.filter = filter
        if self.mode == "query":
            # Query filters can't include the key columns
            column_blacklist = (self.index or self.model.Meta).keys
        else:
            column_blacklist = set()
        available_columns = (self.index or self.model.Meta).projection["available"]
        validate_filter_condition(self.filter, available_columns, column_blacklist)

    def prepare_constraints(self, forward, parallel):
        self.forward = forward
        self.parallel = parallel

    def prepare_request(self):
        request = self._request = {}
        request["TableName"] = self.engine._compute_table_name(self.model)
        request["ConsistentRead"] = self.consistent

        if self.mode == "scan":
            if self.parallel:
                request["Segments"], request["TotalSegments"] = self.parallel
        else:
            request["ScanIndexForward"] = self.forward

        if self.index:
            request["IndexName"] = self.index.dynamo_name
            # GSI isn't strongly consistent
            if isinstance(self.index, GlobalSecondaryIndex):
                del request["ConsistentRead"]

        if self._projection_mode == "count":
            request["Select"] = "COUNT"
            projected = None
        else:
            request["Select"] = "SPECIFIC_ATTRIBUTES"
            projected = self._projected_columns

        request.update(render(self.engine, filter=self.filter, projection=projected, key=self.key))

    def __repr__(self):
        return search_repr(self.__class__, self.model, self.index)

    def __iter__(self):
        return self._iterator_cls(
            engine=self.engine,
            model=self.model,
            index=self.index,
            request=self._request,
            projected=self._projected_columns
        )


class SearchIterator:
    """Reusable search iterator.

    :param session: :class:`~bloop.session.SessionWrapper` to make Query, Scan calls.
    :param model: :class:`~bloop.models.BaseModel` for repr only.
    :param index: :class:`~bloop.models.Index` to search, or None.
    :param dict request: The base request dict for each search.
    :param set projected: Set of :class:`~bloop.models.Column` that should be included in each result.
    """
    mode = "<mode-placeholder>"

    def __init__(self, *, session, model, index, request, projected):
        self.session = session
        self.request = request

        self.model = model
        self.index = index
        self.projected = projected

        self.buffer = collections.deque()

        self._count = 0
        self._scanned = 0
        self._exhausted = False
        self._last_yielded = None

    @property
    def token(self):
        """
        JSON-serializable representation of the current SearchIterator state.

        Use :func:`iterator.move_to(token) <bloop.search.SearchIterator.move_to>` to move an iterator to this position.

        Implementations will always include a "ExclusiveStartKey" key but may include additional metadata.
        The iterator's ``count`` and ``scanned`` values are not preserved.

        :returns: Iterator state as a json-friendly dict
        """
        if self._last_yielded is None:
            # If the iterator isn't advanced but the user calls .move_to, ESK will not be None
            # Otherwise, this returns {"ESK": None}
            esk = self.request.get("ExclusiveStartKey")
        else:
            # table keys are always included (since they're always loaded)
            # index keys are included if there's an index
            keys = self.model.Meta.keys | (self.index.keys if self.index else set())
            keys = (k.dynamo_name for k in keys)
            esk = {k: self._last_yielded[k] for k in keys}
        return {"ExclusiveStartKey": esk}

    @property
    def count(self):
        """Number of items that have been loaded from DynamoDB so far, including buffered items."""
        if self.request["Select"] == "COUNT":
            while not self.exhausted:
                next(self, None)
        return self._count

    @property
    def scanned(self):
        """Number of items that DynamoDB evaluated, before any filter was applied."""
        if self.request["Select"] == "COUNT":
            while not self.exhausted:
                next(self, None)
        return self._scanned

    def all(self):
        """Eagerly load all results and return a single list.  If there are no results, the list is empty.

        :return: A list of results.
        """
        self.reset()
        return list(self)

    def first(self):
        """Return the first result.  If there are no results, raises :exc:`~bloop.exceptions.ConstraintViolation`.

        :return: The first result.
        :raises bloop.exceptions.ConstraintViolation: No results.
        """
        self.reset()
        value = next(self, None)
        if value is None:
            raise ConstraintViolation("{} did not find any results.".format(self.mode.capitalize()))
        return value

    def move_to(self, token):
        """Restore an iterator to the state stored in a token.  This will reset all iterator state, including
        ``count``, ``scanned``, and ``exhausted`` properties.

        :param token: a :attr:`SearchIterator.token <bloop.search.SearchIterator.token>`
        """
        esk = token["ExclusiveStartKey"]
        self.reset()
        # Don't set to None since boto3 doesn't like an explicit None
        if esk:
            self.request["ExclusiveStartKey"] = esk
        self._last_yielded = esk

    def one(self):
        """Return the unique result.  If there is not exactly one result,
        raises :exc:`~bloop.exceptions.ConstraintViolation`.

        :return: The unique result.
        :raises bloop.exceptions.ConstraintViolation: Not exactly one result.
        """
        first = self.first()
        second = next(self, None)
        if second is not None:
            raise ConstraintViolation("{} found more than one result.".format(self.mode.capitalize()))
        return first

    def reset(self):
        """Reset to the initial state, clearing the buffer and zeroing count and scanned."""
        self.buffer.clear()
        self._count = 0
        self._scanned = 0
        self._exhausted = False
        self._last_yielded = None
        self.request.pop("ExclusiveStartKey", None)

    @property
    def exhausted(self):
        """True if there are no more results."""
        return self._exhausted and len(self.buffer) == 0

    def __repr__(self):
        return search_repr(self.__class__, self.model, self.index)

    def __iter__(self):
        return self

    def __next__(self):
        while (not self._exhausted) and len(self.buffer) == 0:
            response = self.session.search_items(self.mode, self.request)
            continuation_token = response.get("LastEvaluatedKey", None)
            if continuation_token:
                self.request["ExclusiveStartKey"] = continuation_token
            self._exhausted = not continuation_token

            self._count += response["Count"]
            self._scanned += response["ScannedCount"]

            # Each item is a dict of attributes
            self.buffer.extend(response.get("Items", []))

        if self.buffer:
            self._last_yielded = self.buffer.popleft()
            return self._last_yielded

        # Buffer must be empty (if _buffer)
        # No more continue tokens (while not _exhausted)
        raise StopIteration


class SearchModelIterator(SearchIterator):
    """Reusable search iterator that unpacks result dicts into model instances.

    :param engine: :class:`~bloop.engine.Engine` to unpack models with.
    :param model: :class:`~bloop.models.BaseModel` being searched.
    :param index: :class:`~bloop.models.Index` to search, or None.
    :param dict request: The base request dict for each search call.
    :param set projected: Set of :class:`~bloop.models.Column` that should be included in each result.
    """
    def __init__(self, *, engine, model, index, request, projected):
        self.engine = engine

        self.model = model

        super().__init__(
            session=engine.session, model=model, index=index,
            request=request, projected=projected)

    def __next__(self):
        attrs = super().__next__()
        obj = unpack_from_dynamodb(
            attrs=attrs,
            expected=self.projected,
            model=self.model,
            engine=self.engine)
        object_loaded.send(self.engine, engine=self.engine, obj=obj)
        return obj


# noinspection PyUnresolvedReferences
class ScanIterator(SearchModelIterator):
    """Reusable scan iterator that unpacks result dicts into model instances.

    Returned from :func:`Engine.scan <bloop.engine.Engine.scan>`.

    :param engine: :class:`~bloop.engine.Engine` to unpack models with.
    :param model: :class:`~bloop.models.BaseModel` being scanned.
    :param index: :class:`~bloop.models.Index` to scan, or None.
    :param dict request: The base request dict for each Scan call.
    :param set projected: Set of :class:`~bloop.models.Column` that should be included in each result.
    """
    mode = "scan"


# noinspection PyUnresolvedReferences
class QueryIterator(SearchModelIterator):
    """Reusable query iterator that unpacks result dicts into model instances.

    Returned from :func:`Engine.query <bloop.engine.Engine.query>`.

    :param engine: :class:`~bloop.engine.Engine` to unpack models with.
    :param model: :class:`~bloop.models.BaseModel` being queried.
    :param index: :class:`~bloop.models.Index` to query, or None.
    :param dict request: The base request dict for each Query call.
    :param set projected: Set of :class:`~bloop.models.Column` that should be included in each result.
    """
    mode = "query"
