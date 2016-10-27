import collections

import declare

from .conditions import BaseCondition, iter_columns, render
from .exceptions import (
    ConstraintViolation,
    InvalidFilterCondition,
    InvalidKeyCondition,
    InvalidProjection,
    UnknownSearchMode,
)
from .models import Column, GlobalSecondaryIndex
from .signals import object_loaded
from .util import printable_column_name, printable_query, unpack_from_dynamodb


__all__ = ["Scan", "Query", "ScanIterator", "QueryIterator"]


def search_repr(cls, model, index):
    if model is not None:
        if index is not None:
            return "<{}[{}.{}]>".format(cls.__name__, model.__name__, index.model_name)
        else:
            return "<{}[{}]>".format(cls.__name__, model.__name__)
    else:
        if index is not None:
            return "<{}[None.{}]>".format(cls.__name__, index.model_name)
        else:
            return "<{}[None]>".format(cls.__name__)


def validate_search_mode(mode):
    if mode not in {"query", "scan"}:
        raise UnknownSearchMode("{!r} is not a valid search mode.".format(mode))


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
        raise InvalidProjection("The projection must be 'count', 'all', or a list of Columns to include.")
    if projection == "count":
        return None

    if projection == "all":
        return (index or model.Meta).projection["included"]

    # Keep original around for error messages
    original_projection = projection

    # model_name -> Column
    if all(isinstance(p, str) for p in projection):
        by_model_name = declare.index(model.Meta.columns, "model_name")
        # This could be a list comprehension, but then the
        # user gets a KeyError when they passed a list.  So,
        # do each individually and throw a useful exception.
        converted_projection = []
        for p in projection:
            try:
                converted_projection.append(by_model_name[p])
            except KeyError:
                raise InvalidProjection("{!r} is not a column of {!r}.".format(p, model))
        projection = converted_projection

    # Could have been str/Column mix, or just not Columns.
    if not all(isinstance(p, Column) for p in projection):
        raise InvalidProjection(
            "{!r} is not valid: it must be only Columns or only their model names.".format(original_projection))

    # Can the full available columns support this projection?
    if set(projection) <= (index or model.Meta).projection["available"]:
        return projection

    raise InvalidProjection(
        "{!r} includes columns that are not available for {!r}.".format(
            original_projection, printable_query(index or model.Meta)))


def validate_filter_condition(condition, available_columns, column_blacklist):
    if condition is None:
        return

    for column in iter_columns(condition):
        # All of the columns in the condition must be in the available columns
        if column not in available_columns:
            raise InvalidFilterCondition(
                "{!r} is not available for the projection.".format(column))
        # If this is a query, the condition can't contain the hash or range keys.
        # Those are passed in as the column_blacklist.
        if column in column_blacklist:
            raise InvalidFilterCondition("{!r} can not be included in the filter condition.".format(column))


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
    raise InvalidKeyCondition(msg.format(
        printable_query(query_on), query_on.model.__name__, printable_column_name(query_on.hash_key)))


def fail_bad_range(query_on):
    msg = "Invalid key condition for a Query on {!r}."
    raise InvalidKeyCondition(msg.format(printable_query(query_on)))


class Search:
    mode = None

    def __init__(
            self, engine=None, session=None, model=None, index=None, key=None, filter=None,
            projection=None, limit=None, consistent=False, forward=True):
        self.engine = engine
        self.session = session
        self.model = model
        self.index = index
        self.key = key
        self.filter = filter
        self.projection = projection
        self.limit = limit
        self.consistent = consistent
        self.forward = forward

    def __repr__(self):
        return search_repr(self.__class__, self.model, self.index)

    def prepare(self):
        p = PreparedSearch()
        p.prepare(
            engine=self.engine,
            mode=self.mode,
            session=self.session,
            model=self.model,
            index=self.index,
            key=self.key,
            filter=self.filter,
            projection=self.projection,
            limit=self.limit,
            consistent=self.consistent,
            forward=self.forward
        )
        return p


class Scan(Search):
    mode = "scan"


class Query(Search):
    mode = "query"


class PreparedSearch:
    def __init__(self):
        self.engine = None
        self.session = None
        self.mode = None
        self._iterator_cls = None

        self.model = None
        self.index = None
        self.consistent = None

        self.key = None

        self._projected_columns = None
        self._projection_mode = None

        self.filter = None

        self.limit = None
        self.forward = None

        self._request = None

    def prepare(
            self, engine=None, mode=None, session=None, model=None, index=None, key=None,
            filter=None, projection=None, limit=None, consistent=None, forward=None):

        self.prepare_session(engine, session, mode)
        self.prepare_model(model, index, consistent)
        self.prepare_key(key)
        self.prepare_projection(projection)
        self.prepare_filter(filter)
        self.prepare_constraints(limit, forward)

        self.prepare_request()

    def prepare_session(self, engine, session, mode):
        self.engine = engine
        self.session = session
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

    def prepare_constraints(self, limit, forward):
        self.limit = limit
        self.forward = forward

    def prepare_request(self):
        request = self._request = {}
        request["TableName"] = self.model.Meta.table_name
        request["ConsistentRead"] = self.consistent

        if self.mode != "scan":
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

        request.update(render(
            self.engine, filter=self.filter, projection=projected, key=self.key))

    def __repr__(self):
        return search_repr(self.__class__, self.model, self.index)

    def __iter__(self):
        return self._iterator_cls(
            engine=self.engine,
            session=self.session,
            model=self.model,
            index=self.index,
            limit=self.limit,
            request=self._request,
            projected=self._projected_columns
        )


class SearchIterator:
    mode = "<mode-placeholder>"

    def __init__(self, *, session, model, index, limit, request, projected):
        self.session = session
        self.request = request
        self.limit = limit
        self.model = model
        self.index = index
        self.projected = projected

        self.buffer = collections.deque()
        self.count = 0
        self.scanned = 0
        self.yielded = 0
        self._exhausted = False

    def first(self):
        self.reset()
        value = next(self, None)
        if value is None:
            raise ConstraintViolation("{} did not find any results.".format(self.mode.capitalize()))
        return value

    def one(self):
        first = self.first()
        second = next(self, None)
        if second is not None:
            raise ConstraintViolation("{} found more than one result.".format(self.mode.capitalize()))
        return first

    def reset(self):
        self.buffer.clear()
        self.count = 0
        self.scanned = 0
        self.yielded = 0
        self._exhausted = False

    @property
    def exhausted(self):
        reached_limit = self.limit and self.yielded >= self.limit
        exhausted_buffer = self._exhausted and len(self.buffer) == 0
        return reached_limit or exhausted_buffer

    def __repr__(self):
        return search_repr(self.__class__, self.model, self.index)

    def __iter__(self):
        return self

    def __next__(self):
        if self.limit and self.yielded >= self.limit:
            raise StopIteration

        while (not self._exhausted) and len(self.buffer) == 0:
            response = self.session.search_items(self.mode, self.request)
            continuation_token = self.request["ExclusiveStartKey"] = response.get("LastEvaluatedKey", None)
            self._exhausted = not continuation_token

            self.count += response["Count"]
            self.scanned += response["ScannedCount"]

            # Each item is a dict of attributes
            self.buffer.extend(response["Items"])

        if self.buffer:
            self.yielded += 1
            return self.buffer.popleft()

        # Buffer must be empty (if _buffer)
        # No more continue tokens (while not _exhausted)
        raise StopIteration


class SearchModelIterator(SearchIterator):
    def __init__(self, *, engine, session, model, index, limit, request, projected):
        self.engine = engine
        super().__init__(session=session, model=model, index=index,
                         limit=limit, request=request, projected=projected)

    def __next__(self):
        attrs = super().__next__()
        obj = unpack_from_dynamodb(
            attrs=attrs,
            expected=self.projected,
            model=self.model,
            engine=self.engine)
        object_loaded.send(engine=self.engine, obj=obj)
        return obj


class ScanIterator(SearchModelIterator):
    mode = "scan"


class QueryIterator(SearchModelIterator):
    mode = "query"
