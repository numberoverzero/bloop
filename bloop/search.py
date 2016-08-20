import collections

from .exceptions import ConstraintViolation
from .expressions import render
from .models import GlobalSecondaryIndex, available_columns_for
from .tracking import sync
from .util import unpack_from_dynamodb
from .validation import (
    validate_filter_condition,
    validate_key_condition,
    validate_search_mode,
    validate_search_projection
)

__all__ = ["Search", "PreparedSearch", "SearchIterator", "Scan", "Query", "ScanIterator", "QueryIterator"]


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


class Search:
    mode = None

    def __init__(
            self, engine=None, session=None, model=None, index=None, key=None, filter=None,
            projection=None, limit=None, strict=True, consistent=False, forward=True):
        self.engine = engine
        self.session = session
        self.model = model
        self.index = index
        self.key = key
        self.filter = filter
        self.projection = projection
        self.limit = limit
        self.strict = strict
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
            strict=self.strict,
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

        self.strict = None
        self._available_columns = None
        self._projected_columns = None
        self._projection_mode = None

        self.filter = None

        self.limit = None
        self.forward = None

        self._request = None

    def prepare(
            self, engine=None, mode=None, session=None, model=None, index=None, key=None,
            filter=None, projection=None, limit=None, strict=None, consistent=None, forward=None):

        self.prepare_session(engine, session, mode)
        self.prepare_model(model, index, consistent)
        self.prepare_key(key)
        self.prepare_projection(projection, strict)
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

    def prepare_projection(self, projection, strict):
        self.strict = strict
        self._available_columns = available_columns_for(self.model, self.index, self.strict)
        self._projected_columns = validate_search_projection(self.model, self.index, self.strict, projection)

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
        validate_filter_condition(self.filter, self._available_columns, column_blacklist)

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

        rendered = render(self.engine, filter=self.filter, select=projected, key=self.key)
        request.update(rendered)

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

    def __init__(self, *, session, model, index, limit, request, projected, **kwargs):
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
    def __init__(self, *, engine, **kwargs):
        self.engine = engine
        super().__init__(**kwargs)

    def __next__(self):
        attrs = super().__next__()
        obj = unpack_from_dynamodb(
            attrs=attrs,
            expected=self.projected,
            model=self.model,
            engine=self.engine)
        sync(obj, self.engine)
        return obj


class ScanIterator(SearchModelIterator):
    mode = "scan"


class QueryIterator(SearchModelIterator):
    mode = "query"
