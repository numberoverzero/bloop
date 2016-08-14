import collections

from .exceptions import ConstraintViolation

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
            self, session=None, model=None, index=None, key=None, filter=None,
            select=None, limit=None, strict=True, consistent=False, forward=True):
        self.session = session
        self.model = model
        self.index = index
        self.key = key
        self.filter = filter
        self.select = select
        self.limit = limit
        self.strict = strict
        self.consistent = consistent
        self.forward = forward

    def __repr__(self):  # pragma: no cover
        return search_repr(self.__class__, self.model, self.index)

    def prepare(self):
        p = PreparedSearch()
        p.prepare(
            mode=self.mode,
            session=self.session,
            model=self.model,
            index=self.index,
            key=self.key,
            filter=self.filter,
            select=self.select,
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
        self.session = None
        self.model = None
        self.mode = None
        self.index = None
        self.key = None
        self.filter = None
        self._select_mode = None
        self._projected_columns = None
        self.limit = None
        self._request = None
        self._iterator_cls = None

    def prepare(
            self, mode=None, session=None, model=None, index=None, key=None, filter=None,
            select=None, limit=None, strict=None, consistent=None, forward=None):

        self.prepare_session(session, mode)
        self.prepare_model(model, index, consistent)
        self.prepare_key(key)
        self.prepare_select(select, strict)
        self.prepare_filter(filter)
        self.prepare_constraints(limit, forward)

    def prepare_session(self, session, mode):
        pass

    def prepare_model(self, model, index, consistent):
        pass

    def prepare_key(self, key):
        pass

    def prepare_select(self, select, strict):
        pass

    def prepare_filter(self, filter):
        pass

    def prepare_constraints(self, limit, forward):
        pass

    def __repr__(self):  # pragma: no cover
        return search_repr(self.__class__, self.model, self.index)

    def __iter__(self):
        return self._iterator_cls(
            session=self.session,
            model=self.model,
            index=self.index,
            limit=self.limit,
            request=self._request,
            projected=self._projected_columns
        )


class SearchIterator:
    mode = None

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

    def one(self):
        self.reset()
        first = next(self, None)
        second = next(self, None)
        if (first is None) or (second is not None):
            raise ConstraintViolation("{} did not find exactly one result".format(self.mode.capitalize()))
        return first

    def first(self):
        self.reset()
        value = next(self, None)
        if value is None:
            raise ConstraintViolation("{} did not find any results".format(self.mode.capitalize()))
        return value

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

    def __repr__(self):  # pragma: no cover
        return search_repr(self.__class__, self.model, self.index)

    def __iter__(self):
        return self

    def __next__(self):
        if self.limit and self.yielded >= self.limit:
            raise StopIteration

        while (not self._exhausted) and len(self.buffer) == 0:
            response = self.session.search_items(self.mode, self.request)

            continuation_token = self.request["ExclusiveStartKey"] = response["LastEvaluatedKey"]
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


class ScanIterator(SearchIterator):
    mode = "scan"


class QueryIterator(SearchIterator):
    mode = "query"
