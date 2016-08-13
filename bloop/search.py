import collections

from .exceptions import ConstraintViolation
__all__ = ["Search", "Scan", "Query", "PreparedSearch", "SearchIterator", "ScanIterator", "QueryIterator"]


def search_repr(cls, model, index):
    if model:
        if index:
            return "<{}[{}.{}]>".format(cls.__name__, model.__name__, index.model_name)
        else:
            return "<{}[{}]>".format(cls.__name__, model.__name__)
    else:
        if index:
            return "<{}[None.{}]>".format(cls.__name__, index.model_name)
        else:
            return "<{}[None]>".format(cls.__name__)


class Search:
    mode = None

    def __init__(
            self, session=None,
            model=None, index=None, key=None,
            filter=None, select=None, limit=None,
            strict=True, consistent=False, forward=True):
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

    def __repr__(self):
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
        self._select_columns = None
        self.limit = None
        self._prepared_request = None
        self._iterator_cls = None

    def prepare(self, mode=None, session=None,
                model=None, index=None, key=None, filter=None, select=None,
                limit=None, strict=None, consistent=None, forward=None):

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

    def iterator(self):
        return self._iterator_cls(
            session=self.session,
            model=self.model,
            index=self.index,
            request=self._prepared_request,
            limit=self.limit,
        )


class SearchIterator:
    def __init__(self, session, request, limit, model, index):
        self._session = session
        self._request = request
        self._limit = limit
        self._model = model
        self._index = index

        self._buffer = collections.deque()
        self._count = 0
        self._scanned = 0
        self._yielded = 0
        self._exhausted = False

    def one(self):
        self.reset()
        first = next(self, None)
        second = next(self, None)
        if (first is None) or (second is not None):
            raise ConstraintViolation(
                self.__class__.__name__ + ".one", self._request)
        return first

    def first(self):
        self.reset()
        value = next(self, None)
        if value is None:
            raise ConstraintViolation(
                self.__class__.__name__ + ".first", self._request)
        return value

    def reset(self):
        self._buffer.clear()
        self._count = 0
        self._scanned = 0
        self._yielded = 0
        self._exhausted = False

    @property
    def count(self):
        return self._count

    @property
    def scanned(self):
        return self._scanned

    @property
    def exhausted(self):
        reached_limit = self._limit and self._yielded >= self._limit
        exhausted_buffer = self._exhausted and len(self._buffer) == 0
        return reached_limit or exhausted_buffer

    def __repr__(self):
        return search_repr(self.__class__, self._model, self._index)

    def __iter__(self):
        return self

    def __next__(self):
        #TODO
        pass


class ScanIterator(SearchIterator):
    mode = "scan"


class QueryIterator(SearchIterator):
    mode = "query"
