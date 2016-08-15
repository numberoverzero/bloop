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

# TODO
# swap Engine.scan, Engine.query to use Query, Scan.
#
# User syntax will look like this:
#
#     scan = engine.scan(
#         User,
#         key=(User.name == "foo"),
#         filter=(User.created_on > yesterday),
#         select={User.data, User.final},
#         strict=False
#     )
#
# Engine will do the following:
#
# def scan(...):
#     s = Scan(
#         ...
#         ...
#     )
#     return iter(s.prepare())
#


class Search:
    mode = None

    def __init__(
            self, engine=None, session=None, model=None, index=None, key=None, filter=None,
            select=None, limit=None, strict=True, consistent=False, forward=True):
        self.engine = engine
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
            engine=self.engine,
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
        self.engine = None
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
            self, engine=None, mode=None, session=None, model=None, index=None, key=None,
            filter=None, select=None, limit=None, strict=None, consistent=None, forward=None):

        self.prepare_session(engine, session, mode)
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
            engine=self.engine,
            session=self.session,
            model=self.model,
            index=self.index,
            limit=self.limit,
            request=self._request,
            projected=self._projected_columns
        )


class SearchIterator:
    mode = None

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


class SearchModelIterator(SearchIterator):
    def __init__(self, *, engine, **kwargs):
        self.engine = engine
        super().__init__(**kwargs)

    def __next__(self):
        # TODO unpack super through engine
        object_dict = super().__next__()
        return object_dict


class ScanIterator(SearchModelIterator):
    mode = "scan"


class QueryIterator(SearchModelIterator):
    mode = "query"
