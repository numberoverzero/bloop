from .util import Sentinel

__all__ = ["Search", "Scan", "Query"]

scan = Sentinel("scan")
query = Sentinel("query")


def cls_name(obj):
    return obj.__class__.__name__


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
        if self.model:
            if self.index:
                return "<{}[{}.{}]>".format(cls_name(self), self.model.__name__, self.index.model_name)
            else:
                return "<{}[{}]>".format(cls_name(self), self.model.__name__)
        else:
            if self.index:
                return "<{}[None.{}]>".format(cls_name(self), self.index.model_name)
            else:
                return "<{}[None]>".format(cls_name(self))

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
    mode = scan


class Query(Search):
    mode = query


class PreparedSearch:
    def __init__(self):
        self.model = None
        self.index = None

        self.key = None
        self.filter = None

        self._select_mode = None
        self._select_columns = None

        self.limit = None
        self.strict = None

        self._session_method = None
        self._prepared_request = None

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
