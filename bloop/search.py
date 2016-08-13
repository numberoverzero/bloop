__all__ = ["Search", "Scan", "Query"]


def cls_name(obj):
    return obj.__class__.__name__


class Search:
    def __init__(
            self,
            session=None,
            model=None,
            index=None,
            key=None,
            filter=None,
            select=None,
            limit=None,
            strict=True,
            consistent=False,
            forward=True,
    ):
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


class Scan(Search):
    pass


class Query(Search):
    pass
