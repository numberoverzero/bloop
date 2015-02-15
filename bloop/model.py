import bloop.column
import bloop.dynamo
import declare
missing = object()


def is_column(field):
    return isinstance(field, bloop.column.Column)


def is_index(field):
    return isinstance(field, bloop.column.Index)


class __BaseModel(object):
    '''
    do not subclass directly.  use `BaseModel` which sets
    up the correct metaclass.
    '''
    def __init__(self, **attrs):
        columns = self.__meta__["dynamo.columns"]
        for column in columns:
            value = attrs.get(column.model_name, missing)
            if value is not missing:
                setattr(self, column.model_name, value)

    @classmethod
    def __load__(cls, values):
        ''' dict -> obj '''
        meta = cls.__meta__
        columns = meta["dynamo.columns"]
        engine = meta["bloop.engine"].type_engine
        attrs = {}
        for column in columns:
            value = values.get(column.dynamo_name, missing)
            # Missing expected column
            if value is not missing:
                attrs[column.model_name] = engine.load(column.typedef, value)
        return cls(**attrs)

    @classmethod
    def __dump__(cls, obj):
        ''' obj -> dict '''
        meta = cls.__meta__
        columns = meta["dynamo.columns"]
        engine = meta["bloop.engine"].type_engine
        attrs = {}
        for column in columns:
            value = getattr(obj, column.model_name, missing)
            # Missing expected column
            if value is not missing:
                attrs[column.dynamo_name] = engine.dump(column.typedef, value)
        return attrs

    def __str__(self):
        cls_name = self.__class__.__name__
        columns = self.__class__.__meta__["dynamo.columns"]

        def _attr(attr):
            return "{}={}".format(attr, repr(getattr(self, attr, None)))
        attrs = ", ".join(_attr(c.model_name) for c in columns)
        return "{}({})".format(cls_name, attrs)
    __repr__ = __str__


def BaseModel(engine):
    '''
    A metaclass per engine is necessary so that subclasses of __BaseModel
    are correctly registered with the engine
    '''
    class ModelMetaclass(declare.ModelMetaclass):
        def __new__(metaclass, name, bases, attrs):
            model = super().__new__(metaclass, name, bases, attrs)
            meta = model.__meta__

            # Load columns, indexes, hash_key, range_key
            # ----------------------------------------------------------
            columns = list(filter(is_column, model.__meta__['fields']))
            indexes = list(filter(is_index, columns))

            # Remove indexes from columns since they're treated differently
            for index in indexes:
                index.model = model
                columns.remove(index)

            meta['dynamo.indexes'] = indexes
            meta['dynamo.columns'] = columns
            meta['dynamo.columns.by.model_name'] = declare.index(
                columns, 'model_name')
            meta['dynamo.columns.by.dynamo_name'] = declare.index(
                columns, 'dynamo_name')

            for column in columns:
                if column.hash_key:
                    meta['dynamo.table.hash_key'] = column
                    break
            else:
                meta['dynamo.table.hash_key'] = None

            for column in columns:
                if column.range_key:
                    meta['dynamo.table.range_key'] = column
                    break
            else:
                meta['dynamo.table.range_key'] = None

            meta["bloop.engine"] = engine

            meta["dynamo.table.name"] = meta.get(
                "dynamo.table.name", model.__name__)
            meta["dynamo.table.write_units"] = meta.get(
                "dynamo.table.write_units", 1)
            meta["dynamo.table.read_units"] = meta.get(
                "dynamo.table.read_units", 1)

            # If the engine already has a base, register this model.
            # Otherwise, this probably IS the engine's base model
            if engine.model:
                engine.register(model)
            return model
    return ModelMetaclass("Model", (__BaseModel,), {})
