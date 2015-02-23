import bloop.column
import declare
missing = object()


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

            attrs["write_units"] = attrs.get("write_units", 1)
            attrs["read_units"] = attrs.get("read_units", 1)
            model = super().__new__(metaclass, name, bases, attrs)
            meta = model.__meta__

            # Load columns, indexes, hash_key, range_key
            # ----------------------------------------------------------
            # These are sets instead of lists, because set uses __hash__
            # while some list operations uses __eq__ which will break
            # with the ComparisonMixin
            columns = set(filter(bloop.column.is_column,
                                 model.__meta__['fields']))
            indexes = set(filter(bloop.column.is_index, columns))

            # Remove indexes from columns since they're treated differently
            # Resolve hash and range keys for indexes
            for index in indexes:
                index.model = model
                columns.remove(index)

            meta['dynamo.indexes'] = indexes
            meta['dynamo.columns'] = columns
            meta['dynamo.columns.by.model_name'] = declare.index(
                columns, 'model_name')
            meta['dynamo.columns.by.dynamo_name'] = declare.index(
                columns, 'dynamo_name')

            model.hash_key = None
            model.range_key = None
            for column in columns:
                if column.hash_key:
                    model.hash_key = column
                elif column.range_key:
                    model.range_key = column

            # Can't do this as part of the above loop since we index after
            # mutating the columns set.  Look up the current hash key (string)
            # in indexed columns and relate proper Column object
            cols = meta['dynamo.columns.by.dynamo_name']
            for index in indexes:
                if bloop.column.is_global_index(index):
                    index._hash_key = cols[index.hash_key]
                elif bloop.column.is_local_index(index):
                    if not model.range_key:
                        raise ValueError(
                            "Cannot specify a LocalSecondaryIndex " +
                            "without a table range key")
                    index._hash_key = model.range_key
                if index.range_key:
                    index._range_key = cols[index.range_key]

            # Entry point for model population. By default this is the
            # model class. Custom subclasses of the engine's
            # base model should specify the meta property "bloop.init",
            # which should be a function taking a **kwarg of name:value
            # pairs corresponding to modeled columns.
            meta["bloop.init"] = meta.get("bloop.init", model)
            meta["bloop.engine"] = engine

            meta["dynamo.table.name"] = meta.get(
                "dynamo.table.name", model.__name__)

            # If the engine already has a base, register this model.
            # Otherwise, this probably IS the engine's base model
            if engine.model:
                engine.register(model)
            return model
    return ModelMetaclass("Model", (__BaseModel,), {})
