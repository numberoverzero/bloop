import bloop.index
import bloop.column
import bloop.dynamo
import declare
missing = object()


class __BaseModel(object):
    '''
    do not subclass directly.  use `base_model` which sets
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
        init = meta["bloop.init"]
        engine = meta["bloop.engine"].type_engine
        attrs = {}
        for column in columns:
            value = values.get(column.dynamo_name, missing)
            # Missing expected column
            if value is not missing:
                attrs[column.model_name] = engine.load(column.typedef, value)
        return init(**attrs)

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


def BaseModel(engine):
    '''
    A metaclass per engine is necessary so that subclasses of __BaseModel
    are correctly registered with the engine
    '''
    class ModelMetaclass(declare.ModelMetaclass):
        def __new__(metaclass, name, bases, attrs):
            model = super().__new__(metaclass, name, bases, attrs)
            meta = model.__meta__

            # Load columns, hash_key, range_key
            # ----------------------------------------------------------
            is_column = lambda field: isinstance(field, bloop.column.Column)
            columns = list(filter(is_column, model.__meta__['fields']))
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
                raise AttributeError(
                    "Must define a hash key for model {}".format(
                        model.__name__))
            for column in columns:
                if column.range_key:
                    meta['dynamo.table.range_key'] = column
                    break

            # Load indexes
            # ----------------------------------------------------------
            indexes = []
            for key, value in attrs.items():
                if isinstance(value, bloop.index.Index):
                    value.model_name = key
                    value.model = model
                    indexes.append(value)
            meta['dynamo.indexes'] = indexes

            # Entry point for model population.  By default this is the
            # model class.  Custom subclasses of the engine's
            # base model should specify the meta property "bloop.init",
            # which should be a function taking a **kwarg of name:value
            # pairs corresponding to modeled columns.

            meta["bloop.init"] = meta.get("bloop.init", model)
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
