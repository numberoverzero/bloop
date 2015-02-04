import bloop.column
import bloop.dynamo_util
import declare
missing = object()


class __BaseModel(object):
    '''
    do not subclass directly.  use `base_model` which sets
    up the correct metaclass.
    '''
    def __init__(self, **attrs):
        columns = self.__meta__["columns_by_name"]
        for name, column in columns.items():
            value = attrs.get(name, missing)
            # Missing expected column
            if value is missing:
                continue
            setattr(self, name, value)

    @classmethod
    def __load__(cls, values):
        ''' dict -> obj '''
        meta = cls.__meta__
        columns = meta["columns"]
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
        columns = meta["columns"]
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

            # Load and index columns
            # ----------------------------------------------------------
            is_column = lambda field: isinstance(field, bloop.column.Column)
            columns = list(filter(is_column, model.__meta__['fields']))
            meta = model.__meta__
            meta['columns'] = columns
            meta['columns_by_name'] = declare.index(columns, 'model_name')

            # Entry point for model population.  By default this is the
            # model class.  Custom subclasses of the engine's
            # base model should specify the meta property "bloop.init",
            # which should be a function taking a **kwarg of name:value
            # pairs corresponding to modeled columns.

            meta["bloop.init"] = meta.get("bloop.init", model)
            meta["bloop.engine"] = engine

            meta["table_name"] = meta.get("table_name", model.__name__)
            meta["write_units"] = meta.get("write_units", 1)
            meta["read_units"] = meta.get("read_units", 1)

            # If the engine already has a base, register this model.
            # Otherwise, this probably IS the engine's base model
            if engine.model:
                engine.register(model)
            return model
    return ModelMetaclass("Model", (__BaseModel,), {})
