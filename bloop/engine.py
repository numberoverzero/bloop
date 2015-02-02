import bloop.column
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
        columns = meta["columns_by_name"]
        init = meta["bloop.init"]
        engine = meta["bloop.engine"].type_engine
        attrs = {}
        for name, column in columns.items():
            value = values.get(name, missing)
            # Missing expected column
            if value is not missing:
                attrs[name] = engine.load(column.typedef, value)
        return init(**attrs)

    @classmethod
    def __dump__(cls, obj):
        ''' obj -> dict '''
        meta = cls.__meta__
        columns = meta["columns_by_name"]
        engine = meta["bloop.engine"].type_engine
        attrs = {}
        for name, column in columns.items():
            value = getattr(obj, name, missing)
            # Missing expected column
            if value is not missing:
                attrs[name] = engine.dump(column.typedef, value)
        return attrs


def base_model(engine):
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
            meta['columns_by_name'] = declare.index(
                columns, 'model_name')

            # Entry point for model population.  By default this is the
            # model class.  Custom subclasses of the engine's
            # base model should specify the meta property "bloop.init",
            # which should be a function taking a **kwarg of name:value
            # pairs corresponding to modeled columns.

            if "bloop.init" not in meta:
                meta["bloop.init"] = model
            meta["bloop.engine"] = engine

            # If the engine already has a base, register this model.
            # Otherwise, this probably IS the engine's base model
            if engine.model:
                engine.register(model)
            return model
    return ModelMetaclass("Model", (__BaseModel,), {})


class Engine(object):
    model = None

    def __init__(self, namespace=None):
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
        self.type_engine = declare.TypeEngine.unique()
        self.model = base_model(self)

    def register(self, model):
        self.type_engine.register(model)
        columns = model.__meta__['columns']
        for column in columns:
            self.type_engine.register(column.typedef)
        self.type_engine.bind()

    def load(self, model, value):
        return self.type_engine.load(model, value)
