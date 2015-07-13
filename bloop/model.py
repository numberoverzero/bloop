import bloop.column
import bloop.index
import declare
missing = object()


class __BaseModel(object):
    '''
    DO NOT SUBCLASS DIRECTLY.

    Instead, subclass the `model` attribute of an engine.  This ensures the
    proper metaclass setup has been performed, so that `engine.bind` will
    work.

    Example:

        engine = bloop.Engine()
        BaseModel = engine.model

        class CustomBaseModel(BaseModel):
            # ... cross-model code goes here
    '''
    def __init__(self, **attrs):
        columns = self.Meta.columns
        # Only set values from **attrs if there's a
        # corresponding `model_name` for a column in the model
        for column in columns:
            value = attrs.get(column.model_name, missing)
            if value is not missing:
                setattr(self, column.model_name, value)

    @classmethod
    def __load__(cls, values):
        ''' dict -> obj '''
        attrs = {}
        engine = cls.Meta.bloop_engine.type_engine
        for column in cls.Meta.columns:
            value = values.get(column.dynamo_name, missing)
            if value is not missing:
                attrs[column.model_name] = engine.load(column.typedef, value)
        return cls.Meta.bloop_init(**attrs)

    @classmethod
    def __dump__(cls, obj):
        ''' obj -> dict '''
        attrs = {}
        columns = cls.Meta.columns
        engine = cls.Meta.bloop_engine.type_engine
        for column in columns:
            value = getattr(obj, column.model_name, missing)
            # Missing expected column
            if value is not missing:
                attrs[column.dynamo_name] = engine.dump(column.typedef, value)
        return attrs

    def __str__(self):  # pragma: no cover
        cls_name = self.__class__.__name__
        columns = self.__class__.Meta.columns

        def _attr(attr):
            return "{}={}".format(attr, repr(getattr(self, attr, None)))
        attrs = ", ".join(_attr(c.model_name) for c in columns)
        return "{}({})".format(cls_name, attrs)
    __repr__ = __str__

    def __eq__(self, other):
        ''' Only checks defined columns. '''
        cls = self.__class__
        if not isinstance(other, cls):
            return False
        for column in cls.Meta.columns:
            value = getattr(self, column.dynamo_name, missing)
            other_value = getattr(other, column.dynamo_name, missing)
            if value != other_value:
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)


def BaseModel(engine):
    '''
    Although this returns a class, you should NOT call this function to create
    a base model class.  Instead, subclass the `model` attribute of an engine.
    Doing this ensures the proper metaclass setup has been performed,
    so that `engine.bind` will work.

    Example:

        engine = bloop.Engine()
        BaseModel = engine.model

        class CustomBaseModel(BaseModel):
            # ... cross-model code goes here
    '''
    if engine.model:
        raise ValueError("BaseModel already exists for engine")

    class ModelMetaclass(declare.ModelMetaclass):
        def __new__(metaclass, name, bases, attrs):

            model = super().__new__(metaclass, name, bases, attrs)
            Meta = model.Meta
            Meta.write_units = getattr(Meta, "write_units", 1)
            Meta.read_units = getattr(Meta, "read_units", 1)

            # column.model_name is set by `declare.ModelMetaclass.__new__`

            # Load columns, indexes, hash_key, range_key
            # ----------------------------------------------------------
            # These are sets instead of lists, because set uses __hash__
            # while some list operations uses __eq__ which will break
            # with the ComparisonMixin
            columns = set(filter(bloop.column.is_column, Meta.fields))
            indexes = set(filter(bloop.index.is_index, Meta.fields))

            Meta.indexes = indexes
            Meta.columns = columns
            Meta.columns_by_model_name = declare.index(columns, 'model_name')
            Meta.columns_by_dynamo_name = declare.index(columns, 'dynamo_name')

            Meta.hash_key = None
            Meta.range_key = None
            for column in columns:
                if column.hash_key:
                    if Meta.hash_key:
                        raise ValueError("Model hash_key over-specified")
                    Meta.hash_key = column
                elif column.range_key:
                    if Meta.range_key:
                        raise ValueError("Model range_key over-specified")
                    Meta.range_key = column

            # Can't do this as part of the above loop since we index after
            # mutating the columns set.  Look up the current hash key
            # -- which is specified by model_name, not dynamo_name --
            # in indexed columns and the relate proper `bloop.Column` object
            cols = Meta.columns_by_model_name
            for index in indexes:
                if bloop.index.is_global_index(index):
                    index._hash_key = cols[index.hash_key]
                elif bloop.index.is_local_index(index):
                    if not Meta.range_key:
                        raise ValueError(
                            "Cannot specify a LocalSecondaryIndex " +
                            "without a table range key")
                    index._hash_key = Meta.hash_key
                else:
                    raise ValueError("Index is an abstract class, must specify"
                                     "LocalSecondaryIndex or"
                                     "GlobalSecondaryIndex")

                if index.range_key:
                    index._range_key = cols[index.range_key]

                # Determine projected attributes for the index, including
                # table hash/range keys, index hash/range keys, and any
                # non-key projected attributes.
                projected = index._projection_attributes = set()

                if index.projection == "ALL":
                    projected.update(columns)
                elif index.projection == "KEYS_ONLY":
                    keys = (Meta.hash_key, Meta.range_key,
                            index.hash_key, index.range_key)
                    projected.update(key for key in keys if key)
                # List of column model_names - convert to `bloop.Column`
                # objects and merge with keys in projection_attributes
                else:
                    keys = (Meta.hash_key, Meta.range_key,
                            index.hash_key, index.range_key)
                    projected.update(key for key in keys if key)
                    attrs = (cols[attr] for attr in index.projection)
                    projected.update(attrs)

                    index.projection = "INCLUDE"

            # Entry point for model population. By default this is the
            # model class. Custom subclasses of the engine's
            # base model should specify the Meta attr `bloop_init`,
            # which should be a function taking a **kwarg of name:value
            # pairs corresponding to modeled columns.
            Meta.bloop_init = getattr(Meta, "bloop_init", model)
            Meta.bloop_engine = engine

            Meta.table_name = getattr(Meta, "table_name", model.__name__)

            # If the engine already has a base, register this model.
            # Otherwise, this probably IS the engine's base model
            if engine.model:
                engine.register(model)
            return model
    return ModelMetaclass("Model", (__BaseModel,), {})
