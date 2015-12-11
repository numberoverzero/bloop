import bloop.column
import bloop.index
import bloop.util
import declare
_MISSING = object()


class _BaseModel(object):
    """
    DO NOT SUBCLASS DIRECTLY.

    Instead, subclass the `model` attribute of an engine.  This ensures the
    proper metaclass setup has been performed, so that `engine.bind` will
    work.

    Example:

        engine = bloop.Engine()
        BaseModel = engine.model

        class CustomBaseModel(BaseModel):
            # ... cross-model code goes here
    """
    def __init__(self, **attrs):
        # Only set values from **attrs if there's a
        # corresponding `model_name` for a column in the model
        for column in self.Meta.columns:
            value = attrs.get(column.model_name, _MISSING)
            if value is not _MISSING:
                setattr(self, column.model_name, value)

    @classmethod
    def _load(cls, attrs, *, context=None, **kwargs):
        """ dict (dynamo name) -> obj """
        obj = cls.Meta.init()
        # We want to expect the exact attributes that are passed,
        # since any superset will mark missing fields as expected and None
        expected = set()
        for column in cls.Meta.columns:
            if column.dynamo_name in attrs:
                expected.add(column)
        cls.Meta.bloop_engine._update(obj, attrs, expected)
        return obj

    @classmethod
    def _dump(cls, obj, *, context=None, **kwargs):
        """ obj -> dict """
        attrs = {}
        engine = cls.Meta.bloop_engine.type_engine
        for column in cls.Meta.columns:
            value = getattr(obj, column.model_name, None)
            # Missing expected column - None is equivalent to empty
            if value is not None:
                attrs[column.dynamo_name] = engine.dump(column.typedef, value)
        return attrs

    def __str__(self):  # pragma: no cover
        attrs = []
        for column in self.Meta.columns:
            name = column.model_name
            value = getattr(self, name, None)
            if value is not None:
                attrs.append("{}={}".format(name, value))
        attrs = ", ".join(attrs)
        return "{}({})".format(self.__class__.__name__, attrs)
    __repr__ = __str__

    def __hash__(self):  # pragma: no cover
        return super().__hash__()

    def __eq__(self, other):
        """ Only checks defined columns. """
        cls = self.__class__
        if not isinstance(other, cls):
            return False
        for column in cls.Meta.columns:
            value = getattr(self, column.model_name, None)
            other_value = getattr(other, column.model_name, None)
            if value != other_value:
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)


def _update(obj, field, default):
    """Set an object's field to default if it doesn't have a value"""
    value = getattr(obj, field, default)
    setattr(obj, field, value)


def _is_column(field):
    return isinstance(field, bloop.column.Column)


def _is_index(field):
    return isinstance(field, bloop.index._Index)


def _setup_columns(meta):
    """Filter columns from fields, identify hash and range keys"""

    # This is a set instead of a list, because set uses __hash__
    # while some list operations uses __eq__ which will break
    # with the ComparisonMixin
    meta.columns = set(filter(_is_column, meta.fields))

    meta.hash_key = None
    meta.range_key = None
    for column in meta.columns:
        if column.hash_key:
            if meta.hash_key:
                raise ValueError("Model hash_key over-specified")
            meta.hash_key = column
        elif column.range_key:
            if meta.range_key:
                raise ValueError("Model range_key over-specified")
            meta.range_key = column


def _setup_indexes(meta):
    """Filter indexes from fields, compute projection for each index"""
    # This is a set instead of a list, because set uses __hash__
    # while some list operations uses __eq__ which will break
    # with the ComparisonMixin
    meta.indexes = set(filter(_is_index, meta.fields))

    # Look up the current hash key -- which is specified by
    # model_name, not dynamo_name -- in indexed columns and relate
    # the proper `bloop.Column` object
    columns = declare.index(meta.columns, "model_name")
    for index in meta.indexes:
        index.model = meta.model
        index._bind(columns, meta.hash_key, meta.range_key)


def BaseModel(engine):
    """
    Although this returns a class, you should NOT call this function to create
    a base model class.  Instead, subclass the `model` attribute of an engine.
    Doing this ensures the proper metaclass setup has been performed,
    so that `engine.bind` will work.

    Example:

        engine = bloop.Engine()
        BaseModel = engine.model

        class CustomBaseModel(BaseModel):
            # ... cross-model code goes here
    """
    if engine.model:
        raise ValueError("BaseModel already exists for engine")

    class ModelMetaclass(declare.ModelMetaclass):
        def __new__(metaclass, name, bases, attrs):

            model = super().__new__(metaclass, name, bases, attrs)
            meta = model.Meta
            meta.model = model
            _update(meta, "write_units", 1)
            _update(meta, "read_units", 1)

            _setup_columns(meta)
            _setup_indexes(meta)

            # Entry point for model population. By default this is the
            # class's __init__ function. Custom models can specify the
            # Meta attr `init`, which must be a function taking no
            # arguments that returns an instance of the class
            _update(meta, "init", model)
            _update(meta, "table_name", model.__name__)
            meta.bloop_engine = engine

            # If the engine already has a base, register this model.
            # Otherwise, this IS the engine's base model
            if engine.model:
                engine.unbound_models.add(model)
            return model
    return ModelMetaclass("Model", (_BaseModel,), {})
