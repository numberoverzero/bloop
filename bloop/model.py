import bloop.column
import bloop.index
import bloop.util
import declare
_MISSING = object()
__all__ = ["new_base", "BaseModel"]


def new_base():
    """Return an unbound base model"""
    model = ModelMetaclass("Model", (BaseModel,), {})
    model.Meta.abstract = True
    return model


class ModelMetaclass(declare.ModelMetaclass):
    def __new__(mcs, name, bases, attrs):
        model = super().__new__(mcs, name, bases, attrs)
        meta = model.Meta
        meta.model = model
        # new_class will set abstract to true, all other models are assumed
        # to be concrete unless specified
        setdefault(meta, "abstract", False)
        setdefault(meta, "write_units", 1)
        setdefault(meta, "read_units", 1)

        setup_columns(meta)
        setup_indexes(meta)

        # Entry point for model population. By default this is the
        # class's __init__ function. Custom models can specify the
        # Meta attr `init`, which must be a function taking no
        # arguments that returns an instance of the class
        setdefault(meta, "init", model)
        setdefault(meta, "table_name", model.__name__)

        return model


def setdefault(obj, field, default):
    """Set an object's field to default if it doesn't have a value"""
    setattr(obj, field, getattr(obj, field, default))


def setup_columns(meta):
    """Filter columns from fields, identify hash and range keys"""

    # This is a set instead of a list, because set uses __hash__
    # while some list operations uses __eq__ which will break
    # with the ComparisonMixin
    meta.columns = set(filter(
        lambda field: isinstance(field, bloop.column.Column), meta.fields))

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
        column.model = meta.model


def setup_indexes(meta):
    """Filter indexes from fields, compute projection for each index"""
    # This is a set instead of a list, because set uses __hash__
    # while some list operations uses __eq__ which will break
    # with the ComparisonMixin
    meta.indexes = set(filter(
        lambda field: isinstance(field, bloop.index._Index), meta.fields))

    # Look up the current hash key -- which is specified by
    # model_name, not dynamo_name -- in indexed columns and relate
    # the proper `bloop.Column` object
    columns = declare.index(meta.columns, "model_name")
    for index in meta.indexes:
        index.model = meta.model
        index._bind(columns, meta.hash_key, meta.range_key)


class BaseModel:
    """
    Do not subclass directly, use new_base.

    Example:

        BaseModel = new_base()

        class MyModel(BaseModel):
            ...

        engine = bloop.Engine()
        engine.bind(base=BaseModel)
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
        context["engine"]._update(obj, attrs, expected)
        return obj

    @classmethod
    def _dump(cls, obj, *, context=None, **kwargs):
        """ obj -> dict """
        attrs = {}
        engine = context["engine"].type_engine
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

    __hash__ = object.__hash__

    def __eq__(self, other):
        """ Only checks defined columns. """
        if self is other:
            return True
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
