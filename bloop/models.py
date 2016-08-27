import collections.abc

import declare

from .conditions import NewComparisonMixin
from .exceptions import InvalidIndex, InvalidModel
from .util import missing, printable_column_name, signal, unpack_from_dynamodb


__all__ = [
    "BaseModel", "Column",
    "GlobalSecondaryIndex", "LocalSecondaryIndex",
    "model_created"]

# Signals!
model_created = signal("model_created")
object_modified = signal("object_modified")


def loaded_columns(obj):
    """Yields each (model_name, value) tuple for all columns in an object that aren't missing"""
    for column in sorted(obj.Meta.columns, key=lambda c: c.model_name):
        value = getattr(obj, column.model_name, missing)
        if value is not missing:
            yield column.model_name, value


def validate_projection(projection):
    validated_projection = {
        "mode": None,
        "included": None,
        "available": None,
        "strict": True
    }

    # String check first since it is also an Iterable.
    # Without this, the following will make "unknown" a list
    if isinstance(projection, str):
        if projection not in ("keys", "all"):
            raise InvalidIndex("{!r} is not a valid Index projection.".format(projection))
        validated_projection["mode"] = projection
    elif isinstance(projection, collections.abc.Iterable):
        projection = list(projection)
        # These checks aren't done together; that would allow a mix
        # of column instances and column names.  There aren't any cases
        # where a mix is required, over picking a style.  Much more likely,
        # the user is trying to do something odd and doesn't understand what
        # the index projection means.
        if (
                all(isinstance(p, str) for p in projection) or
                all(isinstance(p, Column) for p in projection)):
            validated_projection["mode"] = "include"
            validated_projection["included"] = projection
        else:
            raise InvalidIndex(
                "Index projection must be a list of strings or Columns to select specific Columns.")
    else:
        raise InvalidIndex(
            "Index projection must be 'all', 'keys', or a list of Columns or Column names.")
    return validated_projection


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

        model_created.send(model=model)
        return model

    def __repr__(cls):
        return "<Model[{}]>".format(cls.__name__)


def setdefault(obj, field, default):
    """Set an object's field to default if it doesn't have a value"""
    setattr(obj, field, getattr(obj, field, default))


def setup_columns(meta):
    """Filter columns from fields, identify hash and range keys"""

    # This is a set instead of a list, because set uses __hash__
    # while some list operations uses __eq__ which will break
    # with the ComparisonMixin
    meta.columns = set(filter(
        lambda field: isinstance(field, Column), meta.fields))

    meta.hash_key = None
    meta.range_key = None
    meta.keys = set()

    if not meta.abstract:
        cls_name = meta.model.__name__

        hash_keys = [c for c in meta.columns if c.hash_key]
        range_keys = [c for c in meta.columns if c.range_key]

        if len(hash_keys) == 0:
            raise InvalidModel("{!r} has no hash key.".format(cls_name))
        elif len(hash_keys) > 1:
            raise InvalidModel("{!r} has more than one hash key.".format(cls_name))

        if len(range_keys) > 1:
            raise InvalidModel("{!r} has more than one range key.".format(cls_name))

        if range_keys:
            if hash_keys[0] is range_keys[0]:
                raise InvalidModel("{!r} has the same hash and range key.".format(cls_name))
            meta.range_key = range_keys[0]
            meta.keys.add(meta.range_key)
        meta.hash_key = hash_keys[0]
        meta.keys.add(meta.hash_key)

    for column in meta.columns:
        column.model = meta.model

    # API consistency with an Index, so (index or model.Meta) can be
    # used interchangeably to get the available columns from that
    # object.
    meta.projection = {
        "mode": "all",
        "included": meta.columns,
        "available": meta.columns,
        "strict": True
    }


def setup_indexes(meta):
    """Filter indexes from fields, compute projection for each index"""
    # Don't put these in the metadata until they bind successfully.
    gsis = set(filter(lambda field: isinstance(field, GlobalSecondaryIndex), meta.fields))
    lsis = set(filter(lambda field: isinstance(field, LocalSecondaryIndex), meta.fields))
    indexes = set.union(gsis, lsis)

    for index in indexes:
        index._bind(meta.model)

    meta.gsis = gsis
    meta.lsis = lsis
    meta.indexes = indexes


class BaseModel(metaclass=ModelMetaclass):
    """An unbound, abstract base model"""
    class Meta:
        abstract = True

    def __init__(self, **attrs):
        # Only set values from **attrs if there's a
        # corresponding `model_name` for a column in the model
        for column in self.Meta.columns:
            value = attrs.get(column.model_name, missing)
            if value is not missing:
                setattr(self, column.model_name, value)

    @classmethod
    def _load(cls, attrs, *, context, **kwargs):
        """ dict (dynamo name) -> obj """
        return unpack_from_dynamodb(
            model=cls,
            attrs=attrs or {},
            expected=cls.Meta.columns,
            context=context, **kwargs)

    @classmethod
    def _dump(cls, obj, *, context, **kwargs):
        """ obj -> dict """
        if obj is None:
            return None
        dump = context["engine"]._dump
        filtered = filter(
            lambda item: item[1] is not None,
            ((
                column.dynamo_name,
                dump(column.typedef, getattr(obj, column.model_name, None), context=context, **kwargs)
            ) for column in cls.Meta.columns))
        return dict(filtered) or None

    @classmethod
    def _register(cls, type_engine):
        for column in cls.Meta.columns:
            type_engine.register(column.typedef)

    def __repr__(self):
        attrs = ", ".join("{}={}".format(*item) for item in loaded_columns(self))
        return "{}({})".format(self.__class__.__name__, attrs)


class Index(declare.Field):
    def __init__(self, *, projection, hash_key=None, range_key=None, name=None, **kwargs):
        self.model = None
        self.hash_key = hash_key
        self.range_key = range_key
        self._dynamo_name = name
        super().__init__(**kwargs)

        self.projection = validate_projection(projection)

    def __repr__(self):
        if isinstance(self, LocalSecondaryIndex):
            cls_name = "LSI"
        elif isinstance(self, GlobalSecondaryIndex):
            cls_name = "GSI"
        else:
            cls_name = self.__class__.__name__

        # <GSI[User.by_email=all]>
        # <GSI[User.by_email=keys]>
        # <LSI[User.by_email=include]>
        return "<{}[{}.{}={}]>".format(
            cls_name,
            self.model.__name__, self.model_name,
            self.projection["mode"]
        )

    @property
    def dynamo_name(self):
        if self._dynamo_name is None:
            return self.model_name
        return self._dynamo_name

    def _bind(self, model):
        """Set up hash, range keys and compute projection"""
        self.model = model

        # Index by model_name so we can replace hash_key, range_key with the proper `bloop.Column` object
        columns = declare.index(model.Meta.columns, "model_name")
        if isinstance(self.hash_key, str):
            self.hash_key = columns[self.hash_key]
        if not isinstance(self.hash_key, Column):
            raise InvalidIndex("Index hash key must be a Column or Column model name.")
        if self.range_key:
            if isinstance(self.range_key, str):
                self.range_key = columns[self.range_key]
            if not isinstance(self.range_key, Column):
                raise InvalidIndex("Index range key (if provided) must be a Column or Column model name.")

        self.keys = {self.hash_key}
        if self.range_key:
            self.keys.add(self.range_key)

        # Compute and the projected columns
        # All projections include model + index keys
        projection_keys = set.union(model.Meta.keys, self.keys)

        if self.projection["mode"] == "keys":
            self.projection["included"] = projection_keys
        elif self.projection["mode"] == "all":
            self.projection["included"] = model.Meta.columns
        elif self.projection["mode"] == "include":  # pragma: no branch
            # model_name -> Column
            if all(isinstance(p, str) for p in self.projection["included"]):
                projection = set(columns[name] for name in self.projection["included"])
            else:
                projection = set(self.projection["included"])
            projection.update(projection_keys)
            self.projection["included"] = projection

        # Strict has the same availability as the included columns,
        # while non-strict has access to the full range of columns
        if self.projection["strict"]:
            self.projection["available"] = self.projection["included"]
        else:
            self.projection["available"] = model.Meta.columns

    # TODO: disallow set/get/del for an index; these don't store values.  Raise AttributeError.


class GlobalSecondaryIndex(Index):
    def __init__(self, *, projection, hash_key, range_key=None, read_units=1, write_units=1, name=None, **kwargs):
        super().__init__(hash_key=hash_key, range_key=range_key, name=name, projection=projection, **kwargs)
        self.write_units = write_units
        self.read_units = read_units


class LocalSecondaryIndex(Index):
    """ LSIs don't have individual read/write units """
    def __init__(self, *, projection, range_key, name=None, strict=True, **kwargs):
        # Hash key MUST be the table hash; do not specify
        if "hash_key" in kwargs:
            raise InvalidIndex("An LSI shares its hash key with the Model.")
        if ("write_units" in kwargs) or ("read_units" in kwargs):
            raise InvalidIndex("An LSI shares its provisioned throughput with the Model.")
        super().__init__(range_key=range_key, name=name, projection=projection, **kwargs)
        self.projection["strict"] = strict

    def _bind(self, model):
        """Raise if the model doesn't have a range key"""
        if not model.Meta.range_key:
            raise InvalidIndex("An LSI requires the Model to have a range key.")
        # this is model_name (string) because super()._bind will do the string -> Column lookup
        self.hash_key = model.Meta.hash_key.model_name
        super()._bind(model)

    @property
    def read_units(self):
        return self.model.Meta.read_units

    @read_units.setter
    def read_units(self, value):
        self.model.Meta.read_units = value

    @property
    def write_units(self):
        return self.model.Meta.write_units

    @write_units.setter
    def write_units(self, value):
        self.model.Meta.write_units = value


class Column(declare.Field, NewComparisonMixin):
    def __init__(self, typedef, hash_key=None, range_key=None,
                 name=None, **kwargs):
        self.hash_key = hash_key
        self.range_key = range_key
        self._dynamo_name = name
        kwargs['typedef'] = typedef
        super().__init__(**kwargs)

    __hash__ = object.__hash__

    def __repr__(self, path=None):
        if self.hash_key:
            extra = "=hash"
        elif self.range_key:
            extra = "=range"
        else:
            extra = ""

        # <Column[Pin.url]>
        # <Column[User.id=hash]>
        # <Column[File.fragment=range]>
        return "<Column[{}.{}{}]>".format(self.model.__name__, printable_column_name(self, path), extra)

    @property
    def dynamo_name(self):
        if self._dynamo_name is None:
            return self.model_name
        return self._dynamo_name

    def set(self, obj, value):
        super().set(obj, value)
        # Notify the tracking engine that this value was intentionally mutated
        object_modified.send(obj=obj, column=self, value=value)

    def delete(self, obj):
        try:
            super().delete(obj)
        finally:
            # Unlike set, we always want to mark on delete.  If we didn't, and the column wasn't loaded
            # (say from a query) then the intention "ensure this doesn't have a value" wouldn't be captured.
            object_modified.send(obj=obj, column=self, value=None)
