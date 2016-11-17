import collections.abc

import declare

from .conditions import ComparisonMixin
from .exceptions import InvalidIndex, InvalidModel, InvalidStream
from .signals import model_created, object_modified
from .util import missing, unpack_from_dynamodb


__all__ = ["BaseModel", "Column", "GlobalSecondaryIndex", "LocalSecondaryIndex"]


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


def validate_stream(stream):
    if stream is None:
        return

    if not isinstance(stream, collections.abc.MutableMapping):
        raise InvalidStream("Stream must be None or a dict.")

    if "include" not in stream:
        raise InvalidStream("Specify what the stream will return with the 'include' key.")
    include = stream["include"] = set(stream["include"])

    # []
    if not include:
        raise InvalidStream("Must include at least one of 'keys', 'old', or 'new'.")

    # ["what is this", "keys"]
    for value in include:
        if value not in {"new", "keys", "old"}:
            raise InvalidStream("Streams can only contain 'keys', 'old', and/or 'new'.")

    # ["keys", "old"]
    if include == {"new", "keys"} or include == {"old", "keys"}:
        raise InvalidStream("The option 'keys' cannot be used with either 'old' or 'new'.")
    stream.setdefault("arn", None)


class ModelMetaclass(declare.ModelMetaclass):
    def __new__(mcs, name, bases, attrs):
        hash_fn = attrs.get("__hash__", missing)
        if hash_fn is None:
            raise InvalidModel("Models must be hashable.")
        elif hash_fn is missing:
            # Any base class's explicit (not object.__hash__)
            # hash function has priority over the default.
            # If there aren't any bases with explicit hash functions,
            # just use object.__hash__
            for base in bases:
                hash_fn = getattr(base, "__hash__")
                if hash_fn:
                    break
            else:
                hash_fn = object.__hash__
            attrs["__hash__"] = hash_fn

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
        setdefault(meta, "stream", None)

        validate_stream(meta.stream)

        model_created.send(None, model=model)
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
    meta.columns = set(filter(lambda field: isinstance(field, Column), meta.fields))

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
    """Abstract base that all models derive from.

    Provides a basic ``__init__`` method that takes \*\*kwargs whose
    keys are columns names:

    .. code-block:: python

        class URL(BaseModel):
            id = Column(UUID, hash_key=True)
            ip = Column(IPv6)
            name = Column(String)

        url = URL(id=uuid.uuid4(), name="google")
    """
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
        attrs = ", ".join("{}={!r}".format(*item) for item in loaded_columns(self))
        return "{}({})".format(self.__class__.__name__, attrs)


class Index(declare.Field):
    """Abstract base class for GSIs and LSIs.

    An index needs to be bound to a model by calling :func:`Index._bind(model) <bloop.models.Index._bind>`, which
    lets the index compute projected columns, validate hash and range keys, etc.

    .. seealso::

        :class:`~bloop.models.GlobalSecondaryIndex` and :class:`~bloop.models.LocalSecondaryIndex`

    :param projection: Either "keys", "all", or a list of column name or objects.
        Included columns will be projected into the index.  Key columns are always included.
    :param hash_key: The column that the index can be queried against.  Always the table hash_key for LSIs.
    :param range_key: The column that the index can be sorted on.  Always required for an LSI.  Default is None.
    :param str name: *(Optional)* The index's name in in DynamoDB. Defaults to the index’s name in the model.
    """
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
        """Compute attributes and resolve column names.

        * If hash and/or range keys are strings, resolve them to :class:`~bloop.models.Column` instances from
          the model by ``model_name``.
        * If projection is a list of strings, resolve each to a Column instance.
        * Compute :data:`~Index.projection` dict from model Metadata and Index's temporary ``projection``
          attribute.

        :param model: The :class:`~bloop.models.BaseModel` this Index is attached to.
        :raises bloop.exceptions.InvalidIndex: If the hash or range keys are misconfigured.
        """
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

    def set(self, obj, value):
        raise AttributeError(
            "{}.{} is a {}".format(
                self.model.__name__, self.model_name, self.__class__.__name__))

    def delete(self, obj):
        raise AttributeError(
            "{}.{} is a {}".format(
                self.model.__name__, self.model_name, self.__class__.__name__))

    def get(self, obj):
        raise AttributeError(
            "{}.{} is a {}".format(
                self.model.__name__, self.model_name, self.__class__.__name__))


class GlobalSecondaryIndex(Index):
    """See `GlobalSecondaryIndex`_ in the DynamoDB Developer Guide for details.

    :param projection: Either "keys", "all", or a list of column name or objects.
        Included columns will be projected into the index.  Key columns are always included.
    :param hash_key: The column that the index can be queried against.
    :param range_key: *(Optional)* The column that the index can be sorted on.  Default is None.
    :param int read_units: *(Optional)* Provisioned read units for the index.  Default is 1.
    :param int write_units:  *(Optional)* Provisioned write units for the index.  Default is 1.
    :param str name: *(Optional)* The index's name in in DynamoDB. Defaults to the index’s name in the model.

    .. _GlobalSecondaryIndex: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
    """
    def __init__(self, *, projection, hash_key, range_key=None, read_units=1, write_units=1, name=None, **kwargs):
        super().__init__(hash_key=hash_key, range_key=range_key, name=name, projection=projection, **kwargs)
        self.write_units = write_units
        self.read_units = read_units


class LocalSecondaryIndex(Index):
    """See `LocalSecondaryIndex`_ in the DynamoDB Developer GUide for details.

    Unlike :class:`~bloop.models.GlobalSecondaryIndex`\, LSIs share their throughput with the table,
    and their hash key is always the table hash key.

    :param projection: Either "keys", "all", or a list of column name or objects.
        Included columns will be projected into the index.  Key columns are always included.
    :param range_key: The column that the index can be sorted against.
    :param str name: *(Optional)* The index's name in in DynamoDB. Defaults to the index’s name in the model.
    :param bool strict: *(Optional)* Restricts queries and scans on the LSI to columns in the projection.
        When False, DynamoDB may silently incur additional reads to load results.  You should not disable this
        unless you have an explicit need.  Default is True.

    .. _LocalSecondaryIndex: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html
    """
    def __init__(self, *, projection, range_key, name=None, strict=True, **kwargs):
        # Hash key MUST be the table hash; do not specify
        if "hash_key" in kwargs:
            raise InvalidIndex("An LSI shares its hash key with the Model.")
        if ("write_units" in kwargs) or ("read_units" in kwargs):
            raise InvalidIndex("An LSI shares its provisioned throughput with the Model.")
        super().__init__(range_key=range_key, name=name, projection=projection, **kwargs)
        self.projection["strict"] = strict

    def _bind(self, model):
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


class Column(declare.Field, ComparisonMixin):
    """Represents a single attribute in DynamoDB.

    :param typedef: The type of this attribute.  Can be either a :class:`~bloop.types.Type` or
        an instance thereof.  If a type class is provided, the column will call the constructor without arguments
        to create an instance.  For example, ``Column(Integer)`` and ``Column(Integer())`` are equivalent.
    :param bool hash_key: *(Optional)* True if this is the model's hash key.
        A model must have exactly one Column with ``hash_key=True``.  Default is False.
    :param bool range_key:  *(Optional)* True if this is the model's range key.
        A model can have at most one Column with
        ``range_key=True``.  Default is False.
    :param str name: *(Optional)* The index's name in in DynamoDB. Defaults to the index’s name in the model.
    """
    def __init__(self, typedef, hash_key=False, range_key=False, name=None, **kwargs):
        self.hash_key = hash_key
        self.range_key = range_key
        self._dynamo_name = name
        kwargs['typedef'] = typedef
        super().__init__(**kwargs)

    __hash__ = object.__hash__

    def __repr__(self):
        if self.hash_key:
            extra = "=hash"
        elif self.range_key:
            extra = "=range"
        else:
            extra = ""

        # <Column[Pin.url]>
        # <Column[User.id=hash]>
        # <Column[File.fragment=range]>
        return "<Column[{}.{}{}]>".format(
            self.model.__name__,
            self.model_name,
            extra
        )

    @property
    def dynamo_name(self):
        if self._dynamo_name is None:
            return self.model_name
        return self._dynamo_name

    def set(self, obj, value):
        super().set(obj, value)
        # Notify the tracking engine that this value was intentionally mutated
        object_modified.send(self, obj=obj, column=self, value=value)

    def delete(self, obj):
        try:
            super().delete(obj)
        finally:
            # Unlike set, we always want to mark on delete.  If we didn't, and the column wasn't loaded
            # (say from a query) then the intention "ensure this doesn't have a value" wouldn't be captured.
            object_modified.send(self, obj=obj, column=self, value=None)
