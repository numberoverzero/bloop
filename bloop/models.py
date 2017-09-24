from typing import Set, Optional, Dict, Callable
import collections.abc
import functools
import logging

from . import util
from .conditions import ComparisonMixin
from .exceptions import InvalidIndex, InvalidModel, InvalidStream
from .signals import model_created, object_modified
from .types import Type


__all__ = ["BaseModel", "Column", "GlobalSecondaryIndex", "LocalSecondaryIndex"]
logger = logging.getLogger("bloop.models")
missing = util.missing
non_proxied_attrs = {"model", "_name", "_proxied_obj"}


class IMeta:
    """This class exists solely to help autocomplete with variables set on a model's Meta object"""
    abstract: bool
    table_name: str
    read_units: Optional[int]
    write_units: Optional[int]
    stream: Optional[Dict]

    hash_key: Optional["Column"]
    range_key: Optional["Column"]
    keys: Set["Column"]

    columns: Set["Column"]
    indexes: Set["Index"]
    gsis: Set["GlobalSecondaryIndex"]
    lsis: Set["LocalSecondaryIndex"]

    init: Callable[[], "BaseModel"]
    projection: Dict


class BaseModel:
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
    class Meta(IMeta):
        abstract = True

    def __init__(self, **attrs):
        # Only set values from **attrs if there's a
        # corresponding `name` for a column in the model
        for column in self.Meta.columns:
            value = attrs.get(column.name, missing)
            if value is not missing:
                setattr(self, column.name, value)

    def __init_subclass__(cls: type, **kwargs):
        ensure_hash(cls)
        meta = initialize_meta(cls)

        # list of items because bind_column and bind_index call setattr() on the model
        for name, attr in list(cls.__dict__.items()):
            if isinstance(attr, Column):
                meta.bind_column(name, attr)
        for name, attr in cls.__dict__.items():
            if isinstance(attr, Index):
                meta.bind_index(name, attr)

        for base in cls.__mro__:
            # cls was handled above, BaseModel.meta has no columns/indexes attributes
            if base is cls or not issubclass(base, BaseModel):
                continue
            for column in base.Meta.columns:
                name = column.name
                if (column.hash_key and meta.hash_key) or (column.range_key and meta.range_key):
                    continue
                if name not in cls.__dict__:
                    meta.bind_column(name, proxy(column))
            for index in base.Meta.indexes:
                name = index.name
                if name not in cls.__dict__:
                    meta.bind_index(name, proxy(index))

        if not meta.abstract and not meta.hash_key:
            raise InvalidModel(f"{meta.model.__name__!r} has no hash key.")

        model_created.send(None, model=cls)

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
                dump(column.typedef, getattr(obj, column.name, None), context=context, **kwargs)
            ) for column in cls.Meta.columns))
        return dict(filtered) or None

    def __repr__(self):
        attrs = ", ".join("{}={!r}".format(*item) for item in loaded_columns(self))
        return f"{self.__class__.__name__}({attrs})"


class Index:
    """Abstract base class for GSIs and LSIs.

    An index must be bound to a model by calling :func:`bind_index(meta, model) <bloop.models.bind_index>`,
    which lets the index compute projected columns, validate hash and range keys, etc.

    .. seealso::

        :class:`~bloop.models.GlobalSecondaryIndex` and :class:`~bloop.models.LocalSecondaryIndex`

    :param projection: Either "keys", "all", or a list of column name or objects.
        Included columns will be projected into the index.  Key columns are always included.
    :param hash_key: The column that the index can be queried against.  Always the table hash_key for LSIs.
    :param range_key: The column that the index can be sorted on.  Always required for an LSI.  Default is None.
    :param str dynamo_name: *(Optional)* The index's name in in DynamoDB. Defaults to the index’s name in the model.
    """
    def __init__(self, *, projection, hash_key=None, range_key=None, dynamo_name=None, **kwargs):
        self.model = None
        self.hash_key = hash_key
        self.range_key = range_key
        self._name = None
        self._dynamo_name = dynamo_name

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
        return f"<{cls_name}[{self.model.__name__}.{self.name}={self.projection['mode']}]>"

    @property
    def name(self):
        """Name of the model's attr that references self"""
        return self._name

    @property
    def dynamo_name(self):
        if self._dynamo_name is None:
            return self.name
        return self._dynamo_name

    def __set__(self, obj, value):
        raise AttributeError(f"{self.model.__name__}.{self.name} is a {self.__class__.__name__}")

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        raise AttributeError(f"{self.model.__name__}.{self.name} is a {self.__class__.__name__}")

    def __delete__(self, obj):
        raise AttributeError(f"{self.model.__name__}.{self.name} is a {self.__class__.__name__}")


class GlobalSecondaryIndex(Index):
    """See `GlobalSecondaryIndex`_ in the DynamoDB Developer Guide for details.

    :param projection: Either "keys", "all", or a list of column name or objects.
        Included columns will be projected into the index.  Key columns are always included.
    :param hash_key: The column that the index can be queried against.
    :param range_key: *(Optional)* The column that the index can be sorted on.  Default is None.
    :param int read_units: *(Optional)* Provisioned read units for the index.  Default is None.
        When no value is provided and the index does not exist, it will be created with 1 read unit.  If the index
        already exists, it will use the actual index's read units.
    :param int write_units:  *(Optional)* Provisioned write units for the index.  Default is None.
        When no value is provided and the index does not exist, it will be created with 1 write unit.  If the index
        already exists, it will use the actual index's write units.
    :param str dynamo_name: *(Optional)* The index's name in in DynamoDB. Defaults to the index’s name in the model.

    .. _GlobalSecondaryIndex: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
    """
    def __init__(
            self, *, projection,
            hash_key, range_key=None,
            read_units=None, write_units=None,
            dynamo_name=None, **kwargs):
        super().__init__(
            hash_key=hash_key, range_key=range_key,
            dynamo_name=dynamo_name, projection=projection, **kwargs)
        self.write_units = write_units
        self.read_units = read_units


class LocalSecondaryIndex(Index):
    """See `LocalSecondaryIndex`_ in the DynamoDB Developer GUide for details.

    Unlike :class:`~bloop.models.GlobalSecondaryIndex`\, LSIs share their throughput with the table,
    and their hash key is always the table hash key.

    :param projection: Either "keys", "all", or a list of column name or objects.
        Included columns will be projected into the index.  Key columns are always included.
    :param range_key: The column that the index can be sorted against.
    :param str dynamo_name: *(Optional)* The index's name in in DynamoDB. Defaults to the index’s name in the model.
    :param bool strict: *(Optional)* Restricts queries and scans on the LSI to columns in the projection.
        When False, DynamoDB may silently incur additional reads to load results.  You should not disable this
        unless you have an explicit need.  Default is True.

    .. _LocalSecondaryIndex: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html
    """
    def __init__(self, *, projection, range_key, dynamo_name=None, strict=True, **kwargs):
        # Hash key MUST be the table hash; do not specify
        if "hash_key" in kwargs:
            raise InvalidIndex("An LSI shares its hash key with the Model.")
        if ("write_units" in kwargs) or ("read_units" in kwargs):
            raise InvalidIndex("An LSI shares its provisioned throughput with the Model.")
        super().__init__(range_key=range_key, dynamo_name=dynamo_name, projection=projection, **kwargs)
        self.projection["strict"] = strict

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


class Column(ComparisonMixin):
    model: BaseModel
    """Represents a single attribute in DynamoDB.

    :param typedef: The type of this attribute.  Can be either a :class:`~bloop.types.Type` or
        an instance thereof.  If a type class is provided, the column will call the constructor without arguments
        to create an instance.  For example, ``Column(Integer)`` and ``Column(Integer())`` are equivalent.
    :param bool hash_key: *(Optional)* True if this is the model's hash key.
        A model must have exactly one Column with ``hash_key=True``.  Default is False.
    :param bool range_key:  *(Optional)* True if this is the model's range key.
        A model can have at most one Column with
        ``range_key=True``.  Default is False.
    :param str dynamo_name: *(Optional)* The index's name in in DynamoDB. Defaults to the index’s name in the model.
    """
    def __init__(self, typedef, hash_key=False, range_key=False, dynamo_name=None, **kwargs):
        self.hash_key = hash_key
        self.range_key = range_key
        self._name = None
        self._dynamo_name = dynamo_name
        if subclassof(typedef, Type):
            typedef = typedef()
        if instanceof(typedef, Type):
            self.typedef = typedef
        else:
            raise TypeError(f"Expected {typedef} to be instance or subclass of Type")
        super().__init__(**kwargs)

    __hash__ = object.__hash__

    def __set__(self, obj, value):
        self.set(obj, value)

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        return self.get(obj)

    def __delete__(self, obj):
        self.delete(obj)

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
        return f"<{self.__class__.__name__}[{self.model.__name__}.{self.name}{extra}]>"

    @property
    def name(self):
        """Name of the model's attr that references self"""
        return self._name

    @property
    def dynamo_name(self):
        if self._dynamo_name is None:
            return self.name
        return self._dynamo_name

    def set(self, obj, value):
        if self._name is None:
            raise AttributeError("Can't set field without binding to model")
        obj.__dict__[self._name] = value
        # Notify the tracking engine that this value was intentionally mutated
        object_modified.send(self, obj=obj, column=self, value=value)

    def get(self, obj):
        if self._name is None:
            raise AttributeError("Can't get field without binding to model")
        try:
            return obj.__dict__[self._name]
        except KeyError:
            raise AttributeError(f"'{obj.__class__}' has no attribute '{self._name}'")

    def delete(self, obj):
        try:
            if self._name is None:
                raise AttributeError("Can't delete field without binding to model")
            try:
                del obj.__dict__[self._name]
            except KeyError:
                raise AttributeError(f"'{obj.__class__}' has no attribute '{self._name}'")
        finally:
            # Unlike set, we always want to mark on delete.  If we didn't, and the column wasn't loaded
            # (say from a query) then the intention "ensure this doesn't have a value" wouldn't be captured.
            object_modified.send(self, obj=obj, column=self, value=None)


class ProxyColumn(Column):
    # noinspection PyMissingConstructor
    def __init__(self, base_column):
        self._proxied_obj = base_column

    def __getattr__(self, name):
        return getattr(self._proxied_obj, name)

    def __setattr__(self, name, value):
        if name in non_proxied_attrs:
            object.__setattr__(self, name, value)
        else:
            setattr(self._proxied_obj, name, value)

    def __delattr__(self, name):
        if name in non_proxied_attrs:
            try:
                object.__delattr__(self, name)
                return
            except AttributeError:
                pass
        delattr(self._proxied_obj, name)


class ProxyIndex(Index):
    # noinspection PyMissingConstructor
    def __init__(self, base_index):
        self._proxied_obj = base_index

    def __getattr__(self, name):
        return getattr(self._proxied_obj, name)

    def __setattr__(self, name, value):
        if name in non_proxied_attrs:
            object.__setattr__(self, name, value)
        else:
            setattr(self._proxied_obj, name, value)

    def __delattr__(self, name):
        if name in non_proxied_attrs:
            try:
                object.__delattr__(self, name)
                return
            except AttributeError:
                pass
        delattr(self._proxied_obj, name)


class ProxyLSI(ProxyIndex, LocalSecondaryIndex):
    pass


class ProxyGSI(ProxyIndex, GlobalSecondaryIndex):
    pass


def proxy(obj):
    if isinstance(obj, Column):
        return ProxyColumn(obj)
    elif isinstance(obj, LocalSecondaryIndex):
        return ProxyLSI(obj)
    elif isinstance(obj, GlobalSecondaryIndex):
        return ProxyGSI(obj)
    else:
        raise ValueError(f"Can't proxy unknown type {type(obj)}")


def subclassof(obj, classinfo):
    """Wrap issubclass to only return True/False"""
    try:
        return issubclass(obj, classinfo)
    except TypeError:
        return False


def instanceof(obj, classinfo):
    """Wrap isinstance to only return True/False"""
    try:
        return isinstance(obj, classinfo)
    except TypeError:  # pragma: no cover
        # No coverage since we never call this without a class,
        # type, or tuple of classes, types, or such typles.
        return False


def loaded_columns(obj: BaseModel):
    """Yields each (name, value) tuple for all columns in an object that aren't missing"""
    for column in sorted(obj.Meta.columns, key=lambda c: c.name):
        value = getattr(obj, column.name, missing)
        if value is not missing:
            yield column.name, value


def unpack_from_dynamodb(*, attrs, expected, model=None, obj=None, engine=None, context=None, **kwargs):
    """Push values by dynamo_name into an object"""
    context = context or {"engine": engine}
    engine = engine or context.get("engine", None)
    if not engine:
        raise ValueError("You must provide engine or a context with an engine.")
    if model is None and obj is None:
        raise ValueError("You must provide a model or obj to unpack.")
    if model is not None and obj is not None:
        raise ValueError("Only specify model or obj.")
    if model:
        obj = model.Meta.init()

    for column in expected:
        value = attrs.get(column.dynamo_name, None)
        value = engine._load(column.typedef, value, context=context, **kwargs)
        setattr(obj, column.name, value)
    return obj


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
            raise InvalidIndex(f"{projection!r} is not a valid Index projection.")
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
            raise InvalidIndex("Index projection must be a list of strings or Columns to select specific Columns.")
    else:
        raise InvalidIndex("Index projection must be 'all', 'keys', or a list of Columns or Column names.")
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


def unbound_repr(obj):
    class UNBOUND:
        pass

    original_model = getattr(obj, "model", missing)
    obj.model = UNBOUND
    r = repr(obj)
    if original_model is missing:
        delattr(obj, "model")
    else:
        setattr(obj, "model", original_model)
    return r


def setdefault(obj, field, default):
    """Set an object's field to default if it doesn't have a value"""
    setattr(obj, field, getattr(obj, field, default))


def ensure_hash(cls) -> None:
    if getattr(cls, "__hash__", None) is not None:
        return
    logger.info(f"searching for nearest __hash__ impl in {cls.__name__}.__mro__")
    hash_fn = object.__hash__
    for base in cls.__mro__:  # pragma: no branch (because __mro__ will never be an empty list)
        hash_fn = getattr(base, "__hash__")
        if hash_fn:
            break
    cls.__hash__ = hash_fn


def initialize_meta(cls: type):
    meta = getattr(cls, "Meta", missing)
    for base in cls.__mro__:
        if base is cls:
            continue
        parent_meta = getattr(base, "Meta", None)
        if meta is parent_meta:
            meta = missing
            break
    if meta is missing:
        class Meta:
            pass

        meta = cls.Meta = Meta

    meta.model = cls

    setdefault(meta, "init", cls)
    setdefault(meta, "abstract", False)

    setdefault(meta, "table_name", cls.__name__)
    setdefault(meta, "write_units", None)
    setdefault(meta, "read_units", None)
    setdefault(meta, "stream", None)

    setdefault(meta, "hash_key", None)
    setdefault(meta, "range_key", None)
    setdefault(meta, "keys", set())

    setdefault(meta, "columns", set())
    setdefault(meta, "indexes", set())
    setdefault(meta, "gsis", set())
    setdefault(meta, "lsis", set())

    # API consistency with an Index, so (index or model.Meta) can be
    # used interchangeably to get the available columns from that
    # object.
    setdefault(meta, "projection", {
        "mode": "all",
        "included": meta.columns,
        "available": meta.columns,
        "strict": True
    })

    setdefault(meta, "bind_column", functools.partial(bind_column, meta))
    setdefault(meta, "bind_index", functools.partial(bind_index, meta))

    validate_stream(meta.stream)
    return meta


def bind_column(meta, name, column, bind_subclasses=True, replace_keys=False):
    column._name = name
    safe_repr = unbound_repr(column)

    # Guard against dynamo_name collisions in columns *and* indexes
    same = (
        util.index(meta.columns, "dynamo_name").get(column.dynamo_name) or
        util.index(meta.indexes, "dynamo_name").get(column.dynamo_name)
    )
    if same and same.name != name:
        raise InvalidModel(
            f"The column {safe_repr} has the same dynamo_name as an "
            f"existing column or index {same} but has a different name.")

    if not replace_keys:
        if meta.hash_key:
            # Trying to add a second hash_key
            if column.hash_key and column.name != meta.hash_key.name:
                raise InvalidModel(
                    f"Tried to bind {safe_repr} but {meta.model} "
                    f"already has a different hash_key: {meta.hash_key}")
            # Trying to replace same name with non-hash_key
            elif not column.hash_key and column.name == meta.hash_key.name:
                raise InvalidModel(
                    f"Tried to bind {safe_repr} to {meta.model} but it would "
                    f"replace hash_key column {meta.hash_key} with non-hash_key column")
        if meta.range_key:
            # Trying to add a second range_key
            if column.range_key and column.name != meta.range_key.name:
                raise InvalidModel(
                    f"Tried to bind {safe_repr} but {meta.model} "
                    f"already has a different range_key: {meta.range_key}")
            # Trying to replace same name with non-range_key
            elif not column.range_key and column.name == meta.range_key.name:
                raise InvalidModel(
                    f"Tried to bind {safe_repr} to {meta.model} but it would "
                    f"replace range_key column {meta.range_key} with non-range_key column")
    if column.hash_key and column.range_key:
        raise InvalidModel(f"Tried to bind {safe_repr} as both a hash and range key.")

    # success!
    # --------------------------------
    column.model = meta.model
    setattr(meta.model, name, column)

    # if another column has this name, overwrite it
    same = util.index(meta.columns, "name").get(name)
    if same:
        meta.columns.remove(same)
    meta.columns.add(column)

    if column.hash_key:
        if meta.hash_key:
            meta.keys.remove(meta.hash_key)
            meta.columns.remove(meta.hash_key)
        meta.hash_key = column
        meta.keys.add(column)
    if column.range_key:
        if meta.range_key:
            meta.keys.remove(meta.range_key)
            meta.columns.remove(meta.range_key)
        meta.range_key = column
        meta.keys.add(column)
    for idx in meta.indexes:
        recalculate_projection(meta, idx)

    if bind_subclasses:
        for subclass in util.walk_subclasses(meta.model):
            subclass.Meta.bind_column(name, proxy(column), bind_subclasses=False)


def bind_index(meta, name, index, bind_subclasses=True):
    index._name = name
    safe_repr = unbound_repr(index)

    # Guard against dynamo_name collisions in columns *and* indexes
    same = (
        util.index(meta.indexes, "dynamo_name").get(index.dynamo_name) or
        util.index(meta.columns, "dynamo_name").get(index.dynamo_name)
    )
    if same and same.name != name:
        raise InvalidModel(
            f"The index {safe_repr} has the same dynamo_name as an "
            f"existing column or index {same} but has a different name.")

    # We have to roundtrip through the name to handle any `ProxyColumn`s
    by_name = util.index(meta.columns, "name")

    if isinstance(index, LocalSecondaryIndex):
        if not meta.range_key:
            raise InvalidIndex("An LSI requires the Model to have a range key.")
        index.hash_key = meta.hash_key.name
    if isinstance(index.hash_key, str):
        index.hash_key = by_name[index.hash_key]
    elif isinstance(index.hash_key, Column):
        index.hash_key = by_name[index.hash_key.name]
    if not isinstance(index.hash_key, Column):
        raise InvalidIndex("Index hash key must be a Column or Column model name.")

    if index.range_key:
        if isinstance(index.range_key, str):
            index.range_key = by_name[index.range_key]
        elif isinstance(index.range_key, Column):
            index.range_key = by_name[index.range_key.name]
        if not isinstance(index.range_key, Column):
            raise InvalidIndex("Index range key (if provided) must be a Column or Column model name.")

    index.keys = {index.hash_key}
    if index.range_key:
        index.keys.add(index.range_key)

    # success!
    # --------------------------------
    index.model = meta.model
    setattr(meta.model, name, index)

    # if another index has this name, overwrite it
    same = util.index(meta.indexes, "name").get(name)
    if isinstance(index, LocalSecondaryIndex):
        if same:
            meta.lsis.remove(same)
        meta.lsis.add(index)
    elif isinstance(index, GlobalSecondaryIndex):
        if same:
            meta.gsis.remove(same)
        meta.gsis.add(index)
    if same:
        meta.indexes.remove(same)
    meta.indexes.add(index)

    recalculate_projection(meta, index)

    if bind_subclasses:
        for subclass in util.walk_subclasses(meta.model):
            subclass.Meta.bind_index(name, proxy(index), bind_subclasses=False)


def recalculate_projection(meta, index):
    # All projections include model + index keys
    projection_keys = set.union(meta.keys, index.keys)

    proj = index.projection
    mode = proj["mode"]
    strict = proj["strict"]

    if mode == "keys":
        proj["included"] = projection_keys
    elif mode == "all":
        proj["included"] = meta.columns
    elif mode == "include":  # pragma: no branch
        by_name = util.index(meta.columns, "name")
        if all(isinstance(p, str) for p in proj["included"]):
            projection = set(by_name[n] for n in proj["included"])
        else:
            # This roundtrips by_name to handle any `ProxyColumn`s
            projection = set(by_name[c.name] for c in proj["included"])
        projection.update(projection_keys)
        proj["included"] = projection

    if strict:
        proj["available"] = proj["included"]
    else:
        proj["available"] = meta.columns


# required to bootstrap BaseModel.__init_subclass__
initialize_meta(BaseModel)
