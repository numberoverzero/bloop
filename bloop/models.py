import collections
import collections.abc
import inspect
import logging
from copy import copy as copyfn
from typing import Callable, Dict, Optional, Set

from . import util
from .conditions import ComparisonMixin
from .exceptions import InvalidModel, InvalidStream
from .signals import model_created, object_modified
from .types import DateTime, Number, Type


__all__ = [
    "BaseModel", "Column",
    "Index", "GlobalSecondaryIndex", "LocalSecondaryIndex",
    "subclassof", "unpack_from_dynamodb"
]

logger = logging.getLogger("bloop.models")
missing = util.missing


class IMeta:
    """This class exists to provide autocomplete hints for computed variables on a model's Meta object.

    Subclassing IMeta is **OPTIONAL** and rarely necessary; it is primarily available for users writing generic code
    over a class of models, eg. transforms on all columns of a model or a Marshmallow adapter.


    .. code-block:: python

        import bloop.models


        class User(BaseModel):
            id = Column(String, hash_key=True)
            email = Column(String, dynamo_name="e")

            class Meta(bloop.models.IMeta):
                read_units = 500

        User.Meta.co  # Pycharm renders:
                      #     +---------------------------+
                      #     | User.Meta.columns         |
                      #     | User.Meta.columns_by_name |
                      #     +---------------------------+

    """
    abstract: bool
    table_name: str
    read_units: Optional[int]
    write_units: Optional[int]
    stream: Optional[Dict]
    ttl: Optional[Dict]
    encryption: Optional[Dict]
    backups: Optional[Dict]
    billing: Optional[Dict]

    model: "BaseModel"

    hash_key: Optional["Column"]
    range_key: Optional["Column"]
    keys: Set["Column"]

    columns: Set["Column"]
    columns_by_name: Dict[str, "Column"]
    indexes: Set["Index"]
    gsis: Set["GlobalSecondaryIndex"]
    lsis: Set["LocalSecondaryIndex"]

    init: Callable[[], "BaseModel"]
    projection: Dict


class BaseModel:
    """Abstract base that all models derive from.

    Provides a basic ``__init__`` method that takes ``**kwargs`` whose
    keys are columns names:

    .. code-block:: python

        class URL(BaseModel):
            id = Column(UUID, hash_key=True)
            ip = Column(IPv6)
            name = Column(String)

        url = URL(id=uuid.uuid4(), name="google")

    By default, the ``__init__`` method is not called when new instances are
    required, for example when iterating results from Query, Scan or a Stream.

    """
    class Meta(IMeta):
        abstract = True

    def __init__(self, **attrs):
        # Only set values from **attrs if there's a
        # corresponding `name` for a column in the model
        for column in self.Meta.columns:
            value = attrs.get(column.name, missing)
            if value is missing:
                value = column.default()
            if value is not missing:
                setattr(self, column.name, value)

    def __init_subclass__(cls: type, **kwargs):
        ensure_hash(cls)
        meta = initialize_meta(cls)

        # before we start binding, we should ensure that no combination of parent classes
        # will cause conflicts.  For example:
        #   class C(A, B) where
        #       A has a column named "foo" and dynamo_name "ddb"
        #       B has a column named "bar" and dynamo_name "ddb"
        # both A and B are valid mixins, but C must fail because there isn't a 1:1 binding to the "ddb" column.
        #
        # TODO | for now, we'll assume that the class being defined is special, and can replace columns with the
        # TODO | same dynamo_name.  In the example above, that would mean C has a column named "baz" and dynamo_name
        # TODO | "ddb" which would prevent the parent columns "foo" and "bar" from binding to the child class.

        modeled_attrs = set((attr for (_, attr) in inspect.getmembers(cls, lambda x: isinstance(x, (Column, Index)))))
        local_attrs = {
            name: value
            for name, value in cls.__dict__.items()
            if isinstance(value, (Column, Index))
        }
        derived_attrs = modeled_attrs - set(local_attrs.values())

        # 0.0 Pre-validation for collisions in derived columns/indexes
        dynamo_names = [x.dynamo_name for x in derived_attrs]
        collisions = [name for name, count in collections.Counter(dynamo_names).items() if count > 1]
        if collisions:
            collisions.sort()
            raise InvalidModel(
                f"The model {cls.__name__} subclasses one or more models with conflicting "
                f"column or index definitions for the following values of dynamo_name: {collisions}")
        derived_hash_keys = set((x.name for x in derived_attrs if isinstance(x, Column) and x.hash_key))
        if len(derived_hash_keys) > 1:
            derived_hash_keys = sorted(derived_hash_keys)
            raise InvalidModel(
                f"The model {cls.__name__} subclasses one or more models that declare multiple "
                f"columns as the hash key: {derived_hash_keys}")
        derived_range_keys = set((x.name for x in derived_attrs if isinstance(x, Column) and x.range_key))
        if len(derived_range_keys) > 1:
            derived_range_keys = sorted(derived_range_keys)
            raise InvalidModel(
                f"The model {cls.__name__} subclasses one or more models that declare multiple "
                f"columns as the range key: {derived_range_keys}")

        # 0.1 Pre-validation for collisions in local columns/indexes
        dynamo_names = [x.dynamo_name for x in local_attrs.values()]
        collisions = [name for name, count in collections.Counter(dynamo_names).items() if count > 1]
        if collisions:
            collisions.sort()
            raise InvalidModel(
                f"The model {cls.__name__} contains conflicting column or index definitions for the "
                f"following values of dynamo_name: {collisions}")
        local_hash_keys = [x.name for x in local_attrs.values() if isinstance(x, Column) and x.hash_key]
        if len(local_hash_keys) > 1:
            local_hash_keys = sorted(local_hash_keys)
            raise InvalidModel(
                f"The model {cls.__name__} defines multiple columns as hash columns: {local_hash_keys}")
        local_range_keys = [x.name for x in local_attrs.values() if isinstance(x, Column) and x.range_key]
        if len(local_range_keys) > 1:
            local_range_keys = sorted(local_range_keys)
            raise InvalidModel(
                f"The model {cls.__name__} defines multiple columns as range columns: {local_range_keys}")

        # 1.0 Bind derived columns so they can be referenced by derived indexes
        for attr in derived_attrs:
            if isinstance(attr, Column):
                bind_column(cls, attr.name, attr, copy=True)

        # 1.1 Bind derived indexes
        for attr in derived_attrs:
            if isinstance(attr, Index):
                bind_index(cls, attr.name, attr, copy=True)

        # 1.2 Bind local columns, allowing them to overwrite existing columns
        for name, attr in local_attrs.items():
            if isinstance(attr, Column):
                bind_column(cls, name, attr, force=True)

        # 1.3 Bind local indexes, allowing them to overwrite existing indexes
        for name, attr in local_attrs.items():
            if isinstance(attr, Index):
                bind_index(cls, name, attr, force=True)

        # 2.0 Ensure concrete models are valid
        # Currently, this just checks that a hash key is defined
        if not meta.abstract and not meta.hash_key:
            raise InvalidModel(f"{meta.model.__name__!r} has no hash key.")

        validate_stream(meta)
        validate_ttl(meta)
        validate_encryption(meta)
        validate_backups(meta)
        validate_billing(meta)

        # 3.0 Fire model_created for customizing the class after creation
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
        if not isinstance(hash_key, (str, Column, type(None))):
            raise InvalidModel(f"Index hash_key must be a str or Column, but was {type(hash_key)!r}")
        if not isinstance(range_key, (str, Column, type(None))):
            raise InvalidModel(f"Index range_key must be a str or Column, but was {type(range_key)!r}")
        self._hash_key = hash_key
        self._range_key = range_key
        self._name = None
        self._dynamo_name = dynamo_name

        self.projection = validate_projection(projection)

    def __copy__(self):
        """
        Create a shallow copy of this Index.  Primarily used when initializing models that subclass other abstract
        models or mixins (baseless classes that contain Columns and Indexes).  You can override this method to
        change how derived models are created:

        .. code-block:: python

            import copy


            class MyIndex(Index):
                def __copy__(self):
                    new = super().__copy__()
                    new.derived = True
                    return new


            index = MyIndex(projection="keys", hash_key="some_column")
            same = copy.copy(index)
            assert same.derived  # True

        :return: A shallow copy of this Index, with the ``model`` and ``_name`` attributes unset, and the
                 computed projection invalidated.
        """
        cls = self.__class__
        obj = cls.__new__(cls)
        obj.__dict__.update(self.__dict__)
        obj.model = None
        obj._name = None
        obj.projection = {
            "mode": self.projection["mode"],
            "included": None,
            "available": None,
            "strict": self.projection["strict"]
        }
        return obj

    def __set_name__(self, owner, name):
        self._name = name

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

    @property
    def hash_key(self):
        if isinstance(self._hash_key, Column):
            # replacement is late-binding to handle direct references in models
            # before BaseModel.__init_subclass__ can name each column
            self._hash_key = self._hash_key.name
        return self.model.Meta.columns_by_name[self._hash_key]

    @property
    def range_key(self):
        if self._range_key is None:
            return None
        if isinstance(self._range_key, Column):
            # replacement is late-binding to handle direct references in models
            # before BaseModel.__init_subclass__ can name each column
            self._range_key = self._range_key.name
        return self.model.Meta.columns_by_name[self._range_key]

    @property
    def keys(self):
        keys = {self.hash_key}
        if self.range_key:
            keys.add(self.range_key)
        return keys

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
    """See `LocalSecondaryIndex`_ in the DynamoDB Developer Guide for details.

    Unlike :class:`~bloop.models.GlobalSecondaryIndex` each LSI shares its throughput with the table
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
            raise InvalidModel("An LSI shares its hash key with the Model.")
        if ("write_units" in kwargs) or ("read_units" in kwargs):
            raise InvalidModel("An LSI shares its provisioned throughput with the Model.")
        super().__init__(range_key=range_key, dynamo_name=dynamo_name, projection=projection, **kwargs)
        self.projection["strict"] = strict

    @property
    def hash_key(self):
        return self.model.Meta.hash_key

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
    :param str dynamo_name: *(Optional)* The column's name in in DynamoDB. Defaults to the index’s name in the model.
    """
    def __init__(self, typedef, hash_key=False, range_key=False, dynamo_name=None, default=missing):
        self.hash_key: bool = hash_key
        self.range_key: bool = range_key
        self._name: str = None
        self._dynamo_name: str = dynamo_name

        if not callable(default):
            self.default = lambda: default
        else:
            self.default = default

        if subclassof(typedef, Type):
            typedef = typedef()
        if instanceof(typedef, Type):
            self.typedef = typedef

        else:
            raise TypeError(f"Expected {typedef} to be instance or subclass of Type")
        super().__init__()

    def __copy__(self):
        """
        Create a shallow copy of this Column.  Primarily used when initializing models that subclass other abstract
        models or mixins (baseless classes that contain Columns and Indexes).  You can override this method to
        change how derived models are created:

        .. code-block:: python

            import copy


            class MyColumn(Column):
                def __copy__(self):
                    new = super().__copy__()
                    new.derived = True
                    return new


            column = MyColumn(Integer)
            same = copy.copy(column)
            assert same.derived  # True

        :return: A shallow copy of this Column, with the ``model`` and ``_name`` attributes unset.
        """
        cls = self.__class__
        obj = cls.__new__(cls)
        obj.__dict__.update(self.__dict__)
        obj.model = None
        obj._name = None
        return obj

    def __set_name__(self, owner, name):
        self._name = name

    __hash__ = object.__hash__

    def __set__(self, obj, value):
        if self._name is None:
            raise AttributeError("Can't set field without binding to model")
        obj.__dict__[self._name] = value
        # Notify the tracking engine that this value was intentionally mutated
        object_modified.send(self, obj=obj, column=self, value=value)

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        if self._name is None:
            raise AttributeError("Can't get field without binding to model")
        try:
            return obj.__dict__[self._name]
        except KeyError:
            raise AttributeError(f"'{obj.__class__}' has no attribute '{self._name}'")

    def __delete__(self, obj):
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
        # type, or tuple of classes, types, or such tuples.
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
            raise InvalidModel(f"{projection!r} is not a valid Index projection.")
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
            raise InvalidModel("Index projection must be a list of strings or Columns to select specific Columns.")
    else:
        raise InvalidModel("Index projection must be 'all', 'keys', or a list of Columns or Column names.")
    return validated_projection


def validate_stream(meta):
    stream = meta.stream
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


def validate_encryption(meta):
    encryption = meta.encryption
    if encryption is None:
        return

    if not isinstance(encryption, collections.abc.MutableMapping):
        raise InvalidModel("Encryption must be None or a dict.")
    if "enabled" not in encryption:
        raise InvalidModel("Encryption must specify whether it is enabled with the 'enabled' key.")


def validate_backups(meta):
    backups = meta.backups
    if backups is None:
        return

    if not isinstance(backups, collections.abc.MutableMapping):
        raise InvalidModel("Backups must be None or a dict.")
    if "enabled" not in backups:
        raise InvalidModel("Backups must specify whether it is enabled with the 'enabled' key.")


def validate_billing(meta):
    billing = meta.billing
    if billing is None:
        return
    if not isinstance(billing, collections.abc.MutableMapping):
        raise InvalidModel("Billing must be None or a dict.")
    if "mode" not in billing:
        raise InvalidModel("Billing must specify whether it is enabled with the 'enabled' key.")
    mode = billing["mode"]
    if mode not in {"provisioned", "on_demand"}:
        raise InvalidModel("Billing mode must be one of 'provisioned' or 'on_demand'")


def validate_ttl(meta):
    ttl = meta.ttl
    if ttl is None:
        return
    if not isinstance(ttl, collections.abc.MutableMapping):
        raise InvalidModel("TTL must be None or a dict.")
    if "column" not in ttl:
        raise InvalidModel("TTL must specify the column to use with the 'column' key.")
    ttl_column = ttl["column"]
    if isinstance(ttl_column, Column):
        # late-bind to column by name in case it was re-bound since declaration
        ttl["column"] = meta.columns_by_name[ttl_column.name]
    elif isinstance(ttl_column, str):
        ttl["column"] = meta.columns_by_name[ttl_column]
    else:
        raise InvalidModel("TTL column must be a column name or column instance.")

    typedef = ttl["column"].typedef
    if typedef.backing_type != Number.backing_type:
        # special case this check for common confusion between DateTime and Timestamp
        if isinstance(typedef, DateTime):
            raise InvalidModel(
                "TTL column must be a unix timestamp but was a bloop.DateTime instead.  "
                "Did you mean to use bloop.Timestamp?")
        else:
            raise InvalidModel(
                "TTL column must be a unix timestamp with backing_type 'N' but was "
                f"{typedef.backing_type!r} instead.")
    ttl.setdefault("enabled", "disabled")


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
        class Meta(IMeta):
            pass

        meta = cls.Meta = Meta

    meta.model = cls

    setdefault(meta, "init", lambda: cls.__new__(cls))
    setdefault(meta, "abstract", False)

    setdefault(meta, "table_name", cls.__name__)
    setdefault(meta, "write_units", None)
    setdefault(meta, "read_units", None)
    setdefault(meta, "stream", None)
    setdefault(meta, "ttl", None)
    setdefault(meta, "encryption", None)
    setdefault(meta, "backups", None)
    setdefault(meta, "billing", None)

    setdefault(meta, "hash_key", None)
    setdefault(meta, "range_key", None)
    setdefault(meta, "keys", set())

    setdefault(meta, "columns", set())
    setdefault(meta, "columns_by_name", dict())
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

    return meta


def bind_column(model, name, column, force=False, recursive=False, copy=False) -> Column:
    """Bind a column to the model with the given name.

    This method is primarily used during BaseModel.__init_subclass__, although it can be used to easily
    attach a new column to an existing model:

    .. code-block:: python

        import bloop.models

        class User(BaseModel):
            id = Column(String, hash_key=True)


        email = Column(String, dynamo_name="e")
        bound = bloop.models.bind_column(User, "email", email)
        assert bound is email

        # rebind with force, and use a copy
        bound = bloop.models.bind_column(User, "email", email, force=True, copy=True)
        assert bound is not email

    If an existing index refers to this column, it will be updated to point to the new column
    using :meth:`~bloop.models.refresh_index`, including recalculating the index projection.
    Meta attributes including ``Meta.columns``, ``Meta.hash_key``, etc. will be updated if necessary.

    If ``name`` or the column's ``dynamo_name`` conflicts with an existing column or index on the model, raises
    :exc:`~bloop.exceptions.InvalidModel` unless ``force`` is True. If ``recursive`` is ``True`` and there are
    existing subclasses of ``model``, a copy of the column will attempt to bind to each subclass.  The recursive
    calls will not force the bind, and will always use a new copy.  If ``copy`` is ``True`` then a copy of the
    provided column is used.  This uses a shallow copy via :meth:`~bloop.models.Column.__copy__`.

    :param model:
        The model to bind the column to.
    :param name:
        The name to bind the column as.  In effect, used for ``setattr(model, name, column)``
    :param column:
        The column to bind to the model.
    :param force:
        Unbind existing columns or indexes with the same name or dynamo_name.  Default is False.
    :param recursive:
        Bind to each subclass of this model.  Default is False.
    :param copy:
        Use a copy of the column instead of the column directly.  Default is False.
    :return:
        The bound column.  This is a new column when ``copy`` is True, otherwise the input column.
    """
    if not subclassof(model, BaseModel):
        raise InvalidModel(f"{model} is not a subclass of BaseModel")
    meta = model.Meta
    if copy:
        column = copyfn(column)
    # TODO elif column.model is not None: logger.warning(f"Trying to rebind column bound to {column.model}")
    column._name = name
    safe_repr = unbound_repr(column)

    # Guard against name, dynamo_name collisions; if force=True, unbind any matches
    same_dynamo_name = (
        util.index(meta.columns, "dynamo_name").get(column.dynamo_name) or
        util.index(meta.indexes, "dynamo_name").get(column.dynamo_name)
    )
    same_name = (
        meta.columns_by_name.get(column.name) or
        util.index(meta.indexes, "name").get(column.name)
    )

    if column.hash_key and column.range_key:
        raise InvalidModel(f"Tried to bind {safe_repr} as both a hash and range key.")

    if force:
        if same_name:
            unbind(meta, name=column.name)
        if same_dynamo_name:
            unbind(meta, dynamo_name=column.dynamo_name)
    else:
        if same_name:
            raise InvalidModel(
                f"The column {safe_repr} has the same name as an existing column "
                f"or index {same_name}.  Did you mean to bind with force=True?")
        if same_dynamo_name:
            raise InvalidModel(
                f"The column {safe_repr} has the same dynamo_name as an existing "
                f"column or index {same_name}.  Did you mean to bind with force=True?")
        if column.hash_key and meta.hash_key:
            raise InvalidModel(
                f"Tried to bind {safe_repr} but {meta.model} "
                f"already has a different hash_key: {meta.hash_key}")
        if column.range_key and meta.range_key:
            raise InvalidModel(
                f"Tried to bind {safe_repr} but {meta.model} "
                f"already has a different range_key: {meta.range_key}")

    # success!
    # --------------------------------
    column.model = meta.model
    meta.columns.add(column)
    meta.columns_by_name[name] = column
    setattr(meta.model, name, column)

    if column.hash_key:
        meta.hash_key = column
        meta.keys.add(column)
    if column.range_key:
        meta.range_key = column
        meta.keys.add(column)

    try:
        for index in meta.indexes:
            refresh_index(meta, index)
    except KeyError as e:
        raise InvalidModel(
            f"Binding column {column} removed a required column for index {unbound_repr(index)}") from e

    if recursive:
        for subclass in util.walk_subclasses(meta.model):
            try:
                bind_column(subclass, name, column, force=False, recursive=False, copy=True)
            except InvalidModel:
                pass

    return column


def bind_index(model, name, index, force=False, recursive=True, copy=False) -> Index:
    """Bind an index to the model with the given name.

        This method is primarily used during BaseModel.__init_subclass__, although it can be used to easily
        attach a new index to an existing model:

        .. code-block:: python

            import bloop.models

            class User(BaseModel):
                id = Column(String, hash_key=True)
                email = Column(String, dynamo_name="e")


            by_email = GlobalSecondaryIndex(projection="keys", hash_key="email")
            bound = bloop.models.bind_index(User, "by_email", by_email)
            assert bound is by_email

            # rebind with force, and use a copy
            bound = bloop.models.bind_index(User, "by_email", by_email, force=True, copy=True)
            assert bound is not by_email

        If ``name`` or the index's ``dynamo_name`` conflicts with an existing column or index on the model, raises
        :exc:`~bloop.exceptions.InvalidModel` unless ``force`` is True. If ``recursive`` is ``True`` and there are
        existing subclasses of ``model``, a copy of the index will attempt to bind to each subclass.  The recursive
        calls will not force the bind, and will always use a new copy.  If ``copy`` is ``True`` then a copy of the
        provided index is used.  This uses a shallow copy via :meth:`~bloop.models.Index.__copy__`.

        :param model:
            The model to bind the index to.
        :param name:
            The name to bind the index as.  In effect, used for ``setattr(model, name, index)``
        :param index:
            The index to bind to the model.
        :param force:
            Unbind existing columns or indexes with the same name or dynamo_name.  Default is False.
        :param recursive:
            Bind to each subclass of this model.  Default is False.
        :param copy:
            Use a copy of the index instead of the index directly.  Default is False.
        :return:
            The bound index.  This is a new column when ``copy`` is True, otherwise the input index.
        """
    if not subclassof(model, BaseModel):
        raise InvalidModel(f"{model} is not a subclass of BaseModel")
    meta = model.Meta
    if copy:
        index = copyfn(index)
    # TODO elif index.model is not None: logger.warning(f"Trying to rebind index bound to {index.model}")
    index._name = name
    safe_repr = unbound_repr(index)

    # Guard against name, dynamo_name collisions; if force=True, unbind any matches
    same_dynamo_name = (
        util.index(meta.columns, "dynamo_name").get(index.dynamo_name) or
        util.index(meta.indexes, "dynamo_name").get(index.dynamo_name)
    )
    same_name = (
        meta.columns_by_name.get(index.name) or
        util.index(meta.indexes, "name").get(index.name)
    )

    if isinstance(index, LocalSecondaryIndex) and not meta.range_key:
        raise InvalidModel("An LSI requires the Model to have a range key.")

    if force:
        if same_name:
            unbind(meta, name=index.name)
        if same_dynamo_name:
            unbind(meta, dynamo_name=index.dynamo_name)
    else:
        if same_name:
            raise InvalidModel(
                f"The index {safe_repr} has the same name as an existing index "
                f"or column {same_name}.  Did you mean to bind with force=True?")
        if same_dynamo_name:
            raise InvalidModel(
                f"The index {safe_repr} has the same dynamo_name as an existing "
                f"index or column {same_name}.  Did you mean to bind with force=True?")

    # success!
    # --------------------------------
    index.model = meta.model
    meta.indexes.add(index)
    setattr(meta.model, name, index)

    if isinstance(index, LocalSecondaryIndex):
        meta.lsis.add(index)
    if isinstance(index, GlobalSecondaryIndex):
        meta.gsis.add(index)

    try:
        refresh_index(meta, index)
    except KeyError as e:
        raise InvalidModel("Index expected a hash or range key that does not exist") from e

    if recursive:
        for subclass in util.walk_subclasses(meta.model):
            try:
                bind_index(subclass, name, index, force=False, recursive=False, copy=True)
            except InvalidModel:
                pass

    return index


def refresh_index(meta, index) -> None:
    """Recalculate the projection, hash_key, and range_key for the given index.

    :param meta: model.Meta to find columns by name
    :param index: The index to refresh
    """
    # All projections include model + index keys
    projection_keys = set.union(meta.keys, index.keys)

    proj = index.projection
    mode = proj["mode"]

    if mode == "keys":
        proj["included"] = projection_keys
    elif mode == "all":
        proj["included"] = meta.columns
    elif mode == "include":  # pragma: no branch
        if all(isinstance(p, str) for p in proj["included"]):
            proj["included"] = set(meta.columns_by_name[n] for n in proj["included"])
        else:
            proj["included"] = set(proj["included"])
        proj["included"].update(projection_keys)

    if proj["strict"]:
        proj["available"] = proj["included"]
    else:
        proj["available"] = meta.columns


def unbind(meta, name=None, dynamo_name=None) -> None:
    """Unconditionally remove any columns or indexes bound to the given name or dynamo_name.

    .. code-block:: python

        import bloop.models


        class User(BaseModel):
            id = Column(String, hash_key=True)
            email = Column(String, dynamo_name="e")
            by_email = GlobalSecondaryIndex(projection="keys", hash_key=email)


        for dynamo_name in ("id", "e", "by_email"):
            bloop.models.unbind(User.Meta, dynamo_name=dynamo_name)

        assert not User.Meta.columns
        assert not User.Meta.indexes
        assert not User.Meta.keys

    .. warning::
        This method does not pre- or post- validate the model with the requested changes.  You are responsible
        for ensuring the model still has a hash key, that required columns exist for each index, etc.

    :param meta: model.Meta to remove the columns or indexes from
    :param name: column or index name to unbind by.  Default is None.
    :param dynamo_name: column or index name to unbind by.  Default is None.
    """
    if name is not None:
        columns = {x for x in meta.columns if x.name == name}
        indexes = {x for x in meta.indexes if x.name == name}
    elif dynamo_name is not None:
        columns = {x for x in meta.columns if x.dynamo_name == dynamo_name}
        indexes = {x for x in meta.indexes if x.dynamo_name == dynamo_name}
    else:
        raise RuntimeError("Must provide name= or dynamo_name= to unbind from meta")

    # Nothing in bloop should allow name or dynamo_name
    # collisions to exist, so this is either a bug or
    # the user manually hacked up meta.
    assert len(columns) <= 1
    assert len(indexes) <= 1
    assert not (columns and indexes)

    if columns:
        [column] = columns
        meta.columns.remove(column)

        # If these don't line up, there's likely a bug in bloop
        # or the user manually hacked up columns_by_name
        expect_same = meta.columns_by_name[column.name]
        assert expect_same is column
        meta.columns_by_name.pop(column.name)

        if column in meta.keys:
            meta.keys.remove(column)
        if meta.hash_key is column:
            meta.hash_key = None
        if meta.range_key is column:
            meta.range_key = None

        delattr(meta.model, column.name)

    if indexes:
        [index] = indexes
        meta.indexes.remove(index)
        if index in meta.gsis:
            meta.gsis.remove(index)
        if index in meta.lsis:
            meta.lsis.remove(index)

        delattr(meta.model, index.name)


# required to bootstrap BaseModel.__init_subclass__
initialize_meta(BaseModel)
