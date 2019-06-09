import base64
import collections.abc
import datetime
import decimal
import numbers
import uuid
from typing import ClassVar

from . import actions


ENCODING = "utf-8"
STRING = "S"
NUMBER = "N"
BINARY = "B"
BOOLEAN = "BOOL"
MAP = "M"
LIST = "L"

PRIMITIVES = {"S", "N", "B"}
SETS = {"SS", "NS", "BS"}
DOCUMENTS = {"L", "M"}
ALL = {*PRIMITIVES, *SETS, *DOCUMENTS, BOOLEAN}

# Dynamo takes numbers as strings to reduce inter-language problems
DYNAMODB_CONTEXT = decimal.Context(
    Emin=-128, Emax=126, rounding=None, prec=38,
    traps=[
        decimal.Clamped, decimal.Overflow, decimal.Inexact,
        decimal.Rounded, decimal.Underflow
    ]
)

OPERATION_SUPPORT_BY_OP = {
    "==": ALL,
    "!=": ALL,
    "<": PRIMITIVES,
    ">": PRIMITIVES,
    "<=": PRIMITIVES,
    ">=": PRIMITIVES,
    "begins_with": {STRING, BINARY},
    "between": PRIMITIVES,
    "contains": {*SETS, STRING, BINARY, LIST},
    "in": ALL
}
OPERATION_SUPPORT_BY_TYPE = {
    t: {op for (op, supported) in OPERATION_SUPPORT_BY_OP.items() if t in supported}
    for t in ALL
}


class Type:
    """Abstract base type."""

    python_type = None
    backing_type = None

    def supports_operation(self, operation: str) -> bool:
        """
        Used to ensure a conditional operation is supported by this type.

        By default, uses a hardcoded table of operations that maps to each backing DynamoDB type.

        You can override this method to implement your own conditional operators, or to dynamically
        adjust which operations your type supports.
        """
        return operation in OPERATION_SUPPORT_BY_TYPE[self.backing_type]

    def __init__(self):
        if not hasattr(self, "inner_typedef"):
            self.inner_typedef = self
        super().__init__()

    def __getitem__(self, key):
        raise RuntimeError(f"{self!r} does not support document paths")

    def dynamo_dump(self, value, *, context, **kwargs):
        """Converts a local value into a DynamoDB value.

        For example, to store a string enum as an integer:

        .. code-block:: python

            def dynamo_dump(self, value, *, context, **kwargs):
                colors = ["red", "blue", "green"]
                return colors.index(value.lower())
        """
        raise NotImplementedError

    def dynamo_load(self, value, *, context, **kwargs):
        """Converts a DynamoDB value into a local value.

        For example, to load a string enum from an integer:

        .. code-block:: python

            def dynamo_dump(self, value, *, context, **kwargs):
                colors = ["red", "blue", "green"]
                return colors[value]
        """
        raise NotImplementedError

    def _dump(self, value, **kwargs):
        """Entry point for serializing values.  Most custom types should use :func:`~bloop.types.Type.dynamo_dump`.

        This wraps the return value of :func:`~bloop.types.Type.dynamo_dump` in DynamoDB's wire format.
        For example, serializing a string enum to an int:

        .. code-block:: python

            value = "green"
            # dynamo_dump("green") = 2
            _dump(value) == {"N": 2}

        If a complex type calls this function with ``None``, it will forward ``None`` to
        :func:`~bloop.types.Type.dynamo_dump`.  This can happen when dumping eg. a sparse
        :class:`~.bloop.types.Map`, or a missing (not set) value.
        """
        def real_dump(v):
            v = self.dynamo_dump(v, **kwargs)
            if v is None:
                return v
            return {self.backing_type: v}

        # TODO in 3.0 the code path will simplify by first calling ``value = actions.wrap(value)``
        #   but for 2.4 we don't return an Action unless one is passed
        if isinstance(value, actions.Action):
            value.value = real_dump(value.value)
            return value

        return real_dump(value)

    def _load(self, value, **kwargs):
        """Entry point for deserializing values.  Most custom types should use :func:`~bloop.types.Type.dynamo_load`.

        This unpacks DynamoDB's wire format and calls :func:`~bloop.types.Type.dynamo_load` on the inner value.
        For example, deserializing an int to a string enum:

        .. code-block:: python

            value = {"N": 2}
            # dynamo_load(2) = "green"
            _load(value) == "green"

        If a complex type calls this function with ``None``, it will forward ``None`` to
        :func:`~bloop.types.Type.dynamo_load`.  This can happen when loading eg. a sparse :class:`~bloop.types.Map`.
        """
        if value is not None:
            value = next(iter(value.values()))
        return self.dynamo_load(value, **kwargs)

    def __repr__(self):
        # Render class python types by name
        python_type = self.python_type
        if isinstance(python_type, type):
            python_type = python_type.__name__

        return "<{}[{}:{}]>".format(
            self.__class__.__name__,
            self.backing_type, python_type
        )


class String(Type):
    python_type = str
    backing_type = STRING

    def dynamo_load(self, value, *, context, **kwargs):
        if not value:
            return ""
        return value

    def dynamo_dump(self, value, *, context, **kwargs):
        if not value:
            return None
        return value


class UUID(String):
    python_type = uuid.UUID

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return None
        return uuid.UUID(value)

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        return str(value)


FIXED_ISO8601_FORMAT = "%Y-%m-%dT%H:%M:%S.%f+00:00"


class DateTime(String):
    """Always stored in DynamoDB using the :data:`~bloop.types.FIXED_ISO8601_FORMAT` format.

    Naive datetimes (``tzinfo is None``) are not supported, and trying to use one will raise ``ValueError``.

    .. code-block:: python

        from datetime import datetime, timedelta, timezone

        class Model(Base):
            id = Column(Integer, hash_key=True)
            date = Column(DateTime)
        engine.bind()

        obj = Model(id=1, date=datetime.now(timezone.utc))
        engine.save(obj)

        one_day_ago = datetime.now(timezone.utc) - timedelta(days=1)

        query = engine.query(
            Model,
            key=Model.id==1,
            filter=Model.date >= one_day_ago)

        query.first().date

    .. note::

        To use common datetime libraries such as `arrow`_, `delorean`_, or `pendulum`_,
        see :ref:`DateTime and Timestamp Extensions <user-extensions-datetime>` in the user guide.  These
        are drop-in replacements and support non-utc timezones:

        .. code-block:: python

            from bloop import DateTime  # becomes:
            from bloop.ext.pendulum import DateTime

    .. _arrow: http://crsmithdev.com/arrow
    .. _delorean: https://delorean.readthedocs.io/en/latest/
    .. _pendulum: https://pendulum.eustace.io
    """
    python_type = datetime.datetime

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return None
        dt = datetime.datetime.strptime(value, FIXED_ISO8601_FORMAT)
        # we assume all stored values are utc, so we simply force timezone to utc
        # without changing the day/time values
        return dt.replace(tzinfo=datetime.timezone.utc)

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError(
                "naive datetime instances are not supported.  You can set a timezone with either "
                "your_dt.replace(tzinfo=) or your_dt.astimezone(tz=).  WARNING: calling astimezone on a naive "
                "datetime will assume the naive datetime is in the system's timezone, even though "
                "datetime.utcnow() creates a naive object!  You almost certainly don't want to do that."
            )
        dt = value.astimezone(tz=datetime.timezone.utc)
        return dt.strftime(FIXED_ISO8601_FORMAT)


class Number(Type):
    """Base for all numeric types.

    :param context: *(Optional)* :class:`decimal.Context` used to translate numbers.  Default is a context that
        matches DynamoDB's `stated limits`__, taken from `boto3`__.

    __ https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-data-types-numbers
    __ https://github.com/boto/boto3/blob/dffeb393a795204f375b951d791c768be6b1cb8c/boto3/dynamodb/types.py#L32
    """
    python_type = decimal.Decimal
    backing_type = NUMBER

    def __init__(self, context=None):
        self.context = context or DYNAMODB_CONTEXT
        super().__init__()

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return None
        return self.context.create_decimal(value)

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        n = str(self.context.create_decimal(value))
        if any(filter(lambda x: x in n, ("Infinity", "NaN"))):
            raise TypeError("{!r} does not support Infinity and NaN.".format(self))
        return n


class Integer(Number):
    """Truncates values when loading or dumping.

    For example, ``3.14`` in DynamoDB is loaded as ``3``. If a value is ``7.5``
    locally, it's stored in DynamoDB as ``7``.
    """
    python_type = int

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return None
        number = super().dynamo_load(value, context=context, **kwargs)
        return int(number)

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        value = int(value)
        return super().dynamo_dump(value, context=context, **kwargs)


class Timestamp(Integer):
    """Stores the unix (epoch) time in seconds.  Milliseconds are truncated to 0 on load and save.

    Naive datetimes (``tzinfo is None``) are not supported, and trying to use one will raise ``ValueError``.

    .. code-block:: python

        from datetime import datetime, timedelta, timezone

        class Model(Base):
            id = Column(Integer, hash_key=True)
            date = Column(Timestamp)
        engine.bind()

        obj = Model(id=1, date=datetime.now(timezone.utc))
        engine.save(obj)

        one_day_ago = datetime.now(timezone.utc) - timedelta(days=1)

        query = engine.query(
            Model,
            key=Model.id==1,
            filter=Model.date >= one_day_ago)

        query.first().date

    .. note::

        To use common datetime libraries such as `arrow`_, `delorean`_, or `pendulum`_,
        see :ref:`DateTime and Timestamp Extensions <user-extensions-datetime>` in the user guide.  These
        are drop-in replacements and support non-utc timezones:

        .. code-block:: python

            from bloop import Timestamp  # becomes:
            from bloop.ext.pendulum import Timestamp

    .. _arrow: http://crsmithdev.com/arrow
    .. _delorean: https://delorean.readthedocs.io/en/latest/
    .. _pendulum: https://pendulum.eustace.io
    """
    python_type = datetime.datetime

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return None
        value = super().dynamo_load(value, context=context, **kwargs)
        # we assume all stored values are utc, so we simply force timezone to utc
        # without changing the day/time values
        return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError(
                "naive datetime instances are not supported.  You can set a timezone with either "
                "your_dt.replace(tzinfo=) or your_dt.astimezone(tz=).  WARNING: calling astimezone on a naive "
                "datetime will assume the naive datetime is in the system's timezone, even though "
                "datetime.utcnow() creates a naive object!  You almost certainly don't want to do that."
            )
        value = value.timestamp()
        return super().dynamo_dump(value, context=context, **kwargs)


class Binary(Type):
    python_type = bytes
    backing_type = BINARY

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return b""
        return base64.b64decode(value)

    def dynamo_dump(self, value, *, context, **kwargs):
        if not value:
            return None
        return base64.b64encode(value).decode("utf-8")


class Boolean(Type):
    python_type = bool
    backing_type = BOOLEAN

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return None
        return bool(value)

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        return bool(value)


def subclassof(c, b):
    """Wrap issubclass to return True/False without throwing TypeError"""
    try:
        return issubclass(c, b)
    except TypeError:
        return False


def type_instance(typedef):
    """Returns an instance of a type class, or the instance if provided"""
    if subclassof(typedef, Type):
        # Type class passed, create no-arg instance
        typedef = typedef()
    return typedef


def guard_no_action(func):
    def call(value, *, context, **kwargs):
        # guard call to _load or _dump
        if isinstance(value, actions.Action):
            if not value.type.nestable:
                raise ValueError(f"cannot nest the action type {value.type}")
            value = actions.unwrap(value)
        value = func(value, context=context, **kwargs)

        # guard response from _dump
        if isinstance(value, actions.Action):
            if not value.type.nestable:
                raise ValueError(f"cannot nest the action type {value.type}")
            value = actions.unwrap(value)

        return value

    return call


class Set(Type):
    """Generic set type.  Must provide an inner type.

    .. code-block:: python

        class Customer(BaseModel):
            id = Column(Integer, hash_key=True)
            account_ids = Column(Set(UUID))

    :param typedef: The type to use when loading and saving values in this set.
        Must have a ``backing_type`` of "S", "N", or "B".
    """
    python_type = collections.abc.Set

    def __init__(self, typedef):
        self.inner_typedef = type_instance(typedef)
        self.backing_type = typedef.backing_type + "S"
        if self.backing_type not in {"NS", "SS", "BS"}:
            raise TypeError("{!r} is not a valid set type.".format(self.backing_type))
        super().__init__()

    def dynamo_load(self, values, *, context, **kwargs):
        if values is None:
            return set()
        return set(
            self.inner_typedef.dynamo_load(value, context=context, **kwargs)
            for value in values)

    def dynamo_dump(self, values, *, context, **kwargs):
        if values is None:
            return None
        dumped = []
        dump = guard_no_action(self.inner_typedef.dynamo_dump)
        for value in values:
            value = dump(value, context=context, **kwargs)
            if value is not None:
                dumped.append(value)
        return dumped or None


class List(Type):
    """Holds values of a single type.

    Similar to :class:`~bloop.types.Set` because it requires a single type.  However, that type
    can be another List, or :class:`~bloop.types.Map`, or :class:`~bloop.types.Boolean`.  This is restricted
    to a single type even though DynamoDB is not because there is no way to know which Type to load a DynamoDB value
    with.

    For example, ``{"S": "6d8b54a2-fa07-47e1-9305-717699459293"}`` could be loaded with
    :class:`~bloop.types.UUID`, :class:`~bloop.types.String`, or any other class that is backed by "S".

    .. code-block:: python

        SingleQuizAnswers = List(String)

        class AnswerBook(BaseModel):
            ...
            all_answers = Column(List(SingleQuizAnswers))

    .. seealso::

        To store arbitrary lists, see :class:`~bloop.types.DynamicList`.

    :param typedef: The type to use when loading and saving values in this list.
    """
    python_type = collections.abc.Iterable
    backing_type = LIST

    def __init__(self, typedef):
        self.inner_typedef = type_instance(typedef)
        super().__init__()

    def __getitem__(self, key):
        return self.inner_typedef

    def dynamo_load(self, values, *, context, **kwargs):
        if values is None:
            return list()
        # noinspection PyProtectedMember
        load = self.inner_typedef._load
        return [
            load(value, context=context, **kwargs)
            for value in values]

    def dynamo_dump(self, values, *, context, **kwargs):
        if values is None:
            return None
        # noinspection PyProtectedMember
        dump = guard_no_action(self.inner_typedef._dump)
        dumped = (dump(value, context=context, **kwargs) for value in values)
        return [value for value in dumped if value is not None] or None


class Map(Type):
    """Mapping of fixed keys and their Types.

    .. code-block:: python

        Metadata = Map(**{
            "created": DateTime,
            "referrer": UUID,
            "cache": String
        })

        Product = Map(
            id=Integer,
            metadata=Metadata,
            price=Number
        )

        class ProductCatalog(BaseModel):
            ...
            all_products = Column(List(Product))

    .. seealso::

        To store arbitrary documents, see :class:`~bloop.types.DynamicMap`.

    :param types: *(Optional)* specifies the keys and their Types when loading and dumping the Map.
        Any keys that aren't specified in ``types`` are ignored when loading and dumping.
    """
    python_type = collections.abc.Mapping
    backing_type = MAP

    def __init__(self, **types):
        self.types = {k: type_instance(t) for k, t in types.items()}
        super().__init__()

    def __getitem__(self, key):
        """Overload allows easy nested access to types"""
        return self.types[key]

    def dynamo_load(self, values, *, context, **kwargs):
        if values is None:
            values = dict()
        loaded = {}
        for key, typedef in self.types.items():
            # noinspection PyProtectedMember
            value = typedef._load(values.get(key, None), context=context, **kwargs)
            loaded[key] = value
        return loaded

    def dynamo_dump(self, values, *, context, **kwargs):
        if values is None:
            return None
        dumped = {}
        for key, typedef in self.types.items():
            # noinspection PyProtectedMember
            dump = guard_no_action(typedef._dump)
            value = dump(values.get(key, None), context=context, **kwargs)
            if value is not None:
                dumped[key] = value
        return dumped or None


class DynamicType(Type):
    """
    Dynamically dumps a value based on its python type.

    This is used by DynamicList, DynamicMap to handle path resolution before the value for an arbitrary path is known.
    For example, given the following model:

    .. code-block:: python

        class UserUpload(BaseModel):
            id = Column(String, hash_key=True)
            doc = Column(DynamicMap)

    And an instance as follows:

    .. code-block:: python

        u = UserUpload(id="numberoverzero")
        u.doc = {
            "foo": ["bar", {0: "a", 1: "b"}, True]
        }

    The renderer must know a type for ``UserUpload.doc["foo"][1][0]`` before the value is provided.
    An instance of this type will return itself for any value during ``__getitem__``, and then inspects the value type
    during _dump to create the correct simple type.

    Because ``DynamicType`` requires access to the DynamoDB type annotation, you must call ``_load`` and ``_dump``,
    as ``dynamo_load`` and ``dynamo_dump`` can't be implemented.  For example:

    .. code-block:: python

        DynamicType.i._load({"S": "2016-08-09T01:16:25.322849+00:00"})
            -> "2016-08-09T01:16:25.322849+00:00"
        DynamicType.i._load({"N": "3.14"}) -> Decimal('3.14')

        DynamicType.i._dump([1, True, "f"])
            -> {"L": [{"N": "1"}, {"BOOL": true}, {"S": "f"}]}
        DynamicType.i._dump({b"1", b"2"}) -> {"BS": ["MQ==", b"Mg=="]}
    """
    i: ClassVar["DynamicType"]

    def supports_operation(self, operation: str) -> bool:
        """Always True, because the type of the value passing through DynamicType is late bound"""
        return True

    def __getitem__(self, key):
        """Overload allows easy nested access to types"""
        return self

    def _load(self, value, **kwargs):
        if value is None:
            return None
        vtype = DynamicType.extract_backing_type(value)
        return DYNAMIC_TYPES[vtype]._load(value, **kwargs)

    def _dump(self, value, **kwargs):
        def real_dump(v):
            if v is None:
                return None
            vtype = DynamicType.backing_type_for(v)
            return DYNAMIC_TYPES[vtype]._dump(v, **kwargs)

        # TODO in 3.0 the code path will simplify by first calling ``value = actions.wrap(value)``
        #   but for 2.4 we don't return an Action unless one is passed
        if isinstance(value, actions.Action):
            value.value = real_dump(value.value)
            return value
        else:
            return real_dump(value)

    def dynamo_load(self, value, *, context, **kwargs):
        raise NotImplementedError

    def dynamo_dump(self, value, *, context, **kwargs):
        raise NotImplementedError

    @staticmethod
    def extract_backing_type(value: dict) -> str:
        """Returns the DynamoDB backing type from a given wire dict

        ::

            {'S': 'foo'} -> 'S'
        """
        return next(iter(value.keys()))

    @staticmethod
    def backing_type_for(value):
        """Returns the DynamoDB backing type for a given python value's type

        ::

            4 -> 'N'
            ['x', 3] -> 'L'
            {2, 4} -> 'SS'
        """
        if isinstance(value, str):
            vtype = "S"
        elif isinstance(value, bytes):
            vtype = "B"
        # NOTE: numbers.Number check must come **AFTER** bool check since isinstance(True, numbers.Number)
        elif isinstance(value, bool):
            vtype = "BOOL"
        elif isinstance(value, numbers.Number):
            vtype = "N"
        elif isinstance(value, dict):
            vtype = "M"
        elif isinstance(value, list):
            vtype = "L"
        elif isinstance(value, set):
            if not value:
                vtype = "SS"  # doesn't matter, Set(x) should dump an empty set the same for all x
            else:
                inner = next(iter(value))
                if isinstance(inner, str):
                    vtype = "SS"
                elif isinstance(inner, bytes):
                    vtype = "BS"
                elif isinstance(inner, numbers.Number):
                    vtype = "NS"
                else:
                    raise ValueError(f"Unknown set type for inner value {inner!r}")
        else:
            raise ValueError(f"Can't dump unexpected type {type(value)!r} for value {value!r}")
        return vtype


# Singleton instance for re-use.
# It's unlikely we'll ever need more than one.
DynamicType.i = DynamicType()


class DynamicList(Type):
    """Holds a list of arbitrary values, including other DynamicLists and DynamicMaps.

    Similar to :class:`~bloop.types.List` but is not constrained to a single type.

    .. code-block:: python

        value = [1, True, "f"]
        DynamicList()._dump(value)
            -> {"L": [{"N": "1"}, {"BOOL": true}, {"S": "f"}]}

    .. note::

        Values will only be loaded and dumped as their DynamoDB backing types.  This means datetimes and uuids are
        stored and loaded as strings, and timestamps are stored and loaded as integers.  For more information, see
        :ref:`dynamic-documents`.
    """
    python_type = collections.abc.Iterable
    backing_type = LIST

    def __getitem__(self, key):
        """Overload allows easy nested access to types"""
        return DynamicType.i

    # noinspection PyProtectedMember
    def dynamo_load(self, values, *, context, **kwargs):
        if values is None:
            return []
        load = DynamicType.i._load
        return [
            load(value, context=context, **kwargs)
            for value in values]

    def dynamo_dump(self, values, *, context, **kwargs):
        if values is None:
            return None
        # noinspection PyProtectedMember
        dump = guard_no_action(DynamicType.i._dump)
        dumped = (dump(value, context=context, **kwargs) for value in values)
        return [value for value in dumped if value is not None] or None


class DynamicMap(Type):
    """Holds a dictionary of arbitrary values, including other DynamicLists and DynamicMaps.

        Similar to :class:`~bloop.types.Map` but is not constrained to a single type.

        .. code-block:: python

            value = {"f": 1, "in": [True]]
            DynamicMap()._dump(value)
                -> {"M": {"f": {"N": 1}, "in": {"L": [{"BOOL": true}]}}}

        .. note::

            Values will only be loaded and dumped as their DynamoDB backing types.  This means datetimes and uuids are
            stored and loaded as strings, and timestamps are stored and loaded as integers.  For more information, see
            :ref:`dynamic-documents`.
        """
    python_type = collections.abc.Mapping
    backing_type = MAP

    def __getitem__(self, key):
        """Overload allows easy nested access to types"""
        return DynamicType.i

    def dynamo_load(self, values, *, context, **kwargs):
        if values is None:
            return {}
        # noinspection PyProtectedMember
        return {
            key: DynamicType.i._load(value, context=context, **kwargs)
            for (key, value) in values.items()
        }

    def dynamo_dump(self, values, *, context, **kwargs):
        if values is None:
            return None
        dumped = {}
        # noinspection PyProtectedMember
        dump = guard_no_action(DynamicType.i._dump)
        for key, value in values.items():
            value = dump(value, context=context, **kwargs)
            if value is not None:
                dumped[key] = value
        return dumped or None


DYNAMIC_TYPES = {
    "S": String(),
    "N": Number(),
    "B": Binary(),
    "BOOL": Boolean(),
    "SS": Set(String),
    "NS": Set(Number),
    "BS": Set(Binary),
    "M": DynamicMap(),
    "L": DynamicList()
}
