import base64
import collections.abc
import datetime
import decimal
import uuid


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

SUPPORTED_OPERATIONS = {
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


def supports_operation(operation, typedef):
    return typedef.backing_type in SUPPORTED_OPERATIONS[operation]


class Type:
    """Abstract base type."""

    python_type = None
    backing_type = None

    def __init__(self):
        if not hasattr(self, "inner_typedef"):
            self.inner_typedef = self
        super().__init__()

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
        value = self.dynamo_dump(value, **kwargs)
        if value is None:
            return value
        return {self.backing_type: value}

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
        return value

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
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
            return None
        return base64.b64decode(value)

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
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
        for value in values:
            value = self.inner_typedef.dynamo_dump(value, context=context, **kwargs)
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
            all_answers = List(SingleQuizAnswers)

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
        return [
            self.inner_typedef._load(value, context=context, **kwargs)
            for value in values]

    def dynamo_dump(self, values, *, context, **kwargs):
        if values is None:
            return None
        dumped = (self.inner_typedef._dump(value, context=context, **kwargs) for value in values)
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
            products = List(Product)

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
            value = typedef._load(values.get(key, None), context=context, **kwargs)
            loaded[key] = value
        return loaded

    def dynamo_dump(self, values, *, context, **kwargs):
        if values is None:
            return None
        dumped = {}
        for key, typedef in self.types.items():
            value = typedef._dump(values.get(key, None), context=context, **kwargs)
            if value is not None:
                dumped[key] = value
        return dumped or None
