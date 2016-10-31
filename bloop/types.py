import base64
import collections.abc
import decimal
import numbers
import uuid

import arrow
import declare


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
    "contains": {*SETS, STRING, BINARY, LIST},  # TODO confirm LIST is actually supported
    "in": ALL,  # FIXME this isn't correct, but haven't tested in_ yet
}


def supports_operation(operation, typedef):
    return typedef.backing_type in SUPPORTED_OPERATIONS[operation]


class Type(declare.TypeDefinition):
    """Abstract base type.

    .. code-block:: python

        class ReversedString(Type):
            python_type = str
            backing_type = "S"

            def dynamo_load(self, value, *, context, **kwargs):
                return str(value[::-1])

            def dynamo_dump(self, value, *, context, **kwargs):
                return str(value[::-1])

    If a type's constructor doesn't have required args, a :class:`~bloop.Column`
    can use the class directly:

    .. code-block:: python

        class SomeModel(BaseModel):
            custom_hash_key = Column(ReversedString, hash_key=True)

    Complex types may need to implement :func:`~_dump`, :func:`~_load`, or :func:`~_register`.
    """

    #: The local Python type.  Optional, not validated.
    python_type = None

    #: This is the DynamoDB type that Bloop will store values under.
    #:
    #: * ``"S"`` -- string
    #: * ``"N"`` -- number
    #: * ``"B"`` -- binary
    #: * ``"BOOL"`` -- boolean
    #: * ``"SS"`` -- string set
    #: * ``"NS"`` -- number set
    #: * ``"BS"`` -- binary set
    #: * ``"M"`` -- map
    #: * ``"L"`` -- list
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
        """Entry point for serializing values.  Most custom types should use :func:`~dynamo_dump`.

        This wraps the return value of :func:`~dynamo_dump` in DynamoDB's wire format.

        For example, serializing a string enum to an int:

        .. code-block:: python

            value = "green"
            # dynamo_dump("green") = 2
            _dump(value) == {"N": 2}

        If a complex type calls this function with ``None``, it will forward ``None`` to :func:`~dynamo_dump`.  This
        can happen when dumping eg. a sparse :class:`~.Map`, or a missing (not set) value.
        """
        value = self.dynamo_dump(value, **kwargs)
        if value is None:
            return value
        return {self.backing_type: value}

    def _load(self, value, **kwargs):
        """Entry point for deserializing values.  Most custom types should use :func:`~dynamo_load`.

        This unpacks DynamoDB's wire format and calls :func:`~dynamo_load` on the inner value.

        For example, deserializing an int to a string enum:

        .. code-block:: python

            value = {"N": 2}
            # dynamo_load(2) = "green"
            _load(value) == "green"

        If a complex type calls this function with ``None``, it will forward ``None`` to :func:`~dynamo_load`.  This
        can happen when loading eg. a sparse :class:`~.Map`.
        """
        if value is not None:
            value = next(iter(value.values()))
        return self.dynamo_load(value, **kwargs)

    def _register(self, engine):
        """Called when the type is registered.

        Register any types this type depends on.  For example, :class:`~.List` and :class:`~.Set`

        .. code-block:: python

            class Container(Type):
                def __init__(self, container_type):
                    self._type = container_type

                def _register(self, engine):
                    engine.register(self._type)
        """
        super()._register(engine)

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
        return str(value)


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


class DateTime(String):
    """Always stored in UTC, backed by :class:`arrow.Arrow` instances.

    A local timezone may be specified when initializing the type - otherwise UTC is used.

    For example, comparisons can be done in any timezone since they
    will all be converted to UTC on request and from UTC on response:

    .. code-block:: python

        class Model(Base):
            id = Column(Integer, hash_key=True)
            date = Column(DateTime(timezone="US/Pacific"))
        engine.bind()

        obj = Model(id=1, date=arrow.now().to("US/Pacific"))
        engine.save(obj)

        paris_one_day_ago = arrow.now().to("Europe/Paris").replace(days=-1)

        query = engine.query(
            Model,
            key=Model.id==1,
            filter=Model.date >= paris_one_day_ago)

        query.first().date

    """
    python_type = arrow.Arrow

    def __init__(self, timezone="utc"):
        self.timezone = timezone
        super().__init__()

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return None
        return arrow.get(value).to(self.timezone)

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        # ALWAYS store in UTC - we can manipulate the timezone on load
        return value.to("utc").isoformat()


class Float(Type):
    python_type = numbers.Number
    backing_type = NUMBER

    def dynamo_load(self, value, *, context, **kwargs):
        if value is None:
            return None
        return DYNAMODB_CONTEXT.create_decimal(value)

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        n = str(DYNAMODB_CONTEXT.create_decimal(value))
        if any(filter(lambda x: x in n, ("Infinity", "NaN"))):
            raise TypeError("{!r} does not support Infinity and NaN.".format(self))
        return n


class Integer(Float):
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
    """Adapter for sets of objects"""
    python_type = collections.abc.Set

    def __init__(self, typedef):
        self.inner_typedef = type_instance(typedef)
        self.backing_type = typedef.backing_type + "S"
        if self.backing_type not in {"NS", "SS", "BS"}:
            raise TypeError("{!r} is not a valid set type.".format(self.backing_type))
        super().__init__()

    def _register(self, engine):
        """Register the set's type"""
        engine.register(self.inner_typedef)

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
    python_type = collections.abc.Iterable
    backing_type = LIST

    def __init__(self, typedef):
        self.inner_typedef = type_instance(typedef)
        super().__init__()

    def __getitem__(self, key):
        return self.inner_typedef

    def _register(self, engine):
        engine.register(self.inner_typedef)

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
    python_type = collections.abc.Mapping
    backing_type = MAP

    def __init__(self, **types):
        self.types = {k: type_instance(t) for k, t in types.items()}
        super().__init__()

    def __getitem__(self, key):
        """Overload allows easy nested access to types"""
        return self.types[key]

    def _register(self, engine):
        """Register all types for the map"""
        for typedef in self.types.values():
            engine.register(typedef)

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
