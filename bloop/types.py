import arrow
import base64
import collections.abc
import decimal
import declare
import numbers
import uuid

TYPES = []
ENCODING = "utf-8"
STRING = "S"
NUMBER = "N"
BINARY = "B"
BOOLEAN = "BOOL"
MAP = "M"
LIST = "L"

# Dynamo takes numbers as strings to reduce inter-language problems
DYNAMODB_CONTEXT = decimal.Context(
    Emin=-128, Emax=126, rounding=None, prec=38,
    traps=[
        decimal.Clamped, decimal.Overflow, decimal.Inexact,
        decimal.Rounded, decimal.Underflow
    ]
)


class Type(declare.TypeDefinition):
    def _load(self, value):
        """
        take a {type: value} dictionary from dynamo and return a python value
        """
        value = next(iter(value.values()))
        if value is None:
            return None
        return self.dynamo_load(value)

    def can_load(self, value):
        """
        whether this type can load the given
        {type: value} dictionary from dynamo
        """
        backing_type = next(iter(value.keys()))
        return backing_type == self.backing_type

    def _dump(self, value):
        """
        dump a python value to a {type: value} dictionary for dynamo storage
        """
        if value is None:
            return {self.backing_type: None}
        return {self.backing_type: self.dynamo_dump(value)}

    def dynamo_load(self, value):
        return value

    def dynamo_dump(self, value):
        return value

    def can_dump(self, value):
        """ whether this type can dump the given value to dynamo """
        return isinstance(value, self.python_type)

    def __repr__(self, *a, **kw):  # pragma: no cover
        return "{}(python_type={}, backing_type={})".format(
            self.__class__.__name__, self.python_type, self.backing_type)
    __str__ = __repr__


class String(Type):
    python_type = str
    backing_type = STRING


class UUID(String):
    python_type = uuid.UUID

    def dynamo_load(self, value):
        return uuid.UUID(value)

    def dynamo_dump(self, value):
        return str(value)


class DateTime(String):
    """DateTimes are ALWAYS stored in UTC, backed by arrow.Arrow instances.

    A local timezone may be specified when initializing the type - otherwise
    UTC is used.

    For example, comparisons can be done in any timezone since they
    will all be converted to UTC on request and from UTC on response::

        class Model(engine.model):
            id = Column(Integer, hash_key=True)
            date = Column(DateTime(timezone="US/Pacific"))
        engine.bind()

        obj = Model(id=1, date=arrow.now().to("US/Pacific"))
        engine.save(obj)

        paris_one_day_ago = arrow.now().to("Europe/Paris").replace(days=-1)

        query = (engine.query(Model)
                       .key(Model.id==1)
                       .filter(Model.date >= paris_one_day_ago))

        results = list(query)
        print(results[0].date)

    """
    python_type = arrow.Arrow
    default_timezone = "UTC"

    def __init__(self, timezone=None):
        self.timezone = timezone or DateTime.default_timezone
        super().__init__()

    def dynamo_load(self, value):
        iso8601_string = super().dynamo_load(value)
        return arrow.get(iso8601_string).to(self.timezone)

    def dynamo_dump(self, value):
        # ALWAYS store in UTC - we can manipulate the timezone on load
        iso8601_string = value.to("utc").isoformat()
        return super().dynamo_dump(iso8601_string)


class Float(Type):
    python_type = numbers.Number
    backing_type = NUMBER

    def dynamo_load(self, value):
        return DYNAMODB_CONTEXT.create_decimal(value)

    def dynamo_dump(self, value):
        n = str(DYNAMODB_CONTEXT.create_decimal(value))
        if any(filter(lambda x: x in n, ("Infinity", "NaN"))):
            raise TypeError("Infinity and NaN not supported")
        return n

    def can_dump(self, value):
        """ explicitly disallow bool and subclasses """
        return (isinstance(value, self.python_type) and not
                isinstance(value, bool))


class Integer(Float):
    python_type = int

    def dynamo_load(self, value):
        number = super().dynamo_load(value)
        return int(number)

    def dynamo_dump(self, value):
        value = int(value)
        return super().dynamo_dump(value)


class Binary(Type):
    python_type = bytes
    backing_type = BINARY

    def dynamo_load(self, value):
        return base64.b64decode(value)

    def dynamo_dump(self, value):
        return base64.b64encode(value).decode("utf-8")


def subclassof(C, B):
    """ Wrap issubclass to return True/False without throwing TypeError """
    try:
        return issubclass(C, B)
    except TypeError:
        return False


class Set(Type):
    """ Adapter for sets of objects """
    python_type = collections.abc.Set

    def __init__(self, typedef):
        if subclassof(typedef, Type):
            # Type class passed, create no-arg instance
            typedef = typedef()
        self.typedef = typedef
        self.backing_type = typedef.backing_type + "S"
        super().__init__()

    def dynamo_load(self, value):
        return set(self.typedef.dynamo_load(v) for v in value)

    def dynamo_dump(self, value):
        return [self.typedef.dynamo_dump(v) for v in sorted(value)]

    def can_dump(self, value):
        return (super().can_dump(value) and
                all(map(self.typedef.can_dump, value)))


class Boolean(Type):
    python_type = bool
    backing_type = BOOLEAN

    def dynamo_load(self, value):
        return bool(value)

    def dynamo_dump(self, value):
        return bool(value)


class Map(Type):
    python_type = collections.abc.Mapping
    backing_type = MAP

    def dynamo_load(self, value):
        return {k: self.serializer.load(v) for (k, v) in value.items()}

    def dynamo_dump(self, value):
        return {k: self.serializer.dump(v) for (k, v) in value.items()}


class List(Type):
    python_type = collections.abc.Iterable
    backing_type = LIST

    def dynamo_load(self, value):
        return [self.serializer.load(v) for v in value]

    def dynamo_dump(self, value):
        return [self.serializer.dump(v) for v in value]


TYPES.extend([
    String(),
    UUID(),
    DateTime(),
    Float(),
    Integer(),
    Binary(),
    Boolean(),
    Map(),
    List(),
    Set(String),
    Set(Float),
    Set(Binary)
])


class _DefaultSerializer:
    """ Default load/dump for Maps and Lists. """

    def __init__(self, types=None):
        self.types = []
        for typedef in (types or TYPES):
            self.types.append(typedef)

    def load(self, value):
        """ value is a dictionary {dynamo_type: value} """
        for typedef in self.types:
            if typedef.can_load(value):
                return typedef._load(value)
        raise TypeError("Don't know how to load " + str(value))

    def dump(self, value):
        for typedef in self.types:
            if typedef.can_dump(value):
                return typedef._dump(value)
        raise TypeError("Don't know how to dump " + str(value))

# Have to set default serializers for Map, List after all Types have been
# defined

Map.serializer = List.serializer = _DefaultSerializer()
