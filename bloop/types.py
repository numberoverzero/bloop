import arrow
import base64
import collections.abc
import decimal
import declare
import numbers
import uuid

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
    def _load(self, value, *, context=None, **kwargs):
        """
        take a {type: value} dictionary from dynamo and return a python value
        """
        value = next(iter(value.values()))
        if value is None:
            return None
        return self.dynamo_load(value, context=context, **kwargs)

    def _dump(self, value, *, context=None, **kwargs):
        """
        dump a python value to a {type: value} dictionary for dynamo storage
        """
        if value is None:
            return {self.backing_type: None}
        return {
            self.backing_type:
                self.dynamo_dump(value, context=context, **kwargs)}

    def dynamo_load(self, value, *, context=None, **kwargs):
        return value

    def dynamo_dump(self, value, *, context=None, **kwargs):
        return value

    def __repr__(self, *a, **kw):  # pragma: no cover
        return "{}(python_type={}, backing_type={})".format(
            self.__class__.__name__, self.python_type, self.backing_type)
    __str__ = __repr__


class String(Type):
    python_type = str
    backing_type = STRING

    # No need to override dynamo_load, since all values come in as strings

    def dynamo_dump(self, value, *, context=None, **kwargs):
        return str(value)


class UUID(String):
    python_type = uuid.UUID

    def dynamo_load(self, value, *, context=None, **kwargs):
        return uuid.UUID(value)

    def dynamo_dump(self, value, *, context=None, **kwargs):
        return str(value)


class DateTime(String):
    """DateTimes are ALWAYS stored in UTC, backed by arrow.Arrow instances.

    A local timezone may be specified when initializing the type - otherwise
    UTC is used.

    For example, comparisons can be done in any timezone since they
    will all be converted to UTC on request and from UTC on response::

        class Model(Base):
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
        results[0].date

    """
    python_type = arrow.Arrow
    default_timezone = "UTC"

    def __init__(self, timezone=None):
        self.timezone = timezone or DateTime.default_timezone
        super().__init__()

    def dynamo_load(self, value, *, context=None, **kwargs):
        iso8601_string = super().dynamo_load(value, context=context, **kwargs)
        return arrow.get(iso8601_string).to(self.timezone)

    def dynamo_dump(self, value, *, context=None, **kwargs):
        # ALWAYS store in UTC - we can manipulate the timezone on load
        iso8601_string = value.to("utc").isoformat()
        return super().dynamo_dump(iso8601_string, context=context, **kwargs)


class Float(Type):
    python_type = numbers.Number
    backing_type = NUMBER

    def dynamo_load(self, value, *, context=None, **kwargs):
        return DYNAMODB_CONTEXT.create_decimal(value)

    def dynamo_dump(self, value, *, context=None, **kwargs):
        n = str(DYNAMODB_CONTEXT.create_decimal(value))
        if any(filter(lambda x: x in n, ("Infinity", "NaN"))):
            raise TypeError("Infinity and NaN not supported")
        return n


class Integer(Float):
    python_type = int

    def dynamo_load(self, value, *, context=None, **kwargs):
        number = super().dynamo_load(value, context=context, **kwargs)
        return int(number)

    def dynamo_dump(self, value, *, context=None, **kwargs):
        value = int(value)
        return super().dynamo_dump(value, context=context, **kwargs)


class Binary(Type):
    python_type = bytes
    backing_type = BINARY

    def dynamo_load(self, value, *, context=None, **kwargs):
        return base64.b64decode(value)

    def dynamo_dump(self, value, *, context=None, **kwargs):
        return base64.b64encode(value).decode("utf-8")


def subclassof(C, B):
    """ Wrap issubclass to return True/False without throwing TypeError """
    try:
        return issubclass(C, B)
    except TypeError:
        return False


def type_instance(typedef):
    """ Returns an instance of a type class, or the instance if provided """
    if subclassof(typedef, Type):
        # Type class passed, create no-arg instance
        typedef = typedef()
    return typedef


class Set(Type):
    """ Adapter for sets of objects """
    python_type = collections.abc.Set

    def __init__(self, typedef=None):
        # Default None allows the TypeEngine to call without args,
        # and still provide a helpful error message for a required param
        if typedef is None:
            raise TypeError("Sets requires a type")
        self.typedef = type_instance(typedef)
        self.backing_type = typedef.backing_type + "S"
        super().__init__()

    def dynamo_load(self, value, *, context=None, **kwargs):
        load = self.typedef.dynamo_load
        return set(load(v, context=context, **kwargs) for v in value)

    def dynamo_dump(self, value, *, context=None, **kwargs):
        dump = self.typedef.dynamo_dump
        return [dump(v, context=context, **kwargs) for v in sorted(value)]


class Boolean(Type):
    python_type = bool
    backing_type = BOOLEAN

    def dynamo_load(self, value, *, context=None, **kwargs):
        return bool(value)

    def dynamo_dump(self, value, *, context=None, **kwargs):
        return bool(value)


class Map(Type):
    python_type = collections.abc.Mapping
    backing_type = MAP

    def __init__(self, **types):
        self.types = {
            k: type_instance(t) for k, t in types.items()
        }
        super().__init__()

    def __getitem__(self, key):
        """Overload allows easy nested access to types"""
        return self.types[key]

    def _register(self, engine):
        """Register all types for the map"""
        for typedef in self.types.values():
            engine.register(typedef)

    def dynamo_load(self, values, *, context=None, **kwargs):
        obj = {}
        for key, typedef in self.types.items():
            value = values.get(key, None)
            if value is not None:
                value = typedef._load(value, context=context, **kwargs)
            obj[key] = value
        return obj

    def dynamo_dump(self, values, *, context=None, **kwargs):
        obj = {}
        for key, typedef in self.types.items():
            value = values.get(key, None)
            if value is not None:
                value = typedef._dump(value, context=context, **kwargs)
            # Never push a literal `None` back to DynamoDB
            if value is not None:
                obj[key] = value
        return obj


class TypedMap(Type):
    python_type = collections.abc.Mapping
    backing_type = MAP

    def __init__(self, typedef=None):
        # Default None allows the TypeEngine to call without args,
        # and still provide a helpful error message for a required param
        if typedef is None:
            raise TypeError("TypedMap requires a type")
        self.typedef = type_instance(typedef)
        super().__init__()

    def __getitem__(self, key):
        """Overload allows easy nested access to types"""
        return self.typedef

    def _register(self, engine):
        """Register all types for the map"""
        engine.register(self.typedef)

    def dynamo_load(self, values, *, context=None, **kwargs):
        load = self.typedef._load
        return {
            k: load(v, context=context, **kwargs) for k, v in values.items()
        }

    def dynamo_dump(self, values, *, context=None, **kwargs):
        dump = self.typedef._dump
        return {
            k: dump(v, context=context, **kwargs) for k, v in values.items()
        }


class List(Type):
    python_type = collections.abc.Iterable
    backing_type = LIST

    def __init__(self, typedef=None):
        # Default None allows the TypeEngine to call without args,
        # and still provide a helpful error message for a required param
        if typedef is None:
            raise TypeError("List requires a type")
        self.typedef = type_instance(typedef)
        super().__init__()

    def __getitem__(self, key):
        return self.typedef

    def _register(self, engine):
        engine.register(self.typedef)

    def dynamo_load(self, value, *, context=None, **kwargs):
        load = self.typedef._load
        return [load(v, context=context, **kwargs) for v in value]

    def dynamo_dump(self, value, *, context=None, **kwargs):
        dump = self.typedef._dump
        return [dump(v, context=context, **kwargs) for v in value]
