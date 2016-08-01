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

# Dynamo takes numbers as strings to reduce inter-language problems
DYNAMODB_CONTEXT = decimal.Context(
    Emin=-128, Emax=126, rounding=None, prec=38,
    traps=[
        decimal.Clamped, decimal.Overflow, decimal.Inexact,
        decimal.Rounded, decimal.Underflow
    ]
)


class Type(declare.TypeDefinition):
    def _load(self, value, **kwargs):
        """take a {type: value} dictionary (or None) from dynamo and return a python value"""
        if value is not None:
            value = next(iter(value.values()))
        return self.dynamo_load(value, **kwargs)

    def _dump(self, value, **kwargs):
        """dump a python value to a {type: value} dictionary for dynamo storage"""
        if value is None:
            return {self.backing_type: None}
        return {self.backing_type: self.dynamo_dump(value, **kwargs)}

    def dynamo_load(self, value, *, context, **kwargs):
        raise NotImplementedError()

    def dynamo_dump(self, value, *, context, **kwargs):
        raise NotImplementedError()

    def __repr__(self, *a, **kw):  # pragma: no cover
        return "{}(python_type={}, backing_type={})".format(
            self.__class__.__name__, self.python_type, self.backing_type)
    __str__ = __repr__


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
            raise TypeError("Infinity and NaN not supported")
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


def subclassof(C, B):
    """Wrap issubclass to return True/False without throwing TypeError"""
    try:
        return issubclass(C, B)
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

    def __init__(self, typedef=None):
        # Default None allows the TypeEngine to call without args,
        # and still provide a helpful error message for a required param
        if typedef is None:
            raise TypeError("Sets requires a type")
        self.typedef = type_instance(typedef)
        if typedef.backing_type not in {"N", "S", "B"}:
            raise TypeError("Set's typedef must be backed by one of N/S/B but was '{}'".format(typedef.backing_type))
        self.backing_type = typedef.backing_type + "S"
        super().__init__()

    def _register(self, engine):
        """Register the set's type"""
        engine.register(self.typedef)

    def dynamo_load(self, values, *, context, **kwargs):
        if values is None:
            return set()
        # local lookup in a tight loop
        load = context["engine"]._load
        typedef = self.typedef
        return set(load(typedef, value, context=context, **kwargs) for value in values)

    def dynamo_dump(self, values, *, context, **kwargs):
        if values is None:
            return None
        # local lookup in a tight loop
        dump = context["engine"]._dump
        typedef = self.typedef

        # TODO replace when Type._dump returns None instead of {str: None}
        def not_none(value):
            return (value is not None) and next(iter(value.values())) is not None
        filtered = filter(
            not_none,
            (
                dump(typedef, value, context=context, **kwargs)
                for value in values))
        return list(filtered) or None


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

    def dynamo_load(self, values, *, context, **kwargs):
        if values is None:
            return list()
        # local lookup in a tight loop
        load = context["engine"]._load
        typedef = self.typedef
        return [load(typedef, value, context=context, **kwargs) for value in values]

    def dynamo_dump(self, values, *, context, **kwargs):
        if values is None:
            return None
        # local lookup in a tight loop
        dump = context["engine"]._dump
        typedef = self.typedef

        # TODO replace when Type._dump returns None instead of {str: None}
        def not_none(value):
            return (value is not None) and next(iter(value.values())) is not None

        filtered = filter(
            not_none,
            (
                dump(typedef, value, context=context, **kwargs)
                for value in values))
        return list(filtered) or None


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

    def dynamo_load(self, values, *, context, **kwargs):
        if values is None:
            return dict()
        # local lookup in a tight loop
        load = context["engine"]._load
        typedef = self.typedef
        return {k: load(typedef, v, context=context, **kwargs) for k, v in values.items()}

    def dynamo_dump(self, values, *, context, **kwargs):
        if values is None:
            return None
        # local lookup in a tight loop
        dump = context["engine"]._dump
        typedef = self.typedef

        # TODO replace when Type._dump returns None instead of {str: None}
        def not_none(item):
            key, value = item
            if (value is not None) and next(iter(value.values())) is not None:
                return key, value
        filtered = filter(
            not_none,
            (
                (key, dump(typedef, value, context=context, **kwargs))
                for key, value in values.items()
            ))
        return dict(filtered) or None


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
        load = context["engine"]._load
        get = values.get
        return {
            key: load(typedef, get(key, None), context=context, **kwargs)
            for key, typedef in self.types.items()
        }

    def dynamo_dump(self, values, *, context, **kwargs):
        if values is None:
            return None
        dump = context["engine"]._dump
        get = values.get

        # TODO replace when Type._dump returns None instead of {str: None}
        def not_none(item):
            key, value = item
            if (value is not None) and next(iter(value.values())) is not None:
                return key, value

        filtered = filter(
            not_none,
            (
                (key, dump(typedef, get(key, None), context=context, **kwargs))
                for key, typedef in self.types.items()
            ))
        return dict(filtered) or None
