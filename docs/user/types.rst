.. _types:

Types
=====

Together with ``Column``, ``Type`` is responsible for converting python
representations of values into DynamoDB representations, and back again.

bloop uses eight of the nine backing types that DynamoDB offers::

    "S"    String
    "N"    Number
    "B"    Binary
    "SS"   String Set
    "NS"   Number Set
    "BS"   Binary Set
    "M"    Map
    "L"    List
    "BOOL" Boolean

The ``NULL`` type is not used because it can have only one value (True) which,
while useful when a column can have arbitrarily typed values, is useless for
an Object Mapper which enforces types per-column.

Type
----

Before diving into the available types, here's the structure of the base
``Type``::

    class Type(...):
        python_type = str
        backing_type = "S"


        def can_load(self, value):
            ...

        def can_dump(self, value):
            ...

        def dynamo_load(self, value):
            ...

        def dynamo_dump(self, value):
            ...

By default the ``can_*`` functions simply check the Type's python/backing type
attribute against the value.  This check is only performed when loading
arbitrary structures like List and Map.

The ``dynamo_*`` functions return a value, and are called by the Type's base
\_load and \_dump functions that tie into the engine's recursive loading
machinery.

String
------

String is the base class for most custom classes.

::

    class String(Type):
        python_type = str
        backing_type = STRING

UUID
----

Used to store any type of ``uuid.UUID``.  This stores the value in a
human-readable format in DynamoDB, whereas the uuid bytes could be stored in
a Binary format to save space.

::

    class UUID(String):
        python_type = uuid.UUID

        def dynamo_load(self, value):
            return uuid.UUID(value)

        def dynamo_dump(self, value):
            return str(value)

DateTime
--------

Often one of the most frustrating types to work with, bloop opts to use the
fantastic `arrow`_ library to manage datetimes.  If you're new to arrow, it's
the requests of datetime.

The DateTime type takes an optional timezone string, which is used when loading
values from DynamoDB.  For sorting and consistency purposes, DateTime values
are **ALWAYS** stored in UTC ISO8601-formatted strings.  If no timezone is
specified, they will be loaded in UTC.

.. _arrow: http://crsmithdev.com/arrow/

::

    class DateTime(String):
        python_type = arrow.Arrow
        default_timezone = "UTC"

        def __init__(self, timezone=None):
            self.timezone = timezone or DateTime.default_timezone
            super().__init__()

        def dynamo_load(self, value):
            iso8601_string = super().dynamo_load(value)
            return arrow.get(iso8601_string).to(self.timezone)

        def dynamo_dump(self, value):
            iso8601_string = value.to("utc").isoformat()
            return super().dynamo_dump(iso8601_string)

Float
-----

Float is the base numeric class.  It uses a ``decimal.Context`` to control how
values are represented.  This includes a refusal to round, which often means
inconspicuous values like ``1/3`` will fail to dump due to rounding.  You
should always use a ``decimal.Decimal`` object::

    import decimal
    from bloop import Float

    #alias for brevity
    D = decimal.Decimal
    dump = Float().dynamo_dump

    # raises
    dump(1/3)
    # also raises
    dump(D(1/3))

    # This is fine
    dump(D(1) / D(3))

::

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

Integer
-------

Based off of Float, this numeric type will truncate according to ``int``::

    class Integer(Float):
        python_type = int

        def dynamo_load(self, value):
            number = super().dynamo_load(value)
            return int(number)

        def dynamo_dump(self, value):
            value = int(value)
            return super().dynamo_dump(value)

Binary
------

DynamoDB stores binary data in its raw form, but requires it to be transferred
as a base64 encoded string::

    class Binary(Type):
        python_type = bytes
        backing_type = BINARY

        def dynamo_load(self, value):
            return base64.b64decode(value)

        def dynamo_dump(self, value):
            return base64.b64encode(value).decode("utf-8")

Sets
----

Unlike the other types, ``Set`` cannot be used as a raw type.  For instance,
the following are fine::

    Column(Integer)
    Column(Float)
    Column(DateTime)

Set requires an argument; the following is illegal::

    Column(Set)

Every Set's ``backing_type`` must be one of ``SS``, ``NS``, or ``BS`` depending
on the type passed to its ``__init__``.  After that, loading and dumping values
is entirely delegated to the instance's typedef::

    class Set(Type):
        python_type = collections.abc.Set

        def __init__(self, typedef):
            ...

        def dynamo_load(self, value):
            return set(self.typedef.dynamo_load(v) for v in value)

        def dynamo_dump(self, value):
            return [self.typedef.dynamo_dump(v) for v in sorted(value)]

        def can_dump(self, value):
            return (super().can_dump(value) and
                    all(map(self.typedef.can_dump, value)))

Boolean
-------

Boolean is the final scalar base type, and coerces everything to True or
False::

    class Boolean(Type):
        python_type = bool
        backing_type = BOOLEAN

        def dynamo_load(self, value):
            return bool(value)

        def dynamo_dump(self, value):
            return bool(value)

Documents
---------

``Map`` and ``List`` are the newest Types to bloop, and are still undergoing
refinement.  It's currently not possible to load custom types in either
structure, including UUIDs, DateTimes, and Integers.

At present it's also not possible to construct conditions on paths within
documents, which limits a significant amount of their flexibility.

Eventually, a reasonable syntax will be developed to specify types for certain
keys and indexes.

You can track the work on documents in `Issue #18`_ and `Issue #19`_.

.. _Issue #18: https://github.com/numberoverzero/bloop/issues/18
.. _Issue #19: https://github.com/numberoverzero/bloop/issues/19

::

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
