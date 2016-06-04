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

        def dynamo_load(self, value, *, context=None, **kwargs):
            return value

        def dynamo_dump(self, value, *, context=None, **kwargs):
            return value

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

        def dynamo_load(self, value, *, context=None, **kwargs):
            return uuid.UUID(value)

        def dynamo_dump(self, value, *, context=None, **kwargs):
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

        def dynamo_load(self, value, *, context=None, **kwargs):
            iso8601_string = super().dynamo_load(
                value, context=context, **kwargs)
            return arrow.get(iso8601_string).to(self.timezone)

        def dynamo_dump(self, value, *, context=None, **kwargs):
            iso8601_string = value.to("utc").isoformat()
            return super().dynamo_dump(
                iso8601_string, context=context, **kwargs)

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

        def dynamo_load(self, value, *, context=None, **kwargs):
            return DYNAMODB_CONTEXT.create_decimal(value)

        def dynamo_dump(self, value, *, context=None, **kwargs):
            n = str(DYNAMODB_CONTEXT.create_decimal(value))
            if any(filter(lambda x: x in n, ("Infinity", "NaN"))):
                raise TypeError("Infinity and NaN not supported")
            return n

Integer
-------

Based off of Float, this numeric type will truncate according to ``int``::

    class Integer(Float):
        python_type = int

        def dynamo_load(self, value, *, context=None, **kwargs):
            number = super().dynamo_load(value, context=context, **kwargs)
            return int(number)

        def dynamo_dump(self, value, *, context=None, **kwargs):
            value = int(value)
            return super().dynamo_dump(value, context=context, **kwargs)

Binary
------

DynamoDB stores binary data in its raw form, but requires it to be transferred
as a base64 encoded string::

    class Binary(Type):
        python_type = bytes
        backing_type = BINARY

        def dynamo_load(self, value, *, context=None, **kwargs):
            return base64.b64decode(value)

        def dynamo_dump(self, value, *, context=None, **kwargs):
            return base64.b64encode(value).decode("utf-8")

Sets
----

Unlike the other types, ``Set`` cannot be used as a raw type.  For instance,
the following are fine::

    Column(Integer)
    Column(Float)
    Column(DateTime)

Set requires an argument, which is also a type::

    # Ok - set of strings
    Column(Set(String))

    # Wrong - sets can't be untyped
    Column(Set)

Every Set's ``backing_type`` must be one of ``SS``, ``NS``, or ``BS`` depending
on the type passed to its ``__init__``.  After that, loading and dumping values
is entirely delegated to the instance's typedef::

    class Set(Type):
        python_type = collections.abc.Set

        def __init__(self, typedef):
            ...

        def dynamo_load(self, value, *, context=None, **kwargs):
            load = self.typedef.dynamo_load
            return set(load(v, context=context, **kwargs) for v in value)

        def dynamo_dump(self, value, *, context=None, **kwargs):
            dump = self.typedef.dynamo_dump
            return [dump(v, context=context, **kwargs) for v in sorted(value)]

Boolean
-------

Boolean is the final scalar base type, and coerces everything to True or
False::

    class Boolean(Type):
        python_type = bool
        backing_type = BOOLEAN

        def dynamo_load(self, value, *, context=None, **kwargs):
            return bool(value)

        def dynamo_dump(self, value, *, context=None, **kwargs):
            return bool(value)

Documents
---------

While Dynamo's ``Map`` and ``List`` structures support arbitrary types and
nesting, DynamoDB does not offer the ability to store enough type information
alongside the values to unpack custom types (like DateTime, UUID) losslessly.
For instance, ``{"S": "acd67186-8faa-48b2-9300-7f12bc969e76"}`` COULD represent
a UUID or a String that happens to be a valid UUID.  Without storing some type
metadata alongside the string, it's impossible to tell the difference.

Bloop provides two document types: ``Map`` and ``TypedMap``.

Instead of storing additional type information (either in another column,
table, or concatenation with the data) ``Map`` requires you to explicitly model
your document types.  This means that for any key you expect to read from a
Map, you must have specified the type that loads it::

    Product = Map(**{
        'Name': String,
        'Rating': Float,
        'Updated': DateTime('US/Pacific'),
        'Description': Map(**{
            'Title': String,
            'Body': String,
            'Specifications': Map(**{
                ...
            })
        })
    })


    class Item(Base):
        id = Column(Integer, hash_key=True)
        data = Column(Product)
    engine.bind(base=Base)

Omitted keys will not be loaded from, or saved to, Dynamo.  In the above
example, ``item.data['other']`` will not be persisted because there is no
type provided for the key ``other``.

TypedMaps, however, allow arbitrary keys for a single type definition.  This
is useful when you know that all values conform to a single shape, but the key
space is unbounded::

    InstanceStatus = TypedMap(String)

    class Cluster(Base):
        id = Column(Integer, hash_key=True)
        statuses = Column(InstanceStatus)
    engine.bind(base=Base)

Now we can store an arbitrary (up to Dynamo's limits) set of keys::

    cluster = Cluster(0)
    cluster.statuses = {'instance1': 'Healthy', 'instance2': 'Rebooting'}
    engine.load(cluster)
    print(cluster.statuses['instanceN'])

Similarly for Map, the values in a List must be tied to a type.  All values in
the list must be of the chosen type.  While this doesn't leverage the full
flexibility of the DynamoDB List type (which can store objects with different
types) it simplifies the modeling required to load types::

    class Item(Base):
        id = Column(Integer, hash_key=True)
        ratings = Column(List(Float))
    engine.bind(base=Bae)

To create your own List type that can store arbitrary types, see an example in
:ref:`advanced-types`.
