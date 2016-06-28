Built-in Types
^^^^^^^^^^^^^^

These types come with bloop, and cover most common data types.  To hook your custom classes into bloop's type system,
see :ref:`user-custom-types`.  By building on the base types below, most custom classes can be used directly with
less than a dozen lines of code (an Integer-backed type supporting any enum.Enum is exactly 12 lines).

Primitive Types
===============

These are the building blocks for all other types, and map 1:1 to DynamoDB types.  These hold scalar values, unlike the
document (paths) types ``Map`` and ``List``, or the set types ``Set``.

.. _user-string-type:

String
------

Since everything has to become a string to build the request body, this is the simplest type.  String is one of three
base types that can be used as a hash or range key.

The constructor takes no args, and values are stored in DynamoDB 1:1 with their python type::

    # equivalent
    Column(String)
    Column(String())

    string_type = String()
    string_type.dynamo_dump("foo")  # "foo"

Binary
------

Binary data corresponds to the ``bytes`` type in python, sent over the wire as a base64 encoded string, and stored in
DynamoDB as bytes.  Binary is one of three base types that can be used as a hash or range key.

The constructor takes no args::

    # equivalent
    Column(Binary)
    Column(Binary())

    bytes_type = Binary()
    bytes_type.dynamo_dump(b"foo")  # "Zm9vCg=="

Float
-----

This is the basic numeric type.  Float is one of three base types that can be used as a hash or range key.  Because
languages implement floating point numbers differently, DynamoDB specifies constraints on how numeric values should
be constructed; they are stored as strings.  To ensure accuracy, it is highly recommended to use the
:py:class:`decimal.Decimal` class.  Alternatively, the Integer type below can be used (which is backed by Float,
but makes the translation easier for some uses).

You should absolutely review the documentation_ before using python floats, as errors can be subtle.

The constructor takes no args::

    # equivalent
    Column(Float)
    Column(Float())

    float_type = Float()
    float_type.dynamo_dump(3)  # "3"
    float_type.dynamo_dump(decimal.Decimal("3.5"))  # "3.5"

.. _documentation: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes.Number

Boolean
-------

Unlike String, Binary, and Float, the Boolean type cannot be used as a hash or range key.  Like the other basic types,
it takes no args.  It will coerce any value using ``bool``::

    bool_type = Boolean()
    bool_type.dynamo_dump(["foo", "bar"])  # true


Derived Types
=============

These types provide a convenient mapping to python objects for common patterns (unique identifiers, timestamps).

UUID
----

Backed by the ``String`` type, this stores a UUID as its string representation.  It can handle any
:py:class:`uuid.UUID`, and its constructor takes no args::

    import uuid

    u = UUID()
    some_id = uuid.uuid4()

    u.dynamo_dump(some_id)  # "8f5ec651-5997-4843-ad6f-065c22fd8971"

DateTime
--------

DateTime is backed by the ``String`` type and maps to an :py:class:`arrow.arrow.Arrow` object.  While the loaded values
can be instructed to use a particular timezone, values are always stored in UTC ISO8601_ to enable the full range of
comparison operators.

::

    import arrow
    u = DateTime()
    p = DateTime(timezone="US/Pacific")

    now_str = "2016-06-28T05:18:02.633634+00:00"
    now = arrow.get(now_str)

    # Both stored in UTC
    u.dynamo_dump(now)  # "2016-06-28T05:18:02.633634+00:00"
    p.dynamo_dump(now)  #  "2016-06-28T05:18:02.633634+00:00"

    # When loaded, they use the specified timezone
    u.dynamo_load(now_str)  # <Arrow [2016-06-28T05:18:02.633634+00:00]>
    p.dynamo_load(now_str)  #  <Arrow [2016-06-27T22:18:02.633634-07:00]>

.. _ISO8601: https://en.wikipedia.org/wiki/ISO_8601

.. _user-integer-type:

Integer
-------

Integer is a very thin wrapper around the ``Float`` type, and simply calls ``int()`` on the values passed to and from
its parent type::

    int_type = Integer()
    int_type.dynamo_dump(3.5)  # "3"
    int_type.dynamo_dump(5)  # "5"

    # Even if the stored value in Dynamo is a float,
    # this type truncates it on load
    int_type.dynamo_load("3.5")  # 3
    int_type.dynamo_load("5")  # 5

List and Document Types
=======================

Unlike the types above, these types are non-scalar and can hold multiple values.  DynamoDB introduced document types
Map and List, which provide path lookups -- ``some_column[0]`` and ``some_column["foo"]["bar"]``.

.. _user-set-type:

Set
---

Unlike the scalar types above, ``Set`` is a dependent type; that is, you must provide the type of the items in the set.
The set type can be ``String``, ``Binary``, ``Float``, or any subclass thereof (more generally, it can be any type
whose ``backing_type`` is one of ``S``, ``N``, or ``B``).  This is because the DynamoDB set type must be one of ``SS``,
``SN``, or ``SB``.

When loading or dumping a set, the inner type's load and dump functions will be used for each item in the set.  If the
set type does not need any arguments, you may provide the class instead of an instance::

    # type class uses no-arg __init__
    float_set = Set(Float)
    # type instance is used directly
    timestamp_set = Set(DateTime(timezone="US/Pacific"))

    # This fails, because the inner type is
    # backed by BOOL, not S, N, or B
    Set(Boolean())

    floats = set([3.5, 2, -1.0])
    float_set.dynamo_dump(floats)  # ["3.5", "2", "-1.0"]

List
----

While DynamoDB's ``LIST`` type allows any combination of types, bloop's built-in ``List`` type requires you to
constrain the list to a single type.  This type is constructed the same way as ``Set`` above.

While it is possible to dump differently typed objects through a List to DynamoDB, there isn't enough type information
when loading a list to tell subclasses apart.  That means that we can't tell if the string
``2016-06-28T05:18:02.633634+00:00`` should be loaded as a ``String`` or as a ``DateTime``, without bloop using
some additional overhead to store that type information alongside the data.

This is too high an overhead for bloop in general; the type system is general enough that you can define your own
List type that supports unmapped types (perhaps by wrapping each type so that it stores the type information alongside
the data).

::

    # type class uses no-arg __init__
    float_list = List(Float)
    # type instance is used directly
    timestamp_list = List(DateTime(timezone="US/Pacific"))

    # This is fine; List's inner type can be anything,
    # including another List
    List(Boolean())

    floats = [3.5, 2, -1.0]
    float_list.dynamo_dump(floats)  # ["3.5", "2", "-1.0"]

Map
---

General document that expects a type for each key.

TypedMap
--------

Map with a single type for values.  Any number of string keys.
