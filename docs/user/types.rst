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

These are built off of the primitive types above.

UUID
----

Built off of string

DateTime
--------

Stored as ISO8601 UTC string

.. _user-integer-type:

Integer
-------

Truncates values with ``int`` before passing them to Float.

List and Document Types
=======================

These hold multiple values, in different ways.

.. _user-set-type:

Set
---

Can be a set of anything, backed by String Set, Numeric Set, or Binary Set.

List
----

Single type, backed by native DynamoDB LIST.

Map
---

General document that expects a type for each key.

TypedMap
--------

Map with a single type for values.  Any number of string keys.
