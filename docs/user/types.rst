Built-in Types
^^^^^^^^^^^^^^

The built-in types

Primitive Types
===============

These are the building blocks for all other types, and map 1:1 to DynamoDB types.

.. _user-string-type:

String
------

These are strings

Binary
------

Binary blobs, b64 encoded

Float
-----

All numbers are backed by the float type

Boolean
-------

True or False


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
