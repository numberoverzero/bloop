.. _types:

Types
^^^^^

Types are used when defining :ref:`Columns <property-typedef>` and are responsible for translating between
local values and their DynamoDB representations.  For example, ``DateTime`` maps between
``arrow.now()`` and ``"2016-08-09T01:16:25.322849+00:00"``.

With just two methods, you can :ref:`create new types <custom-types>` that automatically integrate
with all of Bloop's features.

============
Scalar Types
============

Bloop supports all scalar types except NULL.

----------
``Binary``
----------

.. code-block:: python

    class Binary(bloop.Type):
        backing_type = "B"
        python_type = bytes

-----------
``Boolean``
-----------

.. code-block:: python

    class Boolean(bloop.Type):
        backing_type = "BOOL"
        python_type = bool

---------
``Float``
---------

.. code-block:: python

    class Float(bloop.Type):
        backing_type = "N"
        python_type = numbers.Number

-----------
``Integer``
-----------

.. code-block:: python

    class Integer(bloop.Float):
        python_type = int

----------
``String``
----------

.. code-block:: python

    class String(bloop.Type):
        backing_type = "S"
        python_type = str

--------
``UUID``
--------

.. code-block:: python

    class UUID(bloop.String):
        python_type = uuid.UUID

.. code-block:: python

    >>> import bloop
    >>> import uuid
    >>> guid = uuid.uuid4()
    >>> typedef = bloop.UUID()
    >>> guid
    UUID('9eca3291-f1d6-4f19-afe2-b3116b2c0a9f')
    >>> typedef.dynamo_dump(guid, context={})
    '9eca3291-f1d6-4f19-afe2-b3116b2c0a9f'

------------
``DateTime``
------------

DateTime stores an :py:class:`arrow.arrow.Arrow` as an ISO8601 UTC String.

.. code-block:: python

    class DateTime(bloop.String):
        python_type = arrow.Arrow

        def __init__(self, timezone="utc"):
            ...

.. attribute:: timezone

    Used for any values loaded from DynamoDB.  Defaults to "utc".

    Note that values in DynamoDB are **always** stored in UTC.

.. code-block:: python

    >>> import arrow
    >>> import bloop
    >>> now = arrow.now()
    >>> typedef = bloop.DateTime()
    >>> now
    <Arrow [2016-08-08T23:03:22.948742-07:00]>
    >>> typedef.dynamo_dump(now, context={})
    '2016-08-09T06:03:22.948742+00:00'

==================
Sets and Documents
==================

Because ``{"S": "red"}`` could be loaded by any type backed by ``S``, there's no way to know which type to
use.  Therefore, any types that can hold an arbitrary group of inner values must provide enough information to
unambiguously load all inner values.  Set and List only support a single type, so they can be specified with eg.
``Set(DateTime)``.

DynamoDB's ``Map`` can have arbitrary keys with different types per key.  No single type can allow both,
while still preserving type information.  Instead, Bloop provides two Map types that cover common uses for Maps:

* ``TypedMap`` is a single typed dict with any number of keys:

  .. code-block:: python

      class Model:
        scores = Column(TypedMap(Integer))

      obj = Model()
      obj.scores = {"amanda": 94, "tom": 90}

* ``Map`` is a dict where each key must specify its type:

  .. code-block:: python

      class Model:
          item = Column(Map(**{
              "name": String,
              "rating": Float,
              "stock": Integer}))

      obj = Model()
      obj.item = {
        "name": "Pogs",
        "rating": 0.7,
        "stock": 1e9}

-------
``Set``
-------

.. code-block:: python

    class Set(bloop.Type):
        python_type = set

        def __init__(self, typedef):
            ...

.. attribute:: typedef

    The type for values in this Set.  Must be backed by one of ``S, N, B``.

When a Set is created, its ``backing_type`` is based on the inner type and will be one of ``SS, NS, BS``.
This does not mean that the inner type must subclass ``String``, ``Float``, or ``Binary``.
As long as the backing type is valid, custom types are fine:

.. code-block:: python

    # Both valid
    Set(UUID)
    Set(DateTime)

    class Hash(bloop.Type):
        backing_type = "N"
        python_type = int

    # Also valid
    Set(Hash)

--------
``List``
--------

.. code-block:: python

    class List(bloop.Type):
        backing_type = "L"
        python_type = list

        def __init__(self, typedef):
            ...

.. attribute:: typedef

    The type for values in this List.

Unlike Set, a List's inner type can be anything, including other Lists, Sets, and Maps.

.. code-block:: python

    # Both valid
    List(UUID)
    List(Set(DateTime))

------------
``TypedMap``
------------

.. code-block:: python

    class TypedMap(bloop.Type):
        backing_type = "M"
        python_type = collections.abc.Mapping

        def __init__(self, typedef):
            ...

-------
``Map``
-------

.. code-block:: python

    class Map(bloop.Type):
        backing_type = "M"
        python_type = collections.abc.Mapping

        def __init__(self, **types):
            ...

.. _custom-types:

============
Custom Types
============

Things to consider:

1. Must be able to load ``None``
2. Must be able to dump ``None``
3. Must return ``None`` from dump to signal no value.
4. Call ``_register`` for any types you depend on.

----------------
Missing and None
----------------

None is missing, etc etc.  Return None to omit.

--------------
``bloop.Type``
--------------

.. code-block:: python

    class Type:
        backing_type = "S"

        def dynamo_load(self, value, *, context, **kwargs):
            return value

        def dynamo_dump(self, value, *, context, **kwargs):
            return value

        def _register(self, type_engine):
            pass

-------------
Example: Enum
-------------

String-backed Enum
