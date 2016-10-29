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

------
Binary
------

.. code-block:: python

    class Binary(bloop.Type):
        backing_type = "B"
        python_type = bytes

-------
Boolean
-------

.. code-block:: python

    class Boolean(bloop.Type):
        backing_type = "BOOL"
        python_type = bool

-----
Float
-----

.. code-block:: python

    class Float(bloop.Type):
        backing_type = "N"
        python_type = numbers.Number

-------
Integer
-------

.. code-block:: python

    class Integer(bloop.Float):
        python_type = int

------
String
------

.. code-block:: python

    class String(bloop.Type):
        backing_type = "S"
        python_type = str

----
UUID
----

.. code-block:: python

    class UUID(bloop.String):
        python_type = uuid.UUID

.. code-block:: pycon

    >>> import bloop
    >>> import uuid
    >>> guid = uuid.uuid4()
    >>> typedef = bloop.UUID()
    >>> guid
    UUID('9eca3291-f1d6-4f19-afe2-b3116b2c0a9f')
    >>> typedef.dynamo_dump(guid, context={})
    '9eca3291-f1d6-4f19-afe2-b3116b2c0a9f'

--------
DateTime
--------

DateTime stores an :py:class:`arrow.arrow.Arrow` as an ISO8601 UTC String.

.. code-block:: python

    class DateTime(bloop.String):
        python_type = arrow.Arrow

        def __init__(self, timezone="utc"):
            ...

.. attribute:: timezone

    Used for any values loaded from DynamoDB.  Defaults to "utc".

    Note that values in DynamoDB are **always** stored in UTC.

.. code-block:: pycon

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

DynamoDB's ``Map`` can have keys with different types per key, but must identify all of the keys it will use:

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

---
Set
---

.. code-block:: python

    class Set(bloop.Type):
        python_type = set

        def __init__(self, typedef):
            ...

.. attribute:: typedef
    :noindex:

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

----
List
----

Unlike Set, a List's inner type can be anything, including other Lists, Sets, and Maps.

.. code-block:: python

    class List(bloop.Type):
        backing_type = "L"
        python_type = list

        def __init__(self, typedef):
            ...

.. attribute:: typedef
    :noindex:

    The type for values in this List.

.. code-block:: python

    # Both valid
    List(UUID)
    List(Set(DateTime))

---
Map
---

This type requires you to specify the modeled keys in the Map, but values don't have to have the same type.

.. code-block:: python

    class Map(bloop.Type):
        backing_type = "M"
        python_type = collections.abc.Mapping

        def __init__(self, **types):
            ...

.. attribute:: types
    :noindex:

    The type for each key in the Map's structure.  Any keys that aren't included in ``types``
    will be ignored.

.. code-block:: python

    # Using kwargs directly
    Map(username=String, wins=Integer)

    # Unpacking from a dict
    Metadata = Map(**{
        "created": DateTime,
        "referrer": UUID,
        "cache": String
    })

    class Pin(...):
        metadata = Column(Metadata)

    pin.metadata = {
        "created": arrow.now(),
        "referrer": referrer.id,
        "cache": "https://img-cache.s3.amazonaws.com/" + img.filename
    }

.. warning::

    Saving a DynamoDB Map ``"M"`` fully replaces the existing value.

    Despite my desire to `support partial updates`__, DynamoDB does not expose a way to reliably
    update a path within a Map.  `There is no way to upsert along a path`__:

        I attempted a few other approaches, like having two update statements - first setting it to an
        empty map with the if_not_exists function, and then adding the child element, but that doesn't work
        because **paths cannot overlap between expressions**.

        -- `DavidY@AWS`__ (emphasis added)

    If DynamoDB ever allows overlapping paths in expressions, Bloop will be refactored to use
    partial updates for arbitrary types.

    Given the thread's history, it doesn't look promising.

    __ https://github.com/numberoverzero/bloop/issues/28
    __ https://forums.aws.amazon.com/thread.jspa?threadID=162907
    __ https://forums.aws.amazon.com/message.jspa?messageID=576069#576069

.. _custom-types:

============
Custom Types
============

Creating new types is straightforward.  Here's a type that stores an :class:`~PIL.Image.Image`
as bytes:

.. code-block:: python

    import io
    from PIL import Image

    class GIF(bloop.Binary):
        python_type = Image.Image

        def dynamo_dump(self, image, *, context, **kwargs):
            if image is None:
                return None
            buffer = io.BytesIO()
            image.save(buffer, format="GIF")
            return super().dynamo_dump(
                buffer.getvalue(), context=context, **kwargs)

        def dynamo_load(self, value, *, context, **kwargs):
            image_bytes = super().dynamo_load(
                value, context=context, **kwargs)
            if image_bytes is None:
                return None
            buffer = io.BytesIO(image_bytes)
            image = Image.open(buffer)
            return image

Now the model doesn't need to know about the storage format:

.. code-block:: python

    class User(BaseModel):
        name = Column(String, hash_key=True)
        profile_gif = Column(GIF)
    engine.bind(User)

    user = User(name="numberoverzero")
    engine.load(user)

    user.profile_gif.rotate(90)
    engine.save(user)

----------------
Missing and None
----------------

When there's no value for a :class:`~bloop.Column` that's being loaded, your type will need to handle None.
For many types, None is the best sentinel to return for "this has no value" -- Most of the built-in types use None.

:class:`~bloop.types.Set` returns an empty ``set``, so that you'll never need to check for None before adding and
removing elements. :class:`~bloop.types.Map` will load None for the type associated with each of its keys,
and insert those in the dict.


You will also need to handle ``None`` when dumping values to DynamoDB.  This can happen when a value is deleted
from a Model instance, or it's explicitly set to None.  In almost all cases, your ``dynamo_dump`` function should
simply return None to signal omission (or deletion, depending on the context).

You should return ``None`` when dumping empty values like ``list()``, or DynamoDB will complain about setting
something to an empty list or set.  By returning None, Bloop will know to put that column in
the DELETE section of the UpdateItem.

-------------
Example: Enum
-------------

This is a simple Type that stores an :py:class:`enum.Enum` by its string value.

.. code-block:: python

    class Enum(bloop.String):
        def __init__(self, enum_cls=None):
            if enum_cls is None:
                raise TypeError("Must provide an enum class")
            self.enum_cls = enum_cls
            super().__init__()

        def dynamo_dump(self, value, *, context, **kwargs):
            if value is None:
                return value
            return value.name

        def dynamo_load(self, value, *, context, **kwargs):
            if value is None:
                return value
            return self.enum_cls[value]

That's it!  To see it in action, here's an enum:

.. code-block:: python

    import enum
    class Color(enum.Enum):
        red = 1
        green = 2
        blue = 3

And using that in a model:

.. code-block:: python

    class Shirt(BaseModel):
        id = Column(String, hash_key=True)
        color = Column(Enum(Color))
    engine.bind(Shirt)

    shirt = Shirt(id="t-shirt", color=Color.red)
    engine.save(shirt)

This is stored in DynamoDB as:

+---------+-------+
| id      | color |
+---------+-------+
| t-shirt | red   |
+---------+-------+
