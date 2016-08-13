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

--------
``List``
--------

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

------------
``TypedMap``
------------

TypedMap is one of two built-in Map types.  This type allows any number of keys, but values must be the same type.

.. code-block:: python

    class TypedMap(bloop.Type):
        backing_type = "M"
        python_type = collections.abc.Mapping

        def __init__(self, typedef):
            ...

.. attribute:: typedef
    :noindex:

    The type for values in this dict.

This is useful when all of your data is the same type, but you don't know what keys may be used.  The inner
type can be anything, like ``TypedMap(Set(UUID))``.

.. code-block:: python

    Tags = TypedMap(String)
    class User(...):
        tags = Column(Tags)

    user.tags["#wat"] = "destroyallsoftware/talks/wat"
    user.tags["#bigdata"] = "twitter/garybernhardt/600783770925420546"

-------
``Map``
-------

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

Creating new types is straightforward.  Here's a type that stores an ``Image`` as bytes, and loads it back again:

.. code-block:: python

    import io
    import Image

    class GIF(bloop.Binary):
        python_type = Image

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

Now it's all ``Image``, all the time:

.. code-block:: python

    class User(BaseModel):
        name = Column(String, hash_key=True)
        profile_gif = Column(GIF)

    user = User(name="numberoverzero")
    engine.load(user)
    user.profile_gif.rotate(90)
    engine.save(user)

----------------
Missing and None
----------------

Well, almost all the time.  What about that ``return None`` up there?

When there's no value for a Column that's being loaded, your type will need to handle None.  For many types,
None is the best sentinel to return for "this has no value" -- Most of the built-in types use None.

Set returns an empty ``set``, so that you'll never need to check for None before adding and removing elements.
Map will load None for the type associated with each of its keys, and insert those in the dict.


You will also need to handle ``None`` when dumping values to DynamoDB.  This can happen when a value is deleted
from a Model instance, or it's explicitly set to None.  In almost all cases, your ``dynamo_dump`` function should
simply return None to signal omission (or deletion, depending on the context).

You should return ``None`` when dumping empty values like ``list()``, or DynamoDB will complain about setting
something to an empty list or set.  By returning None, Bloop will know to put that column in
the DELETE section of the UpdateItem.


--------------
``bloop.Type``
--------------

.. code-block:: python

    class Type:
        backing_type = "S"

        def dynamo_load(self, value: Optional[str],
                        *, context, **kwargs) -> Any:
            return value

        def dynamo_dump(self, value: Any,
                        *, context, **kwargs) -> Optional[str]:
            return value

        def _register(self, type_engine):
            pass

.. attribute:: backing_type
    :noindex:

    This is the DynamoDB type that Bloop will store values under.  The available types are::

        S -- string
        N -- number
        B -- binary
        BOOL -- boolean
        SS -- string set
        NS -- number set
        BS -- binary set
        M -- map
        L -- list

.. function:: dynamo_load(value, *, context, **kwargs)

    Takes a ``str`` or ``None`` and returns a value to use locally.

.. function:: dynamo_dump(value, *, context, **kwargs)

    Takes a local value or ``None`` and returns a string or ``None``.  This should return ``None`` to indicate a
    missing or deleted value - DynamoDB will fail if you try to send an empty set or list.

.. function:: _register(type_engine)

    You only need to implement this if your type references another type.  This is called when your type is registered
    with the type engine.  Bloop will fail to load or dump your type unless you register the inner type with this
    method.

    Here's the simplified implementation for ``Set``:

    .. code-block:: python

        class Set(bloop.Type):
            def __init__(self, typedef):
                self.typedef = typedef

            def _register(self, type_engine):
                type_engine.register(self.typedef)

The ``context`` arg is a dict to hold extra information about the current call.  It will always contain
at least ``{"engine": bloop.Engine}`` which is the Bloop engine that this call came from.  You must perform
any recursive load/dump calls through the context engine, and must not call ``dynamo_dump`` on another type
directly.  The engine exposes ``_load`` and ``_dump`` functions, with the following signatures:

.. code-block:: python

    Engine._load(self, typedef, value, *, context, **kwargs)
    Engine._dump(self, typedef, value, *, context, **kwargs)

This is nearly the same interface as the Type functions, but you must pass the ``bloop.Type`` that the value should
go through.  For example, here's the simplified ``dynamo_load`` for Set:

.. code-block:: python

    class Set(bloop.Type):
        def __init__(self, typedef):
            self.typedef = typedef

        def dynamo_load(self, values, *, context, **kwargs):
            engine = context["engine"]
            loaded_set = set()
            for value in values:
                value = engine._load(
                    self.typedef, value, context=context, **kwargs)
                loaded_set.add(value)
            return loaded_set

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
    engine.bind(base=Shirt)

    shirt = Shirt(id="t-shirt", color=Color.red)
    engine.save(shirt)

This is stored in DynamoDB as:

+---------+-------+
| id      | color |
+---------+-------+
| t-shirt | red   |
+---------+-------+
