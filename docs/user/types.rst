.. _types:

Types
^^^^^

Types are used when defining :ref:`Columns <user-models-columns>` and are responsible for translating between
local values and their DynamoDB representations.  For example, :class:`~bloop.types.DateTime` maps between
``datetime.now(timezone.utc)`` and ``"2016-08-09T01:16:25.322849+00:00"``.

DynamoDB is split into scalar types ("S", "N", "B", "BOOL") and vector types ("SS", "NS", "BS", "L", "M").
Bloop provides corresponding types, as well as a handful of useful derived types, such as DateTime and UUID.

For the full list of built-in types, see :ref:`the Public API Reference <public-types>`.

===============
 Backing Types
===============

In bloop, each Type must have a :attr:`~bloop.types.Type.backing_type` that is one of the DynamoDB types (except NULL).
The valid DynamoDB types are:

.. hlist::
    :columns: 3

    * ``"S"`` -- string
    * ``"N"`` -- number
    * ``"B"`` -- binary
    * ``"SS"`` -- string set
    * ``"NS"`` -- number set
    * ``"BS"`` -- binary set
    * ``"M"`` -- map
    * ``"L"`` -- list
    * ``"BOOL"`` -- boolean

Most types have a fixed backing_type, such as :class:`~bloop.types.String` and :class:`~bloop.types.Map`.
Others like :class:`~bloop.types.Set` construct the backing_type when a new instance is created, based on the inner
typedef.


===============
 Instantiation
===============

In many cases, a :class:`~bloop.models.Column` will use a Type class.  For example, this and similar constructs
have been used throughout the User Guide:

.. code-block:: pycon

    >>> from bloop import Column, Number
    >>> balance = Column(Number)

This is syntactic sugar for a common pattern, and the column is actually creating an instance of the ``Number`` type:

.. code-block:: pycon

    >>> balance = Column(Number())

Most types are simply a binding between a local python format and DynamoDB's wire format, and won't have any
parameters.  Some types have optional parameters that configure their behavior, such as :class:`~bloop.types.Number`:

.. code-block:: pycon

    >>> from bloop import Number
    >>> from decimal import Context
    >>> context = Context(Emin=-128, Emax=126, rounding=None, prec=38, traps=[...])
    >>> created_at = Column(Number(context=context))

Finally, some types have required parameters and can't be instantiated by the Column directly:

.. code-block:: pycon

    >>> from bloop import Set
    >>> Column(Set)
    Traceback (most recent call last):
      ...
    TypeError: __init__() missing 1 required positional argument: 'typedef'

These types must be instantiated when defining a column:

    >>> from bloop import Integer
    >>> Column(Set(Integer))

Note that :class:`~bloop.types.Set` is providing the same sugar, and actually creates an instance of its inner type:

.. code-block:: pycon

    >>> Column(Set(Integer()))


==============
 Scalar Types
==============

Bloop provides the following 4 primitive scalar types:

* "S" -- :class:`~bloop.types.String`
* "N" -- :class:`~bloop.types.Number`
* "B" -- :class:`~bloop.types.Binary`
* "BOOL" -- :class:`~bloop.types.Boolean`

These can be instantiated without a constructor, and map to the expected python types:

.. code-block:: python

    from bloop import BaseModel, Column, String, Number, Binary, Boolean

    class Account(BaseModel):
        email = Column(String, hash_key=True)
        balance = Column(Number)
        public_key = Column(Binary)
        verified = Column(Boolean)

    account = Account(
        email="user@domain.com",
        balance=4100,
        public_key=public_bytes(some_key),
        verified=False
    )

Bloop also includes a handful of common scalar types that are built on top of the primitive types.  The
following demonstrates that hash and range key columns can be any Type that is backed by "S", "N", or "B"
and not just the primitive types above.

.. code-block:: python

    import uuid
    from datetime import datetime, timedelta, timezone
    from bloop import DateTime, Timestamp, UUID, Integer


    class Tweet(BaseModel):
        account_id = Column(Integer, hash_key=True)
        tweet_id = Column(UUID, range_key=True)
        created_at = Column(DateTime)
        delete_after = Column(Timestamp)

    now = datetime.now(timezone.utc)
    tomorrow = now + timedelta(days=1)
    tweet = Tweet(
        account_id=3,
        tweet_id=uuid.uuid4(),
        created_at=now,
        delete_after=tomorrow
    )

.. note::

    Bloop's :class:`~bloop.types.Number` type uses a :class:`decimal.Context` to control rounding and exactness.
    When exactness is not required, many people find the default context too conservative for practical use.
    For example, the default context can't save ``float('3.14')`` due to inexactness.

    As noted in the :ref:`Public API Reference <api-public-number>`, you can provide your own context or use an
    :ref:`existing pattern <patterns-float>`.  Keep in mind that the convenience comes at the expense of exactness.

======
 Sets
======

Bloop exposes a single :class:`~bloop.types.Set` for all three sets.  The particular set type is determined by the
Set's inner type.  For example, ``Set(Integer)`` has backing_type "NS" and ``Set(DateTime)`` has backing_type "SS".

The inner type must have a backing type of "S", "N", or "B".  When Bloop loads or dumps a set, it defers to the
inner type for each value in the set.  Using the :ref:`enum example <user-types-enum-str>` below, a set of enums
can be stored as follows:

.. code-block:: pycon

    >>> from bloop import BaseModel, Column, Set, Integer
    >>> from my_types import StringEnum
    >>> import enum
    >>> class Colors(enum.Enum):
    ...     red = 1
    ...     green = 2
    ...     blue = 3
    ...
    >>> class Palette(BaseModel):
    ...     id = Column(Integer, hash_key=True)
    ...     colors = Column(Set(StringEnum(Colors)))
    ...
    >>> palette = Palette(id=0, colors={Colors.red, Colors.green})

The ``pallete.colors`` value would be persisted in DynamoDB as::

    {"SS": ["red", "green"]}


===========
 Documents
===========

DynamoDB's Map and List types can store arbitrarily-types values.  For example, a single attribute can hold a string,
number, and a nested List::

    {"L": [{"S": {"foo"}}, {"N": {"3.4"}}, {"L": []}]}

Unfortunately, Bloop's built-in :class:`~bloop.types.Map` and :class:`~bloop.types.List` types can't provide the same
generality.  List and Map must explicitly declare the Type to use when loading and dumping values.  Otherwise, Bloop
can't know if the following should be loaded as a String or DateTime::

    {"S": "2016-08-09T01:16:25.322849+00:00"}


------
 List
------

Unlike Set, a List's inner type can be anything, including other Lists, Sets, and Maps.  Due to the lack of type
information when loading values, Bloop's built-in :class:`~bloop.types.List` can only hold one type of value:

.. code-block:: pycon

    >>> from bloop import List, Set, Integer
    >>> exams = Set(Integer)  # Unique scores for one student
    >>> from bloop import BaseModel, Column
    >>> class Semester(BaseModel):
    ...     id = Column(Integer, hash_key=True)
    ...     scores = List(exam_scores)  # All student scores
    ...
    >>> semester = Semester(id=0, scores=[
    ...     {95, 98, 64, 32},
    ...     {0},
    ...     {64, 73, 75, 50, 52}
    ... ])

The semester's scores would be saved as (formatted for readability)::

    {"L": [
        {"NS": ['95', '98', '64', '32']},
        {"NS": ['0']},
        {"NS": ['64', '73', '75', '50', '52']},
    ]}

-----
 Map
-----

As stated, :class:`~bloop.types.Map` doesn't support arbitrary types out of the box.  Instead, you must provide
the type to use for each key in the Map:

.. code-block:: python

    # Using kwargs directly
    Map(username=String, wins=Integer)

    # Unpacking from a dict
    Metadata = Map(**{
        "created": DateTime,
        "referrer": UUID,
        "cache": String
    })

Only defined keys will be loaded or saved.  In the following, the impression's "version" metadata will not be saved:

.. code-block:: python

    class Impression(BaseModel):
        id = Column(UUID, hash_key=True)
        metadata = Column(Metadata)

    impression = Impression(id=uuid.uuid4())
    impression.metadata = {
        "created": datetime.now(timezone.utc),
        "referrer": referrer.id,
        "cache": "https://img-cache.s3.amazonaws.com/" + img.filename,
        "version": 1.1  # NOT SAVED
    }

.. warning::

    Saving a Map ``M`` in DynamoDB fully replaces the existing value.

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

.. _user-types-custom:

==============
 Custom Types
==============

Creating new types is straightforward.  Most of the time, you'll only need to implement
:func:`~bloop.types.Type.dynamo_dump` and :func:`~bloop.types.Type.dynamo_load`.
Here's a type that stores an :class:`PIL.Image.Image` as bytes:

.. code-block:: python

    import io
    from PIL import Image

    class ImageType(bloop.Binary):
        python_type = Image.Image

        def __init__(self, fmt="JPEG"):
            self.fmt = fmt
            super().__init__()

        def dynamo_dump(self, image, *, context, **kwargs):
            if image is None:
                return None
            buffer = io.BytesIO()
            image.save(buffer, format=self.fmt)
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

Now the model doesn't need to know how to load or save the image bytes, and just interacts with
instances of :class:`~PIL.Image.Image`:

.. code-block:: python

    class User(BaseModel):
        name = Column(String, hash_key=True)
        profile_image = Column(ImageType("PNG"))
    engine.bind(User)

    user = User(name="numberoverzero")
    engine.load(user)

    user.profile_image.rotate(90)
    engine.save(user)

------------------
 Missing and None
------------------

When there's no value for a :class:`~bloop.models.Column` that's being loaded, your type will need to handle None.
For many types, None is the best sentinel to return for "this has no value" -- Most of the built-in types use None.

Bloop will transparently map None to empty values for types.  For example, :class:`~bloop.types.Set` returns an empty
``set``, so that you'll never need to check for None before adding and removing elements.
:class:`~bloop.types.Map` will load None for the type associated with each of its keys, and insert those in the dict.
``String`` and ``Binary`` will replace ``None`` with ``""`` and ``b""``, respectively.

You will also need to handle ``None`` when dumping values to DynamoDB.  This can happen when a value is deleted
from a Model instance, or it's explicitly set to None.  In almost all cases, your ``dynamo_dump`` function should
simply return None to signal omission (or deletion, depending on the context).

You should return None when dumping empty values like ``list()``, or DynamoDB will complain about setting
something to an empty list or set.  By returning None, Bloop will know to put that column in
the DELETE section of the UpdateItem.

.. _user-types-enum-str:

----------------------
 Example: String Enum
----------------------

This is a simple Type that stores an :py:class:`enum.Enum` by its string value.

.. code-block:: python

    class StringEnum(bloop.String):
        def __init__(self, enum_cls):
            self.enum_cls = enum_cls
            super().__init__()

        def dynamo_dump(self, value, *, context, **kwargs):
            if value is None:
                return value
            value = value.name
            return super().dynamo_dump(value, context=context, **kwargs)

        def dynamo_load(self, value, *, context, **kwargs):
            if value is None:
                return value
            value = super().dynamo_load(value, context=context, **kwargs)
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
        color = Column(StringEnum(Color))
    engine.bind(Shirt)

    shirt = Shirt(id="t-shirt", color=Color.red)
    engine.save(shirt)

-----------------------
 Example: Integer Enum
-----------------------

To instead store enums as their integer values, we can modify the enum class above:

.. code-block:: python
    :emphasize-lines: 1, 9, 16

    class IntEnum(bloop.Integer):
        def __init__(self, enum_cls):
            self.enum_cls = enum_cls
            super().__init__()

        def dynamo_dump(self, value, *, context, **kwargs):
            if value is None:
                return value
            value = value.value
            return super().dynamo_dump(value, context=context, **kwargs)

        def dynamo_load(self, value, *, context, **kwargs):
            if value is None:
                return value
            value = super().dynamo_load(value, context=context, **kwargs)
            return self.enum_cls(value)

.. _type-validation:

=================
 Type Validation
=================

By default Bloop does not verify that each model's values have the correct types.  For example, consider this model:

.. code-block:: python

    class Appointment(BaseModel):
        id = Column(UUID, hash_key=True)
        date = Column(DateTime)
        location = Column(String)


The following code won't throw type errors until we try to persist to DynamoDB:

.. code-block:: pycon

    >>> engine.bind(Appointment)
    >>> a = Appointment(id="not-a-uuid")
    >>> a.location = 421
    >>> a
    Appointment(id='not-a-uuid', location=421)

    >>> engine.save(a)
    ParamValidationError: ...

This is because Bloop is designed to be maximally customizable, and easily extend your existing object model framework.
There's also no built-in way to specify that a column is non-nullable.  For an example of adding both these constraints
to your :class:`~bloop.models.Column`, see :ref:`custom-column`.  Alternatively, consider a more robust option such as
the exceptional `marshmallow`__.  An example integrating with marshmallow and flask is
:ref:`available here <marshmallow-pattern>`.

__ https://marshmallow.readthedocs.io
