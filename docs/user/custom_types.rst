.. _user-custom-types:

Custom Types
^^^^^^^^^^^^

In most cases you'll create a custom type by subclassing one of ``String``, ``Float``, or ``Binary`` and implementing
two methods: ``dynamo_load`` and ``dynamo_dump``.  Sometimes, you'll need to implement ``_register`` for nested or
referenced types.  See the quick example below to get going.

Quick Example
=============

Here's a trivial type that prepends a string with its length.  So ``"hello world"`` is stored as ``"11:hello world"``:

.. code-block:: python

    class LengthString(bloop.String):
        def __init__(self, sep=":"):
            self.sep = sep

        def dynamo_load(self, value, *, context, **kwargs):
            value = super().dynamo_load(
                value, context=context, **kwargs)
            start_at = value.index(self.sep) + 1
            return value[start_at:]

        def dynamo_dump(self, value, *, context, **kwargs):
            prefix = str(len(value)) + self.sep
            return super().dynamo_dump(
                prefix + value, context=context, **kwargs)

We can now use this type anywhere a String would be allowed:

.. code-block:: python

    class SomeModel(bloop.new_base()):
        id = Column(LengthString("|"), hash_key=True)
        data = Column(LengthString)
    engine.bind(base=SomeModel)

    obj = SomeModel(id="hello world", data="hello, world!")
    engine.save(obj)

This is stored in Dynamo like so:

+-----------------+------------------+
| id              | data             |
+-----------------+------------------+
| 11\|hello world | 13:hello, world! |
+-----------------+------------------+

Because the ``dynamo_load`` function strips the length prefix off, it's not present when loading from DynamoDB:

.. code-block:: python

  obj = SomeModel(id="hello world")
  engine.load(obj)
  assert obj.data == "hello, world!"


You'll want to review the following information if you are implementing complex or nested types, or you want to
write your own base type that doesn't leverage the machinery in ``bloop.Type``.

Backing Types
=============

Bloop uses eight of the nine backing types that DynamoDB offers::

    "S"    String
    "N"    Number
    "B"    Binary
    "SS"   String Set
    "NS"   Number Set
    "BS"   Binary Set
    "M"    Map
    "L"    List
    "BOOL" Boolean

The ``NULL`` type is not used because it can have only one value (True).  This is useful when a column can have
arbitrarily typed values, but is useless for an Object Mapper which enforces a type per column.

.. _base-type:

Base Type
=========

Before diving into the available types, let's talk about the base ``bloop.Type``.  Most of your types will only need
to implement the following:

.. code-block:: python

    class CustomType(bloop.Type):
        backing_type = "S"  # Backed by DynamoDB string type

        def dynamo_load(self, value, *, context, **kwargs):
            value = do_some_processing(value)
            return value

        def dynamo_dump(self, value, *, context, **kwargs):
            value = do_some_processing(value)
            return value

While the full ``bloop.Type`` class looks like this:

.. code-block:: python

    class Type(declare.TypeDefinition):
        python_type = None
        backing_type = None

        def bind(self, type_engine, **config):
            return self._load, self._dump

        def _load(self, value, **kwargs):
            value = next(iter(value.values()))
            if value is None:
                return None
            return self.dynamo_load(value, **kwargs)

        def _dump(self, value, **kwargs):
            if value is None:
                return {self.backing_type: None}
            return {self.backing_type: self.dynamo_dump(value, **kwargs)}

        def dynamo_load(self, value, *, context, **kwargs):
            raise NotImplementedError()

        def dynamo_dump(self, value, *, context, **kwargs):
            raise NotImplementedError()

        def _register(self, type_engine):
            pass

When subclassing ``bloop.Type`` you must not override ``bind``, ``_load``, or ``_dump``.  If you need to customize how
your type binds to the type engine, or want to unpack DynamoDB's wire format manually, see :ref:`user-advanced-types`.

``python_type``
---------------

This attribute is purely informational, and is only used in ``__str__`` and ``__repr__``.  This attribute isn't
checked against an incoming or outgoing value, although your types could choose to enforce them.

``backing_type``
----------------

Unlike ``python_type``, this field is **required** when subclassing ``bloop.Type`` and must be one of the types defined
in :ref:`base-type`.  This is used to dump a value eg. ``"some string"`` into the proper DynamoDB wire format
``{"S": "some string"}``.  Usually, you'll want to define this on your type.  In some cases, however, you won't know
this value until the type is instantiated.  For example, the built-in :ref:`user-set-type` type constructs the backing
type based on its inner type's backing type with roughly the following:

.. code-block:: python

    def __init__(self, typedef=None):
        if typedef is None:
            raise TypeError(...)
        if typedef.backing_type not in {"N", "S", "B"}:
            raise TypeError(...)

        # Good to go, backing type will be NS, SS, or BS
        self.backing_type = typedef.backing_type + "S"

``dynamo_load``
---------------

.. code-block:: python

    def dynamo_load(self, value, *, context, **kwargs):
        ...

Because ``bloop.Type`` unpacks the wire format's single-key dict for you, this will always be the value as a string.
If there was no value, or the value was ``None``, ``dynamo_load`` won't be called, and will instead return None.

The bloop engine that is loading the value can always be accessed through ``context["engine"]``.  This is useful to
return different values depending on how the engine is configured, or performing chained operations.  For example, you
could implement a reference type that loads a value from a different model like so:

.. code-block:: python

    class ReferenceType(bloop.Type):
        def __init__(self, model=None, blob_name=None):
            self.model = model
            self.blob_name = blob_name
            self.python_type = model

        def dynamo_load(self, value, *, context, **kwargs):
            # Load through super first
            value = super().dynamo_load(value, context=context, **kwargs)

            # For simplicity, value is the referenced model's hash_key
            obj = self.model()
            hash_key_name = self.model.Meta.hash_key.model_name
            setattr(obj, hash_key_name, value)

            # TODO try/catch NotModified
            context["engine"].load(obj)
            return obj

And its usage:

.. code-block:: python

    class Data(bloop.new_base()):
        id = Column(String, hash_key=True)
        blob = Column(Binary)


    class IndirectData(Base):
        id = Column(String, hash_key=True)
        blob = Column(ReferenceType(Data))

    engine.bind(base=Data)

``dynamo_dump``
---------------

.. code-block:: python

    def dynamo_dump(self, value, *, context, **kwargs):
        ...

The exact reverse of ``dynamo_load``, this method takes the modeled value and turns it into a string that contains a
DynamoDB-compatible format for the given backing value.  For binary objects this means base64 encoding the value.

Here is the corresponding ``dynamo_dump`` for the ``ReferenceType`` defined above:

.. code-block:: python

    def dynamo_dump(self, value, *, context, **kwargs):
        # value is an instance of the loaded object,
        # so its hash key is the value to return
        # from this object (after saving value to Dynamo)
        context["engine"].save(value)

        # Get the model name of the hash key
        hash_key_name = self.model.Meta.hash_key.model_name
        return getattr(obj, hash_key_name)

Enum Example
============

Here are two simple enum types that can be built off existing types with minimal work.  The first is based off of
the :ref:`user-integer-type` type and consumes little space, while the second is based on :ref:`user-string-type` and
stores the Enum values.

Consider the following :py:class:`enum.Enum`:

.. code-block:: python

    import enum
    class Color(enum.Enum):
        red = 1
        green = 2
        blue = 3

We can store this in DynamoDB with two different types, without changing how we interact with the models that they
represent.

Integer Enum
------------

In this type, dump will transform ``Color -> int`` using ``color.value`` and hand the int to ``super``.  Meanwhile,
load will transform ``int -> Color`` using ``Color(value)`` where value comes from ``super``.

.. code-block:: python

    class EnumType(bloop.Integer):
        def __init__(self, enum_cls=None):
            if enum_cls is None:
                raise TypeError("Must provide an enum class")
            self.enum_cls = enum_cls
            super().__init__()

        def dynamo_dump(self, value, *, context, **kwargs):
            value = value.value
            return super().dynamo_dump(value, context=context, **kwargs)

        def dynamo_load(self, value, *, context, **kwargs):
            value = super().dynamo_load(value, context=context, **kwargs)
            return self.enum_cls(value)

Usage:

.. code-block:: python

    class Shirt(new_base()):
        id = Column(String, hash_key=True)
        color = Column(EnumType(Color))
    engine.bind(base=Shirt)

    shirt = Shirt(id="t-shirt", color=Color.red)
    engine.save(shirt)

Stored in DynamoDB as:

+---------+-------+
| id      | color |
+---------+-------+
| t-shirt | 1     |
+---------+-------+

String Enum
-----------

The only difference is that ``Enum.name`` gives us a string and ``Enum[value]`` gives us an enum value by string.

.. code-block:: python

    class EnumType(bloop.String):
        def __init__(self, enum_cls=None):
            if enum_cls is None:
                raise TypeError("Must provide an enum class")
            self.enum_cls = enum_cls
            super().__init__()

        def dynamo_dump(self, value, *, context, **kwargs):
            # previously: value = value.value
            value = value.name
            return super().dynamo_dump(value, context=context, **kwargs)

        def dynamo_load(self, value, *, context, **kwargs):
            value = super().dynamo_load(value, context=context, **kwargs)
            # previously: self.enum_cls(value)
            return self.enum_cls[value]

Usage is exactly the same:

.. code-block:: python

    class Shirt(new_base()):
        id = Column(String, hash_key=True)
        color = Column(EnumType(Color))
    engine.bind(base=Shirt)

    shirt = Shirt(id="t-shirt", color=Color.red)
    engine.save(shirt)

This time stored in Dynamo as:

+---------+-------+
| id      | color |
+---------+-------+
| t-shirt | red   |
+---------+-------+

RSA Example
===========

This is a quick type for storing a public RSA key in binary:

.. code-block:: python

    from Crypto.PublicKey import RSA


    class PublicKeyType(bloop.Binary):
        """Stored in Dynamo in DER.  Locally, an RSA._RSAobj"""
        python_type = RSA._RSAobj

        def dynamo_load(self, value: str, *, context=None, **kwargs):
            value = super().dynamo_load(value, context=context, **kwargs)
            return RSA.importKey(value)

        def dynamo_dump(self, value, *, context, **kwargs):
            value = value.exportKey(format="DER")
            return super().dynamo_dump(value, context=context, **kwargs)

Note that the parent class handles base64-encoding the bytes during dump, and base64-decoding the bytes during load.

Usage:

.. code-block:: python

    class PublicKey(bloop.new_base()):
        id = Column(String, hash_key=True)
        public = Column(PublicKeyType, name="pub")
    engine.bind(base=PublicKey)


    rsa_pub = RSA.generate(2048).publickey()
    key = PublicKey(id="my-key", public=rsa_pub)
    engine.save(key)

    same_key = PublicKey(id="my-key")
    engine.load(same_key)

    assert same_key.public == rsa_pub

.. _user-advanced-types:

Advanced Custom Types
=====================

The type system does not require all types to subclass ``bloop.Type`` or its parent ``declare.TypeDefinition``.

The only methods you must implement are ``bind`` and ``_register``.  The following shows off the required signature
for the returned load, dump methods from ``bind``, but doesn't handle unpacking DynamoDB's wire format:

.. code-block:: python

    class MyType:

        def _register(self, type_engine):
            pass

        def bind(self, type_engine, **config):
            # Some load function
            def load(self, value, **kwargs):
                return value

            # Some dump function
            def dump(self, value, **kwargs):
                return dump

            # These could be different functions based
            # on the available **config
            return load, dump

None vs Omitted
---------------

Although DynamoDB doesn't return values for missing columns, bloop may send ``None`` through the type to load, and may
ask the type to dump ``None``.  The base ``bloop.Type`` takes care of this and the DynamoDB wire format in ``_load``
and ``_dump`` above, so that the dynamo_* functions only handle non-null data.

You SHOULD NOT map None to a value other than None and vice versa, as bloop leverages in multiple areas the convention
that None represents omission; from the tracking system to the base model's load/dump methods.

Recursive Load, Dump
--------------------

Because ``bind`` can return any two functions, you MUST NOT rely on a type having ``_load``, ``_dump``,
``dynamo_load``, or ``dynamo_dump`` methods.  If you need to load or dump a value through a different type, you MUST
do so through the type engine that's accessible through the ``context`` kwarg:

.. code-block:: python

    class HasInnerType(bloop.Type):
        def __init__(self, inner_type):
            # This will be something like bloop.String()
            self.inner_type = inner_type

        def dynamo_load(self, value, *, context, **kwargs):
            load = context["engine"].type_engine.load
            return load(
                self.inner_type, value,
                context=context, **kwargs)

        def dynamo_dump(self, value, *, context, **kwargs):
            dump = context["engine"].type_engine.dump
            return dump(
                self.inner_type, value,
                context=context, **kwargs)

bloop will always pass the kwarg ``context`` with a dict containing at least ``{"engine": bloop_engine}`` where the
value of ``bloop_engine`` is the engine currently serializing a value through this type.

``_register``
-------------

.. code-block:: python

    def _register(self, type_engine):
        ...

The ``_register`` method is called on a type when ``bloop.Engine.bind`` registers the type from each of a model's
columns.  If your type depends on another type that may not have been bound to the type engine yet, ``_register`` is
the place to do so.  Registering a type that is already bound is a noop, so it's safe to always register your
referenced types.

For example, the built-in :ref:`user-set-type` uses a type passed as an argument during ``__init__`` to load and dump
values from a String Set, Number Set, or Binary Set.  To ensure the type engine can handle the nested load/dump calls
for that type, it implements ``_register`` like so:

.. code-block:: python

    class Set(Type):
        """Adapter for sets of objects"""
        python_type = collections.abc.Set

        def __init__(self, typedef=None):
            ...
            self.typedef = type_instance(typedef)
            super().__init__()

        def _register(self, engine):
            # If the set's type is already registered,
            # this is a noop.  Otherwise, this ensures
            # that we can delegate dynamo_dump to
            # the inner type.
            engine.register(self.typedef)

``bind``
--------

.. code-block:: python

    def bind(self, type_engine, **config):
        ...

The ``bind`` function must return a pair of load and dump functions, which should match the signature:

.. code-block:: python

    def load(value: Any, **kwargs) -> Any
        ...

    def dump(value: Any, **kwargs) -> Any
        ...

In ``bloop.Type`` bind returns the ``_load, _dump`` methods on the class, which ensure the type's
corresponding dynamo_* methods are never called with ``None``, and unpacks the nested dicts of DynamoDB's wire
format.

A common pattern to customize the tuple of serialization functions is to inspect the bloop Engine's config and switch
based on whether a config option is present.  An extremely reduced example, which doesn't hook into
bloop's base ``Type`` at all:

.. code-block:: python

    class AdminType:
        def bind(self, type_engine, **config):
            # bloop provides a dict "context" that encapsulates
            # any data that bloop types may want to inspect
            bloop_context = config["context"]
            bloop_engine = bloop_context["engine"]
            engine_config = bloop_engine.config

            # Alternatively:
            engine_config = config["context"]["engine"].config

            if engine_config["is_admin_engine"] is True:
                return self.admin_load, self.admin_dump
            else:
                return self.user_load, self.user_dump

        def _register(self, type_engine):
            # No nested types to register when this one is
            pass

This is a great opportunity to take advantage of :py:func:`functools.partial`:

.. code-block:: python

    import functools

    class AdminType:
        def load(self, is_admin, value, *, context, **kwargs):
            ...
        def dump(self, is_admin, value, *, context, **kwargs):
            ...

        def bind(self, *, context, **config):
            is_admin = context["engine"].config["is_admin"]
            return (
                functools.partial(self.load, is_admin),
                functools.partial(self.dump, is_admin))

There's no difference in how bloop interacts with the type:

.. code-block:: python

    class PlayerReport(bloop.new_base()):
        id = Column(Integer, hash_key=True)
        reported_by = Column(AdminType)
        description = Column(AdminType)

    admin_engine = bloop.Engine()
    admin_engine.config["is_admin_engine"] = True

    user_engine = bloop.Engine()
    user_engine.config["is_admin_engine"] = False

    admin_engine.bind(base=PlayerReport)
    user_engine.bind(base=PlayerReport)

