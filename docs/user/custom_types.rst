.. _user-custom-types:

Custom Types
^^^^^^^^^^^^

You'll typically create a custom type by subclassing one of ``String``, ``Float``, or ``Binary`` and implementing two
methods: ``dynamo_load`` and ``dynamo_dump``.  For nested or reference types, you may need to implement ``_register``.

Quick Example
=============

Here's a trivial type that prepends a string with its length.  So ``"hello world"`` is stored as ``"11:hello world"``:

.. code-block:: python

    class LengthString(bloop.String):
        def __init__(self, sep=":"):
            self.sep = sep

        def dynamo_load(self, value, *, context, **kwargs):
            if value is None:
                return None
            start_at = value.index(self.sep) + 1
            return value[start_at:]

        def dynamo_dump(self, value, *, context, **kwargs):
            if value is None:
                return None
            prefix = str(len(value)) + self.sep
            return prefix + value

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

        def _load(self, value, **kwargs):
            if value is not None:
                value = next(iter(value.values()))
            return self.dynamo_load(value, **kwargs)

        def _dump(self, value, **kwargs):
            value = self.dynamo_dump(value, **kwargs)
            if value is None:
                return value
            return {self.backing_type: value}

        def _register(self, type_engine):
            pass

        def dynamo_load(self, value, *, context, **kwargs):
            raise NotImplementedError()

        def dynamo_dump(self, value, *, context, **kwargs):
            raise NotImplementedError()

``python_type``
---------------

*(defaults to None)*

This attribute is purely informational, and is only used in ``__str__`` and ``__repr__``.  This attribute isn't
checked against an incoming or outgoing value, although your types could choose to enforce them.

``backing_type``
----------------

*(required)*

This must be one of the types defined in :ref:`base-type`.  ``backing_type`` is used to dump a value eg.
``"some string"`` into the DynamoDB wire format ``{"S": "some string"}``.  Usually, you'll define this on your type.
In some cases, you won't know this value until the type is instantiated.  For example, the built-in
:ref:`user-set-type` type constructs the backing type based on its inner type's backing type:

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

The ``value`` passed to ``dynamo_load`` will either be a ``str`` or ``None``.  When the value is a str, it is the inner
value from the DynamoDB wire format.  For example, ``{"N": "300"}`` will pass ``"300"`` to ``dynamo_load``.

You should interpret ``None`` to mean "missing". For most scalar types (String, Integer, Bool) there's no value that
indicates "missing", so these types return None.  Other types can return empty structures, so that you don't litter
your code with None checks whenever you access a column.

For example, ``Set`` returns an empty ``set()`` instead of None.  The same holds for ``List``, ``TypedMap``, and
``Map``.  All of these return empty (partially empty, in Map's case) objects.

For more information, see :ref:`none-vs-missing` below.

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
            if value is None:
                return None

            # For simplicity, value is the referenced model's hash_key
            obj = self.model.Meta.init()
            hash_key_name = self.model.Meta.hash_key.model_name
            setattr(obj, hash_key_name, value)

            # TODO try/catch NotModified
            context["engine"].load(obj)
            return obj

And its usage:

.. code-block:: python

    class Data(bloop.new_base()):
        id = Column(String, hash_key=True)
        data_blob = Column(Binary)


    class IndirectData(Base):
        id = Column(String, hash_key=True)
        blob = Column(ReferenceType(Data))

    engine.bind(base=Data)
    engine.bind(base=IndirectData)


    data = Data(id="inner", data_blob=b"some data")
    engine.save(data)

    # TODO: dynamo_dump below to save this correctly
    indirect = IndirectData(id="outer", blob=data)
    engine.save(indirect)


    outer = IndirectData(id="outer")

    # 1. First outer.data is loaded; see dynamo_dump
    #    below, this will be the id of the Data obj
    # 2. When ReferenceType.dynamo_load is called,
    #    it takes that id (value), and creates an instance
    #    of Data.
    # 3. It uses the engine in context to load that object.
    # 4. Finally, it returns the loaded object, which becomes
    #    the new value for outer.data
    engine.load(outer)

    assert outer.blob.id == "inner"

``dynamo_dump``
---------------

.. code-block:: python

    def dynamo_dump(self, value, *, context, **kwargs):
        ...

The exact reverse of ``dynamo_load``, this method takes the modeled value and turns it into a string that contains a
DynamoDB-compatible format for the given backing value.  For binary objects this means base64 encoding the value.

You will need to handle ``None`` here as well; if an object is missing a column after an ``engine.load`` then that
column will be ``None``.  Immediately saving it back to DynamoDB will push that ``None`` through ``dynamo_dump``.

Additionally, ``dynamo_dump`` can signal that a value is missing (or should be considered non-existent) by returning
``None``.  This is useful to dump an empty set into None to indicate that the value shouldn't be included in an
UpdateItem call.

Again, you should see :ref:`none-vs-missing` below for more details.

Here is the corresponding ``dynamo_dump`` for the ``ReferenceType`` defined above:

.. code-block:: python

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        # value is an instance of the loaded object,
        # so its hash key is the value to return
        # from this object (after saving value to Dynamo)
        context["engine"].save(value)

        # Get the model name of the hash key
        hash_key_name = self.model.Meta.hash_key.model_name
        return getattr(obj, hash_key_name)

For the example above, here's what happens when we save the Indirect object:

.. code-block:: python

    class Data(bloop.new_base()):
        id = Column(String, hash_key=True)
        data_blob = Column(Binary)


    class IndirectData(Base):
        id = Column(String, hash_key=True)
        blob = Column(ReferenceType(Data))

    engine.bind(base=Data)
    engine.bind(base=IndirectData)


    # Nothing different so far, just a regular save
    data = Data(id="inner", data_blob=b"some data")
    engine.save(data)

    # This is also fine, we're setting attributes locally
    indirect = IndirectData(id="outer", blob=data)

    # 1. When dumping indirect.blob, `value` is the
    #    data object.
    # 2. ReferenceType.dynamo_dump uses the engine from
    #    the context to save the value (data object).
    # 3. The hash key (id) of the value is returned, to
    #    be dumped for the "blob" key in indirect.
    engine.save(indirect)

.. _none-vs-missing:

``None`` as ``missing``
-----------------------

None is effectively the same as "missing" throughout bloop.  When loading, it means "this value was expected but
was not".  If your dump function returns a None, it means "this value should be missing".  When saving, it translates
to a ``DELETE``.  For atomic conditions, it translates to ``attribute_not_exists``.

Note that "missing" does not mean "missing from the wire response" since some calls (queries, scans) may not return
those values.  If a query only loads columns ``{a, b, c}`` on a model with columns ``{a, b, c, d}`` it is incorrect
to say that d is missing, since it shouldn't be loaded.  In this case ``d`` would not be loaded through a Type; it's
not known whether this instance of the model has a value for the ``d`` column.

================
Loading ``None``
================

When loading an object from DynamoDB, it won't return any key for a column that's missing.  If it did, it would likely
be unpacked to ``None`` by ``boto3``.  This means the value that ``Type._load`` sees can be either ``{str: str}`` or
``None``.

While Type._load could short-circuit on ``None`` and return ``None``, that's not the best behavior for all types (and
certainly not for all custom types).  For instance, it's much easier to use the ``List`` type where a missing value
becomes ``[]`` instead of ``None`` - the former lets you use the value as a ``list`` without checking for None
throughout your code.

First, without a default object provided by the type:

.. code-block:: python

    def add_claim(user, claim):
        engine.load(user)
        if user.claims is None:
            user.claims = [claim]
        else:
            user.claims.append(claim)
        engine.save(user)

And the same method, where the ``List`` type returns ``list()`` instead of ``None``:

.. code-block:: python

    def add_claim(user, claim):
        engine.load(user)
        user.claims.append(claim)
        engine.save(user)

By returning a value that isn't ``None``, you simplify every interaction with your type throughout the code.  Any type
that can return None will have to check for it before performing an operation.  This actually makes the falsey
nature of None a problem when dealing with a ``Boolean``, since you can no longer use ``if obj.some_bool:`` which would
conflate False and None.

================
Dumping ``None``
================

You should handle ``None`` when dumping values as well.  If your type loads missing values as None (like Integer)
then a column with that type that doesn't have a value may be None when saving back to DynamoDB.

Boto3 will throw if you try to pass most of the container types (set, list, dict) when they are empty.  Instead, you
should check for empty containers and return ``None`` instead.  All of the built-in types do this; ``Map`` is the most
complex, as it checks for ``None`` values for each key and removes them.

Here's the pair of functions for String:

.. code-block:: python

    def dynamo_load(self, value, *, context, **kwargs):
    return value

    def dynamo_dump(self, value, *, context, **kwargs):
        if value is None:
            return None
        return str(value)

Note that ``dynamo_load`` doesn't check for None, since it will be passed directly back.

Here's the dynamo_dump function for ``List``, which filters out ``None`` from any inner values:

.. code-block:: python

    def dynamo_dump(self, values, *, context, **kwargs):
        if values is None:
            return None
        # local lookup in a tight loop
        dump = context["engine"]._dump
        typedef = self.typedef

        filtered = filter(
            lambda x: x is not None,
            (
                dump(typedef, value, context=context, **kwargs)
                for value in values))
        return list(filtered) or None

The return takes advantage of the fact that an empty list is falsey, and so None is returned.  Here's what this means
for a String Set:

.. code-block:: python

    class Data(new_base()):
        id = Column(String, hash_key=True)
        keys = Column(Set(String))
    engine = bloop.Engine()
    engine.bind(base=Data)

    obj = Data(id="id", keys={"foo", None, "baz"})
    print(engine._dump(Data, obj))

When we print this out, we'll get:

.. code-block:: python

    {
        "id": {"S": "id"},
        "keys": {"SS": ["foo", "baz"]}
    }

Alternatively, here's what we get if we dump a fresh ``Data`` instance without any keys:

.. code-block:: python

    obj = Data(id="id")
    print(engine._dump(Data, obj))

    {
        "id": {"S": "id"}
    }

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
            if value is None:
                return value
            value = value.value
            return super().dynamo_dump(value, context=context, **kwargs)

        def dynamo_load(self, value, *, context, **kwargs):
            if value is None:
                return value
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
            if value is None:
                return value
            # previously: value = value.value
            value = value.name
            return super().dynamo_dump(value, context=context, **kwargs)

        def dynamo_load(self, value, *, context, **kwargs):
            if value is None:
                return value
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
            if value is None:
                return value
            value = super().dynamo_load(value, context=context, **kwargs)
            return RSA.importKey(value)

        def dynamo_dump(self, value, *, context, **kwargs):
            if value is None:
                return value
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

Recursive Load, Dump
--------------------

Not all types will define ``_load``, ``_dump``, or even ``dynamo_load``, or ``dynamo_dump`` methods.
If you need to load or dump a value through a different type, you MUST
do so through the type engine that's accessible through the ``context`` kwarg:

.. code-block:: python

    class HasInnerType(bloop.Type):
        def __init__(self, inner_type):
            # This will be something like bloop.String()
            self.inner_type = inner_type

        def dynamo_load(self, value, *, context, **kwargs):
            load = context["engine"]._load
            return load(
                self.inner_type, value,
                context=context, **kwargs)

        def dynamo_dump(self, value, *, context, **kwargs):
            dump = context["engine"]._dump
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
the place to do so.  It is safe to register a type that is already bound; those types are simply skipped on the next
bind call.

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

