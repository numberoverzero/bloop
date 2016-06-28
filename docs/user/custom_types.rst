.. _user-custom-types:

Custom Types
^^^^^^^^^^^^

In most cases, you'll create a custom type by subclassing one of ``String``, ``Float``, or ``Binary`` and implementing
two methods: ``dynamo_load`` and ``dynamo_dump``.  If you are subclassing the base ``Type``, or any built-in type,
you do not need to do anything else.  See the quick example below to get going.

If you are creating a type that will be backed by Dynamo's ``LIST``, ``MAP``, or one of ``SS/NS/BS`` you will probably
need to implement ``_load``, ``_dump``, and ``_register``.

In rare cases, you may want to implement ``bind`` to provide engine-specific pairs of ``_load``/``_dump`` functions.


Quick example
=============

Here's a trivial type that prepends a string with its length.  So ``"hello world"`` is stored as ``"11:hello world"``::

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

We can now use our custom type anywhere a String would be allowed::

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

Because the ``dynamo_load`` function strips the length prefix off, it's not present when loading from DynamoDB::

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

The ``NULL`` type is not used because it can have only one value (True) which, while useful when a column can have
arbitrarily typed values, is useless for an Object Mapper which enforces types per-column.

.. _base-type:

Base Type
=========

Before diving into the available types, here's the structure of the base ``Type``::

    class Type(declare.TypeDefinition):
        python_type = None
        backing_type = None

        def bind(self, engine, **config):
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

        def _register(self, engine):
            super()._register(engine)

        def dynamo_load(self, value, *, context, **kwargs):
            return value

        def dynamo_dump(self, value, *, context, **kwargs):
            return value


At some point your types **must** subclass ``declare.TypeDefinition``, which hooks into the underlying system that
``bloop.Engine`` relies on for recursively loading/dumping modeled objects.

If you aren't subclassing ``bloop.Type`` you only need to read the sections on ``_register``, ``_load`` and ``_dump``
below.

The definition for ``bind`` above is from ``declare.TypeDefinition`` and is not specific to ``bloop.Type``.  It is
usually enough to implement ``_load`` and ``_dump`` in your type, and rely on ``declare.TypeDefinition`` to handle
type binding.

``python_type``
---------------

This attribute is purely informational, and is only used in ``__str__`` and ``__repr__``.  This attribute isn't
checked against an incoming or outgoing value, although your custom types could choose to enforce them.

``backing_type``
----------------

Unlike ``python_type``, this field is **required** and must be one of the types defined in :ref:`base-type`.  This is
used to dump a value eg. ``"some string"`` into the proper DynamoDB wire format ``{"S": "some string"}``.  Usually,
you'll want to define this on your custom type.  In some cases, however, you won't know this value until the type is
instantiated.  For example, the built-in :ref:`user-set-type` type constructs the backing type based on its inner
type's backing type with roughly the following::

    def __init__(self, typedef=None):
        if typedef is None:
            raise TypeError(...)
        if typedef.backing_type not in ["N", "S", "B"]:
            raise TypeError(...)

        # Good to go, backing type will be NS, SS, or BS
        self.backing_type = "S" + typedef.backing_type

``dynamo_load``
---------------

Because ``bloop.Type`` unpacks the wire format's single-key dict for you, this will always be the value as a string.
If there was no value, or the value was ``None``, ``Type._load`` will not call ``dynamo_load`` and will instead return
None.  If you want to handle ``None``, you will need to implement ``_load`` yourself.

The bloop engine that is loading the value can always be accessed through ``context["engine"]``; this is useful to
return different values depending on how the engine is configured, or performing chained operations.  For example, you
could implement a reference type that loads a value from a different model like so::

    class ReferenceType(bloop.Type):
        def __init__(self, model=None, blob_name=None):
            # TODO Guard against (model is None or blob_name is None)
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

And its usage::

    class Data(bloop.new_base()):
        id = Column(String, hash_key=True)
        blob = Column(Binary)


    class IndirectData(Base):
        id = Column(String, hash_key=True)
        blob = Column(ReferenceType(Data))

    engine.bind(base=Data)

``dynamo_dump``
---------------

The exact reverse of ``dynamo_load``, this method takes the modeled value and turns it a string that contains a
DynamoDB-compatible format for the given backing value.  For binary objects, this means base64 encoding the value.

For the ``ReferenceType`` defined above, here is the corresponding ``dynamo_dump``::

    def dynamo_dump(self, value, *, context, **kwargs):
        # value is an instance of the loaded object,
        # so its hash key is the value to return
        # from this object (after saving value to Dynamo)

        # TODO try/catch NotModified
        context["engine"].save(value)


        # Get the model name of the hash key
        hash_key_name = self.model.Meta.hash_key.model_name
        return getattr(obj, hash_key_name)

``_load``, ``_dump``
--------------------

For most cases, subclassing ``bloop.Type`` should be sufficient.  If however you want to handle ``None`` yourself,
or need to handle recursive load/dump calls (for nested types, like ``Map`` and ``List``) you will probably need to
either implement these methods, or at some point call them.

The signatures for ``dynamo_load`` and ``dynamo_dump`` are intentionally compatible with ``_load`` and ``_dump``; you
should review the sections for those functions above for an example of using them.

The base type short-circuits on ``None`` and does not invoke the corresponding ``dynamo_*`` functions, since Nones are
treated as not present (during load) or not modified (during dump).  This is because Dynamo may elide any missing
values, and will treat Nones as such on the wire.  To keep the logic simple in ``dynamo_*`` functions, which will
almost never care about ``None``, the check is done before those functions.

If you still want to handle None on your own, even with the disclaimer that None may not represent a sentinel for
"not present" but instead be an omission for the sake of wire size, then you will need to implement ``_load`` and
``_dump`` with the same intention as the ``dynamo_*`` equivalents above.

``_register``
-------------

The ``_register`` method is called on a your custom type when it is registered during ``bloop.Engine.bind``.  You will
need to implement ``_register`` if your custom type has a reference to another type that you intend to load or dump.

For example, the built-in :ref:`user-set-type` uses a type passed as an argument during ``__init__`` to load and dump
values from a String Set, Number Set, or Binary Set.  To ensure the type engine can handle the nested load/dump calls
for that type, it implements ``_register`` like so::

    class Set(Type):
        """Adapter for sets of objects"""
        python_type = collections.abc.Set

        def __init__(self, typedef=None):
            ...
            self.typedef = type_instance(typedef)
            super().__init__()

        def _register(self, engine):
            engine.register(self.typedef)

``bind``
--------

To explain when you would implement ``bind`` we need to briefly review how ``bloop.Engine.bind`` leads to the type
engine understanding your custom types.

After validating your model, ``bloop.Engine.bind`` will call ``type_engine.register`` on the type of each column in
the validated model.  The bloop Engine's type_engine is a ``declare.TypeEngine`` (sorry, not the best naming).  When
register is called on a type, it gives the TypeEngine a chance to review the type and then adds it to a list of
unbound types.  When all of the columns' types have been registered, bloop calls ``type_engine.bind()`` with some
context.

``declare.TypeEngine`` then calls ``TypeDefinition.bind(engine, **config)`` on each of the unbound types.  This
function returns a tuple of ``(load_func, dump_func)`` that this engine can use to load and dump values for the type
through.

By default, and **for almost all cases** this will only need to return the ``_load`` and ``_dump`` functions
on the class.  In some cases, however, you will want to return different functions depending on values in the context
provided to the ``bind`` function.  For example, you may want to return a different load and dump for an engine that
doesn't know about a particular type.  You may store a custom config value on your bloop Engine that you use to flag
a full or partial load.  You may want to associate different engines with particular views of data (say, one for users
and one for admins) and return appropriate functions for both.

By implementing a custom ``bind`` you may remove the need to implement the ``_load`` and ``_dump`` functions::

    import declare


    class AdminType(declare.TypeDefinition):
        def bind(self, engine, **config):
            # Note the difference; the first arg is the
            # underlying declare.TypeEngine, while the
            # engine in config is the bloop.Engine
            declare_engine = engine
            bloop_engine = config["context"]["engine"]

            # Check for an admin flag
            if bloop_engine.config.get("is_admin", False):
                return self.admin_load, self.admin_dump
            else:
                return self.user_load, self.user_dump

        def admin_load(self, value, **kwargs):
            return value
        def admin_dump(self, value, **kwargs):
            return value

        def user_load(self, value, **kwargs):
            return "REDACTED"
        def user_dump(self, value, **kwargs):
            # Users can modify this field but only admins can view it
            return value

Its usage is exactly the same as any other type::

    class PlayerReport(bloop.new_base()):
        id = Column(Integer, hash_key=True)
        reported_by = Column(AdminType)
        description = Column(AdminType)

    admin_engine = bloop.Engine()
    admin_engine.config["is_admin"] = True
    user_engine = bloop.Engine()

    admin_engine.bind(base=PlayerReport)
    user_engine.bind(base=PlayerReport)

    report = PlayerReport(
        id=0, reported_by="victim",
        description="someone is cheating!")
    user_engine.save(report)

    admin_report = PlayerReport(id=0)
    admin_engine.load(admin_report)
    assert admin_report.reported_by == "victim"

    user_report = PlayerReport(id=0)
    user_engine.load(user_report)
    assert user_report.reported_by == "REDACTED"

Enum Example
============

Here are two simple enum types that can be built off existing types with minimal work.  The first is based off of
the :ref:`user-integer-type` type and consumes little space, while the second is based on :ref:`user-string-type` and
stores the Enum values.

For both examples, let's say we have the following :py:class:`enum.Enum`::

    import enum
    class Color(enum.Enum):
        red = 1
        green = 2
        blue = 3

Integer Enum
------------

In this type, dump will transform ``Color -> int`` using ``color.value`` and hand the int to ``super``.  Meanwhile,
load will transform ``int -> Color`` using ``Color(value)`` where value comes from ``super``.

::

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

Usage::

    class Shirt(new_base()):
        id = Column(String, hash_key=True)
        color = Column(EnumType(Color))
    engine.bind(base=Shirt)

    tshirt = Shirt(id="tshirt", color=Color.red)
    engine.save(tshirt)

Stored in DynamoDB as:

+--------+-------+
| id     | color |
+--------+-------+
| tshirt | 1     |
+--------+-------+


String Enum
-----------

This will look remarkably similar, with the only difference that ``Enum.name`` gives us a string, and ``Enum[value]``
gives us an enum value by string.

::

    class EnumType(bloop.String):
        def __init__(self, enum_cls=None):
            if enum_cls is None:
                raise TypeError("Must provide an enum class")
            self.enum_cls = enum_cls
            super().__init__()

        def dynamo_dump(self, value, *, context, **kwargs):
            value = value.name
            return super().dynamo_dump(value, context=context, **kwargs)

        def dynamo_load(self, value, *, context, **kwargs):
            value = super().dynamo_load(value, context=context, **kwargs)
            return self.enum_cls[value]

And usage is exactly the same::

    class Shirt(new_base()):
        id = Column(String, hash_key=True)
        color = Column(EnumType(Color))
    engine.bind(base=Shirt)

    tshirt = Shirt(id="tshirt", color=Color.red)
    engine.save(tshirt)

This time stored in Dynamo as:

Stored in DynamoDB as:

+--------+-------+
| id     | color |
+--------+-------+
| tshirt | red   |
+--------+-------+

RSA Example
===========

This is a quick type for storing a public RSA key in binary::

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

Usage::

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
