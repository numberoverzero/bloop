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

The constructor takes no args, and values are stored in DynamoDB 1:1 with their python type:

.. code-block:: python

    # equivalent
    Column(String)
    Column(String())

    string_type = String()
    string_type.dynamo_dump("foo")  # "foo"

Binary
------

Binary data corresponds to the ``bytes`` type in python, sent over the wire as a base64 encoded string, and stored in
DynamoDB as bytes.  Binary is one of three base types that can be used as a hash or range key.

The constructor takes no args:

.. code-block:: python

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

The constructor takes no args:

.. code-block:: python

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
it takes no args.  It will coerce any value using ``bool``:

.. code-block:: python

    bool_type = Boolean()
    bool_type.dynamo_dump(["foo", "bar"])  # true


Derived Types
=============

These types provide a convenient mapping to python objects for common patterns (unique identifiers, timestamps).

UUID
----

Backed by the ``String`` type, this stores a UUID as its string representation.  It can handle any
:py:class:`uuid.UUID`, and its constructor takes no args:

.. code-block:: python

    import uuid

    u = UUID()
    some_id = uuid.uuid4()

    u.dynamo_dump(some_id)  # "8f5ec651-5997-4843-ad6f-065c22fd8971"

DateTime
--------

DateTime is backed by the ``String`` type and maps to an :py:class:`arrow.arrow.Arrow` object.  While the loaded values
can be instructed to use a particular timezone, values are always stored in UTC ISO8601_ to enable the full range of
comparison operators.

.. code-block:: python

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
its parent type:

.. code-block:: python

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
set type does not need any arguments, you may provide the class instead of an instance:

.. code-block:: python

    # type class uses no-arg __init__
    float_set = Set(Float)
    # type instance is used directly
    timestamp_set = Set(DateTime(timezone="US/Pacific"))

    # This fails, because the inner type is
    # backed by BOOL, not S, N, or B
    Set(Boolean())

    floats = set([3.5, 2, -1.0])
    float_set.dynamo_dump(floats)  # ["3.5", "2", "-1.0"]

.. _user-list-type:

List
----

While DynamoDB's ``List`` type allows any combination of types, bloop's built-in ``List`` type requires you to
constrain the list to a single type.  This type is constructed the same way as ``Set`` above.

This limitation exists because there isn't enough type information when loading a list to tell subclasses apart.
That means that we can't tell if the string ``2016-06-28T05:18:02.633634+00:00`` should be loaded as a ``String`` or
as a ``DateTime``.

While bloop could store type information next to the data (or in additional columns), the first option forces bloop's
type patterns on every client that talks to your DynamoDB instance, and both options consume additional space that you
may not be able to spare.  Storing type information also opens up the potential for pickle-like vulnerabilities, and
violates the idea that the same data can be interpreted by different bloop engines (through binding different load
functions).

If you need to support multiple types in lists, the type system is general enough that you can define your own
List type, that stores the type information of each object when your type is dumped to Dynamo.

.. code-block:: python

    # type class uses no-arg __init__
    float_list = List(Float)
    # type instance is used directly
    timestamp_list = List(DateTime(timezone="US/Pacific"))

    # This is fine; List's inner type can be any type
    List(List(Boolean()))

    floats = [3.5, 2, -1.0]
    float_list.dynamo_dump(floats)  # ["3.5", "2", "-1.0"]

Map
---

Like the List type above, ``Map`` is a restricted subset of the general DynamoDB ``Map`` and only loads/dumps the
modeled structure you specify.  For more information on why bloop does not support arbitrary types in Maps, see the
:ref:`user-list-type` type above.

You construct a map type through ``**kwargs``, where each key is the document key, and each value is a type definition
or type instance (``DateTime`` or ``DateTime(timezone="...")``).  There is no restriction on what types can be used for
keys, including nested maps and other document-based types.

.. code-block:: python

    ProductData = Map(**{
        'Rating': Float(),
        'Stock': Integer(),
        'Description': Map(**{
            'Heading': String,
            'Body': String,
            'Specifications': String
        }),
        'Id': UUID,
        'Updated': DateTime
    })


    class Product(new_base()):
        id = Column(Integer, hash_key=True)
        old_data = Column(ProductData)
        new_data = Column(ProductData)

TypedMap
--------

Like ``Map`` above, ``TypedMap`` is not a general map for any typed data.  Unlike Map however, TypedMap allows an
arbitrary number of keys, so long as all of the values have the same type.  This is useful when you are storing data
under user-provided keys, or mapping for an unknown key size.

As with List and Map, you can nest TypedMaps.  For example, storing the event data for an unknown number of
instances might look something like:

.. code-block:: python

    # Modeling some events
    # -----------------------------------
    # The unpacking dict above can also just be
    # direct kwargs
    EventCounter = Map(
        last=DateTime,
        count=Integer,
        source_ips=Set(String))


    class Metric(new_base()):
        name = Column(String, hash_key=True)
        host_events = Column(TypedMap(EventCounter))


    # Initial save, during service setup
    # -----------------------------------
    metric = Metric(name="email-campaign-2016-06-29")
    metric.host_events = {}
    engine.save(metric)


    # Recording an event during request handler
    # -----------------------------------
    host_name = "api.control-plane.host-1"
    metric = Metric(name="...")
    engine.load(metric)

    # If there were no events, create an empty dict
    events = metric.host_events.get(host_name)
    if events is None:
        events = {
            "count": 0,
            "source_ips": set()
        }
        metric.host_events[host_name] = events

    # Record this requester event
    events["count"] += 1
    events["last"] = arrow.now()
    events["source_ips"].add(request.get_ip())

    # Atomic save helps us here because DynamoDB doesn't
    # support multiple updates with overlapping paths yet:
    # https://github.com/numberoverzero/bloop/issues/28
    # https://forums.aws.amazon.com/message.jspa?messageID=711992
    engine.save(metric, atomic=True)
