.. _define-models:

Define Models
^^^^^^^^^^^^^

====================
 A Basic Definition
====================

Every model inherits from :class:`~bloop.models.BaseModel`, and needs at least a hash key:

.. code-block:: pycon

    >>> from bloop import BaseModel, Column, UUID

    >>> class User(BaseModel):
    ...     id = Column(UUID, hash_key=True)
    ...
    >>> User
    <Model[User]>
    >>> User.id
    <Column[User.id=hash]>

Let's add some columns, a range key, and a GSI:

.. code-block:: python

    >>> from bloop import (
    ...     BaseModel, Boolean, Column, DateTime,
    ...     GlobalSecondaryIndex, String, UUID)
    ...
    >>> class User(BaseModel):
    ...     id = Column(UUID, hash_key=True)
    ...     version = Column(String, range_key=True)
    ...     email = Column(String)
    ...     created_on = Column(DateTime)
    ...     verified = Column(Boolean)
    ...     profile = Column(String)
    ...     by_email = GlobalSecondaryIndex(projection="keys", hash_key="email")
    ...
    >>> User
    <Model[User]>
    >>> User.by_email
    <GSI[User.by_email=keys]>


Then create the table in DynamoDB:

.. code-block:: pycon

    >>> from bloop import Engine
    >>> engine = Engine()
    >>> engine.bind(User)

.. hint::

    Alternatively, we could have called ``engine.bind(BaseModel)`` to bind all non-abstract models that subclass
    :class:`~bloop.models.BaseModel`.  If any model doesn't match its backing table, ``TableMismatch`` is raised.

.. note::

    Models :ref:`must be hashable <implementation-model-hash>`.  If you implement ``__eq__`` without
    ``__hash__``, Bloop will inject the first hash method it finds by walking the model's :meth:`class.mro`.

====================
 Creating Instances
====================

The default ``__init__`` takes \*\*kwargs and applies them by each column's model name:

.. code-block:: pycon

    >>> import datetime, uuid
    >>> now = datetime.datetime.now(datetime.timezone.utc)
    >>> user = User(
    ...     id=uuid.uuid4(),
    ...     version="1",
    ...     email="user@domain.com",
    ...     created_at=now)
    >>> user.email
    'user@domain.com'
    >>> user
    User(created_on=datetime.datetime(2016, 10, 29, ...), ...)

A local object's hash and range keys don't need values until you're ready to interact with DynamoDB:

.. code-block:: pycon

    >>> user = User(email="u@d.com", version="1")
    >>> engine.save(user)
    MissingKey: User(email='u@d.com') is missing hash_key: 'id'
    >>> user.id = uuid.uuid4()
    >>> engine.save(user)

.. _user-model-meta:

===============================
 Metadata: Table Configuration
===============================

You can provide an inner ``Meta`` class to configure the model's DynamoDB table:

.. code-block:: pycon

    >>> class Tweet(BaseModel):
    ...     class Meta:
    ...         table_name = "custom-table-name"
    ...         read_units = 200
    ...     user = Column(Integer, hash_key=True)
    ...
    >>> Tweet.Meta.read_units
    200
    >>> Tweet.Meta.keys
    {<Column[Tweet.user=hash]}
    >>> Tweet.Meta.indexes
    set()

Table configuration defaults are:

.. code-block:: python

        class Meta:
            abstract = False
            table_name = __name__  # model class name
            read_units = None  # uses DynamoDB value, or 1 for new tables
            write_units = None  # uses DynamoDB value, or 1 for new tables
            stream = None
            ttl = None
            encryption = None
            backups = None


----------
 abstract
----------

If ``abstract`` is true, no backing table will be created in DynamoDB.  Instances of abstract models can't be saved
or loaded.  You can use abstract models, or even plain classes with Columns and Indexes, as mixins.  Derived models
never copy their parents' Meta value.  For more information, see the :ref:`user-models-inheritance` section.

------------
 table_name
------------

The default ``table_name`` is simply the model's ``__name__``.  This property is useful for mapping a model
to an existing table, or mapping multiple models to the same table:

.. code-block:: python

    class Employee(BaseModel):
        class Meta:
            table_name = "employees-uk"
        ...

.. versionchanged:: 2.0.0

    Engines can customize table names using ``table_name_template``.  This does not change the value of
    ``Meta.table_name``.  For example, the template "dev-{table_name}" would cause the ``Employee`` model
    above to use the table "dev-employees-uk"

-------------------------
 read_units, write_units
-------------------------

Default ``read_units`` and ``write_units`` are None.  These do not include provisioned throughput for any
:class:`~bloop.models.GlobalSecondaryIndex`, which has its own read and write units.

If you do not specify the read or write units of a table or GSI, the existing values in DynamoDB are used.  When
the table or GSI does not exist, they fall back to 1.

.. versionchanged:: 1.2.0

    Previously, ``read_units`` and ``write_units`` defaulted to ``1``.  This was inconvenient when throughput
    is controlled by an external script, and totally broken with the new auto-scaling features.

---------
 backups
---------

You can use ``backups`` to enable `Continuous Backups`_ and Point-in-Time Recovery.  By default continuous backups
are not enabled, and this is ``None``.  To enable continuous backups, use:

.. code-block:: python

    class Meta:
        backups = {
            "enabled": True
        }

.. _Continuous Backups: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BackupRestore.html

---------
 billing
---------

You can use ``billing`` to enable `On-Demand Billing`_ or explicitly require provisioned throughput.  By default
billing is None.

If you do not specify the billing mode, the existing configuration in DynamoDB is used.  When
the table does not exist and billing mode is None, the table is created using provisioned throughput.

.. code-block:: python

    class Meta:
        billing = {
            "mode": "on_demand"
        }

    class Meta:
        billing = {
            "mode": "provisioned"  # if not specified, provisioned billing is used for new tables
        }

.. _On-Demand Billing: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html#HowItWorks.OnDemand

------------
 encryption
------------

You can use ``encryption`` to enable `Server-Side Encryption`_.  By default encryption is not enabled, and
this is ``None``.  To enable server-side encryption, use:

.. code-block:: python

    class Meta:
        encryption = {
            "enabled": True
        }

.. _Server-Side Encryption: https://aws.amazon.com/blogs/aws/new-encryption-at-rest-for-dynamodb/

---------
 stream
---------

You can use ``stream`` to enable DynamoDBStreams on the table.  By default streaming is not enabled, and this
is ``None``.  To enable a stream with both new and old images, use:

.. code-block:: python

    class Meta:
        stream = {
            "include": ["new", "old"]
        }

See the :ref:`user-streams` section of the user guide to get started.  Streams are awesome.

-----
 ttl
-----

You can use ``ttl`` to enable the TTL feature on the table.  By default a TTL attribute is not set, and this
is ``None``.  To enable a ttl on the attribute ``"delete_after"``, use:

.. code-block:: python

    class Meta:
        ttl = {
            "column": "delete_after"
        }

The ``Column.typedef`` of the ttl column must be :class:`~bloop.types.Number` and per the DynamoDB documents, must
represent the deletion time as number of seconds since the epoch.  The :class:`~bloop.types.Timestamp` type is provided
for your convenience, and is used as a class:`datetime.datetime`:

.. code-block:: python

    class TemporaryPaste(BaseModel):
        id = Column(UUID, hash_key=True)
        private = Column(Boolean)
        delete_after = Column(Timestamp)

        class Meta:
            ttl = {"column": "delete_after"}

Like :class:`~bloop.types.DateTime`, ``bloop.ext`` exposes drop-in replacements for ``Timestamp`` for each of three
popular python datetime libraries: arrow, delorean, and pendulum.


===============================
 Metadata: Model Introspection
===============================

When a new model is created, a number of attributes are computed and stored in ``Meta``.  These can be used to
generalize conditions for any model, or find columns by their name in DynamoDB.

These top-level properties can be used to describe the model in broad terms:

* ``model`` -- The model this Meta is attached to
* ``columns`` -- The set of all columns in the model
* ``columns_by_name`` -- Dictionary of model Column objects by their ``name`` attribute.
* ``columns_by_dynamo_name`` -- Dictionary of model Column objects by their ``dynamo_name`` attribute.
* ``keys`` -- The set of all table keys in the model (hash key, or hash and range keys)
* ``indexes`` -- The set of all indexes (gsis, lsis) in the model

Additional properties break down the broad categories, such as splitting ``indexes`` into ``gsis`` and ``lsis``:

* ``hash_key`` -- The table hash key
* ``range_key`` -- The table range key or None
* ``gsis`` -- The set of all :class:`~bloop.models.GlobalSecondaryIndex` in the model
* ``lsis`` -- The set of all :class:`~bloop.models.LocalSecondaryIndex` in the model
* ``projection`` A pseudo-projection for the table, providing API parity with an Index

Here's the User model we just defined:

.. code-block:: pycon

    >>> User.Meta.hash_key
    <Column[User.id=hash]>
    >>> User.Meta.gsis
    {<GSI[User.by_email=keys]>}
    >>> User.Meta.keys
    {<Column[User.version=range]>,
     <Column[User.id=hash]>}
    >>> User.Meta.columns
    {<Column[User.created_on]>,
     <Column[User.profile]>,
     <Column[User.verified]>,
     <Column[User.id=hash]>,
     <Column[User.version=range]>,
     <Column[User.email]>}

================================
 Metadata: Using Generic Models
================================

A common pattern involves saving an item only if it doesn't exist.  Instead of creating a specific
condition for every model, we can use ``Meta.keys`` to make a function for any model:

.. code-block:: python

    from bloop import Condition

    def if_not_exist(obj):
        condition = Condition()
        for key in obj.Meta.keys:
            condition &= key.is_(None)
        return condition

Now, saving only when an object doesn't exist is as simple as:

.. code-block:: python

    engine.save(some_obj, condition=if_not_exist(some_obj))

(This is also available in the :ref:`patterns section <patterns-if-not-exist>` of the user guide)

.. _user-models-columns:

=========
 Columns
=========

Every :class:`~bloop.models.Column` must have a :class:`~bloop.types.Type` that is used to load and dump values to
and from DynamoDB.  The ``typedef`` argument can be a type class, or a type instance.  When you provide a
class, the Column will create an instance by calling the constructor without args.  This is a convenience for
common types that do not require much configuration.  The following are functionally equivalent:

.. code-block:: python

    Column(Integer)
    Column(Integer())

Some types require an argument, such as :class:`~bloop.types.Set`.  Sets must have an inner type so they can map to
a string set, number set, or binary set.  For example:

.. code-block:: python

    # FAILS: Set must have a type
    Column(Set)

    # GOOD: Set will instantiate the inner type
    Column(Set(Integer))
    Column(Set(Integer()))

To make a column the model's hash or range key, use ``hash_key=True`` or ``range_key=True``.  The usual rules apply:
a column can't be both, there can't be more than one of each, and there must be a hash key.

.. code-block:: python

    class Impression(BaseModel):
        referrer = Column(String, hash_key=True)
        version = Column(Integer, range_key=True)

By default values will be stored in DynamoDB under the name of the column in the model definition (its ``name``).
If you want to conserve read and write units, you can use shorter names for attributes in DynamoDB (attribute names
are counted against your provisioned throughput).  Like the ``table_name`` in Meta, the optional ``dynamo_name`` parameter
lets you use descriptive model names without binding you to those names in DynamoDB.  This is also convenient when
mapping an existing table, or multi-model tables where an attribute can be interpreted multiple ways.

The following model is identical to the one just defined, except that each attribute is stored using a short name:

.. code-block:: python

    class Impression(BaseModel):
        referrer = Column(String, hash_key=True, dynamo_name="ref")
        version = Column(Integer, range_key=True, dynamo_name="v")

Locally, the model names "referrer" and "version" are still used.  An instance would be constructed as usual:

.. code-block:: python

    >>> click = Impression(
    ...     referrer="google.com",
    ...     version=get_current_version())
    >>> engine.save(click)


----------------
 Default Values
----------------

You can provide a default value or a no-arg function that returns a default value when specifying a Column:

.. code-block:: python

    class User(BaseModel):
        id = Column(UUID)
        verified = Column(Boolean, default=False)
        created = Column(DateTime, default=lambda: datetime.datetime.now())


Defaults are only applied when new instances are created locally by the default ``BaseModel.__init__`` method.
When new instances are created as part of a Query, Scan, or iterating a Stream, defaults are not applied.  This is
because a projection query may not include an existing value; applying the default would locally overwrite the
previous value in DynamoDB.

.. code-block:: python


    import datetime

    def two_days_later():
        offset = datetime.timedelta(days=2)
        now = datetime.datetime.now()
        return now + offset


    class TemporaryPaste(BaseModel):
        class Meta:
            ttl = {"column": "delete_after"}

        id = Column(UUID, hash_key=True, default=uuid.uuid4)
        delete_after = Column(Timestamp, default=two_days_later)
        verified = Column(Boolean, default=False)
        views = Column(Integer, default=1)


Like default function arguments in python, the provided value is not copied but used directly.  For example, a
default value of ``[1, 2, 3]`` will use the **same list object** on each new instance of the model.  If you want a
copy of a mutable value, you should wrap it in a lambda: ``lambda: [1, 2, 3]``.

If you don't want to set a default value, you can return the special sentinel ``bloop.missing`` from your function:

.. code-block:: python

    import datetime
    import random
    from bloop import missing

    specials = [
        "one free latte",
        "50% off chai for a month",
        "free drip coffee for a year",
    ]

    offer_ends = datetime.datetime.now() + datetime.timedelta(hours=8)


    def limited_time_offer():
        now = datetime.datetime.now()
        if now < offer_ends:
            return random.choice(specials)
        return missing


    class User(BaseModel):
        id = Column(UUID, hash_key=True)
        active_coupon = Column(String, default=limited_time_offer)

In this example, a random special is applied to new users for the next 8 hours.  Afterwards, the
``limited_time_offer`` function will return ``bloop.missing`` and the user won't have an active coupon.

Returning ``bloop.missing`` tells Bloop not to set the value, which is different than setting the value to ``None``.
An explicit ``None`` will clear any existing value on save, while not setting it leaves the value as-is.

=========
 Indexes
=========

Indexes provide additional ways to query and scan your data.  If you have not used indexes, you should first read
the Developer's Guide on `Improving Data Access with Secondary Indexes`__.

A single GSI or LSI can be used by two models with different projections, so long as the projections that each
model expects are a subset of the actual projection.  This can be a useful way to restrict which columns are loaded
by eg. a partially hydrated version of a model, while the table's underlying index still provides access to all
attributes.

__ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html

----------------------
 GlobalSecondaryIndex
----------------------

Every :class:`~bloop.models.GlobalSecondaryIndex` must declare a ``projection``, which describes the columns projected
into the index.  Only projected columns are loaded from queries and scans on the index, and non-projected columns
can't be used in filter expressions.  A projection can be ``"all"`` for all columns in the model; ``"keys"`` for the
hash and range columns of the model and the index; or a set of :class:`~bloop.models.Column` objects or their model
names.  If you specify a set of columns, key columns will always be included.

.. code-block:: python

    class HeavilyIndexed(BaseModel):
        ...
        by_email = GlobalSecondaryIndex("all", hash_key="email")
        by_username = GlobalSecondaryIndex("keys", hash_key="username")
        by_create_date = GlobalSecondaryIndex(
            {"email", "username"}, hash_key="created_on")

A GlobalSecondaryIndex must have a ``hash_key``, and can optionally have a ``range_key``.  This can either be the
name of a column, or the column object itself:

.. code-block:: python

    class Impression(BaseModel):
        id = Column(UUID, hash_key=True)
        referrer = Column(String)
        version = Column(Integer)
        created_on = Column(DateTime)

        by_referrer = GlobalSecondaryIndex("all", hash_key=referrer)
        by_version = GlobalSecondaryIndex("keys", hash_key="version")

Unlike :class:`~bloop.models.LocalSecondaryIndex`, a GSI does not share its throughput with the table.  You can
specify the ``read_units`` and ``write_units`` of the GSI.  If you don't specify the throughput and the GSI already
exists, the values will be read from DynamoDB.  If the table doesn't exist, the GSI's read and write units will
instead default to 1.

.. code-block:: python

    GlobalSecondaryIndex("all", hash_key=version, read_units=500, write_units=20)

As with :class:`~bloop.models.Column` you can provide a ``dynamo_name`` for the GSI in DynamoDB.  This can be used to map
to an existing index while still using a pythonic model name locally:

.. code-block:: python

    class Impression(BaseModel):
        ...
        by_email = GlobalSecondaryIndex("keys", hash_key=email, dynamo_name="index_email")

.. seealso::

    `Global Secondary Indexes`__ in the DynamoDB Developer Guide

    __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html


---------------------
 LocalSecondaryIndex
---------------------

:class:`~bloop.models.LocalSecondaryIndex` is similar to :class:`~bloop.models.GlobalSecondaryIndex` in its use,
but has different requirements.  LSIs always have the same hash key as the model, and it can't be changed.  The model
must have a range key, and the LSI must specify a ``range_key``:

.. code-block:: python

    LocalSecondaryIndex("all", range_key=created_on)

You can specify a name to use in DynamoDB, just like :class:`~bloop.models.Column` and GSI:

.. code-block:: python

    class Impression(BaseModel):
        url = Column(String, hash_key=True)
        user_agent = Column(String, range_key=True, dynamo_name="ua")
        visited_at = Column(DateTime, dynamo_name="at")

        by_date = LocalSecondaryIndex(
        "keys", range_key=visited_at, dynamo_name="index_date")

The final optional parameter is ``strict``, which defaults to True.  This controls whether DynamoDB may incur
additional reads on the table when querying the LSI for columns outside the projection.  Bloop enforces this by
evaluating the key, filter, and projection conditions against the index's allowed columns and raises an exception
if it finds any non-projected columns.

It is recommended that you leave ``strict=True``, to prevent accidentally consuming twice as many read units with
an errant projection or filter condition.  Since this is local to Bloop and not part of the index definition in
DynamoDB, you can always disable and re-enable it in the future.

.. seealso::

    `Local Secondary Indexes`__ in the DynamoDB Developer Guide

    __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html

.. _user-models-inheritance:

========================
 Inheritance and Mixins
========================

Your models will often have identical constructs, especially when sharing a table.  Rather than define these repeatedly
in each model, Bloop provides the ability to derive Columns and Indexes from base classes.  Consider a set of models
that each has a UUID and sorts on a DateTime:

.. code-block:: python

    class HashRangeBase(BaseModel):
        id = Column(UUID, hash_key=True, dynamo_name="i")
        date = Column(DateTime, range_key=True, dynamo_name="d")

        class Meta:
            abstract = True


    class User(HashRangeBase):
        pass


    class Upload(HashRangeBase):
        class Meta:
            write_units = 50
            read_units = 10

Subclassing ``BaseModel`` is optional, and provides early validation against missing columns/indexes.  Mixins do not
need to be specified in any particular order:

.. code-block:: python

    class IndexedEmail:
        by_email = GlobalSecondaryIndex(projection="keys", hash_key="email")


    class WithEmail:
        email = Column(String)


    class User(BaseModel, IndexedEmail, WithEmail):
        id = Column(Integer, hash_key=True)


    assert User.by_email.hash_key is User.email  # True
    assert User.email is not WithEmail.email  # True

Even though the ``by_email`` Index requires the ``email`` Column to exist, it is first in the User's bases.

------------------------
 Modify Derived Columns
------------------------

Bloop uses the ``__copy__`` method to create shallow copies of the base Columns and Indexes.  You can override
this to modify derived Columns and Indexes:

.. code-block:: python

    class MyColumn(Column):
        def __copy__(self):
            copy = super().__copy__()
            copy.derived = True


    class WithEmail:
        email = MyColumn(String)


    class User(BaseModel, WithEmail):
        id = Column(String, hash_key=True)


    assert User.email.derived  # True
    assert not hasattr(WithEmail.email, "derived")  # True

----------------------------
 Conflicting Derived Values
----------------------------

A model cannot derive from two base models or mixins that define the same column or index, or that have an
overlapping ``dynamo_name``.  Consider the following mixins:

.. code-block:: python

    class Id:
        id = Column(String)

    class AlsoId:
        id = Column(String, dynamo_name="shared-id")

    class AnotherId:
        some_id = Column(String, dynamo_name="shared-id")


Each of the following are invalid, and will fail:

.. code-block:: python

    # Id, AlsoId have the same column name "id"
    class Invalid(BaseModel, Id, AlsoId):
        hash = Column(String, hash_key=True)

    # AlsoId, AnotherId have same column dynamo_name "shared-id"
    class AlsoInvalid(BaseModel, AlsoId, AnotherId):
        hash = Column(String, hash_key=True)

For simplicity, Bloop also disallows subclassing more than one model or mixin that defines a hash key, a range key,
or an Index (either by name or dynamo_name).

However, a derived class may always overwrite an inherited column or index.  The following is valid:

.. code-block:: python

    class SharedIds:
        hash = Column(String, hash_key=True)
        range = Column(Integer, range_key=True)


    class CustomHash(BaseModel, SharedIds):
        hash = Column(Integer, hash_key=True)


    assert CustomHash.hash.typedef is Integer  # True
    assert SharedIds.hash.typedef is String  # True  # mixin column is unchanged
    assert CustomHash.range.typedef is Integer  # Still inherited

This also allows you to hide or omit a derived column:

.. code-block:: python

    class SharedColumns:
        foo = Column(String)
        bar = Column(String)


    class MyModel(BaseModel, SharedColumns):
        id = Column(Integer, hash_key=True)

        foo = None


    assert MyModel.foo is None  # True
    assert MyModel.bar.typedef is String  # True
    assert {MyModel.id, MyModel.bar} == MyModel.Meta.columns  # True
