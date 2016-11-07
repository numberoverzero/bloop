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

    >>> import arrow, uuid
    >>> user = User(
    ...     id=uuid.uuid4(),
    ...     version="1",
    ...     email="user@domain.com",
    ...     created_at=arrow.now())
    >>> user.email
    'user@domain.com'
    >>> user
    User(created_on=<Arrow [2016-10-29T22:08:08.930137-07:00]>, ...)

A local object's hash and range keys don't need values until you're ready to interact with DynamoDB:

.. code-block:: pycon

    >>> user = User(email="u@d.com", version="1")
    >>> engine.save(user)
    MissingKey: User(email='u@d.com') is missing hash_key: 'id'
    >>> user.id = uuid.uuid4()
    >>> engine.save(user)

.. _user-model-meta:

==========
 Metadata
==========

---------------------
 Table Configuration
---------------------

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
            read_units = 1
            write_units = 1
            stream = None

If ``abstract`` is true, no backing table will be created in DynamoDB.  Instances of abstract models can't be saved
or loaded.  Currently, abstract models and inheritance don't mix.  `In the future`__, abstract models
may be usable as mixins.

__ https://github.com/numberoverzero/bloop/issues/72

The default ``table_name`` is simply the model's ``__name__``.  This is useful for mapping a model
to an existing table, or mapping multiple models to the same table:

.. code-block:: python

    class Employee(BaseModel):
        class Meta:
            table_name = "employees-uk"
        ...


Default ``read_units`` and ``write_units`` are 1.  These do not include provisioned throughput for any
:class:`~bloop.models.GlobalSecondaryIndex`, which have their own
:attr:`~bloop.models.GlobalSecondaryIndex.read_units`` and :attr:`~bloop.models.GlobalSecondaryIndex.write_units``.

Finally, ``stream`` can be used to enable DynamoDBStreams on the table.  By default streaming is not enabled, and this
is ``None``.  To enable a stream with both new and old images, use:

.. code-block:: python

    class Meta:
        stream = {
            "include": ["new", "old"]
        }

See the :ref:`user-streams` section of the user guide to get started.  Streams are awesome.

---------------------
 Model Introspection
---------------------

When a new model is created, a number of attributes are computed and stored in ``Meta``.  These can be used to
generalize conditions for any model, or find columns by their name in DynamoDB.

These top-level properties can be used to describe the model in broad terms:

* ``model`` -- The model this Meta is attached to
* ``columns`` -- The set of all columns in the model
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

----------------------
 Using Generic Models
----------------------

A common pattern involves saving an item only if it doesn't exist.  Instead of creating a specific
condition for every model, we can use ``keys`` to make a function for any model:

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

(This is also available in the :ref:`patterns section <patterns-if-not-exist>` of the user guide).

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

By default values will be stored in DynamoDB under the name of the column in the model definition (its ``model_name``).
If you want to conserve read and write units, you can use shorter names for attributes in DynamoDB (attribute names
are counted against your provisioned throughput).  Like the ``table_name`` in Meta, the optional ``name`` parameter
lets you use descriptive model names without binding you to those names in DynamoDB.  This is also convenient when
mapping an existing table, or multi-model tables where an attribute can be interpreted multiple ways.

The following model is identical to the one just defined, except that each attribute is stored using a short name:

.. code-block:: python

    class Impression(BaseModel):
        referrer = Column(String, hash_key=True, name="ref")
        version = Column(Integer, range_key=True, name="v")

Locally, the model names "referrer" and "version" are still used.  An instance would be constructed as usual:

.. code-block:: python

    >>> click = Impression(
    ...     referrer="google.com",
    ...     version=get_current_version())
    >>> engine.save(click)

=========
 Indexes
=========

Indexes provide additional ways to query and scan your data.  If you have not used indexes, you should first read
the Developer's Guide on `Improving Data Access with Secondary Indexes`__.

__ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html

----------------------
 GlobalSecondaryIndex
----------------------

Every :class:`~bloop.models.GlobalSecondaryIndex` must declare a ``projection``, which describes the columns projected
into the index.  Only projected columns are loaded from queries and scans on the index, and non-projected columns
can't be used in filter expressions.  A projection can be ``"all"`` for all columns in the model; ``"keys"`` for the
hash and range columns of the model and the index; or a list of :class:`~bloop.models.Column` objects or their model
names.  If you specify a list of columns, key columns will always be included.

.. code-block:: python

    class HeavilyIndexed(BaseModel):
        ...
        by_email = GlobalSecondaryIndex("all", hash_key="email")
        by_username = GlobalSecondaryIndex("keys", hash_key="username")
        by_create_date = GlobalSecondaryIndex(
            ["email", "username"], hash_key="created_on")

A GlobalSecondaryIndex must have a ``hash_key``, and can optionall have a ``range_key``.  This can either be the
model_name of a column, or the column object itself:

.. code-block:: python

    class Impression(BaseModel):
        id = Column(UUID, hash_key=True)
        referrer = Column(String)
        version = Column(Integer)
        created_on = Column(DateTime)

        by_referrer = GlobalSecondaryIndex("all", hash_key=referrer)
        by_version = GlobalSecondaryIndex("keys", hash_key="version")

Unlike :class:`~bloop.models.LocalSecondaryIndex`, a GSI does not share its throughput with the table.  You can
specify the ``read_units`` and ``write_units`` of the GSI.  Both default to 1:

.. code-block:: python

    GlobalSecondaryIndex("all", hash_key=version, read_units=500, write_units=20)

As with :class:`~bloop.models.Column` you can provide a ``name`` for the GSI in DynamoDB.  This can be used to map
to an existing index while still using a pythonic model name locally:

.. code-block:: python

    class Impression(BaseModel):
        ...
        by_email = GlobalSecondaryIndex("keys", hash_key=email, name="index_email")

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
        user_agent = Column(String, range_key=True, name="ua")
        visited_at = Column(DateTime, name="at")

        by_date = LocalSecondaryIndex(
        "keys", range_key=visited_at, name="index_date")

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
