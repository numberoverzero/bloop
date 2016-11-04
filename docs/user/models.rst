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
to an existing table,or mapping multiple models to the same table:

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

See the :ref:`streams` section of the user guide to get started.  Streams are awesome.

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

-----------------------------------
 Developing Against Generic Models
-----------------------------------

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

.. code-block:: python

    Column(typedef, hash_key=False, range_key=False, name=None, **kwargs)

A :class:`~bloop.models.Column` determines how a value in DynamoDB maps to a local value.  The ``typedef`` is a
:class:`~bloop.types.Type` that performs the translation.  There are a number of built-in types for common data types,
such as :class:`~bloop.types.DateTime` and :class:`~bloop.types.UUID`.  See :ref:`user-types-custom` to easily create
your own types.

Models can only have one ``hash_key``, and at most one ``range_key``.  :exc:`~bloop.exceptions.InvalidModel` is raised
if you specify multiple hash keys or forget to specify a hash key.

``name`` is an optional string to store the values in DynamoDB.  This is useful if your model names are long, and you
want to use short names to save on throughput.  It also allows you to rename columns or map pythonic names to
existing DynamoDB column names.  In the following example, the column's :attr:`~bloop.models.Column.model_name` is
``verified_connections`` while it is stored in DynamoDB as ``vc``:

.. code-block:: python

    class SomeModel(BaseModel):
        ...
        verified_connections = Column(Set(String), name='vc')

=========
 Indexes
=========

Indexes provide additional ways to query and scan your data.  If you have not used indexes, you should first read
the Developer's Guide on `Improving Data Access with Secondary Indexes`__.

__ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html

----------------------
 GlobalSecondaryIndex
----------------------

.. code-block:: python

    GlobalSecondaryIndex(
        projection, hash_key, range_key=None,
        name=None, read_units=1, write_units=1)

Every :class:`~bloop.models.GlobalSecondaryIndex` must have a ``hash_key``.  A ``range_key`` is optional.

The ``projection`` can be "all", "keys", or a list of :class:`~bloop.models.Column` objects or model names.

Like Column and LocalSecondaryIndex, ``name`` is used to control the index's name in DynamoDB.  By default,
the index's ``model_name`` is used.

Each GSI has its own ``read_units`` and ``write_units``, independent of the model's throughput
(``model.Meta.read_units``, ``model.Meta.write_units``).

.. seealso::

    `Global Secondary Indexes`__ in the DynamoDB Developer Guide

    __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html


---------------------
 LocalSecondaryIndex
---------------------

.. code-block:: python

    LocalSecondaryIndex(projection, range_key, name=None, strict=True)

Every :class:`~bloop.models.LocalSecondaryIndex` must have a ``range_key``.  An LSI's ``hash_key`` is always the
model's (``model.Meta.hash_key``).

The ``projection`` can be "all", "keys", or a list of :class:`~bloop.models.Column` objects or model names.

Like Column and GlobalSecondaryIndex, ``name`` is used to control the index's name in DynamoDB.  By default,
the index's ``model_name`` is used.

DynamoDB allows you to access columns outside of an LSI's projection during queries and scans,
by consuming an additional read on the table to load those columns.  This can result in unexpected consumption from
a poorly formed query.  By default, Bloop will raise an exception if you try to filter or project columns outside
of an LSI's defined projection.  You can disable this and have DynamoDB incur extra reads automatically by setting
``strict=False``.

.. seealso::

    `Local Secondary Indexes`__ in the DynamoDB Developer Guide

    __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html
