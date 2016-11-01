.. _define-models:

Define Models
^^^^^^^^^^^^^

==================
A Basic Definition
==================

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

==================
Creating Instances
==================

BaseModel provides a basic ``__init__``:

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

========
Metadata
========

-------------------
Table Configuration
-------------------

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


Default ``read_units`` and ``write_units`` are 1.  These do not include provisioned throughput for any Global
Secondary Indexes, which have their own ``read_units`` and ``write_units`` attributes.

Finally, ``stream`` can be used to enable DynamoDBStreams on the table.  By default streaming is not enabled, and this
is ``None``.  To enable a stream with both new and old images, use:

.. code-block:: python

    class Meta:
        stream = {
            "include": ["new", "old"]
        }

See the :ref:`streams` section of the user guide to get started.  Streams are awesome.

-------------------
Model Introspection
-------------------

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
* ``gsis`` -- The set of all :class:`~bloop.models.GlobalSecondaryIndex`\es in the model
* ``lsis`` -- The set of all :class:`~bloop.models.LocalSecondaryIndex`\es in the model
* ``projection`` A pseudo-projection for the table, providing API parity with an Index

For example, a common pattern involves saving an item only if it doesn't exist.  Instead of creating a specific
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

Here's a basic model, and its generated ``Meta`` properties:

.. code-block:: python

    class DataBlob(BaseModel):
        id = Column(UUID, hash_key=True)
        version = Column(Integer, range_key=True)
        data = Column(Binary)
        email = Column(String)
        sourced_at = Column(DateTime)

        by_email = GlobalSecondaryIndex(
            projection="keys", hash_key=email)
        by_origin_date = LocalSecondaryIndex(
            projection="all", range_key="sourced_at")

.. code-block:: python

    >>> meta = DataBlob.Meta
    >>> meta.model
    <Model[DataBlob]>
    >>> meta.columns
    {<Column[DataBlob.id=hash]>,
     <Column[DataBlob.version=range]>,
     <Column[DataBlob.data]>,
     <Column[DataBlob.email]>,
     <Column[DataBlob.sourced_at]>}
    >>> meta.keys
    {<Column[DataBlob.id=hash]>, <Column[DataBlob.version=range]>}
    >>> meta.indexes
    {<LSI[DataBlob.by_origin_date=all]>, <GSI[DataBlob.by_email=keys]>}
    >>> meta.hash_key
    <Column[DataBlob.id=hash]>
    >>> meta.range_key
    <Column[DataBlob.version=range]>
    >>> meta.gsis
    {<GSI[DataBlob.by_email=keys]>}
    >>> meta.lsis
    {<LSI[DataBlob.by_origin_date=all]>}
    >>> meta.projection
    {'available': {<Column[DataBlob.id=hash]>, ...},
     'included': {<Column[DataBlob.id=hash]>, ...},
     'mode': 'all',
     'strict': True}

=======
Columns
=======

.. code-block:: python

    Column(typedef: bloop.Type,
           hash_key: bool=False,
           range_key: bool=False,
           name: Optional[str]=None,
           **kwargs)

.. _property-typedef:

.. attribute:: typedef
    :noindex:

    A type class or instance used to load and save this column.  If a class is provided, an instance will
    be created by calling the constructor without any arguments.  These will have the same result:

    .. code-block:: python

        data = Column(Binary)
        data = Column(Binary())

    Some types like ``Set`` require arguments.  See :ref:`types` for details.

.. attribute:: hash_key
    :noindex:

    True if this column is the model's hash key.  Defaults to False.

.. attribute:: range_key
    :noindex:

    True if this column is the model's range key.  Defaults to False.

.. _property-name:

.. attribute:: name
    :noindex:

    The name this column is stored as in DynamoDB.  Defaults to the column's name in the model.

    DynamoDB includes column names when computing item sizes.  To save space, you'd usually set your attribute
    name to ``c`` instead of ``created_on``.  The ``name`` kwarg allows you to map a readable model name to a
    compact DynamoDB name:

    .. code-block:: python

        created_on = Column(DateTime, name="c")

    See `Item Size`__ for the exact calculation.

    __ https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-items-size

=======
Indexes
=======

.. code-block:: python

    GlobalSecondaryIndex(
        projection: Union[str, List[str], List[Column]],
        hash_key: str,
        range_key: Optional[str]=None,
        name: Optional[str]=None,
        read_units: Optional[int]=1,
        write_units: Optional[int]=1)

    LocalSecondaryIndex(
        projection: Union[str, List[str], List[Column]],
        range_key: str,
        name: Optional[str]=None,
        strict: bool=True)

.. attribute:: projection
    :noindex:

    The columns to project into this Index.  The index and model hash and range keys are always included
    in the projection.  Must be one of ``"all"``, ``"keys"``, a list of Column objects, or a list of
    Column model names.

.. attribute:: hash_key
    :noindex:

    Required for GSIs.  The model name of the column that will be this index's hash key.
    You cannot specify the hash key for an LSI since it always shares the model's hash key.

.. attribute:: range_key
    :noindex:

    Required for LSIs.  Optional for GSIs.  The model name of the column that will be this index's range key.

.. attribute:: name
    :noindex:

    The name this index is stored as in DynamoDB.  Defaults to the index's name in the model.

    See the :ref:`name property <property-name>` above.

.. attribute:: read_units
    :noindex:

    The provisioned read units for the index.  LSIs share the model's read units.  Defaults to 1.

.. attribute:: write_units
    :noindex:

    The provisioned write units for the index.  LSIs share the model's write units.  Defaults to 1.

.. attribute:: strict
    :noindex:

    Whether or not queries and scans against the LSI will be allowed to access the full set of columns,
    even when they are not projected into the LSI.  When this is True, bloop will prevent you from making
    calls that incur additional reads against the table.  If you query or scan a Local Secondary Index
    that has ``strict=False`` and include columns in the projection or filter expressions that are not
    part of the LSI, DynamoDB will incur an additional read against the table in order to return all columns.

    It is highly recommended to keep this enabled.  Defaults to True.


Specific column projections always include key columns.  A query against the following ``User`` index would
return objects that include all columns except ``created_on`` (since ``id`` and ``email`` are the model
and index hash keys).

.. code-block:: python

    by_email = GlobalSecondaryIndex(
            projection=[User.verified, User.profile],
            hash_key="email")

.. seealso::
    | The DynamoDB Developer Guide:
    |     `Global Secondary Indexes`__
    |     `Local Secondary Indexes`__

    __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
    __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html
