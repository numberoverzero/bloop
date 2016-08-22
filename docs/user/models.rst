..

.. _define-models:

Define Models
^^^^^^^^^^^^^

Every model inherits from ``BaseModel``:

.. code-block:: python

    from bloop import BaseModel

    class User(BaseModel):
        pass

Add some Columns to your model.  You'll need at least a hash key:

.. code-block:: python

    from bloop import Boolean, Column, DateTime, String, UUID

    class User(BaseModel):
        id = Column(UUID, hash_key=True)
        email = Column(String)
        created_on = Column(DateTime)
        verified = Column(Boolean)
        profile = Column(String)

Add an Index:

.. code-block:: python

    from bloop import GlobalSecondaryIndex

    class User(BaseModel):
        ...

        by_email = GlobalSecondaryIndex(
            projection="keys", hash_key="email")

Finally, create the table in DynamoDB:

.. code-block:: python

    from bloop import Engine

    engine = Engine()
    engine.bind(BaseModel)

The Engine will bind any subclasses (recursively) of the class passed in.  If your models share the same
base model, you can create all the tables with one call.

Bind will create tables that don't exist; if a table already exists, bind diffs the actual schema against the
one our model expects.  Any mismatch will  cause bind to fail.

==================
Creating Instances
==================

``BaseModel`` provides a kwarg-based ``__init__``, so you can create a new user with:

.. code-block:: python

    import arrow, uuid

    user = User(id=uuid.uuid4(),
                email="user@domain.com",
                created_on=arrow.now())

You need to specify a value for the hash -- and range, if there is one -- keys before you can mutate the object in
DynamoDB, but locally it's not required.  For local use, this is fine:

.. code-block:: python

    user = User()

========
Metadata
========

You can customize how the table is created with an inner ``Meta`` class:

.. code-block:: python

    class Tweet(BaseModel):
        class Meta:
            table_name = "custom_table_name"
            read_units = 1000
            write_units = 300

        user = Column(Integer, hash_key=True)
        created = Column(DateTime, range_key=True)

Available properties:

.. attribute:: Meta.table_name

    The table name for this model in DynamoDB.  Defaults to the class name.

.. attribute:: Meta.read_units

    The provisioned read units for the table.  Defaults to 1.

.. attribute:: Meta.write_units

    The provisioned write units for the table.  Defaults to 1.

.. attribute:: Meta.abstract

    True if this model is not backed by a DynamoDB table.  Defaults to False.

Instances of abstract models can't be used with an Engine since there is no table to modify or query.  Their
columns and indexes are not inherited.

In the future, abstract models may be usable as mixins; subclasses could inherit their columns and indexes.

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
