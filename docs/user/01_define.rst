.. _define-models:

Define Models
^^^^^^^^^^^^^

To start, you'll need to create a base class that your models inherit from:

.. code-block:: python

    from bloop import new_base

    BaseModel = new_base()

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

``BaseModel`` provides a kwarg-based ``__init__``, so we can create a new user with:

.. code-block:: python

    import arrow, uuid

    user = User(id=uuid.uuid4(),
                email="user@domain.com",
                created_on=arrow.now())

You need to specify a value for the hash and range -- if there is one -- keys before you can ``Load``, ``Save``, or
``Delete`` the object, but locally it's not required.  This is also ok:

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

**table_name**
    | *(default is class name)*
    | The table name in DynamoDB.
**read_units**
    | *(default is 1)*
    | The provisioned read units for the table.
**write_units**
    | *(default is 1)*
    | The provisioned write units for the table.
**abstract**
    | *(default is False)*
    | True if this model is not backed by a DynamoDB table.

Instances of abstract models can't be used with an Engine since there is no table to modify or query.  Their
columns and indexes are not inherited.

In the future, abstract models may be usable as mixins; subclasses could inherit their columns and indexes.

=======
Columns
=======

.. code-block:: python

    Column(typedef: [bloop.Type],
           hash_key: bool=False,
           range_key: bool=False,
           name: Optional[str]=None,
           **kwargs)

**typedef**
    | *(required)*
    | A type or instance of a type to use when loading and saving this column.
**hash_key**
    | *(default is False)*
    | True if this column is the model's hash key.
**range_key**
    | *(default is False)*
    | True if this column is the model's range key.
**name**
    | *(default is None)*
    |     If None, the column's model name is used.
    | The name this column is stored as in DynamoDB.

Each ``Column`` must have a type.  Many types can be passed directly without instantiating.  Sometimes, an
instance of a type can provide customization.  These are equivalent:

.. code-block:: python

    Column(DateTime)
    Column(DateTime(timezone="utc"))

DynamoDB includes column names when computing item sizes.  To save space, you'd usually set your attribute
name to ``c`` instead of ``created_on``.  The ``name`` kwarg allows you to map a readable model name to a
compact DynamoDB name:

.. code-block:: python

    created_on = Column(DateTime, name="c")

.. seealso::
    | :ref:`types` -- the built-in types and how to extend them
    | `Item Size`__ -- how item size is calculated

    __ docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-items-size

=======
Indexes
=======

.. code-block:: python

    GlobalSecondaryIndex(
        projection: Union[str, List[str]],
        hash_key: str,
        range_key: Optional[str]=None,
        name: Optional[str]=None,
        read_units: Optional[int]=1,
        write_units: Optional[int]=1)

    LocalSecondaryIndex(
        projection: Union[str, List[str]],
        range_key: str,
        name: Optional[str]=None)

**projection**
    | *(required)*
    | Columns in the index projection.  ``"all"``, ``"keys"``, or a list of column names.
**hash_key**
    | *(required for GSIs)*
    | The model name of the column that will be this index's hash key.
    | ``LocalSecondaryIndex`` always shares the model hash key.
**range_key**
    | *(required for LSIs)*
    | The model name of the column that will be this index's range key.
    | ``GlobalSecondaryIndex`` does not require a range key.
**name**
    | *(defaults is None)*
    |     If None, the index's model name is used.
    | The name this index is stored as in DynamoDB.
**read_units**
    | *(default is 1 for GSIs)*
    | The provisioned read capacity for reads against this index.
    | ``LocalSecondaryIndex`` shares the model's read units.
**write_units**
    | *(default is 1 for GSIs)*
    | The provisioned write capacity for writes through this index.
    | ``LocalSecondaryIndex`` shares the model's write units.

Specific column projections always include key columns.  A query against the following ``User`` index would
return objects that include all columns except ``created_on`` (since ``id`` and ``email`` are the model
and index hash keys).

.. code-block:: python

    by_email = GlobalSecondaryIndex(
            projection=["verified", "profile"],
            hash_key="email")

.. seealso::
    | The DynamoDB Developer Guide:
    |     `Global Secondary Indexes`__
    |     `Local Secondary Indexes`__

    __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
    __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html
