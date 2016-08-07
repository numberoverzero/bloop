Defining Models
^^^^^^^^^^^^^^^

To start, you'll need to create a base class that your models inherit from:

.. code-block:: python

    from bloop import new_base

    BaseModel = new_base()

Add some columns to your model.  You'll need at least a hash key:

.. code-block:: python

    from bloop import Column, DateTime, String, UUID


    class User(BaseModel):
        id = Column(UUID, hash_key=True)
        email = Column(String)
        created_on = Column(DateTime)

Finally, bind the model to an Engine to create the table:

.. code-block:: python

    from bloop import Engine

    engine = Engine()
    engine.bind(User)

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

=======
Columns
=======

.. code-block:: python

    Column(typedef, hash_key=False, range_key=False, name=None, **kwargs)

**hash_key**

-----
Types
-----

Each ``Column`` must have a type.  The built-in types are::

    String
    UUID
    DateTime
    Float
    Integer
    Binary
    Boolean
    Set
    List
    TypedMap
    Map

Many types can be passed directly without instantiating.  These are equivalent:

.. code-block:: python

    Column(String)
    Column(String())

    Column(DateTime)
    Column(DateTime(timezone="utc"))

    Column(Float)
    Column(Float())

Set, List, and TypedMap require an inner type.  Bloop requires type information for List and Map because there isn't
enough type information when loading values from DynamoDB to determine the type to use.

.. code-block:: python

    Column(Set(DateTime))
    Column(Set(Integer))
    Column(Set(Binary))

    Column(List(Boolean))

    Column(TypedMap(Integer))

-----------------
Hash & Range Keys
-----------------

Every model needs a hash key; you may optionally include a range key.  These are specified with the ``hash_key`` and
``range_key`` kwargs:

.. code-block:: python

    id = Column(Integer, hash_key=True)

-----
Names
-----

DynamoDB includes column names when computing item sizes.  Instead of shortening ``created_on`` to ``c`` in the model,
bloop exposes a ``name`` kwarg to map the model name to a different DynamoDB name:

.. code-block:: python

    created_on = Column(DateTime, name="c")
