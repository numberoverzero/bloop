Indexes and Projections
^^^^^^^^^^^^^^^^^^^^^^^

How and when to use indexes is outside the scope of this section.  You should review the `Global Secondary Indexes`_
and `Local Secondary Indexes`_ sections of the developer guide before adding indexes to your models.

Creating an index on a model is similar to creating a column on a model:

.. code-block:: python

    class User(bloop.new_base()):
        name = Column(String, hash_key=True)
        age = Column(Integer)
        email = Column(String, name="e")

        by_email = GlobalSecondaryIndex(
            hash_key="email", projection="keys")

.. _Global Secondary Indexes: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
.. _Local Secondary Indexes: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html

The index's ``hash_key`` and ``range_key`` attributes will be replaced with the corresponding column object for
the given string from the model once the class is defined.  For the example model above, this means:

.. code-block:: python

    User.by_email.hash_key is User.email
    User.by_email.range_key is None

This is the same pattern used for a model's hash and range keys:

.. code-block:: python

    Model.by_index.hash_key
    Model.by_index.range_key

    Model.Meta.hash_key
    Model.Meta.range_key

In the case of a Local Secondary Index the ``hash_key`` attribute will be populated from the ``Model.Meta.hash_key``,
since every LSI shares its hash key with the table.

Global Secondary Index
======================

.. code-block:: python

    GlobalSecondaryIndex(*,
        projection,
        hash_key, range_key=None,
        read_units=1, write_units=1,
        name=None)

**projection**
    | *(required)*
    | Which columns to project into the index.
    | This can be "all", "keys", or a list of column names.
    | See :ref:`user-projections` below for more details.
**hash_key**
    | *(required)*
    | The model name of the column that will be this index's hash key.
**range_key**
    | *(defaults to None)*
    | The model name of the column that will be this index's range key.
    | GSIs are not required to have a range key.
**read_units**
    | *(defaults to 1)*
    | The provisioned read capacity for reads against this index.
**write_units**
    | *(defaults to 1)*
    | The provisioned write capacity for writes through this index.
**name**
    | *(defaults to model name)*
    | The name of this index in DynamoDB.
    | This is the same as the optional name for a Column.

Consistency
-----------

GSIs are eventually consistent, which means they can't be queried or scanned with strongly consistent reads.
Attempting to modify the consistent setting of a query or scan on a GSI (even if you're setting it to False)
will cause a ``ValueError``:

.. code-block:: python

    q = engine.query(MyModel.by_some_gsi)

    # ValueError
    q.consistent(True)
    # ValueError
    q.consistent(False)

Provisioned Throughput
----------------------

Each GSI has its own provisioned throughput, independent of the table's provisioned throughput.
Read units are consumed whenever you query or scan the index,
while write units are consumed whenever an item is created or modified in the table.

The following shows a simple model with a table throughput of 1000 read units and 200 write units,
and a GSI with 20 read units and 10 write units:

.. code-block:: python

    class User(bloop.new_base()):
        class Meta:
            read_units = 1000
            write_units = 200

        id = Column(String, hash_key=True)
        data = Column(Binary)
        email = Column(String)

        by_email = GlobalSecondaryIndex(
            hash_key="email", projection="keys",
            read_units=20, write_units=10)


DynamoDB's developer guide has lots of information about planning the `Provisioned Throughput`_ for your GSI.
Specifically, you should consider the following note from the `Write Capacity`_ section:

    In order for a table write to succeed, the provisioned throughput settings for the table and all of its global
    secondary indexes must have enough write capacity to accommodate the write; otherwise, the write to the table
    will be throttled. Even if no data needs to be written to a particular global secondary index, the table write
    will be throttled if that index has insufficient write capacity.

.. _Provisioned Throughput: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html#GSI.ThroughputConsiderations
.. _Write Capacity: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html#GSI.ThroughputConsiderations.Writes

Local Secondary Index
=====================

To use an LSI, your model must specify a range key.  Because an LSI is an additional range key on the
table, it uses the same hash key as the table and shares the table's read and write units.

The ``hash_key`` of an LSI will be set to the model's ``hash_key`` after class declaration.

.. code-block:: python

    LocalSecondaryIndex(*, projection, range_key, name=None)

**projection**
    | *(required)*
    | Which columns to project into the index.
    | This can be "all", "keys", or a list of column names.
    | See :ref:`user-projections` below for more details.
**range_key**
    | *(required)*
    | The model name of the column that will be this index's range key.
    | LSIs are required to have a range key.
**name**
    | *(defaults to model name)*
    | The name of this index in DynamoDB.
    | This is the same as the optional name for a Column.

Consistency
-----------

Unlike a GSI, an LSI can be queried with strongly consistent reads.  The consistency option can be set in three places:

* ``engine.config["consistent"] = False``
* ``query = engine.query(Model, consistent=True)``
* ``query = query.consistent(False)``

For more information on query and scan options, see :ref:`user-building-queries`.

.. _user-projections:

Projections
===========

The index's ``projection`` kwarg determines which columns can be loaded when querying or scanning a Secondary Index.

This can be one of ``"all"``, ``"keys"``, or a list of column names to project.  Regardless of which projection you
use, the index's projection will always include the table's hash and range keys, and the index's hash and range keys.

When you create a Query or Scan against an index, only the columns in the projection will be loaded [1]_.  You may
also use ``query.select(...)`` to specify a subset of the projected columns.  Only this subset and the key columns
will be included in the query or scan objects.

The select option is covered in more detail in :ref:`Building Queries: Select <user-building-query-select>`.

.. [1] Except when the index is an LSI and the ``strict`` setting has been disabled. See
       :ref:`Building Queries: Strict <user-building-query-strict>`

"all"
-----

This will include all columns in the index.  Compared to "keys" or a list of columns, this projection is
usually much slower, consumes more provisioned throughput, and nearly doubles storage costs.  Unless necessary,
you should avoid projecting all columns into the index.

If your queries against the index would sometimes require loading all of the columns for objects in the query,
it may be more cost-effective to only project the keys and then perform a load on the objects from the query.

.. code-block:: python

    class User(bloop.new_base()):
        name = Column(String, hash_key=True)
        age = Column(Integer)

        email = Column(String, name="e")
        profile = Column(String)
        verified = Column(Boolean)

        by_email = GlobalSecondaryIndex(
            hash_key="email", projection="all")

``by_email`` will project the columns ``name``, ``age``, ``email``, ``profile``, and ``verified``.

"keys"
------

This will include the hash and range keys of the table, and the hash and range keys of the index.  This is the
smallest, fastest projection that an index can have.  Where possible, you should try to use key-only projections.


.. code-block:: python

    class User(bloop.new_base()):
        name = Column(String, hash_key=True)
        age = Column(Integer, range_key=True)

        email = Column(String, name="e")
        profile = Column(String)
        verified = Column(Boolean)

        by_email = GlobalSecondaryIndex(
            hash_key="email", projection="keys")

``by_email`` will project the columns ``name`` ``age``, and ``email``.

Specific Columns
----------------

This option is a list of strings, where each string is the model name [2]_ of a column in the model.  Projecting
specific columns will land somewhere between "keys" (minimum projection) and "all" (maximum projection).

It will always include at least the table and index keys, even if they are not specified.

An empty list is equivalent to using "keys", and a list of every column name is equivalent to "all".

.. code-block:: python

    class User(bloop.new_base()):
        name = Column(String, hash_key=True)
        age = Column(Integer, range_key=True)

        email = Column(String, name="e")
        profile = Column(String, name="p")
        verified = Column(Boolean)

        by_email = GlobalSecondaryIndex(
            hash_key="email",
            projection=["profile"])

``by_email`` will project the columns ``name``, ``age``, and ``profile``.
The first two are included because they are the table keys, while ``profile`` was explicitly listed.

This is equivalent to using "keys":

.. code-block:: python

    # Only pulls the table + index key columns
    GlobalSecondaryIndex(hash_key="email", projection=[])

These are equivalent to using "all":

.. code-block:: python

    # Only mention the non-key columns
    GlobalSecondaryIndex(hash_key="email",
        projection=["verified", "profile"])

    # Explicitly include the index hash key column
    GlobalSecondaryIndex(hash_key="email",
        projection=["email", "verified", "profile"])

    # Explicitly include the table and index key columns
    GlobalSecondaryIndex(hash_key="email",
        projection=[
            "name", "age",
            "email",
            "verified", "profile"])

.. [2] The model name may be distinct from the DynamoDB name.  For example, the following has the model name "age"
       and the DynamoDB name "a":

       .. code-block:: python

           age = Column(String, name="a")

       See :ref:`user-modeling-names` for more info.
