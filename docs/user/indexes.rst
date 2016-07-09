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

        by_email = GlobalSecondaryIndex(hash_key="email")

.. _Global Secondary Indexes: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
.. _Local Secondary Indexes: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html

The index's ``hash_key`` and ``range_key`` attributes will be replaced with the corresponding column object for
the given string from the model once the class is defined.  For the example model above, the following are true:

.. code-block:: python

    User.by_email.hash_key is User.email
    User.by_email.range_key is None

This is intentionally identical to the convention for a model's hash and range keys:

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

    GlobalSecondaryIndex(
        hash_key=None, range_key=None,
        read_units=1, write_units=1,
        name=None, projection="keys")

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
**projection**
    | *(defaults to "keys")*
    | Which columns to project into the index.
    | This can be "keys", "all", or a list of column model names.
    | See :ref:`user-projections` below for more details.

Consistency
-----------

GSIs are eventually consistent, which means they can't be queried or scanned with strongly consistent reads.
Attempting to modify the consistent setting of a query or scan on a GSI (even if you're setting consistent to False)
will cause bloop to raise a ``ValueError``:

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
while write units are consumed whenever an item is saved to the table.

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
            hash_key="email", read_units=20, write_units=10)


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

The ``hash_key`` of a LSI will be set to the model's ``hash_key`` after class declaration.

.. code-block:: python

    LocalSecondaryIndex(range_key=None, name=None, projection="keys")

**range_key**
    | *(required)*
    | The model name of the column that will be this index's range key.
    | LSIs are required to have a range key.
**name**
    | *(defaults to model name)*
    | The name of this index in DynamoDB.
    | This is the same as the optional name for a Column.
**projection**
    | *(defaults to "keys")*
    | Which columns to project into the index.
    | This can be "keys", "all", or a list of column model names.
    | See :ref:`user-projections` below for more details.

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

Quick Example
-------------

All
---

Keys Only
---------

Specific Columns
----------------

Strict
------
