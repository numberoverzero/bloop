Retrieving Objects
^^^^^^^^^^^^^^^^^^

Load
====

Quick Example
-------------

Consistent
----------

Missing Columns
---------------

Internals
---------

.. _user-building-queries:

Building Queries
================

Quick Example
-------------

Indexes
-------

Consistent
----------

Forward
-------

.. _retrieving-query-limit:

Limit
-----

Prefetch
--------

Key Condition
-------------

Filter Conditions
-----------------

.. _user-building-query-strict:

Strict
------

.. _user-building-query-select:

Select
------

TODO copied from another section's draft

This means that any columns can be selected with the ``"all"`` projection; only key columns can be selected with the
``"keys"`` projection; and you can only select a subset of the columns listed in a specific projection.  Trying to
select columns that are not projected will result in a ``ValidationError``.

For example, consider the following model:

.. code-block:: python

    class DataChunk(bloop.new_base()):
        id = Column(String, hash_key=True)
        next_chunk_id = Column(String)

        legacy_id = Column(String)

        data = Column(Binary)

        size = Column(String)
        sha256 = Column(String)
        modified = Column(DateTime)

        by_legacy_id = GlobalSecondaryIndex(
            hash_key="legacy_id",
            projection=["data", "size", "next_chunk_id"],
            read_units=1000,
            write_units=10)

The GSI exists because some application still uses the legacy id scheme, and needs to be able to return the object
by reconstructing the chunks that make it up.

That means for most queries on this index, the application loads a large blob that is the ``data`` column.  Assume
``next_chunk_id`` points to the next chunk, so it provides a way to order those chunks.  The object could be rebuilt
as follows:

.. code-block:: python

    q = Engine.query(DataChunk.by_legacy_id) \
              .key(DataChunk.legacy_id == "0001") \
              .build()
    chunks = sorted(q, key=lambda chunk: chunk.next_chunk_id)
    obj = "".join(chunk.data for chunk in chunks)

Because each object's ``data`` attribute is large, a query on the full index projection will take a long time to load.

If some new feature on the application requires showing the object's size before it's downloaded, we really don't want
to have to pull the full ``data`` attribute; it will significantly increase latency to render the size for an object
that we may not need to reconstruct, and will consume significantly more read units.  By selecting the ``size``
column in the query, we avoid loading ``data``, even though it's projected into the index:

.. code-block:: python

    q = Engine.query(DataChunk.by_legacy_id) \
              .key(DataChunk.legacy_id == "0001") \
              .select(DataChunk.size) \
              .build()
    size = sum(chunk.size for chunk in q)

Executing Queries
=================

``one``
-------

``first``
---------

``build``
---------

Repeating Queries
-----------------

Limit vs Prefetch
-----------------

Count vs Scanned Count
----------------------

Scan
====

Quick Example
-------------

Forward
-------

Key Condition
-------------
