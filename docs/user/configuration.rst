Configuration
^^^^^^^^^^^^^

Bloop tries to minimize the configuration required for the most common uses without restricting your ability to
swap out large portions of the engine, type system, and low-level clients.

Most of the knobs are tucked away so at first, they won't bother you.  There are only three configuration
options for the Engine, and the built-in Types cover common uses.

However, you can entirely drop the Engine and simply use the modeling structures.  Or replace the client and talk to
a different backend.  You can skip the base Type and implement view-specific serialization functions.  Subclass or
replace the Column class to add init kwargs like ``nullable=``.

Engine Configuration
====================

These options are exposed through the dict ``Engine.config``.  The default config options are:

.. code-block:: python

    {
        "atomic": False,
        "consistent": False,
        "strict": True
    }

These can be passed when creating the engine, or updated after creating it:

.. code-block:: python

    engine = bloop.Engine(atomic=True, consistent=True)
    engine.config["strict"] = False

Because the engine is passed through the type system when loading and dumping values, you can access its config from
custom types:

.. code-block:: python

    class MyType(bloop.Type):
        ...

        def dynamo_load(self, value, *, context, **kwargs):
            config = context["engine"].config
            if config["language"] == "en":
                return value
            else:
                return i18n(value, config["language"])

``atomic``
----------

*Defaults to False*

Used as the default value for the ``atomic=`` kwarg of ``Engine.save`` and ``Engine.delete``.

When enabled, modifying operations on objects will construct a minimum condition (that is combined with any other
condition provided) which expects the current value in DynamoDB to match the last values loaded from or saved to
DynamoDB exactly.

For more information see the Atomic Operations section.

``consistent``
--------------

*Defaults to False*

Used as the default value for the ``consistent=`` kwarg of ``Engine.load`` and the default consistent setting for
queries and scans.  Note that a GSI cannot be queried or scanned consistently.

When enabled, retrieving objects through load, query, and scan will be performed with `Strongly Consistent Reads`_.
Strongly consistent reads will generally be slower than the default eventually consistent reads.

.. _Strongly Consistent Reads: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html

``strict``
----------

*Defaults to True*

Used as the default value for the ``strict`` setting of queries and scans when selecting which columns to load.

This is bloop's only deviation from DynamoDB's default behavior.

When enabled, if a query or scan on an LSI tries to select a superset of the projected columns for that index, bloop
will raise a ``ValueError``.

When strict is False, the same attempt to query a superset of projected columns **on an LSI only** will be allowed.
This is because DynamoDB will `automatically fetch`_ those attributes with an additional read against the table:

    If you query a local secondary index and request only attributes that are projected into that index, the operation
    will read only the index and not the table.  If any of the requested attributes are not projected into the local
    secondary index, DynamoDB will fetch each of these attributes from the parent table. This extra fetching incurs
    additional throughput cost and latency.

    If you query a global secondary index, you can only request attributes that are projected into the index. Global
    secondary index queries cannot fetch attributes from the parent table.

It is **strongly encouraged** to keep ``strict=True`` so that your consumed throughput is predictable, and won't change
when a dynamic query adds a non-projected column.  It also makes the behavior consistent with queries against GSIs.

For more information see the Indexes and Projections section.

.. _`automatically fetch`: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-Select

Client
======

Boto Client
-----------

Backoffs
--------

Batch Size
----------


Type Engine
===========

Shared Type Engine
------------------

Binding
-------
