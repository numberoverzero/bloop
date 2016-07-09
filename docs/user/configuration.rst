Configuration
^^^^^^^^^^^^^

Bloop tries to minimize the configuration required for the most common uses without restricting your ability to
swap out large portions of the engine, type system, and low-level clients.

Most of the knobs are tucked away so they won't bother you at first.  There are only three configuration
options for the Engine, and the built-in Types cover common uses.  As your models and types grow more complex, these
features will be easy to integrate with your existing code.

If necessary you can entirely drop the Engine and simply use the modeling structures.  Or replace the client and talk
to a different backend.  You can skip the base Type and implement view-specific serialization functions.  Subclass or
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

Used as the default value for the ``atomic`` kwarg of ``Engine.save`` and ``Engine.delete``.

When enabled, modifying operations on objects will construct a minimum condition (that is combined with any other
condition provided) which expects the current value in DynamoDB to match the last values loaded from or saved to
DynamoDB exactly.

For more information see the Atomic Operations section.

``consistent``
--------------

*Defaults to False*

Used as the default value for the ``consistent`` kwarg of ``Engine.load`` and the default consistent setting for
queries and scans.  Note that a GSI cannot be queried or scanned consistently.

When enabled, retrieving objects through load, query, and scan will be performed with `Strongly Consistent Reads`_.
Strongly consistent reads will generally be slower than the default eventually consistent reads.

.. _Strongly Consistent Reads: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html

``strict``
----------

*Defaults to True*

Used as the default value for the ``strict`` setting of queries and scans when selecting which columns to load.

This is bloop's only deviation from DynamoDB's default behavior.  When enabled, if a query or scan on an LSI tries to
select a superset of the projected columns for that index, bloop will raise a ``ValueError``.

When disabled, the same attempt to query a superset of projected columns **on an LSI only** will be allowed.

This is because DynamoDB will `automatically fetch`_ those attributes with an additional read against the table:

    If you query a local secondary index and request only attributes that are projected into that index, the operation
    will read only the index and not the table.  If any of the requested attributes are not projected into the local
    secondary index, DynamoDB will fetch each of these attributes from the parent table. This extra fetching incurs
    additional throughput cost and latency.

    If you query a global secondary index, you can only request attributes that are projected into the index. Global
    secondary index queries cannot fetch attributes from the parent table.

It is **strongly encouraged** to keep strict enabled so that your consumed throughput is predictable, and won't change
when a dynamic query adds a non-projected column.  It also makes the behavior consistent with queries against GSIs.

For more information see the Indexes and Projections section.

.. _`automatically fetch`: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-Select

Client
======

.. code-block:: python

    Client(boto_client=None, backoff_func=None)

The bloop client ``bloop.Client`` is a convenience layer between the Engine and the boto3 client which handles
batching, some pagination, and retries with backoffs.  Methods with the same name as their boto3 counterpart will often
have the same request format but may unpack the outer wrappers of the response.  For instance,
``Client.batch_get_items`` will return the value of the boto3 client's ``"Response"`` key, automatically chunks
requests with more than 25 items, follows ``UnprocessedKeys`` from responses, and combines paginated results into a
single dict.

Boto Client
-----------

Sometimes you will need to configure a ``boto3.session.Session`` before creating the client, instead of the default
path of using ``boto3.client("dynamodb")``.  For example, the :ref:`patterns-local` pattern demonstrates how to set up
a connection to a DynamoDB Local instance.

Retries
-------

By default, the bloop client will use an exponential backoff when a call raises a ``botocore.exceptions.ClientError``
with an error code of either ``"InternalServerError"`` or ``"ProvisionedThroughputExceededException"``.  The default
settings will try four times, with three backoffs: 100ms, 200ms, and 400ms.  If the call after 400ms of waiting fails,
then the client will raise the last error it encountered.

You may provide a custom backoff function that takes an integer representing the number of attempts made so far, and
returns the number of milliseconds to wait before trying the call again.  When you want to stop retrying the call,
perhaps hitting an upper limit of calls or time just raise a ``RuntimeError``.

Here's a backoff function that waits 2 seconds between calls and allows 10 total attempts (9 retries):

.. code-block:: python

    def constant_backoff(failed_attempts):
        if failed_attempts == 10:
            raise RuntimeError("Failed after 10 attempts")
        # Wait 2 seconds before retrying
        return 2000

Note that the ``failed_attempts`` parameter is the number of attempts so far: the first time it is called,
``failed_attempts`` will be 1.

Type Engine
===========

Shared Type Engine
------------------

Binding
-------
