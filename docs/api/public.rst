.. _api-public:

Public API
^^^^^^^^^^

.. module:: bloop

======
Engine
======

By default, Bloop will build clients directly from :func:`boto3.client`.
To customize the engine's connection, you can provide your own DynamoDB and DynamoDBStreams clients:

.. code-block:: python

    import bloop
    import boto3

    dynamodb_local = boto3.client("dynamodb", endpoint_url="http://127.0.0.1:8000")
    streams_local = boto3.client("dynamodbstreams", endpoint_url="http://127.0.0.1:8001")

    engine = bloop.Engine(
        dynamodb=dynamodb_local,
        dynamodbstreams=streams_local)

.. autoclass:: bloop.engine.Engine
    :members:

======
Models
======

See the :ref:`defining models <define-models>` section of the User Guide to learn how to declare GSIs, LSIs, and
customize column bindings.

.. autoclass:: bloop.models.BaseModel
    :members:

.. autoclass:: bloop.models.Column
    :members:

.. autoclass:: bloop.models.GlobalSecondaryIndex

    .. attribute:: dynamo_name

        The name of this index in DynamoDB.  Defaults to the index's ``model_name``.

    .. attribute:: hash_key

        The column that the index can be queried against.

    .. attribute:: model

        The model this index is attached to.

    .. attribute:: model_name

        The name of this index in the model.  Not settable.

        .. code-block:: pycon

            >>> class Document(BaseModel):
            ...     ...
            ...     by_email = GlobalSecondaryIndex(
            ...         projection="keys", name="ind_e", hash_key="email")
            ...
            >>> Document.by_email.model_name
            by_email
            >>> Document.by_email.dynamo_name
            ind_e

    .. attribute:: projection

        .. code-block:: python

            {
                "available":  # Set of columns that can be returned from a query or search.
                "included":   # Set of columns that can be used in query and scan filters.
                "mode":       # "all", "keys", or "include"
                "strict":     # False if queries and scans can fetch non-included columns
            }

        GSIs can't incur extra reads, so "strict" will always be true and "available" is always the same as "included".

    .. attribute:: range_key

        The column that the index can be sorted on.  May be ``None``.

    .. attribute:: read_units

        Provisioned read units for the index.  GSIs have their own provisioned throughput.

    .. attribute:: write_units

        Provisioned write units for the index.  GSIs have their own provisioned throughput.


.. autoclass:: bloop.models.LocalSecondaryIndex

    .. attribute:: dynamo_name

        The name of this index in DynamoDB.  Defaults to the index's ``model_name``.

    .. attribute:: hash_key

        LSI's hash_key is always the table hash_key.

    .. attribute:: model

        The model this index is attached to.

    .. attribute:: model_name

        The name of this index in the model.  Not settable.

        .. code-block:: pycon

            >>> class Document(BaseModel):
            ...     ...
            ...     by_date = LocalSecondaryIndex(
            ...         projection="keys", name="ind_co", range_key="created_on")
            ...
            >>> Document.by_date.model_name
            by_date
            >>> Document.by_date.dynamo_name
            ind_co

    .. attribute:: projection

        .. code-block:: python

            {
                "available":  # Set of columns that can be returned from a query or search.
                "included":   # Set of columns that can be used in query and scan filters.
                "mode":       # "all", "keys", or "include"
                "strict":     # False if queries and scans can fetch non-included columns
            }

        LSIs can incur extra reads, so "available" may be a superset of "included".

    .. attribute:: range_key

        The column that the index can be sorted on.  LSIs always have a range_key.

    .. attribute:: read_units

        Provisioned read units for the index.  LSIs share the table's provisioned throughput.

    .. attribute:: write_units

        Provisioned write units for the index.  LSIs share the table's provisioned throughput.

=====
Types
=====

.. autoclass:: bloop.types.Type
    :members: python_type, backing_type, dynamo_dump, dynamo_load, _dump, _load, _register
    :member-order: bysource

.. autoclass:: bloop.types.String

.. autoclass:: bloop.types.Float

.. autoclass:: bloop.types.Binary

.. autoclass:: bloop.types.Boolean

.. autoclass:: bloop.types.UUID

.. autoclass:: bloop.types.DateTime

.. autoclass:: bloop.types.Integer

.. autoclass:: bloop.types.Set

.. autoclass:: bloop.types.List

.. autoclass:: bloop.types.Map

=====
Query
=====

.. autoclass:: bloop.search.QueryIterator
    :members:

====
Scan
====

.. autoclass:: bloop.search.ScanIterator
    :members:

======
Stream
======

:func:`Engine.stream() <bloop.engine.Engine.stream>` is the recommended way to create a stream.
If you manually create a stream, you will need to call :func:`~bloop.stream.Stream.move_to` before iterating the
Stream.

.. warning::

    **Chronological order is not guaranteed for high throughput streams.**

    DynamoDB guarantees ordering:

    * within any single shard
    * across shards for a single hash/range key

    There is no way to exactly order records from adjacent shards.  High throughput streams
    provide approximate ordering using each record's "ApproximateCreationDateTime".

    Tables with a single partition guarantee order across all records.

    See :ref:`Stream Internals <internal-streams>` for details.

.. autoclass:: bloop.stream.Stream
    :members:

==========
Conditions
==========

The only public class the conditions system exposes is the empty condition, :class:`~.conditions.Condition`.
The rest of the conditions system is baked into :class:`~.models.Column` and consumed by the various
:class:`~.engine.Engine` functions like :func:`Engine.save() <bloop.engine.Engine.save>`.

This function creates a condition for any model that can be used when saving to ensure you don't overwrite an existing
value.  The model's ``Meta`` attribute describes the required keys:

.. code-block:: python

    from bloop import Condition

    def ensure_unique(model):
        condition = Condition()
        for key in model.Meta.keys:
            condition &= key.is_(None)
        return condition

.. seealso::

    :ref:`conditions` in the :ref:`guide-index` describes the possible conditions, and when and how to use them.

.. autoclass:: bloop.conditions.Condition

.. _public-signals:

=======
Signals
=======

.. autodata:: bloop.signals.before_create_table
    :annotation:

.. autodata:: bloop.signals.object_loaded
    :annotation:

.. autodata:: bloop.signals.object_saved
    :annotation:

.. autodata:: bloop.signals.object_deleted
    :annotation:

.. autodata:: bloop.signals.object_modified
    :annotation:

.. autodata:: bloop.signals.model_bound
    :annotation:

.. autodata:: bloop.signals.model_created
    :annotation:

.. autodata:: bloop.signals.model_validated
    :annotation:

==========
Exceptions
==========

.. module:: bloop.exceptions

Except to configure sessions, Bloop aims to completely abstract the boto3/botocore layers.  If you encounter an
exception from either boto3 or botocore, please `open an issue`__.  Bloop's exceptions are broadly divided into two
categories: unexpected state, and invalid input.

To catch any exception from Bloop, use :exc:`~.BloopException`:

.. code-block:: python

    try:
        engine.stream(User, "latest")
    except BloopException:
        print("Didn't expect an exception, but Bloop raised:")
        raise

.. autoclass:: BloopException

__ https://github.com/numberoverzero/bloop/issues/new

----------------
Unexpected state
----------------

These are exceptions that you should be ready to handle in the normal course of using DynamoDB.  For example,
failing to load objects will raise :exc:`~.MissingObjects`, while conditional operations may fail with
:exc`~.ConstraintViolation`.

.. autoclass:: ConstraintViolation

.. autoclass:: MissingObjects

.. autoclass:: RecordsExpired

.. autoclass:: ShardIteratorExpired

.. autoclass:: TableMismatch

---------
Bad Input
---------

These are thrown when an option is invalid or missing, such as forgetting a key condition for a query,
or trying to use an unknown projection type.

.. autoclass:: InvalidComparisonOperator

.. autoclass:: InvalidCondition

.. autoclass:: InvalidFilterCondition

.. autoclass:: InvalidIndex

.. autoclass:: InvalidKeyCondition

.. autoclass:: InvalidModel

.. autoclass:: InvalidPosition

.. autoclass:: InvalidProjection

.. autoclass:: InvalidSearchMode

.. autoclass:: InvalidShardIterator

.. autoclass:: InvalidStream

.. autoclass:: MissingKey

.. autoclass:: UnboundModel

.. autoclass:: UnknownType

