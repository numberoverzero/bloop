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

See :ref:`defining models <define-models>` in the User Guide.

---------
BaseModel
---------

.. autoclass:: bloop.models.BaseModel

    .. attribute:: Meta

        Holds table configuration and computed properties of the model.
        See :ref:`model meta <user-model-meta>` in the User Guide.

------
Column
------

.. autoclass:: bloop.models.Column
    :members:

    .. attribute:: dynamo_name

        The name of this column in DynamoDB.  Defaults to the column's ``model_name``.

    .. attribute:: hash_key

        True if this is the model's hash key.

    .. attribute:: model

        The model this column is attached to.

    .. attribute:: model_name

        The name of this column in the model.  Not settable.

        .. code-block:: pycon

            >>> class Document(BaseModel):
            ...     ...
            ...     cheat_codes = Column(Set(String), name="cc")
            ...
            >>> Document.cheat_codes.model_name
            cheat_codes
            >>> Document.cheat_codes.dynamo_name
            cc

    .. attribute:: range_key

        True if this is the model's range key.

--------------------
GlobalSecondaryIndex
--------------------

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

-------------------
LocalSecondaryIndex
-------------------

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

Most custom types only need to specify a backing_type (or subclass a built-in type) and override ``dynamo_dump`` and
``dynamo_load``:

.. code-block:: python

    class ReversedString(Type):
        python_type = str
        backing_type = "S"

        def dynamo_load(self, value, *, context, **kwargs):
            return str(value[::-1])

        def dynamo_dump(self, value, *, context, **kwargs):
            return str(value[::-1])

If a type's constructor doesn't have required args, a :class:`~bloop.Column` can use the class directly.  The column
will create an instance of the type by calling the constructor without any args:

.. code-block:: python

    class SomeModel(BaseModel):
        custom_hash_key = Column(ReversedString, hash_key=True)

In rare cases, complex types may need to implement :func:`~_dump`, :func:`~_load`, or :func:`~_register`.

----
Type
----

.. autoclass:: bloop.types.Type
    :members: dynamo_dump, dynamo_load, _dump, _load, _register
    :member-order: bysource

    .. attribute:: python_type

        The type local values will have.  Informational only, this is not used for validation.

    .. attribute:: backing_type

        The DynamoDB type that Bloop will store values as.

        One of:

        .. hlist::
            :columns: 3

            * ``"S"`` -- string
            * ``"N"`` -- number
            * ``"B"`` -- binary
            * ``"SS"`` -- string set
            * ``"NS"`` -- number set
            * ``"BS"`` -- binary set
            * ``"M"`` -- map
            * ``"L"`` -- list
            * ``"BOOL"`` -- boolean

        See the `DynamoDB API Reference`__ for details.

        __ http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html

------
String
------

.. autoclass:: bloop.types.String

    .. attribute:: backing_type
        :annotation: = "S"

    .. attribute:: python_type
        :annotation: = str

------
Number
------

.. autoclass:: bloop.types.Number

    Number uses a :class:`decimal.Context` to accurately send numbers to DynamoDB.
    The default context uses the stated limits in the `Developer Guide`__, which are taken from `boto3`__.

    .. seealso::

        If you don't want to deal with :class:`decimal.Decimal`\, see the
        :ref:`Float <patterns-float>` type in the patterns section.

    .. attribute:: backing_type
        :annotation: = "N"

    .. attribute:: python_type
        :annotation: = decimal.Decimal

    __ https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-data-types-numbers
    __ https://github.com/boto/boto3/blob/dffeb393a795204f375b951d791c768be6b1cb8c/boto3/dynamodb/types.py#L32

------
Binary
------

.. autoclass:: bloop.types.Binary

    .. attribute:: backing_type
        :annotation: = "B"

    .. attribute:: python_type
        :annotation: = bytes

-------
Boolean
-------

.. autoclass:: bloop.types.Boolean

    .. attribute:: backing_type
        :annotation: = "BOOL"

    .. attribute:: python_type
        :annotation: = bool

----
UUID
----

.. autoclass:: bloop.types.UUID

    .. attribute:: backing_type
        :annotation: = "S"

    .. attribute:: python_type
        :annotation: = uuid.UUID

--------
DateTime
--------

.. autoclass:: bloop.types.DateTime

    .. attribute:: backing_type
        :annotation: = "S"

    .. attribute:: python_type
        :annotation: = arrow.Arrow

-------
Integer
-------

.. autoclass:: bloop.types.Integer

    .. attribute:: backing_type
        :annotation: = "N"

    .. attribute:: python_type
        :annotation: = int

---
Set
---

.. autoclass:: bloop.types.Set

    .. attribute:: backing_type
        :annotation: = "SS", "NS", or "BS"

        Set is not a standalone type; its backing type depends on the inner type its constructor receives. For
        example, ``Set(DateTime)`` has backing type "SS" because :class:`~bloop.types.DateTime` has backing type "S".

    .. attribute:: python_type
        :annotation: = set

----
List
----

.. autoclass:: bloop.types.List

    .. attribute:: backing_type
        :annotation: = "L"

    .. attribute:: python_type
        :annotation: = list

---
Map
---

.. autoclass:: bloop.types.Map

    .. attribute:: backing_type
        :annotation: = "M"

    .. attribute:: python_type
        :annotation: = dict

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

