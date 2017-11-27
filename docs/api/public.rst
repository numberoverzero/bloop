.. _api-public:

Public
^^^^^^

========
 Engine
========

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

========
 Models
========

See :ref:`defining models <define-models>` in the User Guide.

-----------
 BaseModel
-----------

.. autoclass:: bloop.models.BaseModel

    .. attribute:: Meta

        Holds table configuration and computed properties of the model.
        See :ref:`model meta <user-model-meta>` in the User Guide.

--------
 Column
--------

.. autoclass:: bloop.models.Column
    :members: __copy__
    :undoc-members:
    :special-members: __copy__

    .. attribute:: default

        A no-arg function used during instantiation of the column's
        model.  Returns ``bloop.util.missing`` when the column does
        not have a default.  Defaults to ``lambda: bloop.util.missing``.

    .. attribute:: dynamo_name

        The name of this column in DynamoDB.  Defaults to the column's
        :data:`~Column.name`.

    .. attribute:: hash_key

        True if this is the model's hash key.

    .. attribute:: model

        The model this column is attached to.

    .. attribute:: name

        The name of this column in the model.  Not settable.

        .. code-block:: pycon

            >>> class Document(BaseModel):
            ...     ...
            ...     cheat_codes = Column(Set(String), name="cc")
            ...
            >>> Document.cheat_codes.name
            cheat_codes
            >>> Document.cheat_codes.dynamo_name
            cc

    .. attribute:: range_key

        True if this is the model's range key.

----------------------
 GlobalSecondaryIndex
----------------------

.. autoclass:: bloop.models.GlobalSecondaryIndex
    :members: __copy__
    :undoc-members:
    :special-members: __copy__


    .. attribute:: dynamo_name

        The name of this index in DynamoDB.  Defaults to the index's
        :data:`~GlobalSecondaryIndex.name`.

    .. attribute:: hash_key

        The column that the index can be queried against.

    .. attribute:: model

        The model this index is attached to.

    .. attribute:: name

        The name of this index in the model.  Not settable.

        .. code-block:: pycon

            >>> class Document(BaseModel):
            ...     ...
            ...     by_email = GlobalSecondaryIndex(
            ...         projection="keys", name="ind_e", hash_key="email")
            ...
            >>> Document.by_email.name
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

---------------------
 LocalSecondaryIndex
---------------------

.. autoclass:: bloop.models.LocalSecondaryIndex
    :members: __copy__
    :undoc-members:
    :special-members: __copy__

    .. attribute:: dynamo_name

        The name of this index in DynamoDB.  Defaults to the index's
        :data:`~LocalSecondaryIndex.name`.

    .. attribute:: hash_key

        LSI's hash_key is always the table hash_key.

    .. attribute:: model

        The model this index is attached to.

    .. attribute:: name

        The name of this index in the model.  Not settable.

        .. code-block:: pycon

            >>> class Document(BaseModel):
            ...     ...
            ...     by_date = LocalSecondaryIndex(
            ...         projection="keys", name="ind_co", range_key="created_on")
            ...
            >>> Document.by_date.name
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

.. _public-types:

=======
 Types
=======

Most custom types only need to specify a backing_type (or subclass a built-in type) and override
:func:`~bloop.types.Type.dynamo_dump` and :func:`~bloop.types.Type.dynamo_load`:

.. code-block:: python

    class ReversedString(Type):
        python_type = str
        backing_type = "S"

        def dynamo_load(self, value, *, context, **kwargs):
            return str(value[::-1])

        def dynamo_dump(self, value, *, context, **kwargs):
            return str(value[::-1])

If a type's constructor doesn't have required args, a :class:`~bloop.models.Column` can use the class directly.
The column will create an instance of the type by calling the constructor without any args:

.. code-block:: python

    class SomeModel(BaseModel):
        custom_hash_key = Column(ReversedString, hash_key=True)

In rare cases, complex types may need to implement :func:`~bloop.types.Type._dump` or :func:`~bloop.types.Type._load`.

------
 Type
------

.. autoclass:: bloop.types.Type
    :members: dynamo_dump, dynamo_load, _dump, _load
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

--------
 String
--------

.. autoclass:: bloop.types.String

    .. attribute:: backing_type
        :annotation: = "S"

    .. attribute:: python_type
        :annotation: = str

.. _api-public-number:

--------
 Number
--------

You should use :class:`decimal.Decimal` instances to avoid rounding errors:

.. code-block:: pycon

    >>> from bloop import BaseModel, Engine, Column, Number, Integer
    >>> class Product(BaseModel):
    ...     id = Column(Integer, hash_key=True)
    ...     rating = Column(Number)

    >>> engine = Engine()
    >>> engine.bind(Rating)

    >>> product = Product(id=0, rating=3.14)
    >>> engine.save(product)
    # Long traceback
    Inexact: [<class 'decimal.Inexact'>, <class 'decimal.Rounded'>]

    >>> from decimal import Decimal
    >>> product.rating = Decimal('3.14')
    >>> engine.save(product)
    >>> # Success!

.. autoclass:: bloop.types.Number

    .. seealso::

        If you don't want to deal with :class:`decimal.Decimal`\, see the
        :ref:`Float <patterns-float>` type in the patterns section.

    .. attribute:: backing_type
        :annotation: = "N"

    .. attribute:: python_type
        :annotation: = decimal.Decimal

    .. attribute:: context
        :annotation: = decimal.Context

        The context used to transfer numbers to DynamoDB.

--------
 Binary
--------

.. autoclass:: bloop.types.Binary

    .. attribute:: backing_type
        :annotation: = "B"

    .. attribute:: python_type
        :annotation: = bytes

---------
 Boolean
---------

.. autoclass:: bloop.types.Boolean

    .. attribute:: backing_type
        :annotation: = "BOOL"

    .. attribute:: python_type
        :annotation: = bool

------
 UUID
------

.. autoclass:: bloop.types.UUID

    .. attribute:: backing_type
        :annotation: = "S"

    .. attribute:: python_type
        :annotation: = uuid.UUID

----------
 DateTime
----------

.. data:: bloop.types.FIXED_ISO8601_FORMAT
    :annotation:

    DateTimes **must** be stored in DynamoDB in UTC with this exact format, and a +00:00 suffix.
    This is necessary for using comparison operators such as ``>`` and ``<=`` on DateTime instance.

    You must not use "Z" or any other suffix than "+00:00" to indicate UTC.  You must not omit the timezone specifier.

.. autoclass:: bloop.types.DateTime

    .. attribute:: backing_type
        :annotation: = "S"

    .. attribute:: python_type
        :annotation: = datetime.datetime

-----------
 Timestamp
-----------

.. autoclass:: bloop.types.Timestamp

    .. attribute:: backing_type
        :annotation: = "N"

    .. attribute:: python_type
        :annotation: = datetime.datetime

---------
 Integer
---------

.. autoclass:: bloop.types.Integer

    .. attribute:: backing_type
        :annotation: = "N"

    .. attribute:: python_type
        :annotation: = int

    .. attribute:: context
        :annotation: = decimal.Context

        The context used to transfer numbers to DynamoDB.

-----
 Set
-----

.. autoclass:: bloop.types.Set

    .. attribute:: backing_type
        :annotation: = "SS", "NS", or "BS"

        Set is not a standalone type; its backing type depends on the inner type its constructor receives. For
        example, ``Set(DateTime)`` has backing type "SS" because :class:`~bloop.types.DateTime` has backing type "S".

    .. attribute:: python_type
        :annotation: = set

    .. attribute:: inner_typedef
        :annotation: = Type

        The typedef for values in this Set.  Has a backing type of "S", "N", or "B".

------
 List
------

.. autoclass:: bloop.types.List

    .. attribute:: backing_type
        :annotation: = "L"

    .. attribute:: python_type
        :annotation: = list

    .. attribute:: inner_typedef
        :annotation: = Type

        The typedef for values in this List.  All types supported.

-----
 Map
-----

.. autoclass:: bloop.types.Map

    .. attribute:: backing_type
        :annotation: = "M"

    .. attribute:: python_type
        :annotation: = dict

    .. attribute:: types
        :annotation: = dict

        Specifies the Type for each key in the Map.  For example, a Map with two keys "id" and "rating" that are
        a UUID and Number respectively would have the following types:

        .. code-block:: python

            {
                "id": UUID(),
                "rating": Number()
            }

=======
 Query
=======

.. autoclass:: bloop.search.QueryIterator

    .. attribute:: count

        Number of items that have been loaded from DynamoDB so far, including buffered items.
        When projection type is "count", accessing this will automatically exhaust the query.

    .. attribute:: exhausted

        True if there are no more results.

    .. function:: first()

        Return the first result.  If there are no results, raises :exc:`~bloop.exceptions.ConstraintViolation`.

    .. function:: one()

        Return the unique result.  If there is not exactly one result,
        raises :exc:`~bloop.exceptions.ConstraintViolation`.

    .. function:: reset()

        Reset to the initial state, clearing the buffer and zeroing count and scanned.

    .. attribute:: scanned

        Number of items that DynamoDB evaluated, before any filter was applied.
        When projection type is "count", accessing this will automatically exhaust the query.

======
 Scan
======

.. autoclass:: bloop.search.ScanIterator

    .. attribute:: count

        Number of items that have been loaded from DynamoDB so far, including buffered items.
        When projection type is "count", accessing this will automatically exhaust the query.

    .. attribute:: exhausted

        True if there are no more results.

    .. function:: first()

        Return the first result.  If there are no results, raises :exc:`~bloop.exceptions.ConstraintViolation`.

    .. function:: one()

        Return the unique result.  If there is not exactly one result,
        raises :exc:`~bloop.exceptions.ConstraintViolation`.

    .. function:: reset()

        Reset to the initial state, clearing the buffer and zeroing count and scanned.

    .. attribute:: scanned

        Number of items that DynamoDB evaluated, before any filter was applied.
        When projection type is "count", accessing this will automatically exhaust the query.

========
 Stream
========

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

============
 Conditions
============

The only public class the conditions system exposes is the empty condition, :class:`~bloop.conditions.Condition`.
The rest of the conditions system is baked into :class:`~bloop.models.Column` and consumed by the various
:class:`~bloop.engine.Engine` functions like :func:`Engine.save() <bloop.engine.Engine.save>`.

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

    :ref:`conditions` in the User Guide describes the possible conditions, and when and how to use them.

.. autoclass:: bloop.conditions.Condition

.. _public-signals:

=========
 Signals
=========

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

============
 Exceptions
============

Except to configure sessions, Bloop aims to completely abstract the boto3/botocore layers.  If you encounter an
exception from either boto3 or botocore, please `open an issue`__.  Bloop's exceptions are broadly divided into two
categories: unexpected state, and invalid input.

To catch any exception from Bloop, use :exc:`~bloop.exceptions.BloopException`:

.. code-block:: python

    try:
        engine.stream(User, "latest")
    except BloopException:
        print("Didn't expect an exception, but Bloop raised:")
        raise

.. autoclass:: bloop.exceptions.BloopException

__ https://github.com/numberoverzero/bloop/issues/new

------------------
 Unexpected state
------------------

These are exceptions that you should be ready to handle in the normal course of using DynamoDB.  For example,
failing to load objects will raise :exc:`~bloop.exceptions.MissingObjects`, while conditional operations may
fail with :exc`~bloop.exceptions.ConstraintViolation`.

.. autoclass:: bloop.exceptions.ConstraintViolation

.. autoclass:: bloop.exceptions.MissingObjects

.. autoclass:: bloop.exceptions.RecordsExpired

.. autoclass:: bloop.exceptions.ShardIteratorExpired

.. autoclass:: bloop.exceptions.TableMismatch

-----------
 Bad Input
-----------

These are thrown when an option is invalid or missing, such as forgetting a key condition for a query,
or trying to use an unknown projection type.

.. autoclass:: bloop.exceptions.InvalidCondition

.. autoclass:: bloop.exceptions.InvalidModel

.. autoclass:: bloop.exceptions.InvalidPosition

.. autoclass:: bloop.exceptions.InvalidSearch

.. autoclass:: bloop.exceptions.InvalidShardIterator

.. autoclass:: bloop.exceptions.InvalidStream

.. autoclass:: bloop.exceptions.InvalidTemplate

.. autoclass:: bloop.exceptions.MissingKey

.. autoclass:: bloop.exceptions.UnknownType

.. _public-extensions:

============
 Extensions
============

.. _public-ext-datetime:

----------
 DateTime
----------

.. class:: DateTime(timezone=datetime.timezone.utc)

    Drop-in replacement for :class:`~bloop.types.DateTime`.  Support for `arrow`_, `delorean`_, and `pendulum`_:

    .. code-block:: python

        from bloop.ext.arrow import DateTime
        from bloop.ext.delorean import DateTime
        from bloop.ext.pendulum import DateTime

    .. attribute:: backing_type
        :annotation: = "S"

    .. attribute:: python_type
        :annotation:

        Depending on where it's imported from, one of:

        * :class:`arrow.Arrow <arrow.arrow.Arrow>`
        * :class:`delorean.Delorean`
        * :class:`pendulum.Pendulum`

    .. attribute:: timezone
        :annotation: = tzinfo

        The timezone that values loaded from DynamoDB will use.  Note that DateTimes are always stored in DynamoDB
        according to :data:`~bloop.types.FIXED_ISO8601_FORMAT`.

.. _arrow: http://crsmithdev.com/arrow
.. _delorean: https://delorean.readthedocs.io/en/latest/
.. _pendulum: https://pendulum.eustace.io

-----------
 Timestamp
-----------

.. class:: Timestamp(timezone=datetime.timezone.utc)

    Drop-in replacement for :class:`~bloop.types.Timestamp`.  Support for `arrow`_, `delorean`_, and `pendulum`_:

    .. code-block:: python

        from bloop.ext.arrow import Timestamp
        from bloop.ext.delorean import Timestamp
        from bloop.ext.pendulum import Timestamp

    .. attribute:: backing_type
        :annotation: = "N"

    .. attribute:: python_type
        :annotation:

        Depending on where it's imported from, one of:

        * :class:`arrow.Arrow <arrow.arrow.Arrow>`
        * :class:`delorean.Delorean`
        * :class:`pendulum.Pendulum`

    .. attribute:: timezone
        :annotation: = tzinfo

        The timezone that values loaded from DynamoDB will use.
