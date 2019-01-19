.. _api-internal:

Internal
^^^^^^^^

In addition to documenting internal classes, this section describes complex internal systems (such as Streams,
atomic tracking via weakrefs) and specific parameters and error handling that Bloop employs when talking to DynamoDB
(such as SessionWrapper's error inspection, and partial table validation).

================
 SessionWrapper
================

.. autoclass:: bloop.session.SessionWrapper
    :members:

==========
 Modeling
==========

-------
 IMeta
-------

.. autoclass:: bloop.models.IMeta

-------
 Index
-------

.. autoclass:: bloop.models.Index
    :members: __copy__
    :undoc-members:
    :special-members: __copy__

    .. attribute:: dynamo_name

        The name of this index in DynamoDB.  Defaults to the index's :data:`~Index.name`.

    .. attribute:: hash_key

        The column that the index can be queried against.
        *(LSI's hash_key is always the table hash_key.)*

    .. attribute:: model

        The model this index is attached to.

    .. attribute:: name

        The name of this index in the model.  Set by :func:`~bloop.models.bind_index`
        during :func:`~bloop.models.BaseModel.__init_subclass__`.

    .. attribute:: projection

        Computed during :func:`~bloop.models.bind_index`
        during :func:`~bloop.models.BaseModel.__init_subclass__`.

        .. code-block:: python

            {
                "available":  # Set of columns that can be returned from a query or search.
                "included":   # Set of columns that can be used in query and scan filters.
                "mode":       # "all", "keys", or "include"
                "strict":     # False if queries and scans can fetch non-included columns
            }

    .. attribute:: range_key

        The column that the index can be sorted on.

---------
 Binding
---------

.. automethod:: bloop.models.bind_column

.. automethod:: bloop.models.bind_index

.. automethod:: bloop.models.refresh_index

.. automethod:: bloop.models.unbind


=======
 Types
=======

-------------
 DynamicType
-------------

.. autoclass:: bloop.types.DynamicType

    .. attribute:: i

        Singleton instance of the class.

    .. attribute:: backing_type
        :annotation: = None

    .. attribute:: python_type
        :annotation: = None

===========
 Searching
===========

--------
 Search
--------

.. autoclass:: bloop.search.Search
    :members:

----------------
 PreparedSearch
----------------

.. autoclass:: bloop.search.PreparedSearch
    :members:

----------------
 SearchIterator
----------------

.. autoclass:: bloop.search.SearchIterator
    :members:


---------------------
 SearchModelIterator
---------------------

.. autoclass:: bloop.search.SearchModelIterator

    .. attribute:: count

        Number of items that have been loaded from DynamoDB so far, including buffered items.
        When projection type is "count", accessing this will automatically exhaust the query.

    .. attribute:: exhausted

        True if there are no more results.

    .. function:: all()

        Eagerly load all results and return a single list.  If there are no results, the list is empty.

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

===========
 Streaming
===========

-------------
 Coordinator
-------------

.. autoclass:: bloop.stream.coordinator.Coordinator
    :members:

-------
 Shard
-------

.. autoclass:: bloop.stream.shard.Shard
    :members:

--------------
 RecordBuffer
--------------

.. autoclass:: bloop.stream.buffer.RecordBuffer
    :members:

==============
 Transactions
==============

.. autoclass:: bloop.transactions.Transaction
    :members:

.. autoclass:: bloop.transactions.PreparedTransaction
    :members:

.. autoclass:: bloop.transactions.TxItem
    :members:

.. autoclass:: bloop.transactions.TxType
    :members:

============
 Conditions
============

------------------
 ReferenceTracker
------------------

.. autoclass:: bloop.conditions.ReferenceTracker
        :members: any_ref, pop_refs

-------------------
 ConditionRenderer
-------------------

.. autoclass:: bloop.conditions.ConditionRenderer
        :members: render, rendered

---------------------
 Built-in Conditions
---------------------

.. autoclass:: bloop.conditions.BaseCondition
        :members:

.. autoclass:: bloop.conditions.AndCondition
        :members:

.. autoclass:: bloop.conditions.OrCondition
        :members:

.. autoclass:: bloop.conditions.NotCondition
        :members:

.. autoclass:: bloop.conditions.ComparisonCondition
        :members:

.. autoclass:: bloop.conditions.BeginsWithCondition
        :members:

.. autoclass:: bloop.conditions.BetweenCondition
        :members:

.. autoclass:: bloop.conditions.ContainsCondition
        :members:

.. autoclass:: bloop.conditions.InCondition
        :members:

.. autoclass:: bloop.conditions.ComparisonMixin
        :members:

===========
 Utilities
===========

.. autoclass:: bloop.util.Sentinel
    :members:

.. autoclass:: bloop.util.WeakDefaultDictionary
    :members:

========================
 Implementation Details
========================

.. _implementation-model-hash:

-------------------------
 Models must be Hashable
-------------------------

By default python makes all user classes are hashable:

.. code-block:: pycon

    >>> class Dict: pass
    >>> hash(Dict())
    8771845190811


Classes are unhashable in two cases:

#. The class declares ``__hash__ = None``.
#. The class implements ``__eq__`` but not ``__hash__``

In either case, during :func:`~bloop.models.BaseModel.__init_subclass__`, the :func:`~bloop.models.ensure_hash`
function will manually locate the closest ``__hash__`` method in the model's base classes:

.. code-block:: python

    if getattr(cls, "__hash__", None) is not None:
        return
    for base in cls.__mro__:
        hash_fn = getattr(base, "__hash__")
        if hash_fn:
            break
    else:
        hash_fn = object.__hash__
    cls.__hash__ = hash_fn

This is required because python doesn't provide a default hash method when ``__eq__`` is implemented,
and won't fall back to a parent class's definition:

.. code-block:: pycon

    >>> class Base:
    ...     def __hash__(self):
    ...         print("Base.__hash__")
    ...         return 0
    ...
    >>> class Derived(Base):
    ...     def __eq__(self, other):
    ...         return True
    ...

    >>> hash(Base())
    Base.__hash__
    >>> hash(Derived())
    TypeError: unhashable type: 'Derived'


.. _internal-streams:

----------------------------
 Stream Ordering Guarantees
----------------------------

The `DynamoDB Streams API`__ exposes a limited amount of temporal information and few options for navigating
within a shard.  Due to these constraints, it was hard to reduce the API down to a single ``__next__`` call
without compromising performance or ordering.

The major challenges described below include:

* Creating a plausible total ordering across shards

* Managing an iterator:

    * Refreshing expired iterators without data loss
    * Preventing low-volume iterators without sequence numbers from expiring
    * Promoting children when a shard runs out of records
    * Distinguishing open shards from gaps between records

* Managing multiple shards:

    * Mapping stream "trim_horizon" and "latest" to a set of shards
    * Buffering records from multiple shards and applying a total ordering

* Loading and saving tokens:

    * Simplifying an entire stream into a human-readable json blob
    * Pruning old shards when loading
    * Inserting new shards when loading
    * Resolving TrimmedDataAccessException for old shards

__ http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/Welcome.html

The following sections use a custom notation to describe shards and records.

``Sn`` and ``Rn`` represent shards and records, where ``n`` is an integer::

    R11, R13, R32  # In general, RnX comes from Sn
    S1, S12, S23   # In general, SnX is a child of Sn

``<`` represents chronological ordering between records::

    R12 < R13  # In general, RX < RX when X < Y

``=>`` represents parent/child relationships between shards::

    S1 => {}          # S1 has no children
    S2 => S21         # S2 has one child
    # In general, SnX and SnY are adjacent children of Sn
    S3 => {S31, S32}

``~`` represents two shards that are not within the same lineage::

    S1 ~ S2  # Not related

    S1 => S12 => S13; S4 => S41
    # Both child shards, but of different lineages
    S12 ~ S41

``:`` represents a set of records from a single shard::

    S1: R11, R12   # no guaranteed order
    S2: R23 < R24  # guaranteed order


Shards and Lineage
==================

DynamoDB only offers three guarantees for chronological ordering:

1. All records **within a single Shard**.
2. All **parent** shard records are before all **child** shard records.
3. Changes to the **same hash** will always go to the same shard.  When a parent splits,
   further changes to that hash will go to **only one child** of that shard, and **always the same child**.

Given the following::

    S1 ~ S2
    S1: R11 < R12 < R13
    R2: R24 < R25 < R26

The first rule offers no guarantees between ``R1x`` and ``R2x`` for any ``x``.

Given the following::

    S1 => {S12, S13}
    S1:  R111 < R112
    S12: R124 < R125
    S13: R136 < R137

The second rule guarantees both of the following::

    R111 < R112 < R124 < R125
    R111 < R112 < R136 < R137

but does not guarantee any ordering between ``R12x`` and ``R13x`` for any ``x``.

Given the following::

    S1 => {S2, S3}
    R40, R41, R42  # all modify the same hash key
    R5, R7, R9     # modify different hash keys

    S1: R40, R5

The third rule guarantees that ``R41`` and ``R42`` will both be in either ``S2`` or ``S3``.  Meanwhile, it offers no
guarantee about where ``R7`` and ``R9`` will be.  Both of the following are possible::

    S1: R40, R5
    S2: R41, R42, R7
    S3: R9

    S1: R40, R5
    S2: R7, R9
    S3: R41, R42

But the following is not possible::

    S1: R40, R5
    S2: R41, R7
    S3: R42, R9

Merging Shards
==============

Low-throughput tables will only have a single open shard at any time, and can rely on the first and second guarantees
above for rebuilding the exact order of changes to the table.

For high throughput tables, there can be more than one root shard, and each shard lineage can have more than one
child open at once.  In this case, Bloop's streaming interface can't guarantee ordering for all records in the
stream, because there is no absolute chronological ordering across a partitioned table.  Instead, Bloop will fall
back to a total ordering scheme that uses each record's ``ApproximateCreationDateTime`` and, when two records have
the same creation time, a monotonically increasing integral clock to break ties.

Consider the following stream::

    S0 => {S1, S2}
    S0: R00
    S1: R11 < R12 < R13
    S2: R24 < R25 < R26

Where each record has the following (simplified) creation times:

======= ===========================
Record  ApproximateCreationDateTime
======= ===========================
``R00`` 7 hours ago
``R11`` 6 hours ago
``R12`` 4 hours ago
``R13`` 2 hours ago
``R24`` 4 hours ago
``R25`` 3 hours ago
``R26`` 3 hours ago
======= ===========================

Bloop performs the following in one step:

1. The second guarantee says all records in ``S0`` are before records in that shard's children::

    R00 < (R11, R12, R13, R24, R25, R26)

2. The first guarantee says all records in the same shard are ordered::

    R00 < ((R11 < R12 < R13), (R24 < R25 < R26)

3. Then, ``ApproximateCreationDateTime`` is used to partially merge ``S1`` and ``S2`` records::

    R00 < R11 < (R12, R24) < (R25 < R26) < R13

4. There were still two collisions after using ``ApproximateCreationDateTime``: ``R12, R24`` and ``R25, R26``.

    1. To resolve ``(R12, R24)`` Bloop breaks the tie with an incrementing clock, and assigns ``R12 < R24``.
    2. ``(R25, R26)`` is resolved because the records are in the same shard.

The final ordering is::

    R00 < R11 < R12 < R24 < R25 < R26 < R13


Record Gaps
===========

Bloop initially performs up to 5 "catch up" calls to GetRecords when advancing an iterator.  If a GetRecords call
returns a ``NextShardIterator`` but no records it's either due to being nearly caught up to "latest" in an open
shard, or from traversing a period of time in the shard with no activity.  Endlessly polling until a record comes back
would cause every open shard to hang for up to 4 hours, while only calling GetRecords once could desynchronize one
shard's iterator from others.

By retrying up to 5 times on an empty GetRecords response (that still has a NextShardIterator) Bloop is confident
that any gaps in the shard have been advanced.  This is because it takes approximately 4-5 calls to traverse an
empty shard completely.  In other words, the 6th empty response almost certainly indicates that the iterator is
caught up to latest in an open shard, and it's safe to cut back to one call at a time.

Why only 5 calls?
-----------------

This number came from `extensive testing`__ which compared the number of empty responses returned for shards with
various activity cadences.  It's reasonable to assume that this number would only decrease with time, as advances in
software and hardware would enable DynamoDB to cover larger periods in time with the same time investment.
Because each call from a customer incurs overhead of creating and indexing each new iterator id, as well as the usual
expensive signature-based authentication, it's in DynamoDB's interest to minimize the number of calls a customer needs
to traverse a sparsely populated shard.

At worst DynamoDB starts requiring more calls to fully traverse an empty shard, which could result in reordering
between records in shards with vastly different activity patterns.  Since the creation-time-based ordering
is approximate, this doesn't relax the guarantees that Bloop's streaming interface provides.

Changing the Limit
------------------

In general you should not need to worry about this value, and leave it alone.  In the unlikely case that DynamoDB
**does** increase the number of calls required to traverse an empty shard, Bloop will be updated soon after.

If you still need to tune this value:

.. code-block:: python

    import bloop.stream.shard
    bloop.stream.shard.CALLS_TO_REACH_HEAD = 5

The exact value of this parameter will have almost no impact on performance in high-activity streams, and there are
so few shards in low-activity streams that the total cost will be on par with the other calls to set up the stream.

__ https://gist.github.com/numberoverzero/8bde1089b5def6cc8c6d5fba61866702
