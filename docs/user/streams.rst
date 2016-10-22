.. _streams:

Streams
^^^^^^^

Bloop provides a simple, pythonic interface to DynamoDB's `complex`__ `Streams API`__.  This abstracts away the
minutiae of managing and refreshing iterators, tracking sequence numbers and shard splits, merging records from
adjacent shards, and saving and loading processing state.

.. warning::

    As with any distributed system, DynamoDB **does not guarantee** chronological ordering of changes across shards.
    This limitation is not specific to Bloop, and exists for any high-throughput stream processing.

    In practice, Bloop **can** guarantee chronological ordering for **single partition** tables,
    which will only have a single open shard at any time.

    For a detailed explanation, see :ref:`stream-merging`.


__ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
__ http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/Welcome.html
.. |ApproximateCreationDateTime| replace:: ``ApproximateCreationDateTime``
.. _ApproximateCreationDateTime: https://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_GetRecords.html#API_GetRecords_ResponseSyntax

Add a stream in the model's ``Meta`` section:

.. code-block:: python

    class User(BaseModel):
        class Meta:
            stream = {
                "include": ["new", "old"]
            }
        id = Column(UUID, hash_key=True)
        email = Column(String)
        verified = Column(Boolean)

``"include"`` has four possible values, matching `StreamViewType`__:

.. code-block:: python

    {"keys"}, {"new"}, {"old"}, {"new", "old"}

__ http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_StreamDescription.html#DDB-Type-StreamDescription-StreamViewType

Next, create a stream.  This example starts at ``"trim_horizon"`` to get all records from the last 24 hours, but
could also be ``"latest"`` to only get records created after the stream will be retrieved.

.. code-block:: python

    stream = engine.stream(User, "trim_horizon")

Then, add a user that will show up in the stream:

.. code-block:: python

    user = User(id=uuid.uuid4(), email="user@domain.com")
    engine.save(user)

Finally, poll the stream and display the first record:

.. code-block:: python

    record = None
    while not record:
        record = next(stream)

    if record["old"]:
        print("user {} updated from {}".format(
            record["new"], record["old"]))
    else:
        print("new user {} created {}".format(
            record["new"], record["meta"]["created_at"].humanize()))

Use ``Stream.token`` to save the current state and resume processing later:

.. code-block:: python

    import json

    with open("/tmp/stream-token", "w") as f:
        json.dump(stream.token, f)

    with open("/tmp/stream-token", "r" as f):
        token = json.load(f)
    stream = engine.stream(User, token)

``Stream.move_to`` can take a token, time, or either end of the stream:

.. code-block:: python

    # Rebuilds the stream in its current location
    stream.move_to(stream.token)

    # Jump back in time 2 hours
    stream.move_to(arrow.now().replace(hours=-2))

    # Move to the oldest record in the stream
    stream.move_to("trim_horizon")

While polling, it's important to periodically call ``Stream.heartbeat``, which will keep iterators from expiring.
Iterators expire every 15 minutes, but due to clock skew it's usually safer to call every 12 minutes.

.. code-block:: python

    next_heartbeat = arrow.now().replace(minutes=12)
    while True:
        record = next(stream)
        process(record)
        if arrow.now() > next_heartbeat:
            next_heartbeat = arrow.now().replace(minutes=12)
            stream.heartbeat()

It's safe to call ``heartbeat`` in a tight loop.  On average, it will only result in a single call
to DynamoDB every four hours per shard.

===================
Internals: Ordering
===================

--------
Notation
--------

This section uses a number of conventions to describe complex relationships between shards and records within a stream:

* ``Sn`` and ``Rn`` represent shards and records, where ``n`` is an integer. ::

    R11, R13, R32  # In general, RnX comes from Sn
    S1, S12, S23   # In general, SnX is a child of Sn

* ``<`` represents chronological ordering between records. ::

    R12 < R13  # In general, RX < RX when X < Y

* ``=>`` represents parent/child relationships between shards. ::

    S1 => {}          # S1 has no children
    S2 => S21         # S2 has one child
    # In general, SnX and SnY are adjacent children of Sn
    S3 => {S31, S32}

* ``~`` represents two shards that are not within the same lineage.  ::

    S1 ~ S2  # Not related

    S1 => S12 => S13; S4 => S41
    # Both child shards, but of different lineages
    S12 ~ S41

* ``:`` represents a set of records from a single shard. ::

    S1: R11, R12   # no guaranteed order
    S2: R23 < R24  # guaranteed order


----------
Guarantees
----------

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

.. _stream-merging:

--------------
Merging Shards
--------------

Low-throughput tables will only have a single open shard at any time, and can rely on the first and second guarantees
above for rebuilding the exact order of changes to the table.

For high throughput tables, there can be more than one root shard, and each shard lineage can have more than one
child open at once.  In this case, Bloop's streaming interface can't guarantees ordering for all records in the
stream, because there is no absolute chronological ordering across a partitioned table.  Instead, Bloop will fall
back to a total ordering scheme that uses each record's ``ApproximateCreationDateTime`` and, when two records have
the same creation time, a monotonically increasing integral clock to break ties.

Consider the following stream::

    S0 => {S1, S2}
    S0: R00
    S1: R11 < R12 < R13
    S2: R24 < R25 < R26

Where each record has the following (simplified) creation times:

======= ===============================
Record  ``ApproximateCreationDateTime``
======= ===============================
``R00`` 7 hours ago
``R11`` 6 hours ago
``R12`` 4 hours ago
``R13`` 2 hours ago
``R24`` 4 hours ago
``R25`` 3 hours ago
``R26`` 3 hours ago
======= ===============================

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

