.. _streams:

Streams
^^^^^^^

Bloop provides a simple, pythonic interface to DynamoDB's `complex`__ `Streams API`__.  This abstracts away the
minutiae of managing and refreshing iterators, tracking sequence numbers and shard splits, merging records from
adjacent shards, and saving and loading processing state.

.. warning::

    In general, DynamoDB **does not guarantee** chronological ordering of changes **across shards**. Chronological
    ordering for the entire stream is only guaranteed for a table with a **single partition**; exactly one shard
    will be open at any time.

    Bloop creates a total ordering across shards using DynamoDB's ordering rules
    and each record's |ApproximateCreationDateTime|_ and |SequenceNumber|_.

    For a detailed explanation, see :ref:`Stream Internals<internal-streams>`.


__ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
__ http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/Welcome.html
.. |ApproximateCreationDateTime| replace:: ``ApproximateCreationDateTime``
.. _ApproximateCreationDateTime: https://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_StreamRecord.html#DDB-Type-StreamRecord-ApproximateCreationDateTime
.. |SequenceNumber| replace:: ``SequenceNumber``
.. _SequenceNumber: https://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_StreamRecord.html#DDB-Type-StreamRecord-SequenceNumber

================
Enable Streaming
================

To add a stream that includes new and old images in each record, add the following to a model's meta:

.. code-block:: python

    class User(BaseModel):
        class Meta:
            stream = {
                "include": ["new", "old"]
            }
        id = Column(Integer, hash_key=True)
        email = Column(String)
        verified = Column(Boolean)

    engine.bind(User)

``"include"`` has four possible values, matching `StreamViewType`__:

.. code-block:: python

    {"keys"}, {"new"}, {"old"}, {"new", "old"}

__ http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_StreamDescription.html#DDB-Type-StreamDescription-StreamViewType


.. _stream-create:

===============
Create a Stream
===============

Next, create a stream on the model.  This example starts at "trim_horizon" to get all records from the last
24 hours, but could also be "latest" to only return records created after the stream was instantiated.

.. code-block:: python

    stream = engine.stream(User, "trim_horizon")

If you want to start at a certain point in time, you can also use an :py:class:`arrow.arrow.Arrow` datetime:

.. code-block:: python

    stream = engine.stream(User, arrow.now().replace(hours=-12))

Creating streams at a specific time is **very expensive**, and will iterate all records since the stream's
trim_horizon until the target time.

If you are trying to resume processing from the same position as another
stream, you should persist the ``Stream.token`` and load from that instead of using a specific time:

.. code-block:: python

    previous_stream = engine.stream(User, "trim_horizon")
    # Do a bunch of processing...
    ...

    # Save the state to a file
    with open("/tmp/state", "w") as f:
        json.dump(previous_stream.token, f)

    ...
    # Some time later, resume processing from the same point
    with open("/tmp/state", "r") as f:
        previous_token = json.load(f)
    stream = engine.stream(User, previous_token)

See :ref:`stream-resume` for an example of a stream token.

================
Retrieve Records
================

You only need to call :py:func:`next` on a Stream to get the next record:

.. code-block:: python

    record = next(stream)

If there are no records at the current position, record will be ``None``.  A common pattern is to poll immediately
when a record is found, but to wait a small amount when no record is found.  Which you use will depend on how
aggressively you want to process new records:

.. code-block:: python

    while True:
        record = next(stream)
        if not record:
            time.sleep(0.2)
        else:
            process(record)

.. _stream-records:

----------------
Record Structure
----------------

Each record is a dict with an instance of the stream model in one or more of ``"key"``, ``"old"``, and ``"new"``.
This will depend on the stream declaration above, as well as the record type.  A key-only stream will have
``None`` in the ``"old"`` and ``"new"`` fields.  If a stream includes both ``old`` and ``new`` images but the
record type is delete, ``"new"`` will be ``None`` because there is no new value.

Save a new user, and then update the email address:

.. code-block:: python

    user = User(id=3, email="user@domain.com")
    engine.save(user)

    user.email = "admin@domain.com"
    engine.save(user)

The first record won't have an ``old`` value, since it was the first time this item was saved:

.. code-block:: python

    first = next(stream)
    print(json.dumps(first, indent=4, default=repr))

    {
        "key": null,
        "old": null,
        "new": "User(email='user@domain.com', id=3, verified=None)",
        "meta": {
            "created_at": "<Arrow [2016-10-23T00:28:00-07:00]>",
            "event": {
                "id": "3fe6d339b7cb19a1474b3d853972c12a",
                "type": "insert",
                "version": "1.1"
            },
            "sequence_number": "700000000007366876916"
        },
    }

The second record shows the change to email, and has both ``old`` and ``new``:

.. code-block:: python

    second = next(stream)
    print(json.dumps(second, indent=4, default=repr))

    {
        "key": null,
        "old": "User(email='user@domain.com', id=3, verified=None)",
        "new": "User(email='admin@domain.com', id=3, verified=None)",
        "meta": {
            "created_at": "<Arrow [2016-10-23T00:28:00-07:00]>",
            "event": {
                "id": "73a4b8568a85a0bcac25799f806df239",
                "type": "modify",
                "version": "1.1"
            },
            "sequence_number": "800000000007366876936"
        },
    }

-------------------
Periodic Heartbeats
-------------------

You should call ``stream.heartbeat()`` every 12 minutes in your processing loop.

Iterators only last 15 minutes which means they need to be refreshed periodically.  There's no way to
safely refresh an iterator that hasn't found a record; refreshing an iterator at "latest" could miss records since
the time that the previous iterator was at "latest".

``Stream.heartbeat`` only refreshes iterators that it needs to.  Once a shard finds a record it's
skipped on every subsequent heartbeat.  In practice the overhead of ``heartbeat()`` is very low,
about one call per shard.

The following pattern will call heartbeat every 12 minutes if ``process`` is quick:

.. code-block:: python

    next_heartbeat = arrow.now()
    while True:
        record = next(stream)
        process(record)
        if arrow.now() > next_heartbeat:
            next_heartbeat = arrow.now().replace(minutes=12)
            stream.heartbeat()

.. _stream-resume:

--------------------
Pausing and Resuming
--------------------

Use ``Stream.token`` to save the current state and resume processing later:

.. code-block:: python

    import json

    with open("/tmp/stream-token", "w") as f:
        json.dump(stream.token, f)

    with open("/tmp/stream-token", "r" as f):
        token = json.load(f)
    stream = engine.stream(User, token)

When reloading from a token, Bloop will automatically prune shards that have expired, and extend the state to include
new shards.  Any iterators that fell behind the current trim_horizon will be moved to their childrens' trim_horizons.

Here is the token from the stream in :ref:`stream-records`:

.. code-block:: python

    {
        "active": [
            "shardId-00000001477207595861-d35d208d"
        ],
        "shards": [
            {
                "iterator_type": "after_sequence",
                "sequence_number": "800000000007366876936",
                "shard_id": "shardId-00000001477207595861-d35d208d"
            }
        ],
        "stream_arn": "arn:.../stream/2016-10-23T07:26:33.312"
    }

There is only one shard because the stream was created less than 4 hours ago.  After 24 hours there will still be
one active shard, but there would be 5 other closed shards that form the lineage of the stream.

-------------
Moving Around
-------------

This function takes the same ``position`` argument as ``engine.stream``:

.. code-block:: python

    # Any stream token; this one rebuilds the
    # stream in its current location
    stream.move_to(stream.token)

    # Jump back in time 2 hours
    stream.move_to(arrow.now().replace(hours=-2))

    # Move to the oldest record in the stream
    stream.move_to("trim_horizon")

As noted in :ref:`stream-create`, moving to a specific time is **very expensive**.
