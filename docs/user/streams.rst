.. _user-streams:

Streams
^^^^^^^

Bloop provides a simple, pythonic interface to DynamoDB's `complex`__ `Streams API`__.  This abstracts away the
minutiae of managing and refreshing iterators, tracking sequence numbers and shard splits, merging records from
adjacent shards, and saving and loading processing state.

.. warning::

        **Chronological order is not guaranteed for high throughput streams.**

        DynamoDB guarantees ordering:

        * within any single shard
        * across shards for a single hash/range key

        There is no way to exactly order records from adjacent shards.  High throughput streams
        provide approximate ordering using each record's "ApproximateCreationDateTime".

        Tables with a single partition guarantee order across all records.

        See :ref:`Stream Internals <internal-streams>` for details.


__ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
__ http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/Welcome.html

================
Enable Streaming
================

Add the following to a model's ``Meta`` to enable a stream with new and old objects in each record:

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

.. code-block:: pycon

    >>> stream = engine.stream(User, "trim_horizon")

If you want to start at a certain point in time, you can also use an :class:`arrow.arrow.Arrow` datetime.
Creating streams at a specific time is **very expensive**, and will iterate all records since the stream's
trim_horizon until the target time.

.. code-block:: pycon

    >>> stream = engine.stream(User, arrow.now().replace(hours=-12))

If you are trying to resume processing from the same position as another stream, you should load from a persisted
:data:`Stream.token <bloop.stream.Stream.token>` instead of using a specific time.
See :ref:`stream-resume` for an example of a stream token.

.. code-block:: pycon

    >>> import json
    >>> original_stream = engine.stream(User, "trim_horizon")
    >>> with open("/tmp/state", "w") as f:
    ...     json.dump(original_stream.token, f)
    ...
    # Some time later
    >>> with open("/tmp/state", "r") as f:
    ...     token = json.load(f)
    ...
    >>> stream = engine.stream(User, token)

================
Retrieve Records
================

You only need to call :func:`next` on a Stream to get the next record:

.. code-block:: pycon

    >>> record = next(stream)

If there are no records at the current position, record will be ``None``.  A common pattern is to poll immediately
when a record is found, but to wait a small amount when no record is found.

.. code-block:: pycon

    >>> while True:
    ...     record = next(stream)
    ...     if not record:
    ...         time.sleep(0.2)
    ...     else:
    ...         process(record)

----------------
Record Structure
----------------

Each record is a dict with instances of the model in one or more of ``"key"``, ``"old"``, and ``"new"``.
These are populated according to the stream's ``"include"`` above, as well as the event type.  A key-only
stream will never have new or old objects.  If a stream includes new and old objects and the event type is delete,
new will be ``None``.

Save a new user, and then update the email address:

.. code-block:: pycon

    >>> user = User(id=3, email="user@domain.com")
    >>> engine.save(user)
    >>> user.email = "admin@domain.com"
    >>> engine.save(user)

The first record won't have an old value, since it was the first time this item was saved:

.. code-block:: pycon

    >>> next(stream)
    {'key': None,
     'old': None,
     'new': User(email='user@domain.com', id=3, verified=None),
     'meta': {
         'created_at': <Arrow [2016-10-23T00:28:00-07:00]>,
         'event': {
             'id': '3fe6d339b7cb19a1474b3d853972c12a',
             'type': 'insert',
             'version': '1.1'},
         'sequence_number': '700000000007366876916'}
    }

The second record shows the change to email, and has both old and new objects:

.. code-block:: pycon

    >>> next(stream)
    {'key': None,
     'old': User(email='user@domain.com', id=3, verified=None),
     'new': User(email='admin@domain.com', id=3, verified=None),
     'meta': {
         'created_at': <Arrow [2016-10-23T00:28:00-07:00]>,
         'event': {
             'id': '73a4b8568a85a0bcac25799f806df239',
             'type': 'modify',
             'version': '1.1'},
         'sequence_number': '800000000007366876936'}
    }

-------------------
Periodic Heartbeats
-------------------

You should call :func:`Stream.heartbeat() <bloop.stream.Stream.heartbeat>`
at least every 14 minutes in your processing loop.

Iterators only last 15 minutes, and need to be refreshed periodically.  There's no way to
safely refresh an iterator that hasn't found a record.  For example, refreshing an iterator at "latest" could miss
records since the time that the previous iterator was at "latest".  If you call this every 15 minutes, an iterator
may expire due to clock skew or processing time.

Only iterators without sequence numbers will be refreshed.  Once a shard finds a record it's
skipped on every subsequent heartbeat.  For a moderately active stream, heartbeat will make about one call per shard.

The following pattern will call heartbeat every 12 minutes (if record processing is quick):

.. code-block:: pycon

    >>> next_heartbeat = arrow.now()
    >>> while True:
    ...     record = next(stream)
    ...     process(record)
    ...     if arrow.now() > next_heartbeat:
    ...         next_heartbeat = arrow.now().replace(minutes=12)
    ...         stream.heartbeat()

.. _stream-resume:

--------------------
Pausing and Resuming
--------------------

Use :data:`Stream.token <bloop.stream.Stream.token>` to save the current state and resume processing later:

.. code-block:: pycon

    >>> with open("/tmp/stream-token", "r" as f):
    ...     token = json.load(f)
    ...
    >>> stream = engine.stream(User, token)

When reloading from a token, Bloop will automatically prune shards that have expired, and extend the
state to include new shards.  Any iterators that fell behind the current trim_horizon will be moved
to each of their children's trim_horizons.

Here's a token from a new stream. After 8-12 hours there will be one active shard, but also a few
closed shards that form the lineage of the stream.

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



-------------
Moving Around
-------------

This function takes the same ``position`` argument as :func:`Engine.stream <bloop.engine.Engine.stream>`:

.. code-block:: pycon

    # Any stream token; this one rebuilds the
    # stream in its current location
    >>> stream.move_to(stream.token)

    # Jump back in time 2 hours
    >>> stream.move_to(arrow.now().replace(hours=-2))

    # Move to the oldest record in the stream
    >>> stream.move_to("trim_horizon")

As noted :ref:`above <stream-create>`, moving to a specific time is **very expensive**.
