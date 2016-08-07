.. _atomic-operations:

Atomic Operations
^^^^^^^^^^^^^^^^^

TODO copied from another draft

For example, if we save the following object:

.. code-block:: python

    now = arrow.now()
    # where id is the hash key, and we
    # didn't set the email or age columns
    user = User(
        id=10, nick="numberoverzero",
        created_on=now)
    engine.save(user)

Then the following two calls are equivalent:

.. code-block:: python

    # Construct the condition to ensure
    # nothing's changed since we saved the object
    condition = (User.id == 10) \
                & (User.nick == "numberoverzero") \
                & (User.created_on == now) \
                & (User.email.is_(None)) \
                & (User.age.is_(None))
    engine.save(user, condition=condition, atomic=False)

    # Or, use an atomic save
    engine.save(user, atomic=True)

These are simple atomic conditions - the condition is built by iterating the set of columns that were loaded from
DynamoDB through ``Engine.load``, ``Engine.query``, ``Engine.scan``, or saved to DynamoDB through ``Engine.save`` or
``Engine.delete``.

Quick Example
=============

Usage
=====

atomic=True

engine config

Consistent vs Atomic
====================

'latest version at time of call' vs 'unchanged since last load'

Scenarios
=========

New Instance
------------

atomic save on an instance before it's been saved or loaded from dynamo

Loaded
------

loaded from dynamo, not changed

Scenario C: Loaded

loaded from dynamo, modified by another writer

Partial Query
-------------

query doesn't load all columns
atomic condition only on loaded
(other writer can modify not loaded column)

Limitations
===========

Atomic conditions (more generally, optimistic concurrency) are not a magic answer to all concurrency issues.

In an ideal network with no timeouts or dropped responses, optimistic concurrency actually would be enough to implement
a perfect distributed atomic counter. Networks are not perfect.  Say two threads perform the same increment (20 -> 21)
and neither gets a response from the server.  They both load the value again, and see that it is now 21.  It's
impossible for either thread to know that **its increment** is the one that succeeded.

At first DynamoDB's `Atomic Counters`_ sound promising.  Reading on, this warning makes it clear that an atomic
counter is not sufficient for a caller to guarantee that they've incremented an integer exactly once [0]_:

    Atomic counter updates are not idempotent. This means that the counter will increment each time you call
    UpdateItem. If you suspect that a previous request was unsuccessful, your application could retry the UpdateItem
    operation; however, this would risk updating the counter twice.

Instead, the documentation suggests using conditional updates where accuracy is needed:

    ...in a banking application, it would be safer to use a conditional update..."

Notice this says "would be safer" and not "would be safe".  **Conditional writes are not always sufficient**.
This is very important, because the next section on `Conditional Writes`_ says the opposite [1]_:

    Note that conditional writes are idempotent. This means that you can send the same conditional write request
    multiple times, but it will have no further effect on the item after the first time DynamoDB performs the
    specified update. For example, suppose you issue a request to update the price of a book item by 10%, with the
    expectation that the price is currently $20.  However, before you get a response, a network error occurs and you
    don't know whether your request was successful or not. Because a conditional update is an idempotent operation,
    you can send the same request again. and DynamoDB will update the price only if the current price is still $20.

Let's go back to the atomic counter problem, and see where the above breaks down.  Our counter is at 20, and two
threads want to increment by exactly one; the counter should eventually have the value 22.

With an ideal network -- no dropped connections or timeouts -- it would look like this::

    A = Thread 1
    B = Thread 2
    S = Server

    A -> S: GET counter
      S -> A: 20
    B -> S: GET counter
      S -> B: 20

    A -> S: SET counter=21 IFF counter=20
      S -> A: SUCCESS
    B -> S: SET counter=21 IFF counter=20
      S -> B: CONDITION FAILED

    # Thread B now retries, first
    # fetching the new counter
    B -> S: GET counter
      S -> B: 21
    B -> S: SET counter 22 IFF counter=21
      S -> B: SUCCESS

Great!  Each thread has incremented the counter exactly once.  Thread B failed its first conditional update, because
Thread A was faster.  Thread B knows that its update failed, and so it needs to retry.  It loads the new value and
then constructs a new update, this time ``IFF counter=21``.

With network failure though, it looks like this::

    A = Thread 1
    B = Thread 2
    S = Server

    A -> S: GET counter
      S -> A: 20
    B -> S: GET counter
      S -> B: 20

    A -> S: SET counter=21 IFF counter=20
    B -> S: SET counter=21 IFF counter=20
      S -> A: TIMEOUT
      S -> B: TIMEOUT

    # Both threads have to check again
    A -> S: GET counter
      S -> A: 21
    B -> S: GET counter
      S -> B: 21

    # A and B can't know if it was their update that succeeded.

The only way to ensure an operation has succeeded is to check for the presence of a value unique to an individual
request.  This unique value must be retrievable for some time after the failed request.

It's not enough to have a ``revision`` column that holds the UUID of the last successful request.  Here's how that
fails with three threads::

    A = Thread 1
    B = Thread 2
    C = Thread 3
    S = Server

    A -> S: SET counter=21, last=A IFF counter=20, last=None
    B -> S: SET counter=21, last=B IFF counter=20, last=None

    # A succeeds and B doesn't.
    # counter=21, last=A
      S -> A: TIMEOUT
      S -> B: TIMEOUT

    # Before A or B can retry, another thread
    # tries to increment by 1

    C -> S: GET counter, last
      S -> C: counter=21, last=A
    C -> SET counter=22, last=C IFF counter=20, last=A
      S -> C: SUCCESS

    # A and B load the values of counter, last
    # to hopefully try again
    A -> S: GET counter, last
      S -> A: counter=22, last=C
    B -> S: GET counter, last
      S -> B: counter=22, last=C

    # A and B can't know if their update succeeded
    # between [last=None, last=C]

All that Thread A and B can know (assuming all updates increment by 1) is that a thread updated the counter from 20 to
21.  It could have been either of them, or even C.  It's impossible to know because only the last update's id is saved.

We need more than the immediately previous caller to know if our particular call succeeded at any point in the past.
One solution is a Set:

.. code-block:: python

    class Counter(...):
        id = ...
        value = Column(Integer)
        updated_by = Column(Set(UUID))

Now, we can generate a UUID and part of our condition is that the UUID isn't in ``updated_by``.  If it is, we know that
call succeeded:

.. code-block:: python

    def increment(counter_id):
        counter = Counter(id=counter_id)

        uid == uuid.uuid4()

        for _ in range(10):
            engine.load(counter)

            # If this is true, one of our previous
            # calls must have succeeded, even though
            # we never heard back
            if uid in counter.updated_by:
                return

            counter.updated_by.add(uid)
            counter.value += 1
            try:
                engine.save(counter, atomic=True)
            except ConstraintViolation:
                continue

Unfortunately, this uses a massive amount of space.  We can't easily put the unique request ids in another table,
since DynamoDB does not have transactions.

.. [0] For this reason, bloop does not expose a mechanism to send an offset-based update.  That is, there is no way
       to `Increment and Decrement a Numeric Attribute`_ such as ``SET Price = Price - :p``.
.. [1] It's not exactly the opposite.  The example *suggests* that Conditional Writes let you just retry calls that
       timed out, and that's that.  In a network failure, it's impossible for two callers making a relative change
       to know if their call is the one that is reflected in the new value.

.. _Atomic Counters: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.AtomicCounters
.. _Conditional Writes: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate
.. _Increment and Decrement a Numeric Attribute: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.SET.IncrementAndDecrement
