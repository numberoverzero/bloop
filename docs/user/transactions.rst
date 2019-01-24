.. _user-transactions:

Transactions
^^^^^^^^^^^^

Bloop supports reading and updating items in `transactions`_ similar to the way you already
load, save, and delete items using an engine.  A single read or write transaction can have at most 10 items.

To create a new transaction, call :func:`Engine.transaction(mode="w") <bloop.engine.Engine.transaction>` and specify
a mode:

.. code-block:: python

    wx = engine.transaction(mode="w")
    rx = engine.transaction(mode="r")

When used as a context manager the transaction will call
:func:`commit() <bloop.transactions.PreparedTransaction.commit>` on exit if no exception occurs:


.. code-block:: python

    # mode defaults to "w"
    with engine.transaction() as tx:
        tx.save(some_obj)
        tx.delete(other_obj)


    # read transaction loads all objects at once
    user = User(id="numberoverzero")
    meta = Metadata(id=to_load.id)
    with engine.transaction(mode="r") as tx:
        tx.load(user, meta)

You may also call :func:`prepare() <bloop.transactions.Transaction.prepare>` and
:func:`commit() <bloop.transactions.PreparedTransaction.commit>` yourself:

.. code-block:: python

    import bloop

    tx = engine.transaction()
    tx.save(some_obj)
    p = tx.prepare()
    try:
        p.commit()
    except bloop.TransactionCanceled:
        print("failed to commit")


See :exc:`~bloop.exceptions.TransactionCanceled` for the conditions that can cause each type of transaction to fail.

.. _transactions: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transactions.html


====================
 Write Transactions
====================

A write transaction can save and delete items, and specify additional conditions on objects not being modified.

As with :ref:`Engine.save <user-engine-save>` and :ref:`Engine.delete <user-engine-delete>` you can provide multiple
objects to each :func:`WriteTransaction.save() <bloop.transactions.WriteTransaction.save>` or
:func:`WriteTransaction.delete() <bloop.transactions.WriteTransaction.delete>` call:

.. code-block:: python

    with engine.transaction() as tx:
        tx.delete(*old_tweets)
        tx.save(new_user, new_tweet)

-----------------
 Item Conditions
-----------------

You can specify a ``condition`` with each save or delete call:

.. code-block:: python

    with engine.transaction() as tx:
        tx.delete(auth_token, condition=Token.last_used <= now())

Or use the ``atomic=`` kwarg to require that object's local state to match DynamoDb's at the time the transaction is
committed.  For more information about the atomic keyword, see the :ref:`Engine.save <user-engine-save>` or
:ref:`Atomic Conditions <user-conditions-atomic>` sections of the user guide.

.. code-block:: python

    with engine.transaction() as tx:
        tx.save(new_user, new_tweet, atomic=True)


------------------------
 Transaction Conditions
------------------------

In addition to specifying conditions on the objects being modified, you can also specify a condition for the
transaction on an object that won't be modified.  This can be useful if you want to check another table without
changing its value:

.. code-block:: python

    user_meta = Metadata(id="numberoverzero")

    with engine.transaction() as tx:
        tx.save(new_tweet)
        tx.check(user_meta, condition=Metadata.verified.is_(True))

In the above example the transaction doesn't modify the user metadata.  If we want to modify that object we should
instead use a condition on the object being modified:

.. code-block:: python

    user_meta = Metadata(id="numberoverzero")
    engine.load(user_meta)
    user_meta.tweets += 1

    with engine.transaction() as tx:
        tx.save(new_tweet)
        tx.save(user_meta, condition=Metadata.tweets <= 500, atomic=True)

-------------
 Idempotency
-------------

Bloop automatically generates timestamped unique tokens (:attr:`~bloop.transactions.PreparedTransaction.tx_id` and
:attr:`~bloop.transactions.PreparedTransaction.first_commit_at`)
to guard against committing a write transaction twice or accidentally committing a transaction that was prepared a
long time ago.  While these are generated for both read and write commits, only `TransactWriteItems`_ respects the
`"ClientRequestToken"`_ stored in tx_id.

When the :attr:`~bloop.transactions.PreparedTransaction.first_commit_at` value is too old,
committing will raise :exc:`~bloop.exceptions.TransactionTokenExpired`.

.. _TransactWriteItems: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
.. _"ClientRequestToken": https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#DDB-TransactWriteItems-request-ClientRequestToken

===================
 Read Transactions
===================

By default :func:`engine.transaction(mode="w") <bloop.engine.Engine.transaction>` will create a
:class:`~bloop.transactions.WriteTransaction`.  To create a :class:`~bloop.transactions.ReadTransaction` pass
``mode="r"``:

.. code-block:: python

    with engine.transaction(mode="r") as rx:
        rx.load(user, tweet)
        rx.load(meta)

All objects in the read transaction will be loaded at the same time, when
:func:`commit() <bloop.transactions.PreparedTransaction.commit>` is called or the transaction context closes.

------------------
 Multiple Commits
------------------

Every time you call commit on the prepared transaction, the objects will be loaded again:

.. code-block:: python

    rx = engine.transaction(mode="r")
    rx.load(user, tweet)
    prepared = rx.prepare()

    prepared.commit()  # first load
    prepared.commit()  # second load

-----------------
 Missing Objects
-----------------

As with :ref:`Engine.load <user-engine-load>` if any objects in the transaction are missing when commit is called,
bloop will raise :exc:`~bloop.exceptions.MissingObjects` with the list of objects that were not found:

.. code-block:: python

    import bloop

    engine = bloop.Engine()
    ...


    def tx_load(*objs):
        with engine.transaction(mode="r") as rx:
            rx.load(*objs)

    ...

    try:
        tx_load(user, tweet)
    except bloop.MissingObjects as exc:
        missing = exc.objects
        print(f"failed to load {len(missing)} objects: {missing}")
