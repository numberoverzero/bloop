Bloop: DynamoDB Modeling
^^^^^^^^^^^^^^^^^^^^^^^^

DynamoDB's concurrency model is great, but using it correctly is tedious and unforgiving.
`Bloop manages that complexity for you.`__

Requires Python 3.6+

__ https://gist.github.com/numberoverzero/9584cfc375de0e087c8e1ae35ab8559c

==========
 Features
==========

* Simple declarative modeling
* Stream interface that makes sense
* Easy transactions
* Extensible type system, useful built-in types
* Secure expression-based wire format
* Simple atomic operations
* Expressive conditions
* Model composition
* Diff-based saves
* Server-Side Encryption
* Time-To-Live
* Continuous Backups

============
 Ergonomics
============

The basics:

.. code-block:: python

    class Account(BaseModel):
        id = Column(UUID, hash_key=True)
        name = Column(String)
        email = Column(String)
        by_email = GlobalSecondaryIndex(
            projection='keys', hash_key='email')

    engine.bind(Account)

    some_account = Account(id=uuid.uuid4(), email='foo@bar.com')
    engine.save(some_account)

    q = engine.query(Account.by_email, key=Account.email == 'foo@bar.com')
    same_account = q.one()

    print(same_account.id)

Iterate over a stream:

.. code-block:: python

    template = "old: {old}\nnew: {new}\ndetails:{meta}"

    stream = engine.stream(User, 'trim_horizon')
    while True:
        record = next(stream)
        if not record:
            time.sleep(0.5)
            continue
        print(template.format(**record))

Use transactions:

.. code-block:: python

    with engine.transaction() as tx:
        tx.save(account, atomic=True)
        tx.delete(update_token, condition=Token.until <= now())

=============
 What's Next
=============

Get started by :ref:`installing <user-install>` Bloop, or check out a :ref:`larger example <user-quickstart>`.

.. toctree::
    :maxdepth: 2
    :caption: User Guide
    :hidden:

    user/install
    user/quickstart
    user/models
    user/engine
    user/transactions
    user/streams
    user/types
    user/conditions
    user/signals
    user/patterns
    user/extensions

.. toctree::
    :maxdepth: 2
    :caption: API
    :hidden:

    api/public
    api/internal

.. toctree::
    :maxdepth: 2
    :caption: Project
    :hidden:

    meta/changelog
    meta/about
