Bloop: DynamoDB Modeling
^^^^^^^^^^^^^^^^^^^^^^^^

DynamoDB's concurrency model is great, but using it correctly is tedious and unforgiving.
`Bloop manages that complexity for you.`__

Requires Python 3.6+

__ https://gist.github.com/numberoverzero/9584cfc375de0e087c8e1ae35ab8559c

========
Features
========

* Simple declarative modeling
* Stream interface that makes sense
* Extensible type system, useful built-in types
* Secure expression-based wire format
* Simple atomic operations
* Expressive conditions
* Model composition
* Diff-based saves
* Server-Side-Encryption
* Time-To-Live
* Continuous Backups

==========
Ergonomics
==========

.. code-block:: python

    class Account(BaseModel):
        id = Column(UUID, hash_key=True)
        name = Column(String)
        email = Column(String)
        by_email = GlobalSecondaryIndex(
            projection='keys', hash_key='email')

    engine.bind(Account)

    some_account = Account(
        id=uuid.uuid4(),
        email='foo@bar.com')
    engine.save(some_account)

    q = engine.query(
        Account.by_email,
        key=Account.email == 'foo@bar.com')

    same_account = q.one()
    print(same_account.id)

Never worry about `iterator types`__, or `tracking shard lineage`__ again:

.. code-block:: python

    template = '''
    Old: {old}
    New: {new}

    Event Details:
    {meta}

    '''

    stream = engine.stream(User, 'trim_horizon')
    while True:
        record = next(stream)
        if record:
            print(template.format(**record)
        else:
            time.sleep(0.5)

__ https://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_GetShardIterator.html#DDB-GetShardIterator-request-ShardIteratorType
__ https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html#Streams.Processing

===========
What's Next
===========

Get started by :ref:`installing <user-install>` Bloop, or check out a :ref:`larger example <user-quickstart>`.

.. toctree::
    :maxdepth: 2
    :caption: User Guide
    :hidden:

    user/install
    user/quickstart
    user/models
    user/engine
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
