Bloop: DynamoDB Modeling
^^^^^^^^^^^^^^^^^^^^^^^^

DynamoDB's concurrency model is great, but using it correctly is tedious and unforgiving.
`Bloop manages that complexity for you.`__

__ https://gist.github.com/numberoverzero/9584cfc375de0e087c8e1ae35ab8559c

========
Features
========

* Simple declarative modeling
* Extensible type system, useful built-in types
* DynamoDBStreams interface that makes sense
* Secure expression-based wire format
* Simple atomic operations
* Expressive conditions
* Diff-based saves

==========
Ergonomics
==========

.. code-block:: python

    class Account(BaseModel):
        id = Column(UUID, hash_key=True)
        name = Column(String)
        email = Column(String)
        by_email = GlobalSecondaryIndex(projection='keys', hash_key='email')

    engine.bind(Account)

    some_account = Account(id=uuid.uuid4(), email='foo@bar.com')
    engine.save(some_account)

    q = engine.query(
        Account.by_email,
        key=Account.email == 'foo@bar.com')

    same_account = q.one()
    print(same_account.id)

Never worry about `trim horizons`__, `iterator types`__, or `tracking shard lineage`__ again:

.. code-block:: python

    # Enable Streams with the following lines in User.Meta:
    stream = {
        'include': {'new', 'old'}
    }

    # Start streaming!

    template = '''
    User event.
    Was: {old}
    Now: {new}
    Event Details:
    {meta}
    '''

    stream = engine.stream(User, 'trim_horizon')
    while True:
        record = next(stream)
        if record: print(template.format(**record)
        else: time.sleep(0.5)

__ http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_GetRecords.html#API_GetRecords_Errors
__ https://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_GetShardIterator.html#DDB-GetShardIterator-request-ShardIteratorType
__ https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html#Streams.Processing

===========
What's Next
===========

Head to the :ref:`user-install` section to install Bloop, or :ref:`user-quickstart` for a larger example!

.. toctree::
    :maxdepth: 2
    :caption: User Guide
    :hidden:

    user/install
    user/quickstart
    user/models
    user/engine
    user/query
    user/streams
    user/types
    user/conditions
    user/signals
    user/patterns

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
