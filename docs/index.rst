Bloop: DynamoDB Modeling
^^^^^^^^^^^^^^^^^^^^^^^^

DynamoDB's concurrency model is great, but using it correctly is tedious and unforgiving.
`Bloop manages that complexity for you.`__

__ https://gist.github.com/numberoverzero/9584cfc375de0e087c8e1ae35ab8559c

Features
========

* Simple declarative modeling
* Extensible type system, useful built-in types
* DynamoDBStreams interface that makes sense
* Secure expression-based wire format
* Simple atomic operations
* Expressive conditions
* Diff-based saves

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

User Guide
==========

.. toctree::
    :maxdepth: 2

    user/index

API Reference
=============

Bloop's API is divided into two sections: public and internal.

Public API
----------

If you're looking for details on :func:`Engine.save <bloop.engine.Engine.save>` or want the first result
from a :class:`Query <bloop.search.QueryIterator>`, the Public API provides a breakdown of each class you'll encounter
during normal usage.

.. toctree::
    :maxdepth: 2

    api/public

Internal API
------------

Most users never need to interact with the Internal API.  For example, you can interact with the model creation
process by connecting to :data:`~bloop.signals.model_created`, instead of modifying the
:class:`metaclass <bloop.models.ModelMetaclass>` directly.

.. toctree::
    :maxdepth: 2

    api/internal

.. warning::

    Breaking changes to the Internal API can occur at any time.

About Bloop
-----------

Any size contribution is welcome!  If there's a section of the docs that was unclear or incorrect,
`open an issue`_ and it'll get some attention.

.. _open an issue: https://github.com/numberoverzero/bloop/issues/new

.. toctree::
    :maxdepth: 2

    meta
