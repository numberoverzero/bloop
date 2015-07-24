Better living through declarative modeling
=================================================

DynamoDB is great.  Unfortunately, it requires some `tricky code`_ for common
operations.  **It doesn't need to be like this**.

Bloop's declarative modeling enables `simpler code`_, while still
exposing advanced DynamoDB features like `conditional saves`_ and
`atomic updates`_.

.. warning::
    While fully usable, bloop is still pre-1.0 software and has **no**
    backwards compatibility guarantees until the 1.0 release occurs!

----

Define some models:

.. literalinclude:: code/models.py
    :lines: 1-26

Create an instance::

    account = Account(id=0, name='@garybernhardt', email='foo@bar.com')
    tweet = Tweet(
        account=account.id, id='600783770925420546', date=arrow.now(),
        content=(
            'Consulting service: you bring your big data problems'
            ' to me, I say "your data set fits in RAM", you pay me'
            ' $10,000 for saving you $500,000.'))
    engine.save([account, tweet])

Query or scan by column values::

    email = 'foo@bar.com'
    yesterday = arrow.now().replace(days=-1)

    acount = engine.query(Account.by_email)\
                   .key(Account.email == email)\
                   .first()
    tweets = engine.query(Tweet)\
                   .key(Tweet.acount == acount.id)

    for tweet in tweets.filter(Tweet.date >= yesterday):
        print(tweet.content)


.. toctree::
    :hidden:
    :maxdepth: 2

    user/installation
    user/quickstart
    user/getting_started
    user/engine
    user/models
    user/advanced
    dev/contributing

.. _tricky code: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-query_boto3-py
.. _simpler code: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-query_bloop-py
.. _conditional saves: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-conditional_save-py
.. _atomic updates: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-atomic_update-py
