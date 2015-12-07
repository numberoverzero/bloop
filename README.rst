.. image:: https://readthedocs.org/projects/bloop/badge?style=flat-square
    :target: http://bloop.readthedocs.org/
.. image:: https://img.shields.io/travis/numberoverzero/bloop/master.svg?style=flat-square
    :target: https://travis-ci.org/numberoverzero/bloop
.. image:: https://img.shields.io/coveralls/numberoverzero/bloop/master.svg?style=flat-square
    :target: https://coveralls.io/github/numberoverzero/bloop
.. image:: https://img.shields.io/pypi/v/bloop.svg?style=flat-square
    :target: https://pypi.python.org/pypi/bloop
.. image:: https://img.shields.io/pypi/status/bloop.svg?style=flat-square
    :target: https://pypi.python.org/pypi/bloop
.. image:: https://img.shields.io/github/issues-raw/numberoverzero/bloop.svg?style=flat-square
    :target: https://github.com/numberoverzero/bloop/issues
.. image:: https://img.shields.io/pypi/l/bloop.svg?style=flat-square
    :target: https://github.com/numberoverzero/bloop/blob/master/LICENSE


DynamoDB object mapper for python 3.4+

Installation
------------
::

    pip install bloop

Usage
-----

Define some models::

    import arrow
    import uuid
    from bloop import (Engine, Column, Integer, DateTime, UUID,
                       GlobalSecondaryIndex, String)
    engine = Engine()


    class Account(engine.model):
        class Meta:
            read_units = 5
            write_units = 2

        id = Column(UUID, hash_key=True)
        name = Column(String)
        email = Column(String)
        by_email = GlobalSecondaryIndex(
            hash_key='email', projection='keys_only',
            write_units=1, read_units=5)


    class Tweet(engine.model):
        class Meta:
            write_units = 10
        account = Column(UUID, hash_key=True)
        id = Column(String, range_key=True)
        content = Column(String)
        date = Column(DateTime)
        favorites = Column(Integer)

        by_date = GlobalSecondaryIndex(
            hash_key='date', projection='keys_only')

    engine.bind()


Create an instance::

    account = Account(id=uuid.uuid4(), name='@garybernhardt',
                      email='foo@bar.com')
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

    account = engine.query(Account.by_email)\
                   .key(Account.email == email).first()
    tweets = engine.query(Tweet)\
                   .key(Tweet.account == account.id)

    for tweet in tweets.filter(Tweet.date >= yesterday):
        print(tweet.content)


Versioning
----------

* bloop follows semver for its **public** API.

  * You should not rely on the internal api staying the same between minor
    versions.
  * Over time, private apis may be raised to become public.  The reverse
    will never occur.

Contributing
------------

Contributions welcome!  Please make sure ``tox`` passes (including flake8)
before submitting a PR.

Development
-----------

bloop uses ``tox``, ``pytest``, ``coverage``, and ``flake8``.  To get
everything set up with `pyenv`_::

    # RECOMMENDED: create a virtualenv with:
    #     pyenv virtualenv 3.4.3 bloop
    git clone https://github.com/numberoverzero/bloop.git
    pip install tox
    tox

.. _pyenv: https://github.com/yyuu/pyenv
