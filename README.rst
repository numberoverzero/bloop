.. image:: https://readthedocs.org/projects/bloop/badge?style=flat-square
    :target: http://bloop.readthedocs.org/
.. image:: https://img.shields.io/travis/numberoverzero/bloop/master.svg?style=flat-square
    :target: https://travis-ci.org/numberoverzero/bloop
.. image:: https://img.shields.io/codecov/c/github/numberoverzero/bloop/master.svg?style=flat-square
    :target: https://codecov.io/gh/numberoverzero/bloop/branch/master
.. image:: https://img.shields.io/gitter/room/numberoverzero/bloop.svg?style=flat-square
    :target: https://gitter.im/numberoverzero/bloop
.. image:: https://img.shields.io/pypi/v/bloop.svg?style=flat-square
    :target: https://pypi.python.org/pypi/bloop

Bloop is an object mapper for DynamoDB and DynamoDBStreams.

Requires Python 3.6+

::

    pip install bloop

=======
 Usage
=======

First, we need to import all the things:

.. code-block:: python

    >>> from bloop import (
    ...     BaseModel, Boolean, Column, String, UUID,
    ...     GlobalSecondaryIndex, Engine
    ... )

Next we'll define the account model (with streaming enabled), and create the backing table:

.. code-block:: python

    >>> class Account(BaseModel):
    ...     class Meta:
    ...         stream = {
    ...             "include": {"old", "new"}
    ...         }
    ...     id = Column(UUID, hash_key=True)
    ...     name = Column(String)
    ...     email = Column(String)
    ...     by_email = GlobalSecondaryIndex(projection='keys', hash_key='email')
    ...     verified = Column(Boolean, default=False)
    ...
    >>> engine = Engine()
    >>> engine.bind(Account)

Let's make a few users and persist them:

.. code-block:: python

    >>> import uuid
    >>> admin = Account(id=uuid.uuid4(), email="admin@domain.com")
    >>> admin.name = "Admin McAdminFace"
    >>> support = Account(name="this-is-fine.jpg", email="help@domain.com")
    >>> support.id = uuid.uuid4()
    >>> engine.save(admin, support)

Or do the same in a transaction:

.. code-block:: python

    >>> with engine.transaction() as tx:
    ...     tx.save(admin)
    ...     tx.save(support)
    ...
    >>>

And find them again:

.. code-block:: python

    >>> q = engine.query(
    ...     Account.by_email,
    ...     key=Account.email=="help@domain.com"
    ... )
    >>> q.first()
    Account(email='help@domain.com',
            id=UUID('d30e343f-f067-4fe5-bc5e-0b00cdeaf2ba'),
            verified=False)

.. code-block:: python

    >>> s = engine.scan(
    ...     Account,
    ...     filter=Account.name.begins_with("Admin")
    ... )
    >>> s.one()
    Account(email='admin@domain.com',
            id=UUID('08da44ac-5ff6-4f70-8a3f-b75cadb4dd79'),
            name='Admin McAdminFace',
            verified=False)

Let's find them in the stream:

.. code-block:: python

    >>> stream = engine.stream(Account, "trim_horizon")
    >>> next(stream)
    {'key': None,
     'meta': {'created_at': datetime.datetime(...),
      'event': {'id': 'cbb9a9b45eb0a98889b7da85913a5c65',
       'type': 'insert',
       'version': '1.1'},
      'sequence_number': '100000000000588052489'},
     'new': Account(
                email='help@domain.com',
                id=UUID('d30e343f-...-0b00cdeaf2ba'),
                name='this-is-fine.jpg',
                verified=False),
     'old': None}
    >>> next(stream)
    {'key': None,
     'meta': {'created_at': datetime.datetime(...),
      'event': {'id': 'cbdfac5671ea38b99017c4b43a8808ce',
       'type': 'insert',
       'version': '1.1'},
      'sequence_number': '200000000000588052506'},
     'new': Account(
                email='admin@domain.com',
                id=UUID('08da44ac-...-b75cadb4dd79'),
                name='Admin McAdminFace',
                verified=False),
     'old': None}
    >>> next(stream)
    >>> next(stream)
    >>>

=============
 What's Next
=============

Check out the `User Guide`_ or `Public API Reference`_ to create your own nested types, overlapping models,
set up cross-region replication in less than 20 lines, and more!

.. _User Guide: https://bloop.readthedocs.io/en/latest/user/quickstart.html
.. _Public API Reference: https://bloop.readthedocs.io/en/latest/api/public.html
