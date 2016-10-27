Getting Started
^^^^^^^^^^^^^^^

============
Installation
============

::

    pip install bloop

    # or

    git clone git://github.com/numberoverzero/bloop.git
    cd bloop && python setup.py install

==========
Quickstart
==========

First define a model and create the backing table in DynamoDB:

.. code-block:: python

    import uuid

    from bloop import (
        BaseModel, Column, Engine,
        GlobalSecondaryIndex, String, UUID)

    class Account(BaseModel):
        id = Column(UUID, hash_key=True)
        name = Column(String)
        email = Column(String)
        by_email = GlobalSecondaryIndex(
            projection='keys',
            hash_key='email')

    engine = Engine()
    engine.bind(Account)

To create an instance and save it in DynamoDB:

.. code-block:: python

    account = Account(
        id=uuid.uuid4(),
        name='username',
        email='foo@bar.com')
    engine.save(account)


You can load the account by ``id``, or query the GSI by ``email``:

.. code-block:: python

    same_account = Account(id=account.id)
    engine.load(same_account)

    q = engine.query(
        Account.by_email,
        key=Account.email == 'foo@bar.com')

    also_same_account = q.first()

Kick it up a notch with conditional operations:

.. code-block:: python

    # Only save if the account doesn't already exist
    if_not_exist = Account.id.is_(None)
    engine.save(account, condition=if_not_exist)

    # Only update the account if the name hasn't changed
    account.email = 'new@email.com'
    engine.save(account, condition=Account.name == 'username')

    # Only delete the account if it hasn't changed at all
    engine.delete(account, atomic=True)
