Bloop: DynamoDB Modeling
^^^^^^^^^^^^^^^^^^^^^^^^

DynamoDB's concurrency model is great, but using it correctly is tedious and unforgiving.
Bloop manages that complexity for you.

Features
========

* Simple declarative modeling
* Extensible type system, useful built-in types
* Secure expression-based wire format
* Simple atomic operations
* Expressive conditions
* Diff-based saves

Installation
============
::

    pip install bloop

Quickstart
==========

First, define and bind our model:

.. code-block:: python

    import uuid

    from bloop import (
        BaseModel, Column, Engine,
        GlobalSecondaryIndex, String, UUID)


    class Account(BaseModel):
        id = Column(UUID, hash_key=True)
        name = Column(String)
        email = Column(String)
        by_email = GlobalSecondaryIndex(hash_key='email')

    engine = Engine()
    engine.bind(Account)

Save an instance, load by key, and get the first query result:

.. code-block:: python

    account = Account(
        id=uuid.uuid4(),
        name='username',
        email='foo@bar.com')
    engine.save(account)


    same_account = Account(id=account.id)
    engine.load(same_account)


    q = engine.query(
        Account.by_email,
        key=Account.email == "foo@bar.com")

    also_same_account = q.first()

Kick it up a notch with conditional operations:

.. code-block:: python

    # Exactly the same save as above, but now we
    # fail if the id isn't unique.
    if_not_exist = Account.id.is_(None)
    engine.save(account, condition=if_not_exist)


    # Update the account, as long as the name hasn't changed
    same_username = Account.name == "username"
    account.email = "new@email.com"
    engine.save(account, condition=same_username)


    # Delete the account, as long as none of the fields have
    # changed since we last loaded the account
    engine.delete(account, atomic=True)

.. toctree::
    :hidden:
    :maxdepth: 3

    user/models
    user/engine
    user/query
    user/types
    user/conditions
