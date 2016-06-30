Bloop: DynamoDB Modeling
^^^^^^^^^^^^^^^^^^^^^^^^

DynamoDB's concurrency model is great, but using it correctly is tedious and unforgiving.  Bloop manages that complexity for you.

Features
========

* Simple declarative modeling
* Extensible type system, useful built-in types
* Safe expression-based wire format
* Simple atomic operations
* Diff-based saves
* Expressive conditions

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
        Engine, Column, UUID, String,
        GlobalSecondaryIndex, new_base)
    Base = new_base()


    class Account(Base):
        class Meta:
            read_units = 5
            write_units = 2
        id = Column(UUID, hash_key=True)
        name = Column(String)
        email = Column(String)
        by_email = GlobalSecondaryIndex(hash_key='email')

    engine = Engine()
    engine.bind(base=Base)

Save an instance, load by key, and get the first query result:

.. code-block:: python

    account = Account(
        id=uuid.uuid4(),
        name='username',
        email='foo@bar.com')
    engine.save(account)


    same_account = Account(id=account.id)
    engine.load(same_account)


    q = engine.query(Account.by_email) \
              .key(Account.email == "foo@bar.com")
    also_same = q.first()

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

    user/declarative_modeling
    user/save_load_delete
    user/query_scan
    user/conditions
    user/atomic
    user/indexes
    user/types
    user/custom_types
    user/configuration
    user/patterns
    user/advanced
    user/sample_calls
    dev/internals


.. _tricky code: https://gist.github.com/numberoverzero/f0633e71a6b0f3f6132e
.. _simpler code: https://gist.github.com/numberoverzero/94c939b4106e88b13e83
.. _conditional saves: https://gist.github.com/numberoverzero/91f15c041a94b66e9365
.. _atomic updates: https://gist.github.com/numberoverzero/cc004d93055cfa224569
