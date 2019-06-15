.. _user-quickstart:

Quickstart
^^^^^^^^^^

First define a model and create the backing table in DynamoDB:

.. code-block:: pycon

    >>> import uuid
    >>> from bloop import (
    ...     BaseModel, Boolean, Column, Engine,
    ...     GlobalSecondaryIndex, String, UUID)
    ...
    >>> class Account(BaseModel):
    ...     id = Column(UUID, hash_key=True)
    ...     name = Column(String)
    ...     email = Column(String)
    ...     by_email = GlobalSecondaryIndex(
    ...         projection='keys',
    ...         hash_key='email')
    ...     verified = Column(Boolean, default=False)
    ...
    >>> engine = Engine()
    >>> engine.bind(Account)

To create an instance and save it in DynamoDB:

.. code-block:: pycon

    >>> account = Account(
    ...     id=uuid.uuid4(),
    ...     name='username',
    ...     email='foo@bar.com')
    ...
    >>> engine.save(account)


You can load the account by ``id``, or query the GSI by ``email``:

.. code-block:: pycon

    >>> same_account = Account(id=account.id)
    >>> engine.load(same_account)
    >>> q = engine.query(
    ...     Account.by_email,
    ...     key=Account.email == 'foo@bar.com')
    ...
    >>> also_same_account = q.first()

Kick it up a notch with conditional operations:

.. code-block:: pycon

    # Only save if the account doesn't already exist
    >>> if_not_exist = Account.id.is_(None)
    >>> engine.save(account, condition=if_not_exist)

    # Only update the account if the name hasn't changed
    >>> account.email = 'new@email.com'
    >>> engine.save(account, condition=Account.name == 'username')

    # Only delete the account if the email hasn't changed since we last saved
    >>> engine.delete(account, condition=Account.email == "new@email.com")


Or load the last state of an object before it was deleted:

.. code-block:: pycon

    >>> engine.delete(account, sync="old")
    >>> print(f"last email was {account.email}")
