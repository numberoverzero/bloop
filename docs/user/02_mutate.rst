Save and Delete Objects
^^^^^^^^^^^^^^^^^^^^^^^

For the following, let's use the model from :ref:`define-models`:

.. code-block:: python

    from bloop import (
        new_base, GlobalSecondaryIndex,
        Boolean, Column, DateTime, String, UUID)
    BaseModel = new_base()

    class User(BaseModel):
        id = Column(UUID, hash_key=True)
        email = Column(String)
        created_on = Column(DateTime)
        verified = Column(Boolean)
        profile = Column(String)

        by_email = GlobalSecondaryIndex(
            projection="keys", hash_key="email")

And create a new instance:

.. code-block:: python

    import arrow, uuid

    user = User(id=uuid.uuid4(),
                email="joe.mcross@gmail.com",
                created_on=arrow.now())

You need an engine to persist the user:

.. code-block:: python

    from bloop import Engine
    engine = Engine()
    engine.bind(BaseModel)

To save:

.. code-block:: python

    engine.save(user)

Deleting the user is also straightforward:

.. code-block:: python

    engine.delete(user)

====
Save
====

.. code-block:: python

    Engine.save(*objs, condition=None, atomic=None)

**\*objs**
    | *(required)*
    | Any number of objects to save (may be from different models).
**condition**
    | *(defaults is None)*
    | Each object will only be saved if the condition holds for that object
**atomic**
    | *(defaults is None, uses engine.config["atomic"])*
    | DynamoDB and the local state must match to perform the save.
