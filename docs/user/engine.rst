Load, Save, and Delete
^^^^^^^^^^^^^^^^^^^^^^

All loading, saving, and deleting is done through a ``bloop.Engine``.

Reusing the model from :ref:`define-models`:

.. code-block:: python

    from bloop import (
        BaseModel, Boolean, Column, DateTime, String,
        UUID, GlobalSecondaryIndex)

    class User(BaseModel):
        id = Column(UUID, hash_key=True)
        email = Column(String)
        created_on = Column(DateTime)
        verified = Column(Boolean)
        profile = Column(String)

        by_email = GlobalSecondaryIndex(
            projection="keys", hash_key="email")

You can create a new instance with the base ``__init__``:

.. code-block:: python

    import arrow, uuid

    user = User(id=uuid.uuid4(),
                email="user@domain.com",
                created_on=arrow.now())

You'll need an engine to persist the user:

.. code-block:: python

    from bloop import Engine
    engine = Engine()
    engine.bind(BaseModel)

Save to DynamoDB:

.. code-block:: python

    engine.save(user)

Load the same user into a new object:

.. code-block:: python

    same_user = User(id=user.id)
    engine.load(same_user)

Delete the user in DynamoDB:

.. code-block:: python

    engine.delete(user)

====
Load
====

.. code-block:: python

    Engine.load(*objs, consistent: bool=False) -> None

.. attribute:: objs
    :noindex:

    Any number of objects to modify (may be from different models).

.. attribute:: consistent
    :noindex:

    Whether or not `strongly consistent reads`__ (which consume 2x read units) should be used.
    Defaults to False.

    __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html

You can load instances of different models at the same time.  Bloop will automatically split requests into the
appropriate chunks for BatchGetItems and then inject the results into the corresponding objects.

Raises ``NotLoaded`` if any items fail to load.  The ``objects`` attribute holds the set of objects not loaded.

.. _save-delete-interface:

===============
Save and Delete
===============

Save and Delete share the same interface; they both conditionally modify the state of an object in DynamoDB.

.. code-block:: python

    Engine.save(*objs,
                condition: Optional[bloop.Condition]=None,
                atomic: Optional[bool]=None) -> None

    Engine.delete(*objs,
                  condition: Optional[bloop.Condition]=None,
                  atomic: Optional[bool]=None) -> None

.. attribute:: objs
    :noindex:

    Any number of objects to modify (may be from different models).

.. attribute:: condition
    :noindex:

    Each object will only be modified if the condition holds for that object.  Defaults to None.

.. attribute:: atomic
    :noindex:

    Whether or not to use an atomic condition for this operation.  When True, DynamoDB and the local state must match
    to perform the operation (in addition to any other condition).  Defaults to False.

.. _save-delete-conditions:

---------------------
Conditions and Atomic
---------------------

:ref:`Conditions <conditions>` are expressed with the usual python comparisons (``<=``, ``>``, ``==``, ...)
as well as a few methods such as ``begins_with``, ``between``, and ``is_``.

For example, if the user must be verified in order to change their profile:

.. code-block:: python

    def update_profile(user_id, new_profile):
        user = User(id=user_id)
        engine.load(user)

        user.profile = new_profile

        # "is_" aliases "==" for equality tests against singletons
        # https://www.python.org/dev/peps/pep-0008/#id49
        is_verified = User.verified.is_(True)

        # Throws bloop.ConstraintViolation on failure
        engine.save(user, condition=is_verified)

This is much better than checking the ``verified`` property locally, since the property could change in DynamoDB
between when the user is loaded and when the save is executed.

When ``atomic`` is True, Bloop inserts a condition (or ANDs with a user-provided condition) that requires the state in
DynamoDB to match the last state that was loaded from DynamoDB.  For new objects, an atomic save requires that the
object not exist in DynamoDB.

Atomics can be tricky.  The generated atomic condition for an object returned from a query against an index
that doesn't project all columns will only include the projected columns.
