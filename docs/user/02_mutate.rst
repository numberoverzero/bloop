Save and Delete Objects
^^^^^^^^^^^^^^^^^^^^^^^

For the following, let's use the model from :ref:`define-models`:

.. code-block:: python

    from bloop import (
        Boolean, Column, DateTime, String, UUID,
        GlobalSecondaryIndex, new_base)
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
                email="user@domain.com",
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

=========
Interface
=========

Save and Delete share the same interface; they both conditionally modify the state of an object in DynamoDB.

.. code-block:: python

    Engine.save(*objs,
                condition: Optional[bloop.Condition]=None,
                atomic: Optional[bool]=None) -> None

    Engine.delete(*objs,
                  condition: Optional[bloop.Condition]=None,
                  atomic: Optional[bool]=None) -> None

.. attribute:: objs

    Any number of objects to modify (may be from different models).

.. attribute:: condition

    Each object will only be modified if the condition holds for that object.  Defaults to None.

.. attribute:: atomic

    Whether or not to use an atomic condition for this operation.  When True, DynamoDB and the local state must match
    to perform the operation (in addition to any other condition).  Defaults to ``engine.config["atomic"]``

=====================
Conditions and Atomic
=====================

:ref:`Conditions <conditions>` are expressed with the usual python comparisons (``<=``, ``>``, ``==``, ...)
as well as a few methods such as ``begins_with``, ``between``, and ``is_``.

For example, if the user must be verified in order to change their profile:

.. code-block:: python

    def update_profile(user_id, new_profile):
        user = User(id=user_id)
        engine.load(user)

        user.profile = new_profile

        # is_ aliases == for equality tests against singletons
        # https://www.python.org/dev/peps/pep-0008/#id49
        is_verified = User.verified.is_(True)

        # Throws bloop.ConstraintViolation on failure
        engine.save(user, condition=is_verified)

This is much better than checking the ``verified`` property locally, since the property could change in DynamoDB
between when the user is loaded and when the save is executed.

When ``atomic`` is True, bloop inserts a condition (or ANDs with a user-provided condition) that requires the state in
DynamoDB to match the last state that was loaded from DynamoDB.  For new objects, an atomic save requires that the
object not exist in DynamoDB.

There are caveats to consider when using automatic atomic conditions.  For example an object loaded a query
against an index that doesn't project all columns will only build an atomic condition against those columns that were
loaded.

.. seealso::

    | :ref:`conditions`:
    |     :ref:`available-conditions` -- the full list of built-in conditions
    |     :ref:`atomic` -- examples and limitations of ``atomic=True``
