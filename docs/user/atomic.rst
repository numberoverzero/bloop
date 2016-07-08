Atomic Operations
=================

TODO copied from another draft

For example, if we save the following object:

.. code-block:: python

    now = arrow.now()
    # where id is the hash key, and we
    # didn't set the email or age columns
    user = User(
        id=10, nick="numberoverzero",
        created_on=now)
    engine.save(user)

Then the following two calls are equivalent:

.. code-block:: python

    # Construct the condition to ensure
    # nothing's changed since we saved the object
    condition = (User.id == 10) \
                & (User.nick == "numberoverzero") \
                & (User.created_on == now) \
                & (User.email.is_(None)) \
                & (User.age.is_(None))
    engine.save(user, condition=condition, atomic=False)

    # Or, use an atomic save
    engine.save(user, atomic=True)

These are simple atomic conditions - the condition is built by iterating the set of columns that were loaded from
DynamoDB through ``Engine.load``, ``Engine.query``, ``Engine.scan``, or saved to DynamoDB through ``Engine.save`` or
``Engine.delete``.
