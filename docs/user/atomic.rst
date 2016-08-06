.. _atomic-operations:

Atomic Operations
^^^^^^^^^^^^^^^^^

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

Quick Example
=============

Network Failures
================

increment an integer and get a 500 - read again and it's incremented.  was that us, or someone else?

Consistent vs Atomic
====================

'latest version at time of call' vs 'unchanged since last load'

Usage
=====

atomic=True
engine config

Scenarios
=========

New Instance
------------

atomic save on an instance before it's been saved or loaded from dynamo

Loaded
------

loaded from dynamo, not changed

Scenario C: Loaded

loaded from dynamo, modified by another writer

Partial Query
-------------

query doesn't load all columns
atomic condition only on loaded
(other writer can modify not loaded column)
