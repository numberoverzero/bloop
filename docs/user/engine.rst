Using the Engine
^^^^^^^^^^^^^^^^

The :class:`~bloop.engine.Engine` is the main way you'll interact with DynamoDB (and DynamoDBStreams).
Once you've :ref:`defined some models <define-models>`, you're ready to start
:func:`loading <bloop.engine.Engine.load>`, :func:`saving <bloop.engine.Engine.save>` and
:func:`querying <bloop.engine.Engine.query>`.

.. attention::

    This section uses the same ``User`` model from the previous section.  If you've haven't already done so,
    :ref:`go back <define-models>` and set that up.


======
 Bind
======

As noted in the previous section, every model must first be bound to a backing table with
:func:`Engine.bind <bloop.engine.Engine.bind>` before we can interact with instances in DynamoDB.

When an engine binds a model, it also binds all non-abstract subclasses.  This means you can bind all models in one
call, centralizing any error handling or table correction.  For example, you may have specialized models for users,
notifications, and impressions.  Each of these can be grouped with an abstract base, and then all specialized models
created at once:

.. code-block:: python

    class BaseUser(BaseModel):
        class Meta:
            abstract = True

    class BaseNotification(BaseModel):
        class Meta:
            abstract = True

    ...

    class Admin(BaseUser):
        ...

    class Moderator(BaseUser):
        ...

    class PriorityNotification(BaseNotification):
        ...

    class EmailNotification(BaseNotification):
        ...


    try:
        engine.bind(BaseUser)
    except TableMismatch:
        print("Failed to bind all user models")

    try:
        engine.bind(BaseNotification)
    except TableMismatch:
        print("Failed to bind all notification models")

Now you can import a single base (:class:`~bloop.models.BaseModel` or a subclass) from your ``models.py`` module
and automatically bind any dynamic models created from that base.

.. _user-engine-save:

======
 Save
======

:func:`Save <bloop.engine.Engine.save>` is performed with `UpdateItem`_ since absolute overwrites (such as PutItem)
are rarely desired in a distributed, optimistic concurrency system.  This is the central decision that enables a
table to back multiple models.  A partial save allows a model to update an item in the table without accidentally
clearing the columns that model doesn't know about.

Saving an item or items is very simple:

.. code-block:: pycon

    >>> user = User(...)
    >>> engine.save(user)
    >>> tweet = Tweet(...)
    >>> user.last_activity = arrow.now()
    >>> engine.save(user, tweet)

You can perform optimistic saves with a ``condition``.  If a condition is not met when DynamoDB tries to apply the
update, the update fails and bloop immediately raises :exc:`~bloop.exceptions.ConstraintViolation`.  Conditions are
specified on columns using the standard ``<, >=, ==, ...`` operators, as well as
``begins_with, between, contains, in_``.  Conditions can be chained together and combined with bitwise operators
``&, |, ~``:

.. code-block:: pycon

    >>> user = User(username="numberoverzero")
    >>> username_available = User.username.is_(None)
    >>> engine.save(user, condition=username_available)
    # Success
    >>> engine.save(user, condition=username_available)
    Traceback (most recent call last):
      ...
    ConstraintViolation: The condition was not met.

A common use for conditions is performing atomic updates.  Save provides a shorthand for this, ``atomic=True``.  By
default saves are not atomic.  Bloop's specific definition of atomic is "only if the state in DynamoDB at time of
save is the same as the local state was aware of".  If you create a new User and perform an atomic save, it will
fail if there was any previous state for that hash/range key (since the expected state before the save was
non-existent).  If you fetch an object from a query which doesn't project all columns, only the columns that are
projected will be part of the atomic condition (not loading a column doesn't say whether we should expect it to have
a value or not).

.. seealso::

    Atomic conditions can be tricky, and there are subtle edge cases.  See the :ref:`Atomic Conditions <atomic>`
    section of the User Guide for detailed examples of generated atomic conditions.

If you provide a ``condition`` and ``atomic`` is True, the atomic condition will be ANDed with the condition to
form a single ConditionExpression.

.. code-block:: pycon

    >>> is_verified = User.verified.is_(True)
    >>> no_profile = User.profile.is_(None)
    >>> engine.save(
    ...     user,
    ...     condition=(is_verified & no_profile),
    ...     atomic=True)

.. _UpdateItem: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html

.. _user-engine-delete:

========
 Delete
========

:func:`Delete <bloop.engine.Engine.delete>` has the same signature as :func:`~bloop.engine.Engine.save`.  Both
operations are mutations on an object that may or may not exist, and simply map to two different apis (Delete calls
`DeleteItem`_).  You can delete multiple objects at once, specify a ``condition``, and use the ``atomic=True``
shorthand to only delete objects unchanged since you last loaded them from DynamoDB.

.. code-block:: pycon

    >>> engine.delete(user, tweet)
    >>> engine.delete(tps_report, atomic=True)
    >>> cutoff = arrow.now().repalce(years=-2)
    >>> engine.delete(
    ...     account,
    ...     condition=Account.last_login < cutoff)

.. _DeleteItem: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html

======
 Load
======

Unlike most existing DynamoDB object mappers, Bloop does not create new instances when loading objects.
This improves performance and makes atomic tracking much easier, and allows you to use thick or thin models by
minimizing how many times the constructor is invoked for effectively the same object (same hash/range keys).

Like :func:`~bloop.engine.Engine.save` and :func:`~bloop.engine.Engine.delete` above,
:func:`Engine.load <bloop.engine.Engine.load>` takes a variable number of objects to load from DynamoDB:

.. code-block:: pycon

    >>> user = User(id="some-id")
    >>> tweet = Tweet(user="some-id", id="some-tweet")
    >>> engine.load(user, tweet)

If ``consistent`` is True, then `strongly consistent reads`__ will be used:

.. code-block:: pycon

    >>> objs = user, tweet
    >>> engine.load(*objs, consistent=True)

If any objects aren't loaded, Bloop raises :exc:`~bloop.exceptions.MissingObjects`:

.. code-block:: pycon

    >>> user = User(username="not-real")
    >>> engine.load(user)
    Traceback (most recent call last):
      ...
    MissingObjects: Failed to load some objects.

You can access :data:`MissingObjects.objects <bloop.exceptions.MissingObjects.objects>` to see which objects failed
to load.

__ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
