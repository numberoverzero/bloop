Working with Models
===================

.. _define:

Define Models
-------------

Every model must subclass a single engine's base model.  It isn't required to
call ``engine.bind`` immediately after the class is constructed - instances
can be created and modified locally without binding the model to DynamoDB.
This allows you to define models and then handle the binding in a try/except
to handle any failure modes (network failure, model mismatches).

When defining a model, you can specify an optional ``Meta`` attribute within
the class, which lets you customize properties of the table, as well as holding
most of the cached data used internally::

    class MyModel(engine.model):
        class Meta:
            # Defaults to class name
            table_name = 'MyCustomTableName'
            write_units = 10  # Defaults to 1
            read_units = 5    # Defaults to 1
        id = Column(Integer, hash_key=True)
        content = Column(Binary)

    engine.bind()

.. seealso::
    * :ref:`meta` for a full list of Meta's attributes.
    * :ref:`bind` for a detailed look at what happens when models are bound.

.. _create:

Create Instances
----------------

The ``engine.model`` base class provides an \_\_init\_\_ method that takes
\*\*kwargs and sets those values on the object (if they match a column).  For
the model above, you could do the following::

    instance = MyModel(content=b'hello, world', id=0, unused='not set')

    # AttributeError, not set by __init__
    print(instance.unused)

In this case ``unused`` is not set, since it is not a defined column for the
model.

It's not necessary to keep the default instantiation signature - for example,
you may want to only allow setting non-key attributes and let your init method
take care of generating a unique key for the instance.  If you change the init
signature, or want bloop to take a different path when instantiating instances
for any reason (to differentiate user creation from engine loading, for
example) you must set the model's ``Meta.bloop_init`` to a function that takes
``**kwargs`` and returns an instance of the model. You can find more details on
custom loading in the advanced section.

.. seealso::
    :ref:`loading` to customize the entry point for model creation.

Load
----

bloop loads data into existing models, instead of returning new instances.
This makes it easier to refresh instance data, as well as simplifying the
process for loading multiple objects at once.

Objects are loaded through an engine - either one at a time, or as a list::

    account = Account(id=uuid.uuid4())
    tweet = Tweet(account=account.id,
                  id='616102582239399936')

    engine.load(account)
    engine.load([account, tweet])
    engine.load(account, consistent=True)

    with engine.context(consistent=True) as consistent:
        consistent.load(account)

If any objects fail to load, a ``NotModified`` exception is raised with the
objects that were not loaded::

    try:
        engine.load([account, tweet])
    except bloop.NotModified as missing:
        if account in missing.objects:
            print("Account not loaded")
        if tweet in missing.objects:
            print("Tweet not loaded")

.. seealso::
    By default, consistent reads are not used.  You can read more about the
    ``consistent`` option in :ref:`config`.

Save
----

Like ``load``, one or more objects can be saved at a time::

    account = Account(id=uuid.uuid4(), name='@garybernhardt',
                      email='foo@bar.com')
    tweet = Tweet(
        account=account.id, id='600783770925420546', date=arrow.now(),
        content=(
            'Consulting service: you bring your big data problems'
            ' to me, I say "your data set fits in RAM", you pay me'
            ' $10,000 for saving you $500,000.'))

    engine.save(account)
    engine.save([account, tweet])

By default bloop uses `UpdateItem`_ to save objects.  Internally, the last
loaded state of an object is tracked.  When an object is saved, its current
values are diffed against the tracked values - only those that have changed
are sent in the update.

In the following example, bloop will send the ``content`` attribute to be
updated, since it was changed from the last loaded (it was never loaded)
value::

    tweet = Tweet(
        account=account.id, id='600783770925420546', date=arrow.now(),
        content=(
            'Consulting service: you bring your big data problems'
            ' to me, I say "your data set fits in RAM", you pay me'
            ' $10,000 for saving you $500,000.'))

    engine.save(tweet)

The following line will trigger an empty update, since none of the fields have
changed since the last load or save::

    engine.save(tweet)

Alternatively, `PutItem`_ can be used for full-overwrite saves.  This will
replace any existing attributes for the object, including deleting existing
values if the new version has no value for them.  To use this mode::

    engine.config['save'] = 'overwrite'

.. warning::

    Using ``overwrite`` saves can have unintented results when you load objects
    from a SecondaryIndex that doesn't project all attributes.

To demonstrate, consider the following::

    class Account(engine.model):
        id = Column(UUID, hash_key=True)
        name = Column(String)
        email = Column(String)
        by_email = GlobalSecondaryIndex(
            hash_key='email', projection='keys_only',
            write_units=1, read_units=5)

    account = Account(id=uuid.uuid4(), name='name',
                      email='foo@domain.com')
    engine.save(account)
    account = (engine.query(Account.by_email)
                     .key(email=='foo@domain.com')
                     .first())

At this point, ``account`` only has the attributes ``id`` and ``email`` because
the GSI ``by_email`` has a 'keys_only' projection.  If you overwrite the
account::

    engine.config['save'] = 'overwrite'
    account.email = 'bar@domain.com'
    engine.save(account)

And then load the account again, the name is missing::

    engine.load(account)
    print(account.name)  # AttributeError

Described below, :ref:`conditions` can be used to ensure attributes have
expected values before persisting a change.  When a condition is provided with
a list of objects, the condition is applied to every object individually.

.. seealso::
    * :ref:`config` to adjust ``save`` and ``atomic`` options
    * :ref:`tracking` to manually adjust the current tracking for an object
    * :ref:`conditions` for using conditions with save and delete
    * :ref:`atomic` for using atomic updates

.. _UpdateItem: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
.. _PutItem: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
.. _Secondary Indexes: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html

Delete
------

Like ``load`` and ``save``, one or more objects can be deleted at a time::

    account = Account(id=uuid.uuid4(), name='@garybernhardt',
                      email='foo@bar.com')
    tweet = Tweet(
        account=account.id, id='600783770925420546', date=arrow.now(),
        content=(
            'Consulting service: you bring your big data problems'
            ' to me, I say "your data set fits in RAM", you pay me'
            ' $10,000 for saving you $500,000.'))

    engine.delete(account)
    engine.delete([account, tweet])

Described below, :ref:`conditions` can be used to ensure attributes have
expected values before persisting a change.  When a condition is provided with
a list of objects, the condition is applied to every object individually.

.. seealso::
    * :ref:`config` to adjust the ``atomic`` option
    * :ref:`tracking` to manually adjust the current tracking for an object
    * :ref:`conditions` for using conditions with save and delete
    * :ref:`atomic` for using atomic updates

.. _conditions:

Conditions
----------

Conditions are a great way to reduce some of the complexities of managing
highly concurrent modifications.  While Dynamo doesn't have native
transactions (yet?), conditions let you do a pretty good impression::

    instance = Model(id='unique', counter=0)
    engine.save(instance)

    instance.counter += 1
    still_zero = Model.counter == 0

    # Succeeds, because the persisted value is 0
    engine.save(instance, condition=still_zero)

    # Fails, because the persisted value is 1,
    # and the condition fails.
    engine.save(instance, condition=still_zero)

There are a `handful of conditions`_ available, which are cleanly exposed in
bloop through the ``Column`` class.  To construct a condition that a tweet's
content contains the word 'secret'::

    has_secrets = Tweet.content.contains("secret")

This condition is independent of any instance of a ``Tweet``, which lets you
re-use it across queries, as a condition when saving or deleting instances, or
combining with other conditions.

Conditions can be combined and mutated with bitwise operators::

    no_secrets = ~has_secrets
    secrets_or_empty = has_secrets | (Tweet.content.is_(None))
    secrets_and_nsa = hash_secrets & (Tweet.user == '@nsa')

All of the conditions use python objects, so datetime comparisons are easy::

    now = arrow.now()
    last_week = now.replace(weeks=-1)

    old_tweets = Tweet.date <= last_week
    tweets = engine.scan(Tweet).filter(old_tweets)

To check between two dates::

    two_days_ago = now.replace(days=-2)
    one_day_ago = now.replace(days=-1)

    yesterday = Tweet.date.between(
        two_days_ago, one_day_ago)

    tweets = (engine.query(Tweet)
                    .key(Tweet.user == '@nsa')
                    .filter(yesterday)
                    .all())

In fact, the ``key`` function aboive is using an equality condition.

When saving or deleting an object, you can use conditions to ensure the row's
data hasn't changed since it was last loaded.  This keeps from racing between
the load and the save, where another caller could modify the value and make the
save or delete violate some business logic.

Let's say user accounts are deleted if the last login was over two years ago.
Without a condition, the following could delete a user right after they logged
in, which would be pretty terrible::

    user = User(id=some_id)
    engine.load(user)
    two_years = arrow.now().replace(years=-2)

    if user.login <= two_years:
        # If the user logs in AFTER we check the condition but BEFORE
        # the following delete, the account will
        # be deleted right after the login!
        engine.delete(user)

Instead, a simple condition will prevent the race::

    user = User(id=some_id)
    engine.load(user)
    two_years = arrow.now().replace(years=-2)

    if user.login <= two_years:
        # If the user logs in AFTER we check the condition but BEFORE
        # the following delete, the condition will
        # fail and the user WON'T be deleted.
        too_old = User.login <= two_years

        engine.delete(user, condition=two_years)

The following comparison operators are available::

* ``==``
* ``!=``
* ``<=``
* ``>=``
* ``<``
* ``>``

Because of how python handles ``__contains__`` internally, you'll need to use
``Model.column.in_(values)`` instead of a simple ``Model.column in values``;
the same is true of ``is`` and ``is not``.  The other operators are:

* ``in_(iterable)``
* ``is_(value)``
* ``is_not(value)``
* ``begins_with(value)``
* ``between(low, high)``
* ``contains(value)``

Note that ``is_`` and ``is_not`` simply alias ``==`` and ``!=``, mostly so you
can avoid lint issues with comparisons against True/False/None.

.. warning::

    Because the ``Column`` class overrides the ``__eq__`` method, functions
    that rely on its return value will almost certainly break.  For example,
    checking if a list of column instances contains a specific column will fail
    because the first check will return a Condition, which is Truthy::

        assert Tweet.date in [0, False, 'Nope']

    It is safe to rely on ``__hash__`` which ensures ``object.__hash__`` is
    used.  Data structures that rely on hash over eq (such as ``set``) are
    perfectly fine (and are used extensively in the model's :ref:`meta`).

.. _handful of conditions: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html

.. _atomic:

Atomic
------

With ``atomic`` you can ensure there have been no changes to the persisted
object between the last load and the current save/delete operation.  This is
useful in highly concurrent systems - without this setting, here's what an
atomic update looks like::

    instance = Model(hash=0, range=1)
    engine.load(instance)

    previous_foo = instance.foo
    previous_bar = instance.bar
    condition = ((Model.foo == previous_foo) &
                 (Model.bar == previous_bar))

    instance.foo = 'new foo'
    try:
        engine.save(instance, condition=condition)
    except bloop.ConstraintViolation:
        # Modified between load and save!
        ...

With atomic updates::

    instance = Model(hash=0, range=1)
    engine.load(instance)

    instance.foo = 'new foo'
    try:
        engine.save(instance)
    except bloop.ConstraintViolation:
        # Modified between load and save!
        ...

Additionally, you don't need to keep track of which attributes were loaded by
the operation that generated the object.  Because a query may not return all
attributes of the object, you would erroneously expect an empty value when the
operation could never populate those attributes.  For example, say the
following only loads the ``hash`` and ``range`` attributes of the model::

    instance = (engine.query(Model.some_index)
                      .key(Model.range == 1)
                      .first())

This instance hasn't loaded the ``foo`` attribute, even though there's a value
persisted in dynamo.  Naively building a condition, you'd have something like::

    condition = bloop.Condition()
    if hasattr(instance, 'foo'):
        condition &= Model.foo == instance.foo
    else:
        condition &= Model.foo.is_(None)

This would fail even if there were no changes, since the persisted row has a
value for ``foo``; it simply wasn't loaded!

bloop takes care of this tracking for us.  Internally, the last persisted state
of an object is stored.  When querying an index, the projected attributes that
are available to the index are used to differentiate which attributes were
expected but missing, and which were not loaded.

Finally, conditions can be used with atomic updates - this allows you to
constrain operations on attributes that may not have been loaded.  Using the
same model above where ``foo`` is a non-key attribute that's not loaded from a
query::

    instance = (engine.query(Model.some_index)
                      .key(Model.range == 1)
                      .first())

    with engine.context(atomic=True) as atomic:
        big_foo = Model.foo >= 500
        atomic.save(instance, condition=big_foo)

.. seealso::
    * :ref:`tracking` for details on the tracking algorithm, as well as ways to
      manually change what is considered tracked.
    * The ``atomic`` option in :ref:`config` to enable/disable atomic
      conditions for save and delete.

Query
-----

.. seealso::
    * The ``strict`` option in :ref:`config` to prevent double reads on LSIs
    * The ``prefetch`` option in :ref:`config` to control how lazily results
      are loaded.

Scan
----

.. seealso::
    * The ``strict`` option in :ref:`config` to prevent double reads on LSIs
    * The ``prefetch`` option in :ref:`config` to control how lazily results
      are loaded.

.. _meta:

Meta
----

.. warning::
    Modifying the generated values in a model's ``Meta`` will result in
    **bad things**, including things like not saving attributes, loading values
    incorrectly, and kicking your dog.

Discussed above, the ``Meta`` attribute of a model class stores info about the
table (read and write units, the table name) as well as metadata used by bloop
internally (like ``bloop_init``).

Meta exposes the following attributes:

* ``read_units`` and ``write_units`` - mentioned above, the table read/write
  units.  Both default to 1.
* ``table_name`` - mentioned above, the name of the table.  Defaults to the
  class name.
* ``bloop_init`` - covered in detail in :ref:`loading`, this is the entry point
  bloop uses when creating new instances of a model.  It is NOT used during
  ``bloop.load`` which updates attributes on existing instances.
* ``colums`` - a ``set`` of ``Column`` objects that are part of the model.
* ``indexes`` - a ``set`` of ``Index`` objects that are part of the model.
* ``hash_key`` - the ``Column`` that is the model's hash key.
* ``range_key`` - the ``Column`` that is the model's range key.  Is ``None`` if
  there is no range key for the table.
* ``bloop_engine`` - the engine that the model is associated with.  It may not
  be bound yet.
