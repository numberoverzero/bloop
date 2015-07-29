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

.. _load:

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

.. _save:

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

.. _delete:

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

.. _query:

Query
-----

Queries can be constructed against tables or an index of the table using the
same syntax::

    table_query = engine.query(Model)
    index_query = engine.query(Model.some_index)

Queries are constructed by chaining methods together- including key conditions,
filter conditions, select methods, and properties to enable consistent reads
and control query order.

Because each chained call returns a copy of the query, it's possible to create
re-usable base queries::

    base_query = engine.query(Model).consistent.ascending

    for obj in base_query.key(Model.hash == 1).all():
        ...
    for obj in base_query.key(Model.hash == 2).all():
        ...

The ``key`` method takes a condition on the hash key.  You may optionally
include a range key condition.  Not all operators are supported for key
conditions.  Valid conditions are::

    ==, <=, <, >=, >, begins_with, between

To include a range key condition, use the bitwise AND operator::

    hash_condition = Model.hash == 1
    range_condition = Model.range == 2

    query = base_query.key(hash_condition & range_condition)

You may also construct the key condition in two pieces, with the hash condition
first::

    query = base_query.key(hash_condition)
    query = query.key(range_condition)

With the ``filter`` method you can construct a `FilterExpression`_ using the
same :ref:`conditions` that you use everywhere else.  Unlike the ``key``
method, you may use any condition type.

When chaining ``filter`` calls together, the conditions will be ANDed together.
From the API reference: `A filter expression lets you apply conditions to the
data after it is queried or scanned, but before it is returned to you. Only the
items that meet your conditions are returned.`

A few examples::

    query = base_query.filter(Model.foo >= 100)
    query = base_query.filter(Model.bar.contains('hello'))

    # (foo is None) AND (bar in [1, 2])
    query = base_query.filter(Model.foo.is_(None))\
                      .filter(Model.bar.in_([1, 2]))

    # equivalent filter with explicit AND
    query = base_query.filter(Model.foo.is_(None) &
                              Model.bar.in_([1, 2]))

By default, **projected** attributes are loaded for a query against a
SecondaryIndex and **all** attributes are loaded for a table query.  You can
change the set of attributes to be loaded with the ``select`` method::

    projected = base_query.select('projected')
    everything = base_query.select('all')

You may specify a set of attributes to load by passing a list of
column objects::

    specific = base_query.select([Model.foo, Model,bar])

There are a few combinations of ``select`` options and table/index
configurations that are invalid.  All of the following will raise an exception:

* ``projected`` for a non-index query
* ``all`` against a GlobalSecondaryIndex whose projection is not ``all``
* list of columns against a GSI where the requested columns are not projected
* ``all`` against a LSI **and the strict option is enabled**
* list of columns against a LSI where the requested columns are not projected
  **and the strict option is enabled**

The first should be obvious - only a SecondaryIndex has a projection.

While it's possible for a GSI with a key-only projection to include all
attributes, this is not guaranteed to be true forever.  Instead of behavior
subtly changing when a column is added, bloop refuses to assume.

When a query against a GSI requests attributes that are not projected into the
index, the Dynamo will raise.  Because GSIs have their own read units, a
second read against the table is not performed for you.

When strict is enabled, LSIs perform the same checks as GSIs.  Without strict,
**Dynamo will incur an additional read per item** to load the requested
attributes.

.. tip::

    Currently ``strict`` defaults to ``False``, matching Dynamo's default
    behavior.  It is **HIGHLY** recommended to set ``strict=True`` at all
    times, as it can be hard to plan which LSI queries will incur additional
    reads - an inconspicuous code change that adds a new attribute to a query's
    ``select`` may suddenly cause a critical-path query to double in consumed
    read units.

To execute a query, either iterate the query object or use the ``all`` method::

    for result in query:
        print(result.foo)

    # Keep a reference to the result container
    results = query.all()
    for result in results:
        ...

Each iteration of the query will result in a new set of calls to Dynamo;
whereas iterating over the return from ``all()`` will iterate over a cached
set of calls to Dynamo.  Additionally, the object returned from ``all``
provides metadata about the query, including ``count`` and ``scanned_count``
attributes::

    results = query.all()

    # Raises, since the query is not fully iterated
    results.count

    # exhaust the query
    list(results)
    print(results.count, results.scanned_count)

    # iterating the results object will iterate the
    # cached results, NOT re-issue the query to Dynamo
    for result in results:
        ...

You may optionally specify a ``prefetch`` value when calling ``all``, that
controls how paginated results are loaded.  The default prefetch is 0, which
means pages are only loaded as the previous results are consumed from the
iterator.  This is useful when you are only interested in the first result of
a query, or otherwise may not need the full set of results::

    results = query.all(prefetch=0)

If you know you need all results, and the set of results is small, you may want
to pre-load all values from Dynamo before continuing::

    results = query.all(prefetch='all')

Finally, you may want to load a certain number of pages in advance::

    results = query.all(prefetch=3)
    results = query.all(prefetch=10)


You can also fetch the first result from a query directly, or from the return
from ``all``::

    first = base_query.first()

    results = query.all(prefetch=0)
    first = results.first

.. seealso::
    * The ``strict`` option in :ref:`config` to prevent double reads on LSIs
    * The ``prefetch`` option in :ref:`config` to control how lazily results
      are loaded.

.. _FilterExpression: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#FilteringResults

.. _scan:

Scan
----

Scan has the same interface as :ref:`query` above, with the following
differences:

* Any ``key`` conditions are ignored completely when constructing the request.
* The ``ascending``, ``consistent``, and ``descending`` properties are ignored.

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
