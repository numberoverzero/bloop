Working with Models
===================

.. _model:

Define Models
-------------

The start of any model is the base model.  You can get one through the
``new_base`` function::

    from bloop import new_base
    Base = new_base()

Every model must subclass a Base (possibly indirectly - see note on abstract
models).  The backing tables for models aren't created at class definition, but
when the model (or any of its parent classes) is bound to an engine using
``engine.bind``.  This allows you to define models and then handle the binding
in a try/except to handle any failure modes (network failure, model
mismatches).

When defining a model, you can specify an optional ``Meta`` attribute within
the class, which lets you customize properties of the table, as well as holding
most of the cached data used internally::

    Base = new_base()


    class MyModel(Base):
        class Meta:
            # Defaults to class name
            table_name = 'MyCustomTableName'
            write_units = 10  # Defaults to 1
            read_units = 5    # Defaults to 1
        id = Column(Integer, hash_key=True)
        content = Column(Binary)

    engine.bind(base=Base)

When determining the layout for your data in DynamoDB, you should carefully
review the `Limits`_ documentation to estimate the throughput required to load
and save one object (or a partial object, when using indexes).  Of particular
note are the **Item Size** and **Attribute name lengths** limits, which are
400 KB and 255 characters, respectively.  Additionally, attribute names count
towards the size limit and the consumed read/write units.

To help save on these limits without using obscure one letter attribute names,
model columns offer the **name** parameter, which can specify a value other
than the model's name for reading and writing.  We can rewrite the above
example as such::

    class MyModel(Base):
        class Meta:
            table_name = 'MyCustomTableName'
            write_units = 10
            read_units = 5
        id = Column(Integer, hash_key=True, name='h')
        content = Column(Binary, name='c')

Now the table will use the short names 'h' and 'c' in Dynamo, and map these
to the model's ``id`` and ``content`` attributes.  For other cross-cutting
columnar concerns (nullable, validation) you'll want to subclass Column and
attach your own kwargs.

.. note::
    A model is considered abstract if its ``Meta.abstract`` attribute is true.
    The model created by ``new_base`` is always abstract.  The same rules of
    model discovery (covered in :ref:`bind`) are followed for abstract and
    concrete models when binding to an engine.  While abstract models can be
    dumped and loaded into dynamo-compatible representations using an engine's
    protected methods, ``load``, ``save``, ``delete`` will fail.

    Abstract models must be explicitly declared so - by default, all model
    subclasses are assumed to be concrete.

.. seealso::
    * :ref:`meta` for a full list of Meta's attributes.
    * :ref:`bind` for a detailed look at what happens when models are bound.
    * :ref:`custom-columns` for extending the Column modeling.
    * :ref:`abstract` for limitations and examples of using abstract models

.. _Limits: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html

.. _create:

Create Instances
----------------

The base model provides an ``__init__`` method that takes
``**kwargs`` and sets those values on the object (if they match a column).  For
the model above::

    instance = MyModel(content=b'hello, world', id=0, unused='not set')

    # AttributeError, not set by __init__
    print(instance.unused)

In this case ``unused`` is not set, since it is not a defined column for the
model.

It's not necessary to keep the default instantiation signature.  For example,
you may want to only allow setting non-key attributes and let your init method
take care of generating a unique key for the instance.  If you change the init
signature, or want bloop to take a different path when instantiating instances
for any reason (to differentiate user creation from engine loading, for
example) you must set the model's ``Meta.init`` to a function with no arguments
that returns an instance of the model. You can find more details on custom
loading in the advanced section.


.. note::
    When using the default init provided by the base model, it expects the
    python version of values; for instance, a UUID should be provided as
    ``uuid.uuid4()`` instead of ``"241b13b9-857b-432d-ac9e-3ae5f054f131"``.

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
    engine.save([account, tweet], atomic=True)

bloop uses `UpdateItem`_ to save objects, tracking which fields on an instance
of a model have been set or deleted.  When an object is saved, any values that
have been loaded (if the object was loaded or part of a query/scan result) or
set/deleted locally are sent in the update.  This is true even if the value
hasn't changed locally; bloop persists the expected local state, not the
expected local delta.

Described below, :ref:`conditions` can be used to ensure attributes have
expected values before persisting a change.  When a condition is provided with
a list of objects, the condition is applied to every object individually.

.. seealso::
    * :ref:`config` to adjust the ``atomic`` option
    * :ref:`conditions` for using conditions with save and delete
    * :ref:`atomic` for using atomic updates

.. _UpdateItem: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
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
    engine.delete([account, tweet], atomic=True)

Described below, :ref:`conditions` can be used to ensure attributes have
expected values before persisting a change.  When a condition is provided with
a list of objects, the condition is applied to every object individually.

.. seealso::
    * :ref:`config` to adjust the ``atomic`` option
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
    # and the condition isn't valid.
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

.. note::

    Keep in mind that in Python, `comparisons have lower priority than bitwise
    operations`_, which means that parentheses should be used when combining
    comparisons::

        # Correct AND of two conditions, one on hash and one on range
        both = (Model.hash == 1) & (Model.range > 2)

        # INCORRECT: & will bind on (1 & Model.range)
        wrong = Model.hash == 1 & Model.range > 2

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
                    .build())

In fact, the ``key`` function above is using an equality condition.

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

The following comparison operators are available:

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

Finally, you can construct conditions on `document`_ `paths`_ with the usual
``[]`` for indexes in lists, and keys in maps::

    high_rating = Model.document["Rating"] >= 4.5

    # Construct a condition in two pieces
    path = Model.document["Reviews"][0]["Name"]
    condition = path.begins_with("J")
    other_condition = path.contains("ohnson")

    first_element = Model.list[0].is_(None)

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
.. _comparisons have lower priority than bitwise operations: https://docs.python.org/3.6/reference/expressions.html#comparisons
.. _document: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModel.DataTypes.Document
.. _paths: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.AccessingItemAttributes.html#DocumentPaths

.. _atomic:

Atomic
------

With ``atomic`` you can ensure there have been no changes to the persisted
object between the last load and the current save/delete operation.  This is
useful in highly concurrent systems.  Without this setting, here's what an
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
        engine.save(instance, atomic=True)
    except bloop.ConstraintViolation:
        # Modified between load and save!
        ...

Additionally, you don't need to keep track of which attributes were loaded by
the operation that generated the object.  Because a query may not return all
attributes of the object, that would erroneously expect an empty value when the
operation could never populate those attributes.  For example, say the
following only loads the ``hash`` and ``range`` attributes of the model::

    instance = engine.query(Model.some_index) \
                      .key((Model.hash == 0) & (Model.range == 1)) \
                      .first()

This instance hasn't loaded the ``foo`` attribute, even though there's a value
persisted in dynamo.  Naively building a condition, for foo and bar, you'd have
something like::

    condition = bloop.Condition()
    condition &= Model.foo == instance.foo
    condition &= Model.bar == instance.bar

This would fail even if there were no changes, since the persisted row has a
value for ``foo``; it simply wasn't loaded!

bloop takes care of this tracking for us; objects that were updated through a
load or query/scan have a copy of their last persisted state stored.
When querying an index, the projected attributes that are available to the
index are used to differentiate which attributes were expected but missing,
and which were not loaded.

Finally, conditions can be used with atomic updates - this allows you to
constrain operations on attributes that may not have been loaded.  Using the
same model above where ``foo`` is a non-key attribute that's not loaded from a
query::

    instance = engine.query(Model.some_index) \
                     .key(Model.hash == 1) \
                     .first()

    big_foo = Model.foo >= 500
    engine.save(instance, condition=big_foo, atomic=True)

.. seealso::
    The ``atomic`` option in :ref:`config` to enable/disable atomic conditions for save and delete.

.. _query:
.. _scan:

Query and Scan
--------------

Queries and Scans are constructed using nearly identical syntax.  Some minor exceptions:

* Query MUST have a ``key`` condition, while Scan MUST NOT have a ``key`` condition.
  Otherwise, ``first``, ``one``, and ``build`` will raise ``ValueError``.
* Trying to set ``forward`` on a Scan will raise a ``ValueError`` (there is no ordering option for scans)

Queries against tables or indexes are constructed using the same syntax::

    table_query = engine.query(Model)
    table_scan = engine.scan(Model)

    index_query = engine.query(Model.some_index)
    index_scan = engine.scan(Model.some_index)

Queries are constructed by chaining methods together in the usual builder pattern.  This includes key conditions,
filter conditions, select, limit, prefetch, forward, consistent::

    q = engine.query(Model.by_some_gsi) \
              .key(Model.gsi_hash_key == "value") \
              .filter(Model.non_key_attr.begins_with("something")) \
              .select("projected") \
              .limit(200) \
              .prefetch(5) \
              .forward(True) \
              .consistent(False)

When you are done building a query, you can get a stateful iterator, the first result, or exactly one result::

    # Can inspect count, scanned, exhausted
    iterator = q.build()
    for i, obj in enumerate(iterator):
        if i > 4:
            break
    # scanned can be greater than count if using a FilterExpression
    print(iterator.count)
    print(iterator.scanned)
    # Reset an iterator to re-execute the query/scan
    print(iterator.exhausted)
    iterator.reset()


    try:
        first = q.first()
    except bloop.ConstraintViolation:
        # 0 results


    try:
        one = q.one()
    except bloop.ConstraintViolation:
        # 0 results, or more than 1 result


To create re-usable base queries, use the ``copy`` method::

    q = engine.query(Model).consistent(True).forward(False)

    for obj in q.copy().key(Model.hash == 1).build():
        ...
    for obj in q.copy().key(Model.hash == 2).build():
        ...

The ``key`` method takes a condition on the hash key (or ``None`` to clear).  You may optionally include a range key
condition.  Not all operators are supported for key conditions.  An equality condition on the hash key MUST be
provided.  Valid conditions against the range key are::

    ==, <=, <, >=, >, begins_with, between

To include a range key condition, use the bitwise AND operator::

    hash_condition = Model.hash == 1
    range_condition = Model.range == 2

    query = q.copy().key(hash_condition & range_condition)

With the ``filter`` method you can construct a `FilterExpression`_ using the same :ref:`conditions` that you use
everywhere else.  Unlike the ``key`` method, you may use any condition type.  You can use ``None`` to clear a filter.

From the API reference: `A filter expression lets you apply conditions to the data after it is queried or scanned, but
before it is returned to you. Only the items that meet your conditions are returned.`

A few examples::

    query = q.copy().filter(Model.foo >= 100)
    query = q.copy().filter(Model.bar.contains('hello'))

    # AND multiple conditions
    query = q.copy().filter(Model.foo.is_(None) &
                     Model.bar.in_([1, 2]))

By default, **projected** attributes are loaded for a query against a SecondaryIndex and **all** attributes are loaded
for a table query.  You can change the set of attributes to be loaded with the ``select`` method::

    projected = base_query.select('projected')
    everything = base_query.select('all')

You may specify a set of attributes to load by passing an iterable of column objects::

    specific = base_query.select([Model.foo, Model,bar])

There are a few combinations of ``select`` options and table/index configurations that are invalid.  All of the
following will raise a ``ValueError``:

* ``projected`` for a non-index query
* ``all`` against a GlobalSecondaryIndex whose projection is not ``all``
* ``all`` against a LSI whose projection is not ``all`` **and the strict option is enabled**
* list of columns against a GSI where the requested columns are not projected
* list of columns against a LSI where the requested columns are not projected **and the strict option is enabled**

In the first case, ``projected`` has no meaning for a table query.

While it's possible for a GSI with a key-only projection to include all attributes, this is not guaranteed to be true
forever.  Instead of behavior subtly changing when a column is added, bloop refuses to assume.

When a query against a GSI requests attributes that are not projected into the index, the Dynamo will raise.  Because
GSIs have their own read units, a second read against the table is not performed for you.

When strict is enabled, LSIs perform the same checks as GSIs.  Without strict,
**Dynamo will incur an additional read per item** to load the requested attributes.

.. tip::

    Currently ``strict`` defaults to ``True``, deviating from Dynamo's default behavior.  It is **HIGHLY** recommended
    to keep ``strict=True``, as it can be hard to plan which LSI queries will incur additional reads - an
    inconspicuous code change that adds a new attribute to a query's ``select`` may suddenly cause a critical-path
    query to double in consumed read units.

To execute a query, use the ``build`` method::

    for result in query.build():
        print(result.foo)

    # Keep a reference to the result container
    results = query.build()
    for result in results:
        ...
    print(results.scanned)

Each iteration of the query will result in a new set of calls to DynamoDB.  If you want to iterate the results multiple
times, you should store the results in a list or other data structure.  The object returned from ``build``
also provides metadata about the query, including ``count`` and ``scanned`` attributes, which map to the
``"Count"`` and ``"ScannedCount"`` json keys, respectively::

    iterator = query.build()

    # Hasn't started calling DynamoDB yet
    assert iterator.count == 0

    # exhaust the query
    results = list(iterator)
    print(iterator.count, iterator.scanned)
    assert iterator.exhausted

    # to iterate multiple times, either
    # 1) store the result in a list (above)

    # 2) reset the iterator, to re-execute the query against DynamoDB
    iterator.reset()
    for value in iterator:
        ...

    # 3) build a new iterator from the query
    new_iterator = query.build()
    for value in new_iterator:
        ...

You may optionally specify a ``prefetch`` to control how many rows are buffered.  Each time you step the iterator,
if the buffer had less objects than your prefetch value, it will continue following pagination tokens until the buffer
reaches the desired prefetch number (or it hits the end of the query).  Then, it will yield back elements until the
buffer drains.  When you ask for the next item after the buffer drains, it will again fetch until the buffer is full.

Note that this is not a **page** prefetch, but an **item** prefetch.  DynamoDB will only consider 25 items per call,
and a FilterExpression may filter out all 25 results before the response is sent back, which will give an empty page,
possibly with a continuation token.  Bloop will follow any number of continuation tokens until it fills the prefetch
buffer.  DynamoDB docs mention that if you notice a big difference between ``count`` and ``scanned``, your query/scan
did a lot of extra work (relative the result set size) and you may want to consider modifying your query.

Specifying a prefetch is the same as any other query parameter::

    query.prefetch(3)

You must specify a non-negative int.  0 will fetch results as they're requested - a small amount of buffering will
occur, as items per page can fluctuate from 0-25.

Note that prefetch is a client-side directive, and controls the buffer size for the stateful filter iterator.  This is
unlike ``limit``, a server-side limit (before the FilterExpression is applied).

``limit`` is the maximum number of items DynamoDB will process and run through a FilterExpression.  If your limit is 50
on a scan where the first 50 items don't match your FilterExpression, you will find 0 results even if the next 50
would match the FilterExpression.



.. seealso::
    * The ``strict`` option in :ref:`config` to prevent double reads on LSIs

.. _FilterExpression: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#FilteringResults

.. _abstract:

Abstract and Inheritance
------------------------

bloop supports the concept of abstract models that are not coupled to actual DynamoDB tables.  This can be useful when
you want to leverage the usual benefits of inheritance, without creating some intermediate classes::

    import uuid
    from bloop import new_base, Engine, UUID, ConstraintViolation


    class AbstractBase(new_base()):
        """
        base model for uuid hash_key-only models.
        provides class method for generating persisted guaranteed
        unique ids.
        """
        class Meta:
            abstract = True

        @classmethod
        def unique(cls, engine, tries=10):
            not_exist = cls.id.is_(None)
            while tries:
                try:
                    obj = cls(id=uuid.uuid4())
                    engine.save(obj, condition=not_exist)
                    return obj
                except ConstraintViolation:
                    pass
            raise RuntimeError("Failed to create unique object")


    class Model(AbstractBase):
        id = Column(UUID, hash_key=True)


    engine = Engine()
    engine.bind(base=AbstractBase)

    instance = Model.unique(engine)

Abstract classes can be anywhere the inheritance chain::

    Abstract = new_base():


    class Concrete(Abstract):
        id = Column(String, hash_key=True)


    class AlsoAbstract(Concrete):
        class Meta:
            abstract = True


    class AlsoConcrete(AlsoAbstract):
        id = Column(UUID, hash_key=True)


.. warning::

    Currently, modelled attributes are **not** inherited, which means they do not correspond to real columns in
    DynamoDB.  If your abstract model relies on subclasses having an ``id`` column like above, then each subclass must
    include that declaration.

.. _meta:

Meta
----

.. warning::
    Modifying the generated values in a model's ``Meta`` will result in **bad things**, including things like not
    saving attributes, loading values incorrectly, and kicking your dog.

Discussed above, the ``Meta`` attribute of a model class stores info about the table (read and write units, the table
name) as well as metadata used by bloop internally (like ``Meta.init``).

Meta exposes the following attributes:

* ``abstract`` - whether the model should be bound to a DynamoDB Table or not.
  Defaults to False.  ``new_base`` returns an abstract model.
* ``read_units`` and ``write_units`` - mentioned above, the table read/write units.  Both default to 1.
* ``table_name`` - mentioned above, the name of the table.  Defaults to the class name.
* ``init`` - covered in detail in :ref:`loading`, this is the entry point bloop uses when creating new instances of a
  model.  It is NOT used during load, which is settings values on existing instances of the model.
  ``bloop.load`` which updates attributes on existing instances.
* ``columns`` - a ``set`` of ``Column`` objects that are part of the model.
* ``indexes`` - a ``set`` of ``Index`` objects that are part of the model.
* ``hash_key`` - the ``Column`` that is the model's hash key.
* ``range_key`` - the ``Column`` that is the model's range key.  ``None`` if there is no range key for the table.
