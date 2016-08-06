Conditions
^^^^^^^^^^

Conditions are pretty straightforward once you start using them.  At the minimum you will need to compose conditions
to build a Query; most likely you will also want to filter a query or scan.

Bloop is intentionally designed to make writing conditions easy.  For example, to ensure a public key hasn't expired:

.. code-block:: python

    PublicKey.expiry > arrow.now()

Quick Examples
==============

This section is meant to serve as a reference for common uses of optimistic concurrency.  If there's a scenario you'd
like to see, please `open an issue`_!  Someone else is probably looking for the same example that you are.

.. _open an issue: https://github.com/numberoverzero/bloop/issues/new

.. _condition-ex-finalize:

Save if not Finalized
---------------------

Imagine a resource that is editable until another process completes:

.. code-block:: python

    class Document(new_base()):
        id = Column(UUID, hash_key=True)
        file_type = Column(String)
        data = Column(Binary)
        created_on = Column(DateTime)
        modified_on = Column(DateTime)
        finalized = Column(Boolean)

A condition guarantees that the document won't change once finalized:

.. code-block:: python

    engine = bloop.Engine()
    engine.bind(base=Document)


    def apply_user_updates(document, delta):
        not_finalized = Document.finalized.is_(False)
        document.apply(delta)
        document.modified_on = arrow.now()

        try:
            engine.save(document, condition=not_finalized)
        except bloop.ConstraintViolation:
            raise falcon.HTTPBadRequest(
                "Document is no longer editable")

Without a condition, there is no way to guarantee that the edit was made while ``finalized`` was ``False``:

.. code-block:: python

    def apply_user_updates(document, delta):
        # Document was loaded from bloop, already finalized
        if document.finalized:
            raise falcon.HTTPBadRequest(
                "Document is no longer editable")

        # ERROR: if the asynchronous processing sets
        # finalized to True here, we won't know.

        document.apply(delta)
        document.modified_on = arrow.now()
        # Unconditional save
        engine.save(document)


Filter by Time Range
--------------------

A website lets users see articles in the current category from a year ago.

.. code-block:: python

    class Article(new_base()):
        id = Column(UUID, hash_key=True)
        headline = Column(String)
        # Bodies stored in S3
        content_url = Column(String)
        category = Column(String)
        publish_date = Column(DateTime)

        by_category = GlobalSecondaryIndex(
            projection=["headline", "content_url"],
            hash_key="category")

A condition can filter items in the query against the ``by_category`` index:

.. code-block:: python

    def articles_from_year_ago(category):
        one_year_ago = arrow.now().replace(years=-1)
        start, end = one_year_ago.span("day")

        return (
            engine
            .query(Article.by_category)
            .key(Article.category == category)
            .filter(Article.publish_date.between(start, end))
        ).build()

.. _condition-ex-atomic:

Atomic Delete
-------------

A celery task periodically cleans up accounts that haven't logged in recently.

.. code-block:: python

    class Account(new_base()):
        id = Column(UUID, hash_key=True)
        username = Column(String)
        last_used = Column(DateTime)

If the task didn't use a conditional delete, it's possible that the user logs in between the load and the delete.  Just
as the user logs in again, their account is blown away!

This can be solved two ways.  First, an explicit constraint on ``last_used``:

.. code-block:: python

    now = arrow.now()
    is_stale = Account.last_used <= now.replace(years=-2)

    engine.delete(account, condition=is_stale)

However, we probably already know the account is expired.  Imagine that we are getting accounts from a table scan that
filters on ``last_used``:

.. code-block:: python

    def get_stale_accounts():
        now = arrow.now()
        is_stale = Account.last_used <= now.replace(years=-2)
        return engine.scan(Account).filter(is_stale).build()

In this case, we only really care that ``last_used`` doesn't change before we delete it.  This is very easy:

.. code-block:: python

    for stale_account in get_stale_accounts():
        engine.delete(stale_account, atomic=True)

The ``atomic`` keyword is attached to an automatically generated condition that means "only perform this operation if
the current state in DynamoDB matches the exact state that I last loaded" (for objects that haven't been loaded/saved,
it requires that they not exist yet in DynamoDB).  You can use ``atomic=True`` alongside a custom condition and they
will be ANDed together.  See :ref:`atomic-operations` for more details on how atomic conditions are computed for
various states of synchronization with DynamoDB.

Available Conditions
====================

Any of these conditions can be combined or negated with :ref:`condition-bitwise`.  To start with an empty base condition
and incrementally build up a condition, you can use ``bloop.Condition()``.

Arguments aren't checked until the request is prepared; this means you can describe nonsensical
conditions, such as ``SomeColumn.between("potato", 3)`` without Bloop complaining.  When the request is being prepared,
the condition values will be dumped through the associated column's Type; at this point the type will probably throw
some ValueError or break in some way.

Demo Model
----------

We'll use the following model to demonstrate different conditions:

.. code-block:: python

    class User(new_base()):
        email = Column(String, hash_key=True)
        joined = Column(DateTime)
        description = Column(String)
        image = Column(Binary)
        friends_list = Column(Set(String))
        invites_left = Column(Integer)

.. _condition-comparisons:

Comparisons
-----------

.. code-block:: python

    Column < value
    Column <= value
    Column == value
    Column >= value
    Column > value
    Column != value

``is_`` and ``is_not`` are aliases for ``==`` and ``!=`` which are useful when comparing to ``None``, ``True``, and
``False``.

If you use ``==`` or ``is_`` with ``None`` or a value that dumps to None (for a Set, ``== set()`` will do this)
then the rendered condition will be ``attribute_not_exists`` instead of the usual equality comparison.

Similarly, if you use ``!=`` or ``is_not`` with None or a value that dumps to None,
then the rendered condition will be ``attribute_exists``.

.. note::

    | Python's chaining is not supported for comparisons.
    | If you would normally use:

    .. code-block:: python

        3 <= User.invites_left <= 10

    You should instead use one of:

    .. code-block:: python

        (User.invites_left => 3) & (User.invites_left <= 10)
        User.invites.between(3, 10)

.. _condition-begins:

``begins_with``
---------------

.. code-block:: python

    Column.begins_with(value)

Besides Strings, you can also use ``begins_with`` with Binary types:

.. code-block:: python

    User.image.begins_with(b"GIF")

There are some limitations:

.. code-block:: python

    engine.save(User(email="u@d", image=b"GIF123"))

    # Finds user
    User.image.begins_with(b"GIF")
    User.image.begins_with(b"GIF123")

    # No match
    User.image.begins_with(b"G")
    User.image.begins_with(b"GI")
    User.image.begins_with(b"GIF1")
    User.image.begins_with(b"GIF12")

``between``
-----------

.. code-block:: python

    Column.between(lower, upper)

Primarily used with String and Numeric (Integer, Float) types.  You can also use ``between`` with Binary and DateTime:

.. code-block:: python

    User.description.between("Hello, my name", "Hi, I'm")

    now = arrow.now()
    one_year_ago = now.replace(years=-1)
    User.joined.between(one_year_ago, now)

``contains``
------------

.. code-block:: python

    Column.contains(value)

Like :ref:`condition-begins`, there are limitations when using Binary columns.

``in_``
-------

.. code-block:: python

    Column.in_(values)

``values`` must be an iterable.  This doesn't work like python's ``"foo" in "foobar"`` even though
strings are iterable.  For example, the following:

.. code-block:: python

    User.email.in_("user@domain, u@domain, user@d")

Is the equivalent of ``"foo" in list("foobar")`` or ``"foo" in ["f", "o", ...]``.  To check that a string matches one
of multiple options, you need to check the exact strings to match:

.. code-block:: python

    User.email.in_([
        "user@domain",
        "u@domain",
        "user@d"
    ])

The only is-substring-of condition available right now is ``begins_with``\, which is limited to the beginning of the
string.

``is_``, ``is_not``
-------------------

.. code-block:: python

    Column.is_(value)
    Column.is_not(value)

Aliases for ``==`` and ``!=``.  As mentioned in :ref:`condition-comparisons`, equality checks against ``None``
will not render as ``(Column == None)`` but as ``attribute_not_exists(Column)``.
Similarly, ``is_not(None)`` translates to ``attribute_exists``.

.. _condition-bitwise:

Bitwise Operators
-----------------

.. code-block:: python

    condition1 = Column <= 2
    condition2 = Column.between(4, 5)

    # AND
    condition1 & condition2

    # OR
    condition1 | condition2

    # NOT
    ~condition1

Keep python's `operator priority`_ in mind, especially when using comparisons.  Specifically:

    Unlike C, all comparison operations in Python have the same priority, which is lower than
    that of any arithmetic, shifting or bitwise operation.

To be safe, use parentheses:

.. code-block:: python

    # TypeError: unsupported operand type(s) for &: 'int' and 'Column'
    User.invites_left > 0 & User.invites_left < 10

    # Correctly parsed
    (User.invites_left > 0) & (User.invites_left < 10)

.. _operator priority: https://docs.python.org/3.6/reference/expressions.html#comparisons

Paths
=====

.. code-block:: python

    Column[0]["key"] <= 3

As with value types, bloop will not validate that the type backing a column supports paths.  That means this won't
fail before being sent to DynamoDB, even though Integer's backing type ``"N"`` does not support paths:

.. code-block:: python

    User.invites_left["foo"]["bar"].in_([1, 3, 5])

Paths can be arbitrarily nested, and support ``int`` indexes for DynamoDB lists, and ``str`` indexes for DynamoDB
maps:

.. code-block:: python

    DocumentType = Map(**{
        'Rating': Float(),
        'Stock': Integer(),
        'Descriptions': List(
            Map(**{
                'Heading': String,
                'Body': String,
                'Specifications': String
            })),
        'Id': UUID,
        'Updated': DateTime
    })

    class Document(new_base()):
        id = Column(Integer, hash_key=True)
        data = Column(DocumentType)

A condition that expects the first description's body to be blank:

.. code-block:: python

    blank = Document.data["Descriptions"][0]["Body"] == ""


Conditional Save
================

.. code-block:: python

    condition = SomeModel.column.contains("@")

    engine.save(some_object, condition=condition)

As you saw in the :ref:`condition-ex-finalize` example above, saving an object with a condition guarantees that the
save happens **only if** the condition is true when the update is performed.  This optimistic concurrency control is
one of the most powerful features of DynamoDB.  We can compose conditions that embed business logic so that we don't
have to read before writing, like "only save this if the object doesn't exist, or the current object has expired":

.. code-block:: python

    does_not_exist = Model.id.is_(None)
    is_expired = Model.until < arrow.now()

    engine.save(obj, condition=(does_not_exist | is_expired))

Conditional Delete
==================

.. code-block:: python

    condition = SomeModel.column < arrow.now()

    engine.delete(some_object, condition=condition)

Conditional deletes are identical in meaning and shape to saves; the delete happens **only if** the condition is true
when the delete is performed.

Query, Scan
===========

Key Conditions
--------------

========
Hash Key
========

=========
Range Key
=========

Filter Condition
----------------

Atomic Conditions
=================

.. code-block:: python

    engine.save(..., atomic=True)
    engine.delete(..., atomic=True)

As you saw in the :ref:`Atomic Condition Example<condition-ex-atomic>` above, atomic conditions are a very easy way to
perform an atomic save or delete in DynamoDB.  An atomic condition ensures that the object hasn't changed since it was
last seen in DynamoDB.

To ensure an object hasn't changed since it was loaded/queried/scanned:

.. code-block:: python

    def atomic_update(obj, updates):
        obj.apply(updates)
        try:
            engine.save(obj, atomic=True)
        except bloop.ConstraintViolation:
            ...

In contrast, many sdks require a ``revision`` column that uses either a GUID or incrementing int for atomic updates:

.. code-block:: python

    class Model(new_base()):
        id = Column(Integer, hash_key=True)
        # other fields here
        ...

        revision = Column(Integer)

Then, an atomic save would look something like:

.. code-block:: python

    def atomic_update(obj, updates):
        previous_revision = obj.revision
        obj.revison += 1

        obj.apply(updates)

        model_rev = obj.__class__.revision
        revision_unchanged = model_rev == previous_revision
        try:
            engine.save(obj, condition=revision_unchanged)
        except bloop.ConstraintViolation:
            ...

Because bloop tracks the state of every column that was loaded, ``atomic=True`` can perform the same work as manually
tracking a revision column.

If you need to atomically update the entire object but only part of the object was loaded (say, against an index with
a keys-only projection) then ``atomic`` will only guarantee atomicity against the columns that were loaded.  In that
case, the best solution is in fact to use a ``revision`` column.  You must ensure that column is included in every
index and returned by every query/scan, so that it can always be checked.

Using an explicit revision column with bloop's atomic conditions is still straightforward, since the previous state is
tracked for you:

.. code-block:: python

    obj.revision += 1
    engine.save(obj, atomic=True)

See :ref:`atomic-operations` for details on atomic condition creation.

By Hand
-------

You can also construct an atomic condition by hand.  This is useful when you only care about atomicity over a subset
of columns.  For example, updating a player's win-loss record doesn't need to ensure that the profile description is
still the same.

.. code-block:: python

    class Player(new_base()):
        id = Column(UUID, hash_key=True)
        description = Column(String)
        wins = Column(Integer)
        losses = Column(Integer)

Both an explicit ``revision`` column or an automatic ``atomic=True`` condition will fail if the description changes
between read and write.  Here's the hand-rolled atomic condition:

.. code-block:: python

    def update_win_loss(player, won_game):
        if won_game:
            condition = Player.wins == player.wins
            player.wins += 1
        else:
            condition = Player.losses == player.losses
            player.losses += 1
        try:
            engine.save(player, condition=condition)
        except bloop.ConstraintViolation:
            ...

There are a few patterns here which will allow us to construct a general atomic condition for any model and any column:

1. The condition is constructed before the model changes
2. The condition is always an equality check
3. The attribute name is the same for the instance and the class
4. The class is always the same as the instance's

From these, we can generalize to the following:

.. code-block:: python

    def atomic_on(obj, column_name):
        model = obj.__class__
        column = getattr(model, column_name)
        value = getattr(obj, column_name, None)
        return column == value

When getting the value from the object, we need to handle the case that the object doesn't have that attribute (not
loaded from DynamoDB, or not set when creating a new instance).

The ``update_win_loss`` function becomes:

.. code-block:: python

    def update_win_loss(player, won_game):
        if won_game:
            condition = atomic_on(player, "wins")
            player.wins += 1
        else:
            condition = atomic_on(player, "losses")
            player.losses += 1
        ...

Now, for any list of columns:

.. code-block:: python

    def atomic_for(obj, *columns):
        # Empty base condition
        condition = bloop.Condition()

        for column_name in columns:
            condition &= atomic_on(obj, column_name)

        # empty condition is Falsey; fall back to None
        # if there were no columns
        return condition or None

    atomic_for(player, ["wins", "losses"])

    # An empty list of columns returns None, which is
    # the default value for condition=
    assert atomic_for(player) is None
