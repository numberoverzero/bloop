Conditions
^^^^^^^^^^

Quick Examples
==============

Save if not Finalized
---------------------

model with a finalized field, save if finalized is False, catch and throw illegal state

Filter by Time Range
--------------------

scan tweets created between two dates

Atomic Delete
-------------

atomic=True

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

Python's chaining is not supported for comparisons.  If you would normally use:

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

You can use ``begins_with`` with Binary types:

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

Besides Numeric and String types, you can use ``begins_with`` with Binary and DateTime:

.. code-block:: python

    one_year_ago = arrow.now().replace(years=-1)
    User.joined.between(one_year_ago, arrow.now())

    User.description.between("Hello, my name", "Hi, I'm")

``contains``
------------

.. code-block:: python

    Column.contains(value)

You can use ``contains`` with Binary, but like :ref:`condition-begins` there are limitations.

``in_``
-------

.. code-block:: python

    Column.in_(values)

``values`` must be an iterable.  This doesn't work like python's ``"foo" in "foobar"`` even though
strings are iterable.  For example, the following:

.. code-block:: python

    User.email.in_("user@domain, u@domain, user@d")

Is the equivalent of ``"foo" in list("foobar")`` or ``"foo" in ["f", "o", ...]``.

Instead, you need to check the exact strings to match:

.. code-block:: python

    User.email.in_([
        "user@domain",
        "u@domain",
        "user@d"
    ])

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

Conditional Delete
==================

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

By Hand
-------

Save, Delete
------------
