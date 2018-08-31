.. _conditions:

Conditions
^^^^^^^^^^

Conditions are used for:

* Optimistic concurrency when :ref:`saving <user-engine-save>` or :ref:`deleting <user-engine-delete>` objects
* To specify a Query's :ref:`key condition <user-query-key>`
* To :ref:`filter results <user-query-filter>` from a Query or Scan

=====================
 Built-In Conditions
=====================

There is no DynamoDB type that supports all of the conditions.  For example, ``contains`` does not work with
a numeric type ``"N"`` such as Number or Integer.  DynamoDB's `ConditionExpression Reference`__ has the full
specification.

.. code-block:: python

    class Model(BaseModel):
        column = Column(SomeType)

    # Comparisons
    Model.column < value
    Model.column <= value
    Model.column == value
    Model.column >= value
    Model.column > value
    Model.column != value

    Model.column.begins_with(value)
    Model.column.between(low, high)
    Model.column.contains(value)
    Model.column.in_([foo, bar, baz])
    Model.column.is_(None)
    Model.column.is_not(False)

    # bitwise operators combine conditions
    not_none = Model.column.is_not(None)
    in_the_future = Model.column > now

    in_the_past = ~in_the_future
    either = not_none | in_the_future
    both = not_none & in_the_future

__ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference

==============================
 Chained Conditions (AND, OR)
==============================

Bloop overloads the ``&`` and ``|`` operators for conditions, allowing you to more easily construct compound
conditions.  Some libraries allow you to chain filters with ``.filter(c1).filter(c2)`` or pass a list of conditions
``.filter([c1, c2])`` but both of these forms struggle to express nested conditions, especially when expressing an
OR operation.

For example, consider a query to find popular articles.  We want either new articles with more than 100 likes,
recent articles with more than 500 likes, or older articles with more than 1000 likes.  We're running a spotlight on
editor of the month "Nancy Stevens" so let's include those as well.

.. code-block:: python

    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(hours=12)
    last_week = now - timedelta(days=7)
    last_year = now - timedelta(weeks=52)

    popular = (
        ((Article.likes >= 100) & (Article.publish_date >= yesterday)) |
        ((Article.likes >= 500) & (Article.publish_date >= last_week)) |
        ((Article.likes >= 1000) & (Article.publish_date >= last_year))
    )
    spotlight = Article.editor == "nstevens"

    articles = engine.scan(Article, filter=popular|spotlight)


We can programmatically build conditions from a base of ``bloop.Condition``, which is an empty condition.  In the
following example, ``editors`` may have come from a query param or form submission:

.. code-block:: python

    editors = ["nstevens", "jsmith", "bholly"]
    condition = bloop.Condition()

    for editor in editors:
        condition |= Article.editor == editor

    articles = engine.scan(Article, filter=condition)


Although less frequently used, there is also the ``~`` operator to negate an existing condition.  This is useful to
flip a compound condition, rather than trying to invert all the intermediate operators.  To find all the unpopular or
non-spotlighted articles, we'll use the variables from the first example above:

.. code-block:: python

    popular = (...)  # see first example
    spotlight = ...

    popular_articles = engine.scan(Article, filter=popular|spotlight)
    unpopular_articles = engine.scan(Article, filter=~(popular|spotlight))

================
 Document Paths
================

You can construct conditions against individual elements of List and Map types with the usual indexing notation.

.. code-block:: python

    Item = Map(
        name=String,
        price=Number,
        quantity=Integer)
    Metrics = Map(**{
        "payment-duration": Number,
        "coupons.used"=Integer,
        "coupons.available"=Integer
    })
    class Receipt(BaseModel):
        transaction_id = Column(UUID, column=True)
        total = Column(Integer)

        items = Column(List(Item))
        metrics = Column(Metrics)

Here are some basic conditions using paths:

.. code-block:: python

    Receipt.metrics["payment-duration"] > 30000
    Receipt.items[0]["name"].begins_with("deli:salami:")

.. _user-conditions-atomic:

===================
 Atomic Conditions
===================

When you specify ``atomic=True`` during :func:`Engine.save <bloop.engine.Engine.save>` or
:func:`Engine.delete <bloop.engine.Engine.delete>`, Bloop will insert a pre-constructed
condition on each object to be modified.

The condition depends on how the local version of your object was last synchronized with the corresponding
row in DynamoDB.  Here are the rules:

.. _atomic-rules:

1. If the object was created locally and hasn't been saved or deleted,
   expect **all** of the object's columns to be None in DynamoDB.

2. If the object came from DynamoDB (load, query, stream), only include columns that should have been in the response.

   1. If a column is missing and **was** expected, include it in the atomic condition
      and expect the value to be None in DynamoDB.
   2. If a column is missing and **wasn't** expected (query on a projected Index), don't include it.

3. Recompute the atomic condition whenever the local state is synchronized with the DynamoDB value.

The following examples use this model:

.. code-block:: python

    class Document(BaseModel):
        id = Column(Integer, hash_key=True)
        folder = Column(String)
        name = Column(String)

        size = Column(Integer)
        data = Column(Binary)

        by_name = GlobalSecondaryIndex(
            projection=["size"], hash_key="name")

---------------------
 Example: New Object
---------------------

This demonstrates :ref:`Rule 1 <atomic-rules>`.

For a new instance created locally but not yet saved:

.. code-block:: python

    document = Document(id=10, folder="~", name=".bashrc")

The following atomic condition would be generated:

.. code-block:: python

    atomic = (
        Document.id.is_(None) &
        Document.folder.is_(None) &
        Document.name.is_(None) &
        Document.size.is_(None) &
        Document.data.is_(None)
    )

In this case, atomic means "only save if this object didn't exist before".

--------------------------------
 Example: Load a Partial Object
--------------------------------

This demonstrates :ref:`Rule 2.1 <atomic-rules>`.

:func:`Engine.load <bloop.engine.Engine.load>` will return all columns for an object; if a column's value is
missing, it hasn't been set.  An atomic save or delete would expect those missing columns to still not have values.

First, save an object and then load it into a new instance:

.. code-block:: python

    original_document = Document(id=10, folder="~", name=".bashrc")
    engine.save(original_document)

    document = Document(id=10)
    engine.load(document)

The document has the following attributes:

.. code-block:: python

    document.id = 10
    document.folder = "~"
    document.name = ".bashrc"
    document.size = None
    document.data = None

Now, modify the object locally:

.. code-block:: python

    document.data = b"# ... for non-login shells."
    document.size = len(document.data)

If you try to save this with an atomic condition, it will expect all of the values to be the same as were last loaded
from DynamoDB -- **not** the values you just set.  The atomic condition is:

.. code-block:: python

    atomic = (
        (Document.id == 10) &
        (Document.folder == "~") &
        (Document.name == ".bashrc") &
        Document.size.is_(None) &
        Document.data.is_(None)
    )

If another call changed folder or name, or set a value for size or data, the atomic save will fail.

--------------------------
 Example: Scan on a Table
--------------------------

This demonstrates :ref:`Rule 2.2 <atomic-rules>`.

Here, the scan uses ``select`` to only return a few columns (and the hash key column).

.. code-block:: python

    scan = engine.scan(Document, projection=[Document.name])
    results = list(scan)

Each result will have values for ``id`` and ``name``, but the scan did not try to load the other columns.
Those columns won't be set to ``None`` - they won't even be loaded by the Column's typedef.  Here's a document the
scan above found:

.. code-block:: python

    scan_doc = Document(id=343, name="john")

If you set the size on this file and then perform an atomic save:

.. code-block:: python

    scan_doc.size = 117
    engine.save(scan_doc, atomic=True)

The following condition is used:

.. code-block:: python

    atomic = (
        (Document.id == 10) &
        (Document.name == ".bashrc")
    )

There's no way to know if the existing row in DynamoDB had a value for eg. ``folder``, since the scan told DynamoDB
not to include that column when it performed the scan.  There's no save assumption for the state of that column
in DynamoDB, so it's not part of the generated atomic condition.

--------------------------------
 Example: Query on a Projection
--------------------------------

This demonstrates :ref:`Rule 2.1 <atomic-rules>`.

The scan above expected a subset of available columns, and finds a value for each.  This query will also expect a
subset of all columns (using the index's projection) but the value will be missing.

.. code-block:: python

    query = engine.query(
        Document.by_name,
        key=Document.name == ".profile")
    result = query.first()

This index projects the ``size`` column, which means it's expected to populate the
``id``, ``name``, and ``size`` columns.  If the result looks like this:

.. code-block:: python

    result = Document(id=747, name="tps-reports.xls", size=None)

Then this document didn't have a value for size.  Take a minute to compare this to the result from the previous
example.  Most importantly, this object has a value (None) for the size column, while the scan doesn't.  This all
comes down to whether the operation expects a value to be present or not.

The atomic condition used for this object will be:

.. code-block:: python

    atomic = (
        (Document.id == 747) &
        (Document.name == "tps-reports.xls") &
        Document.size.is_(None)
    )

If the existing row in DynamoDB has a value for ``size``, the operation will fail.  If the document's ``data``
column has changed since the query executed, this atomic condition won't care.

-------------------------
 Example: Save then Save
-------------------------

This demonstrates :ref:`Rule 3 <atomic-rules>`.

Whenever you save or delete and the operation succeeds, the atomic condition is recomputed to match the current state
of the object.  Again, the condition will only expect values for any columns that have values.

To compare, here are two different Documents:

.. code-block:: python

    data_is_none = Document(id=5, data=None)
    no_data = Document(id=6)

    engine.save(data_is_none, no_data)

By setting a value for ``data``, the first object's atomic condition must expect the value to still be None.
The second object didn't indicate an expectation about the value of ``data``, so there's nothing to expect for the
next operation.  Here are the two atomic conditions after the save:

.. code-block:: python

    # Atomic for data_is_none
    atomic = (
        (Document.id == 5) &
        Document.data.is_(None)
    )

    # Atomic for no_data
    atomic = (
        (Document.id == 6)
    )

You can also hit this case by querying an index with a small projection, and only making changes to the projected
columns.  When you save, the next atomic condition will still only be on the projected columns.
