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
