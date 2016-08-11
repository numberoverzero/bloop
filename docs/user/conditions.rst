.. _conditions:

Using Conditions
^^^^^^^^^^^^^^^^

Conditions are used for:

* Optimistic concurrency when :ref:`Saving or Deleting <save-delete-interface>` objects
* To specify a Query's :ref:`key condition <query-key>`
* To :ref:`filter results <query-filter>` from a Query or Scan

.. _available-conditions:

====================
Available Conditions
====================

There is no DynamoDB type that supports all of the conditions.  For example, ``contains`` does not work with
a numeric type ``"N"`` such as Float or Integer.  DynamoDB's `ConditionExpression Reference`__ has the full
specification.

.. code-block:: python

    class Model(new_base()):
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

.. _condition-paths:

=====
Paths
=====

You can construct conditions against individual elements of List and Map types with the usual indexing notation.

.. code-block:: python

    Item = Map(
        name=String,
        price=Float,
        quantity=Integer)
    # Total checkout time, applying coupons, payment processing...
    TimingData = TypedMap(Float)

    class Receipt(new_base()):
        transaction_id = Column(UUID, column=True)
        total = Column(Integer)

        items = Column(List(Item))
        metrics = Column(TimingData)

Here are some basic conditions using paths:

.. code-block:: python

    Receipt.metrics["payment-duration"] > 30000
    Receipt.items[0]["name"].begins_with("deli:salami:")

.. _atomic:

=================
Atomic Conditions
=================

When you specify ``atomic=True`` during ``Engine.save`` or ``Engine.delete``, Bloop will insert a pre-constructed
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

3. Recompute the atomic condition Whenever the local state is synchronized with the DynamoDB value.

The following examples use this model:

.. code-block:: python

    class Document(new_base()):
        id = Column(Integer, hash_key=True)
        folder = Column(String)
        name = Column(String)

        size = Column(Integer)
        data = Column(Binary)

        by_name = GlobalSecondaryIndex(
            projection=["size"], hash_key="name")

----------
New Object
----------

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

---------------------
Load a Partial Object
---------------------

This demonstrates :ref:`Rule 2.1 <atomic-rules>`.

``Engine.load`` will return all columns for an object; if a column's value is missing, it hasn't been set.  An atomic
save or delete would expect those missing columns to still not have values.

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

---------------
Scan on a Table
---------------

This demonstrates :ref:`Rule 2.2 <atomic-rules>`.

Here, the scan uses ``select`` to only return a few columns (and the hash key column).

.. code-block:: python

    scan = engine.scan(Document)
    scan.select = [Document.name]

    results = list(scan.build())

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

There's no way to know if the previous value for eg. ``folder`` had a value, since the scan told DynamoDB not to
include that column when it performed the scan.  There's no save assumption for the state of that column in DynamoDB,
so it's not part of the generated atomic condition.

---------------------
Query on a Projection
---------------------

This demonstrates :ref:`Rule 2.1 <atomic-rules>`.

The scan above expected a subset of available columns, and finds a value for each.  This query will also expect a
subset of all columns (using the index's projection) but the value will be missing.

.. code-block:: python

    query = engine.query(Document.by_name)
    query.key = name == ".profile"

    result = query.first()

This index projects the ``size`` column, which means it's expected to populate the columns ``id, name, size``.
If the result looks like this:

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

If the value in DynamoDB has a value for ``size``, the operation will fail.  If the document's ``data`` column has
changed since the query executed, this atomic condition won't care.

--------------
Save then Save
--------------

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
