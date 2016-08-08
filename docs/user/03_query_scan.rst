Query and Scan
^^^^^^^^^^^^^^

We'll need a different model than the ``User`` from the previous sections:

.. code-block:: python

    from bloop import (
        Binary, Column, DateTime, Integer, String,
        GlobalSecondaryIndex, LocalSecondaryIndex,
        Engine, new_base)
    BaseModel = new_base()

    class File(BaseModel):
        class Meta:
            write_units = 10
            read_units = 4
        path = Column(String, hash_key=True)
        name = Column(String, range_key=True)
        data = Column(Binary)

        created = Column(DateTime)
        owner = Column(String)
        size = Column(Integer)

        on_created = LocalSecondaryIndex(
            projection="keys", range_key="created")

        by_owner = GlobalSecondaryIndex(
            projection=["size"], hash_key="owner")

        by_size = GlobalSecondaryIndex(
            projection="all", hash_key="size")

    engine = Engine()
    engine.bind(BaseModel)


Find all files named "setup.py" with a scan:

.. code-block:: python

    scan = engine.scan(File)
    scan.filter = File.name == "setup.py"

    for file in scan.build():
        print(file)

Find all files owned by "root" with a query on a GSI:

.. code-block:: python

    query = engine.query(File.by_owner)
    query.key = File.owner == "root"

    for file in query.build():
        print(file)

Find all files in "~/bloop" created over a year ago with a query on the LSI:

.. code-block:: python

    query = engine.query(File.on_created)

    in_bloop = File.path == "~/bloop"
    over_one_year_old = File.created < arrow.now().replace(years=-1)
    query.key = in_bloop & over_one_year_old

    for file in query.build():
        print(file)

The first file with a size of 4096:

.. code-block:: python

    query = engine.query(File.by_size)
    query.key = File.size == 4096

    print(query.first())

Find exactly one file in the path "~/bloop/scripts":

.. code-block:: python

    query = engine.query(File)
    query.key = File.path == "~/bloop/scripts"

    print(query.one())


=========
Interface
=========

Scan and Query have the same interface:

.. code-block:: python

    Engine.query(
        obj: Union[bloop.BaseModel, bloop.Index],
        consistent: Optional[bool]=None) -> bloop.Filter

    Engine.scan(
        obj: Union[bloop.BaseModel, bloop.Index],
        consistent: Optional[bool]=None) -> bloop.Filter

.. attribute:: obj

    This is either an instance of a model, or an index on a model.  From the example above, this can
    be the ``File`` model, or any of its indexes ``Filter.on_created``, ``Filter.by_owner``, or ``Filter.by_size``.

.. attribute:: consistent

    See the :ref:`consistent property <property-consistent>` below.

----------
Properties
----------

.. attribute:: key

    Queries require a key :ref:`condition <conditions>`.  Scans do not use key conditions.

    A key condition must always include an equality condition (``==``) against the hash key of the object (Model
    or Index) being queried.  You may optionally include one condition against the range key of the object.

    The available conditions for a range key are[0]::

        <, <=, ==, >=, >, begins_with, between

    To use a hash key and range key condition together, join them with ``&``:

    .. code-block:: python

        in_home = File.path == "~"
        start_with_a = File.name.begins_with("a")

        query.key = in_home & starts_with_a

.. attribute:: select

    The columns to load.  One of ``"all"``, ``"projected"``, ``"count"``, or a list of columns.
    When select is "count", no objects will be returned, but the ``count`` and ``scanned`` properties
    will be set on the result iterator (see below).  If the Query or Scan is against a Model, you cannot
    use "projected".  Defaults to "all" for Models and "projected" for Indexes.

.. attribute:: filter

    A server-side filter :ref:`condition <conditions>` that DynamoDB applies to objects before returning them.
    Only objects that match the filter will be returned.  Defaults to None.

.. _property-consistent:

.. attribute:: consistent

    Whether or not `strongly consistent reads`_ should be used.  Keep in mind that Strongly Consistent Reads
    consume twice as many read units as Eventually Consistent Reads. This setting has no effect when used
    with a GSI, since strongly consistent reads `can't be used with a Global Secondary Index`__.
    Defaults to ``engine.config["consistent"]``

    __ http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ConsistentRead

.. attribute:: forward

    Whether to scan in ascending order (see `ScanIndexForward`_).  When True, scans are ascending.
    When False, scans are descending.  This setting is not used for Queries.  Defaults to True.

.. attribute:: limit

    The maximum number of objects that will be returned.  This is **NOT** the same as DynamoDB's `Limit`__, which
    is the maximum number of objects evaluated per continuation token.  Once the iterator has returned ``limit``
    object, it will not return any more (even if the internal buffer is not empty).  Defaults to None.

    __ http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-Limit

.. attribute:: prefetch

    The number of objects to buffer from DynamoDB before the iterator will yield objects.  Setting this to a high value
    can be useful if you want to use your read capacity in bursts.  Otherwise, the iterator will only follow
    continuation tokens when the buffer is empty and another object is requested.  Defaults to 0.

After you have finished defining the Query or Scan, you can use ``first()``, ``one()``, or ``build()`` to
retrieve results.  If there are no matching objects, ``first`` will raise a ``ConstraintViolation``.  If
there is not exactly one matching object, ``one`` will raise a ``ConstraintViolation``.

You can use ``build`` to return an iterable, which fetches objects depending on ``prefetch`` and ``limit``.
The object returned by ``build`` does not cache objects.  You can start the iterable over at any time by calling
``reset()``.  The iterator has the following properties for inspecting the state of the scan or query:

.. attribute:: count

    The number of objects loaded from DynamoDB so far.  This includes objects still in the iterator's buffer, which
    may not have been yielded yet.

.. attribute:: scanned

    The number of objects that DynamoDB has scanned so far.  If you are not using a filter, this is equal
    to ``count``.  Otherwise, the difference ``scanned - count`` is the number of objects that so far have
    not met the filter condition.  See `Counting Items`_.

.. attribute:: exhausted

    If there is no limit, this will be True when the buffer is empty and DynamoDB stops returning ContinuationTokens
    to follow.

    If there is a limit, this will be True when the iterator has yielded ``limit`` objects, or the above;
    whichever happens first.  With a limit, there may be objects in the internal buffer when the
    iterator is exhausted.

.. _strongly consistent reads: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
.. _ScanIndexForward: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ScanIndexForward
.. _Counting Items: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#Count
