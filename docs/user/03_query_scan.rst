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

**obj**
    | *(required)*
    | This is either an instance of a model, or an index on a model.
**consistent**
    | *(default is None)*
    |     If None, ``engine.config["consistent"]`` is used.
    |     The default engine config does not enable consistent operations.
    | If True, `Strongly Consistent`_ Reads will be used.

----------
Properties
----------

Of the following, only ``key`` is required, and only for Queries.

**key**
    | *(required for Queries)*
    | *(ignored by Scans)*
**select**
    |
**filter**
    |
**consistent**
    | *(not available for GSIs)*
**forward**
    | *(ignored by Queries)*
**limit**
    |
**prefetch**
    |

After you have finished defining the Query or Scan, you can use ``first()``, ``one()``, or ``build()`` to
retrieve results.

If there are no matching results, ``first`` will raise a ``ConstraintViolation``.

If there is not exactly one matching result, ``one`` will raise a ``ConstraintViolation``.

Finally, ``build`` returns an iterable, fetching results within the constraints of ``prefetch`` and ``limit``.
The object returned by ``build`` does not cache results.  You can start the iterable over by calling ``reset()``.
The iterator has the following properties for inspecting the state of the scan or query:

**count**
    | Number of items loaded so far.
    | Some items may still be in the iterable's buffer.
**scanned**
    | Number of items DynamoDB has seen so far.
    | This number may be greater than ``count``.  See `Counting Items`_.
**exhausted**
    | True if there are no more results to fetch.

.. _Strongly Consistent: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
.. _Counting Items: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#Count
