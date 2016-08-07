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


To get all the files named "setup.py" with a scan:

.. code-block:: python

    scan = engine.scan(File)
    scan.filter = File.name == "setup.py"

    for file in scan.build():
        print(file)

To find all files owned by "root" with a query on a GSI:

.. code-block:: python

    query = engine.query(File.by_owner)
    query.key = File.owner == "root"

    for file in query.build():
        print(file)

To find all files in "~/github/bloop/bloop" created more than one year ago, use the LSI:

.. code-block:: python

    query = engine.query(File.on_created)

    in_bloop = File.path == "~/github/bloop/bloop"
    over_one_year_old = File.created < arrow.now().replace(years=-1)
    query.key = in_bloop & over_one_year_old

    for file in query.build():
        print(file)

The first file with a size of 4096:

.. code-block:: python

    query = engine.query(File.by_size)
    query.key = File.size == 4096

    print(query.first())

Find exactly one file or raise, in the path "~/github/bloop/scripts":

.. code-block:: python

    query = engine.query(File)
    query.key = File.path == "~/github/bloop/scripts"

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

