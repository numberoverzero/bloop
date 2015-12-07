Engine
======

.. _bind:

bind
----

Defining a model does not automatically create a table in DynamoDB with the
appropriate indexes and attributes.  If it did, catching exceptions would be a
mess::

    # Each class definition
    try:
        class Model(engine.model):
            id = Column(...)
            content = Column(...)
    except botocore.exceptions.ClientError, bloop.TableMismatch:
        ...

Instead, models are added to the engine's ``unbound_models`` set on creation,
and the appropriate table create commands are issued the next time ``bind()``
is called.  After CreateTable calls are issued, the engine will poll with
DescribeTable until the table and all its GSIs are in the ``ACTIVE`` state.
Finally, it will verify that the existing table matches the required table for
the model, or throw an exception.  Now the code above becomes::

    class Model(engine.model):
        id = Column(...)
        content = Column(...)

    # Other class definitions
    ...

    try:
        engine.bind()
    except botocore.exceptions.ClientError, bloop.TableMismatch:
        ...

It is safe to call ``bind()`` multiple times - only unbound models will be
bound.

.. tip::
    If a table does not match, bloop will never modify the table to match the
    model; instead, you will need to manually modify or delete the table.

.. seealso::
    * :ref:`define` for options when creating models
    * :ref:`model` for info on the base model class
    * :ref:`meta` for access to generated metadata (columns, indexes...)
    * :ref:`loading` to customize how bloop loads models

client
------

By default bloop will let boto determine what credentials should be used.  When
you want to use a named profile, or connect to a different region, you can
provide a `boto3 session`_ to the engine at initialization::

    import boto3
    import bloop

    session = boto3.session.Session(profile_name='not-default-profile')
    engine = bloop.Engine(session=session)

You should never need to interact with the engine's client directly; the
interface exposed does not translate 1:1 to the boto3 client interface.

.. _boto3 session: http://boto3.readthedocs.org/en/latest/reference/core/session.html

.. _config:

config
------

You can significantly change how you interact with DynamoDB through the
Engine's config attribute.  By default, the following are set::

    engine.config = {
        "atomic": False,
        "consistent": False,
        "prefetch": 0,
        "strict": True,
    }

Setting ``atomic`` to ``True`` will append a condition to every save and delete
operation that expects the row to still have the values it was last loaded
with.  These conditions are ANDed with any optional condition you provide to
the save or delete operations.  For more information, see :ref:`atomic` and
:ref:`conditions`.

Setting ``consistent`` to True will make ``load`` and ``query`` use
`Strongly Consistent Reads`_ instead of eventually consistent reads.

The ``prefetch`` option controls how many pages are fetched at a time during
queries and scans.  By default each page is loaded as necessary, allowing you
to stop following continuation tokens if you only need a partial query.  You
can set this to a positive integer to pre-fetch that number of pages at a time,
or to ``'all'`` to fully iterate the query in one blocking call.

Setting ``strict`` to ``False`` will allow queries and scans against LSIs to
consume additional read units against the table.  By default strict queries
are used.  If you select 'all' attributes for a query against an LSI without
strict enabled, you will incur an additional read **per item** against the
table.  This is also true when selecting specific attributes which are not
present in the index's projection.

.. _Strongly Consistent Reads: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ConsistentRead

context
-------

Sometimes you want to swap config for a batch of calls, without changing the
engine's config for other callers.  Because models are tied to a single
engine's base :ref:`model`, a new engine with different config settings would
not be able to save or load objects from the original engine.

Instead, you can use an engine view::

    with engine.context(atomic=True, consistent=True) as atomic:
        obj = Model(id='foo')
        atomic.load(obj)
        del obj.bar
        atomic.save(obj)

Any config changes passed to ``context`` are applied to the temporary engine,
but not the underlying engine.

delete
------

Delete an object or set of objects, with an optional condition::

    engine.delete(objs, *, condition=None)

It is safe to delete objects that don't exist.  For more info on deleting
objects, see :ref:`delete`.

load
----

Load an object or set of objects, optionally using ConsistentReads::

    engine.load(objs)

    with engine.context(consistent=True) as consistent:
        consistent.load(objs)

Load raises ``NotModified`` if any objects fail to load.  For more info on
loading objects, see :ref:`load`.

.. _model:

model
-----

When an engine is created, a unique base model class is generated for it.  Any
subclass of that engine's ``model``, once bound, can be loaded or dumped
through the engine.  The same is true of any individual column, should you ever
need to partially load or dump values (say, to manually update the tracking
diff).

Models that subclass one engine cannot be loaded through a different engine -
even if they have identical structures.  This is because an engine represents
a way to talk to DynamoDB, which means multiple engines can model the same
table quite differently.

For example, suppose you are migrating a legacy column from an integer-backed
enum to a string-backed enum.  One engine might handle only writes, while
another needs to have a custom type that can interpret both for processing::

    class CompatModel(compat_engine.model):
        id = Column(Integer, hash_key=True)
        content = Column(CompatEnum)

        class Meta:
            table_name = 'Model'

    class Model(engine.model):
        id = Column(Integer, hash_key=True)
        content = Column(String)

    compat_engine.bind()
    engine.bind()

    def load_data(key):
        obj = CompatModel(id=key)
        compat_engine.load(obj)
        return obj

    def save_data(key, data):
        obj = Model(id=key, content=data)
        engine.save(obj)

.. warning::
    Only the base ``engine.model`` can be subclassed.  Subclassing a custom
    model with Columns and Indexes has some opportunities for ambiguity, so
    instead they are not supported.  Trying to subclass a custom model will
    result in a class whose ``Meta`` contains empty ``columns`` and ``indexes``
    attributes.

.. seealso::
    * :ref:`define` for creating models
    * :ref:`loading` to customize how bloop loads models

query
-----

Query a table or index::

    query = engine.query(Model.index)
    query = query.key(Model.hash == value)
    query = query.filter(Model.column.contains(value))

    for result in query.all():
        ...

    print(query.first())

For more info on constructing and iterating queries, see :ref:`query`.

save
----

Save an object or set of objects, with an optional condition::

    engine.save(objs, *, condition=None)

scan
----

Scan a table or index::

    scan = engine.scan(Model.index)
    scan = scan.filter(Model.column.between(low, high))

    for result in scan.all():
        ...

    print(scan.first())

For more info on constructing and iterating scans, see :ref:`scan`.
