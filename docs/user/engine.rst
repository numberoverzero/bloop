Engine
======

.. _bind:

bind
----

engine.bind()

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
        "save": "update",
        "strict": False,
    }

Setting ``atomic`` to ``True`` will append a condition to every save and delete
operation that expects the row to still have the values it was last loaded
with.  THese conditions are ANDed with any optional condition you provide to
the save or delete operations.  For more information, see :ref:`atomic` and
:ref:`conditions`.

Setting ``consistent`` to True will make ``load`` and ``query`` use
`Strongly Consistent Reads`_ instead of eventually consistent reads.

The ``prefetch`` option controls how many pages are fetched at a time during
queries and scans.  By default each page is loaded as necessary, allowing you
to stop following continuation tokens if you only need a partial query.  You
can set this to a positive integer to pre-fetch that number of pages at a time,
or to ``'all'`` to fully iterate the query in one blocking call.

The ``save`` option controls whether ``UpdateItem`` or ``PutItem`` is used.  By
default ``'update'`` will use UpdateItem, which only submits partial changes
for items when saving.  The ``'overwrite'`` option will use PutItem, which will
always replace the entire row; this includes deleting values that were stored
in DynamoDB but not set locally.  It is **highly** recommended that you review
:ref:`save` before changing this option from its default of ``'update'``.

Setting ``strict`` to ``True`` will prevent queries and scans against LSIs from
consuming additional read units against the table.  By default strict queries
are not used; if you select 'all' attributes for a query against an LSI, you
will incur an additional read **per item** against the table.  This is also
true when selecting specific attributes which are not present in the index's
projection.  While ``strict`` *currently* defaults to False (matching
DynamoDB's default behavior) it is **recommended** to always set this value to
True if a query or scan against an LSI can or will incur additional reads
against the table.

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

    engine.load(objs, *, consistent=False)

Load raises ``NotModified`` if any objects fail to load.  For more info on
loading objects, see :ref:`load`.

.. _model:

model
-----

Unique per engine, base for all models in the engine

see also: Models-> define, Advanced-> Custom Loading

query
-----

Query a table or index::

    query = engine.query(Model.index)
    query.filter(Model.column == value)

For more info on constructing and iterating queries, see :ref:`query`.

save
----

Save an object or set of objects, with an optional condition::

    engine.save(objs, *, condition=None)

By default objects are saved using UpdateItem, but can use PutItem instead.
For more info on saving objects, see :ref:`save`.

scan
----

Scan a table or index::

    scan = engine.scan(Model.index)
    scan.filter(Model.column == value)

For more info on constructing and iterating scans, see :ref:`scan`.
