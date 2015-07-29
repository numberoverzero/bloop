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

atomic
consistent
prefetch
save
strict

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
