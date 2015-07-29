Engine
======

.. _bind:

bind
----

engine.bind()

client
------

boto3 sessions, named profiles

.. _config:

config
------

atomic
consistent
persist
prefetch
strict

context
-------

temporary config changes

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
