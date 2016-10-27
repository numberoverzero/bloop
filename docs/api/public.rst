Public API
^^^^^^^^^^

All of the classes in the Public API can be directly imported from ``bloop``, even though the documentation below
uses their full module paths.

===========
Connections
===========

.. autoclass:: bloop.engine.Engine
    :members:

========
Modeling
========

---------------
Building Blocks
---------------

.. autoclass:: bloop.models.BaseModel
    :members:

.. autoclass:: bloop.models.Column
    :members:

.. autoclass:: bloop.types.Type
    :members: python_type, backing_type, dynamo_dump, dynamo_load, _dump, _load, _register
    :member-order: bysource

-------
Indexes
-------

.. autoclass:: bloop.models.GlobalSecondaryIndex
    :members:

.. autoclass:: bloop.models.LocalSecondaryIndex
    :members:

-----
Types
-----

Words explaining primitives/derived/document types.
Each should include an example, the backing type, and the python type.

.. autoclass:: bloop.types.String

.. autoclass:: bloop.types.Float

.. autoclass:: bloop.types.Binary

.. autoclass:: bloop.types.Boolean

.. autoclass:: bloop.types.UUID

.. autoclass:: bloop.types.DateTime

.. autoclass:: bloop.types.Integer

.. autoclass:: bloop.types.Set

.. autoclass:: bloop.types.List

.. autoclass:: bloop.types.Map

=========
Streaming
=========

.. autoclass:: bloop.stream.Stream
    :members:

.. autofunction:: bloop.stream.stream_for

========
Querying
========

.. autoclass:: bloop.search.Query
    :members:

.. autoclass:: bloop.search.Scan
    :members:

.. autoclass:: bloop.search.QueryIterator
    :members:

.. autoclass:: bloop.search.ScanIterator
    :members:


==========
Conditions
==========

.. autoclass:: bloop.conditions.Condition

==========
Exceptions
==========

TODO split into bad input (query without key, unknown projection type, bad stream token) vs unexpected event
(couldn't load objects, constraint violation).

.. automodule:: bloop.exceptions
    :members:

=======
Signals
=======

.. autodata:: bloop.signals.before_create_table
    :annotation:

.. autodata:: bloop.signals.table_validated

.. autodata:: bloop.signals.object_loaded

.. autodata:: bloop.signals.object_saved

.. autodata:: bloop.signals.object_deleted

.. autodata:: bloop.signals.object_modified

.. autodata:: bloop.signals.model_bound

.. autodata:: bloop.signals.model_created

.. autodata:: bloop.signals.model_validated
