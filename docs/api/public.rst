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

Bloop exposes a set of `blinker`_ signals that you can connect to.  Signals allow you to easily extend Bloop and
inject logic without directly replacing entire classes.  For example, the following will keep a list of all models
that have been created:

.. code-block:: python

    from bloop import model_created
    models = []

    @model_created.connect
    def on_new_model(_, *, model, **kwargs):
        models.append(model)

Your receiver must take ``**kwargs``.  This allows you and extension authors to send arbitrary metadata with an
existing built-in signal, or any new signals in the Bloop namespace that extensions may create.

.. note::

    * A signal may **add new kwargs** in a minor version.
    * An anonymous signal may **add a sender** in a minor version.
    * A signal can only **remove a kwarg** in a major version.
    * A signal can only **remove or replace a sender** in a major version.
    * A signal can only **promote a kwarg to a sender** in a major version.

.. note::

    * ``model`` is always a Model class eg. ``User`` or ``Account``
    * ``obj`` is always an instance of a Model  eg. ``User(name='some-user')`` or ``Account(id=343)``

    Bloop will never never send both at the same time.  You can retrieve ``model`` from ``obj.__class__``.

.. _blinker: https://pythonhosted.org/blinker/

.. autodata:: bloop.signals.before_create_table
    :annotation:

.. autodata:: bloop.signals.object_loaded
    :annotation:

.. autodata:: bloop.signals.object_saved
    :annotation:

.. autodata:: bloop.signals.object_deleted
    :annotation:

.. autodata:: bloop.signals.object_modified
    :annotation:

.. autodata:: bloop.signals.model_bound
    :annotation:

.. autodata:: bloop.signals.model_created
    :annotation:

.. autodata:: bloop.signals.model_validated
    :annotation:
