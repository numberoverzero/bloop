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

The only public class the conditions system exposes is :class:`!bloop.conditions.Condition`, which
represents an empty condition.  The rest of the conditions system is baked into :class:`~bloop.models.Column` and
the various :class:`~bloop.engine.Engine` functions like :func:`~bloop.engine.Engine.save`.

This function creates a condition for any model that can be used when saving to ensure you don't overwrite an existing
value.  The model's ``Meta`` attribute describes the required keys:

.. code-block:: python

    from bloop import Condition

    def ensure_unique(model):
        condition = Condition()
        for key in model.Meta.keys:
            condition &= key.is_(None)
        return condition

.. seealso::

    :ref:`conditions` in the :ref:`guide-index` describes the possible conditions, and when and how to use them.

.. autoclass:: bloop.conditions.Condition

=======
Signals
=======

Signals (powered by `blinker`_) allow you to easily respond to events. Bloop exposes a number of signals during
model creation, validation, and as objects are loaded and saved.

.. code-block:: python

    # Keep track of all created models
    from bloop import model_created
    models = []

    @model_created.connect
    def on_new_model(_, *, model, **kwargs):
        models.append(model)

To disconnect a receiver:

.. code-block:: python

    model_created.disconnect(on_new_model)

Specify a sender to filter notifications.  This simplifies many cross-region tasks, and can be set up as a simple
plugin.  Automatically bind and save models to a second region:

.. code-block:: python

    @model_created.connect(sender=primary_engine)
    def on_new_model(_, model, **__):
        secondary_engine.bind(model)

    @object_saved.connect(sender=primary_engine)
    def on_save(_, obj, **__):
        secondary_engine.save(obj)

.. _blinker: https://pythonhosted.org/blinker/

----------
Parameters
----------

Your receiver must accept ``**kwargs``, and should use ``_`` or ``sender`` for the positional argument.
The following templates are recommended for all receivers:

.. code-block:: python

    def receiver(_, *, kwarg1, kwarg2, **__):

    def receiver(sender, *, kwarg1, kwarg2, **__):

It's easy to forget which parameter a signal's sender is.  Some signals are sent by an ``engine`` and have a ``model``.
Another is sent by the ``column`` and has an ``obj``.  Instead of forcing you to keep track of the sender, Bloop
sends **every** parameter as a kwarg.  This means you can build a receiver by cherry picking the parameters you
care about, and always ignore the positional argument. The sender is accessed the same as all other parameters.

For example, :data:`~bloop.signals.object_modified` is sent by ``column`` and includes ``obj``, and ``value``.
Here's an anti-fraud receiver that inspects login patterns that only cares about changes to ``User.last_login``:

.. code-block:: python

    @object_modified.connect(sender=User.last_login)
    def on_new_login(_, obj, value, **__):
        fraud.enqueue_check(obj.id)

Meanwhile, a debugging receiver log the modified column of every change:

.. code-block:: python

    @object_modified.connect
    def attr_change(_, obj, column, value, **__):
        print("{!r} set to {!r} on {}".format(column, value, id(obj)))

In both cases, the sender's name didn't matter.  The first cares that the sender is ``User.last_login``,
and the second doesn't care if the signal is sent by ``obj``, ``column``, or ``value``.

.. note::

    * New parameters can be added in a minor version.
    * A sender can be added to an anonymous signal in a minor version.
    * A major version can remove a parameter and remove or replace a sender.

----------------
Built-in Signals
----------------

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

==========
Exceptions
==========

.. automodule:: bloop.exceptions
    :members:
