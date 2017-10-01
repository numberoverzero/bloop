Signals
^^^^^^^

Signals (powered by `blinker`_) allow you to easily respond to events. Bloop exposes a number of signals during
model creation, validation, and as objects are loaded and saved.

.. code-block:: pycon

    >>> from bloop import model_created
    >>> @model_created.connect
    ... def on_new_model(_, *, model, **__):
    ...     models.append(model)
    ...
    >>> models = []

To disconnect a receiver:

.. code-block:: pycon

    >>> model_created.disconnect(on_new_model)

You can specify a sender to restrict who you receive notifications from.  This simplifies many cross-region
tasks, where multiple engines are sending the same type of notifications.  For example, you can
automatically bind and save models to a second region:

.. code-block:: pycon

    >>> @model_created.connect(sender=primary_engine)
    >>> def on_new_model(_, model, **__):
    ...     secondary_engine.bind(model)
    ...
    >>> @object_saved.connect(sender=primary_engine)
    ... def on_save(_, obj, **__):
    ...     secondary_engine.save(obj)

.. _blinker: https://pythonhosted.org/blinker/

==========
Parameters
==========

Your receiver must accept ``**kwargs``, and should only use ``_`` or ``sender`` for the positional argument.
The following templates are recommended for all receivers:

.. code-block:: python

    def receiver(_, *, kwarg1, kwarg2, **__):

    def receiver(sender, *, kwarg1, kwarg2, **__):

Instead of forcing you to remember which parameter the sender is (engine?  model?)  Bloop sends **every** parameter
as a kwarg.  This means your receiver can always ignore the positional argument, and cherry pick the parameters you
care about. The sender is accessed the same as all other parameters.

You can still specify a sender when you connect, but you should not use that parameter name in your function signature.
For example, :data:`~.signals.model_bound` is sent by ``engine`` and includes ``engine`` and ``model``.
If you set up a receiver that names its first positional arg "engine", this causes a :exc:`TypeError`:

.. code-block:: pycon

    >>> @model_bound.connect
    ... def wrong_receiver(engine, model, **__):
    ...     pass
    ...
    >>> model_bound.send("engine", model="model", engine="engine")
    TypeError: wrong_receiver() got multiple values for argument 'engine'


Here's the correct version, which also filters on sender:

.. code-block:: pycon

    >>> @model_bound.connect(sender="engine")
    ... def correct_receiver(_, model, engine, **__):
    ...     print("Called!")
    ...
    >>> model_bound.send("engine", model="model", engine="engine")
    Called!

.. note::

    * New parameters can be added in a minor version.
    * A sender can be added to an anonymous signal in a minor version.
    * A major version can remove a parameter and remove or replace a sender.


================
Built-in Signals
================

See the :ref:`Public API <public-signals>` for a list of available signals.
