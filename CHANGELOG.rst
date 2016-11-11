===========
 Changelog
===========

This changelog structure is structured based on `Keep a Changelog v0.3.0`__.
Bloop follows `Semantic Versioning 2.0.0`__ and a `draft appendix`__ for its :ref:`Public API <api-public>`.

__ http://keepachangelog.com/en/0.3.0/
__ http://semver.org/spec/v2.0.0.html
__ https://gist.github.com/numberoverzero/c5d0fc6dea624533d004239a27e545ad

--------------
 [Unreleased]
--------------

---------------------
 0.9.13 - 2016-10-31
---------------------

[Fixed]
=======

* ``Set`` was rendering an invalid wire format, and now renders the correct "SS", "NS", or "BS" values.
* ``Set`` and ``List`` were rendering ``contains`` conditions incorrectly, by trying to dump each value in the
  value passed to contains.  For example, ``MyModel.strings.contains("foo")`` would render ``contains(#n0, :v1)``
  where ``:v1`` was ``{"SS": [{"S": "f"}, {"S": "o"}, {"S": "o"}]}``.  Now, non-iterable values are rendered
  singularly, so ``:v1`` would be ``{"S": "foo"}``.  This is a temporary fix, and only works for simple cases.
  For example, ``List(List(String))`` will still break when performing a ``contains`` check.
  **This is fixed correctly in 1.0.0** and you should migrate as soon as possible.

---------------------
 0.9.12 - 2016-06-13
---------------------

[Added]
=======

* ``model.Meta`` now exposes ``gsis`` and ``lsis``, in addition to the existing ``indexes``.  This simplifies code that
  needs to iterate over each type of index and not all indexes.

[Removed]
=========

* ``engine_for_profile`` was no longer necessary, since the client instances could simply be created with a given
  profile.

---------------------
 0.9.11 - 2016-06-12
---------------------

[Changed]
=========

* ``bloop.Client`` now takes ``boto_client``, which should be an instance of ``boto3.client("dynamodb")`` instead of
  a ``boto3.session.Session``.  This lets you specify endpoints and other configuration only exposed during the
  client creation process.
* ``Engine`` no longer uses ``"session"`` from the config, and instead takes a ``client`` param which should be an
  instance of ``bloop.Client``.  **bloop.Client will be going away in 1.0.0** and Engine will simply take the boto3
  clients directly.

---------------------
 0.9.10 - 2016-06-07
---------------------

[Added]
=======

* New exception ``AbstractModelException`` is raised when attempting to perform an operation which requires a
  table, on an abstract model.  Raised by all Engine functions as well as ``bloop.Client`` operations.

[Changed]
=========

* ``Engine`` operations raise ``AbstractModelException`` when attempting to perform operations on abstract models.
* Previously, models were considered non-abstract if ``model.Meta.abstract`` was False, or there was no value.
  Now, ``ModelMetaclass`` will explicitly set ``abstract`` to False so that ``model.Meta.abstract`` can be used
  everywhere, instead of ``getattr(model.Meta, "abstract", False)``.

--------------------
 0.9.9 - 2016-06-06
--------------------

[Added]
=======

* ``Column`` has a new attribute ``model``, the model it is bound to.  This is set during the model's creation by
  the ``ModelMetaclass``.

[Changed]
=========

* ``Engine.bind`` will now skip intermediate models that are abstract.  This makes it easier to pass abstract models,
  or models whose subclasses may be abstract (and have non-abstract grandchildren).

--------------------
 0.9.8 - 2016-06-05
--------------------

(No significant changes)

--------------------
 0.9.7 - 2016-06-05
--------------------

[Changed]
=========

* Conditions implement ``__eq__`` for checking if two conditions will evaluate the same.  For example::

    >>> large = Blob.size > 1024**2
    >>> small = Blob.size < 1024**2
    >>> large == small
    False
    >>> also_large = Blob.size > 1024**2
    >>> large == also_large
    True
    >>> large is also_large
    False

.. _changelog-v0.9.6:

--------------------
 0.9.6 - 2016-06-04
--------------------

0.9.6 is the first significant change to how Bloop binds models, engines, and tables.  There are a few breaking
changes, although they should be easy to update.

Where you previously created a model from the Engine's model:

.. code-block:: python

    from bloop import Engine

    engine = Engine()

    class MyModel(engine.model):
        ...

You'll now create a base without any relation to an engine, and then bind it to any engines you want:

.. code-block:: python

    from bloop import Engine, new_base

    BaseModel = new_base()

    class MyModel(BaseModel):
        ...

    engine = Engine()
    engine.bind(base=MyModel)  # or base=BaseModel

[Added]
=======

* A new function ``engine_for_profile`` takes a profile name for the config file and creates an appropriate session.
  This is a temporary utility, since ``Engine`` will eventually take instances of dynamodb and dynamodbstreams
  clients.  **This will be going away in 1.0.0**.
* A new base exception ``BloopException`` which can be used to catch anything thrown by Bloop.
* A new function ``new_base()`` creates an abstract base for models.  This replaces ``Engine.model`` now that multiple
  engines can bind the same model.  **This will be going away in 1.0.0** which will provide a ``BaseModel`` class.

[Changed]
=========

* The ``session`` parameter to ``Engine`` is now part of the ``config`` kwargs.  The underlying ``bloop.Client`` is
  no longer created in ``Engine.__init__``, which provides an opportunity to swap out the client entirely before
  the first ``Engine.bind`` call.  The semantics of session and client are unchanged.
* ``Engine._load``, ``Engine._dump``, and all Type signatures now pass an engine explicitly through the ``context``
  parameter.  This was mentioned in :ref:`0.9.2 <changelog-v0.9.2>` and ``context`` is now required.
* ``Engine.bind`` now binds the given class **and all subclasses**.  This simplifies most workflows, since you can
  now create a base with ``MyBase = new_base()`` and then bind every model you create with
  ``engine.bind(base=MyBase)``.
* All exceptions now subclass a new base exception ``BloopException`` instead of ``Exception``.
* Vector types ``Set``, ``List``, ``Map``, and ``TypedMap`` accept a typedef of ``None`` so they can raise a more
  helpful error message.  **This will be reverted in 1.0.0** and will once again be a required parameter.


[Removed]
=========

* Engine no longer has ``model``, ``unbound_models``, or ``models`` attributes.  ``Engine.model`` has been replaced
  by the ``new_base()`` function, and models are bound directly to the underlying type engine without tracking
  on the ``Engine`` instance itself.
* EngineView dropped the corresponding attributes above.

--------------------
 0.9.5 - 2016-06-01
--------------------

[Changed]
=========

* ``EngineView`` attributes are now properties, and point to the underlying engine's attributes; this includes
  ``client``, ``model``, ``type_engine``, and ``unbound_models``.  This fixed an issue when using
  ``with engine.context(...) as view:`` to perform operations on models bound to the engine but not the engine view.
  **EngineView will be going away in 1.0.0**.

--------------------
 0.9.4 - 2015-12-31
--------------------

[Added]
=======

* Engine functions now take optional config parameters to override the engine's config.  You should update your code to
  use these values instead of ``engine.config``, since **engine.config is going away in 1.0.0**. ``Engine.delete``
  and ``Engine.save`` expose the ``atomic`` parameter, while ``Engine.load`` exposes ``consistent``.

* Added the ``TypedMap`` class, which provides dict mapping for a single typedef over any number of keys.
  This differs from ``Map``, which must know all keys ahead of time and can use different types.  ``TypedMap`` only
  supports a single type, but can have arbitrary keys.  **This will be going away in 1.0.0**.

.. _changelog-v0.9.2:

--------------------
 0.9.2 - 2015-12-11
--------------------

[Changed]
=========

* Type functions ``_load``, ``_dump``, ``dynamo_load``, ``dynamo_dump`` now take an optional keyword-only arg
  ``context``.  This dict will become required in :ref:`0.9.6 <changelog-v0.9.6>`, and contains the engine
  instance that should be used for recursive types.  If your type currently uses ``cls.Meta.bloop_engine``,
  you should start using ``context["engine"]`` in the next release.  The ``bloop_engine`` attribute is being removed,
  since models will be able to bind to multiple engines.

--------------------
 0.9.1 - 2015-12-07
--------------------

.. _changelog-v0.9.0:

--------------------
 0.9.0 - 2015-12-07
--------------------
