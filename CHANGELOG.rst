===========
 Changelog
===========

This changelog structure is based on `Keep a Changelog v0.3.0`__.
Bloop follows `Semantic Versioning 2.0.0`__ and a `draft appendix`__ for its :ref:`Public API <api-public>`.

__ http://keepachangelog.com/en/0.3.0/
__ http://semver.org/spec/v2.0.0.html
__ https://gist.github.com/numberoverzero/c5d0fc6dea624533d004239a27e545ad

--------------
 [Unreleased]
--------------

--------------------
 1.0.0 - 2016-11-16
--------------------

1.0.0 is the culmination of just under a year of redesigns, bug fixes, and new features.  Over 550 commits, more than
60 issues closed, over 1200 new unit tests.  At an extremely high level:

* The query and scan interfaces have been polished and simplified.  Extraneous methods and configuration settings have
  been cut out, while ambiguous properties and methods have been merged into a single call.
* A new, simple API exposes DynamoDBStreams with just a few methods; no need to manage individual shards, maintain
  shard hierarchies and open/closed polling.  I believe this is a first since the Kinesis Adapter and KCL, although
  they serve different purposes.  When a single worker can keep up with a model's stream, Bloop's interface is
  immensely easier to use.
* Engine's methods are more consistent with each other and across the code base, and all of the configuration settings
  have been made redundant.  This removes the need for ``EngineView`` and its associated temporary config changes.
* Blinker-powered signals make it easy to plug in additional logic when certain events occur: before a table is
  created; after a model is validated; whenever an object is modified.
* Types have been pared down while their flexibility has increased significantly.  It's possible to create a type that
  loads another object as a column's value, using the engine and context passed into the load and dump functions.  Be
  careful with this; transactions on top of DynamoDB are very hard to get right.

See the Migration Guide above for specific examples of breaking changes and how to fix them, or the User Guide for
a tour of the new Bloop.  Lastly, the Public and Internal API References are finally available and should cover
everything you need to extend or replace whole subsystems in Bloop (if not, please open an issue).

Added
=====

* ``bloop.signals`` exposes Blinker signals which can be used to monitor object changes, when
  instances are loaded from a query, before models are bound, etc.

    * ``before_create_table``
    * ``object_loaded``
    * ``object_saved``
    * ``object_deleted``
    * ``object_modified``
    * ``model_bound``
    * ``model_created``
    * ``model_validated``

* ``Engine.stream`` can be used to iterate over all records in a stream, with a total ordering over approximate
  record creation time.  Use ``engine.stream(model, "trim_horizon")`` to get started.  See the User Guide
* New exceptions ``RecordsExpired`` and ``ShardIteratorExpired`` for errors in stream state
* New exceptions ``Invalid*`` for bad input subclass ``BloopException`` and ``ValueError``
* ``DateTime`` types for the three most common date time libraries:

    * ``bloop.ext.arrow.DateTime``
    * ``bloop.ext.delorean.DateTime``
    * ``bloop.ext.pendulum.DateTime``

* ``model.Meta`` has a new optional attribute ``stream`` which can be used to enable a stream on the model's table.
  See the User Guide for details
* ``model.Meta`` exposes the same ``projection`` attribute as ``Index`` so that ``(index or model.Meta).projection``
  can be used interchangeably
* New ``Stream`` class exposes DynamoDBStreams API as a single iterable with powerful seek/jump options, and simple
  json-friendly tokens for pausing and resuming iteration.  See the User Guide for details
* Over 1200 unit tests added
* Initial integration tests added
* *(internal)* ``bloop.conditions.ReferenceTracker`` handles building ``#n0``, ``:v1``, and associated values.
  Use ``any_ref`` to build a reference to a name/path/value, and ``pop_refs`` when backtracking (eg. when a value is
  actually another column, or when correcting a partially valid condition)
* *(internal)* ``bloop.conditions.render`` is the preferred entry point for rendering, and handles all permutations
  of conditions, filters, projections.  Use over ``ConditionRenderer`` unless you need very specific control over
  rendering sequencing.
* *(internal)* ``bloop.session.SessionWrapper`` exposes DynamoDBStreams operations in addition to previous
  ``bloop.Client`` wrappers around DynamoDB client
* *(internal)* New supporting classes ``streams.buffer.RecordBuffer``, ``streams.shard.Shard``, and
  ``streams.coordinator.Coordinator`` to encapsulate the hell^Wjoy that is working with DynamoDBStreams
* *(internal)* New class ``util.Sentinel`` for placeholder values like ``missing`` and ``last_token``
  that provide clearer docstrings, instead of showing ``func(..., default=object<0x...>)`` these will show
  ``func(..., default=Sentinel<[Missing]>)``


Changed
=======

* ``bloop.Column`` emits ``object_modified`` on ``__set__`` and ``__del__``
* Conditions now check if they can be used with a column's ``typedef`` and raise ``InvalidCondition`` when they can't.
  For example, ``contains`` can't be used on ``Number``, nor ``>`` on ``Set(String)``
* ``bloop.Engine`` no longer takes an optional ``bloop.Client`` but instead optional ``dynamodb`` and
  ``dynamodbstreams`` clients (usually created from ``boto3.client("dynamodb")`` etc.)
* ``Engine`` no longer takes ``**config`` -- its settings have been dispersed to their local touch points

    * ``atomic`` is a parameter of ``save`` and ``delete`` and defaults to ``False``
    * ``consistent`` is a parameter of ``load``, ``query``, ``scan`` and defaults to ``False``
    * ``prefetch`` has no equivalent, and is baked into the new Query/Scan iterator logic
    * ``strict`` is a parameter of a ``LocalSecondaryIndex``, defaults to ``True``

* ``Engine`` no longer has a ``context`` to create temporary views with different configuration
* ``Engine.bind`` is no longer by keyword arg only: ``engine.bind(MyBase)`` is acceptable in addition to
  ``engine.bind(base=MyBase)``
* ``Engine.bind`` emits new signals ``before_create_table``, ``model_validated``, and ``model_bound``
* ``Engine.delete`` and ``Engine.save`` take ``*objs`` instead of ``objs`` to easily save/delete small multiples of
  objects (``engine.save(user, tweet)`` instead of ``engine.save([user, tweet])``)
* ``Engine`` guards against loading, saving, querying, etc against abstract models
* ``Engine.load`` raises ``MissingObjects`` instead of ``NotModified`` (exception rename)
* ``Engine.scan`` and ``Engine.query`` take all query and scan arguments immediately, instead of using the builder
  pattern.  For example, ``engine.scan(model).filter(Model.x==3)`` has become
  ``engine.scan(model, filter=Model.x==3)``.
* ``bloop.exceptions.NotModified`` renamed to ``bloop.exceptions.MissingObjects``
* Any code that raised ``AbstractModelException`` now raises ``UnboundModel``
* ``bloop.types.DateTime`` is now backed by ``datetime.datetime`` instead of ``arrow``.  Only supports UTC now, no
  local timezone.  Use the ``bloop.ext.arrow.DateTime`` class to continue using ``arrow``.
* The query and scan interfaces have been entirely refactored: ``count``, ``consistent``, ``ascending`` and other
  properties are part of the ``Engine.query(...)`` parameters.  ``all()`` is no longer needed, as ``Engine.scan`` and
  ``.query`` immediately return an iterable object.  There is no ``prefetch`` setting, or ``limit``.
* The ``complete`` property for Query and Scan have been replaced with ``exhausted``, to be consistent with the Stream
  module
* The query and scan iterator no longer cache results
* The ``projection`` parameter is now required for ``GlobalSecondaryIndex`` and ``LocalSecondaryIndex``
* Calling ``Index.__set__`` or ``Index.__del__`` will raise ``AttributeError``.  For example,
  ``some_user.by_email = 3`` raises if ``User.by_email`` is a GSI
* ``bloop.Number`` replaces ``bloop.Float`` and takes an optional ``decimal.Context`` for converting numbers.
  For a less strict, **lossy** ``Float`` type see the Patterns section of the User Guide
* ``bloop.String.dynamo_dump`` no longer calls ``str()`` on the value, which was hiding bugs where a non-string
  object was passed (eg. ``some_user.name = object()`` would save with a name of ``<object <0x...>``)
* ``bloop.DateTime`` is now backed by ``datetime.datetime`` and only knows UTC in a fixed format.  Adapters for
  ``arrow``, ``delorean``, and ``pendulum`` are available in ``bloop.ext``
* ``bloop.DateTime`` does not support naive datetimes; they must always have a ``tzinfo``
* docs:

    * use RTD theme
    * rewritten three times
    * now includes public and internal api references

* *(internal)* Path lookups on ``Column`` (eg. ``User.profile["name"]["last"]``) use simpler proxies
* *(internal)* Proxy behavior split out from ``Column``'s base class ``bloop.conditions.ComparisonMixin``
  for a cleaner namespace
* *(internal)* ``bloop.conditions.ConditionRenderer`` rewritten, uses a new ``bloop.conditions.ReferenceTracker``
  with a much clearer api
* *(internal)* ``ConditionRenderer`` can backtrack references and handles columns as values (eg.
  ``User.name.in_([User.email, "literal"])``)
* *(internal)* ``_MultiCondition`` logic rolled into ``bloop.conditions.BaseCondition``, ``AndCondition`` and
  ``OrCondition`` no longer have intermediate base class
* *(internal)* ``AttributeExists`` logic rolled into ``bloop.conditions.ComparisonCondition``
* *(internal)* ``bloop.tracking`` rolled into ``bloop.conditions`` and is hooked into the ``object_*`` signals.
  Methods are no longer called directly (eg. no need for ``tracking.sync(some_obj, engine)``)
* *(internal)* update condition is built from a set of columns, not a dict of updates to apply
* *(internal)* ``bloop.conditions.BaseCondition`` is a more comprehensive base class, and handles all manner of
  out-of-order merges (``and(x, y)`` vs ``and(y, x)`` where x is an ``and`` condition and y is not)
* *(internal)* almost all ``*Condition`` classes simply implement ``__repr__`` and ``render``; ``BaseCondition``
  takes care of everything else
* *(internal)* ``bloop.Client`` became ``bloop.session.SessionWrapper``
* *(internal)* ``Engine._dump`` takes an optional ``context``, ``**kwargs``, matching the
  signature of ``Engine._load``
* *(internal)* ``BaseModel`` no longer implements ``__hash__``, ``__eq__``, or ``__ne__`` but ``ModelMetaclass`` will
  always ensure a ``__hash__`` function when the subclass is created
* *(internal)* ``Filter`` and ``FilterIterator`` rewritten entirely in the ``bloop.search`` module across multiple
  classes

Removed
=======

* ``AbstractModelException`` has been rolled into ``UnboundModel``
* The ``all()`` method has been removed from the query and scan iterator interface.  Simply iterate with
  ``next(query)`` or ``for result in query:``
* ``Query.results`` and ``Scan.results`` have been removed and results are no longer cached.  You can begin the
  search again with ``query.reset()``
* The ``new_base()`` function has been removed in favor of subclassing ``BaseModel`` directly
* ``bloop.Float`` has been replaced by ``bloop.Number``
* *(internal)* ``bloop.engine.LoadManager`` logic was rolled into ``bloop.engine.load(...)``
* ``EngineView`` has been removed since engines no longer have a baseline ``config`` and don't need a
  context to temporarily modify it
* *(internal)* ``Engine._update`` has been removed in favor of ``util.unpack_from_dynamodb``
* *(internal)* ``Engine._instance`` has been removed in favor of directly creating instances from
  ``model.Meta.init()`` in ``unpack_from_dynamodb``

Fixed
=====

* ``Column.contains(value)`` now renders ``value`` with the column typedef's inner type.  Previously, the container
  type was used, so ``Data.some_list.contains("foo"))`` would render as ``(contains(some_list, ["f", "o", "o"]))``
  instead of ``(contains(some_list, "foo"))``
* ``Set`` renders correct wire format -- previously, it incorrectly sent ``{"SS": [{"S": "h"}, {"S": "i"}]}`` instead
  of the correct ``{"SS": ["h", "i"]}``
* *(internal)* ``Set`` and ``List`` expose an ``inner_typedef`` for conditions to force rendering of inner values
  (currently only used by ``ContainsCondition``)

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

*(no public changes)*

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

*(no public changes)*

.. _changelog-v0.9.0:

--------------------
 0.9.0 - 2015-12-07
--------------------
