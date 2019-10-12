===========
 Changelog
===========

This changelog structure is based on `Keep a Changelog v0.3.0`__.
Bloop follows `Semantic Versioning 2.0.0`__ and a `draft appendix`__ for its Public API.

__ http://keepachangelog.com/en/0.3.0/
__ http://semver.org/spec/v2.0.0.html
__ https://gist.github.com/numberoverzero/c5d0fc6dea624533d004239a27e545ad

------------
 Unreleased
------------

(no unreleased changes)

--------------------
 2.4.1 - 2019-10-11
--------------------

Bug fix.  Thanks to @wilfre in `PR #141`_!

.. _PR #141: https://github.com/numberoverzero/bloop/pull/141

[Fixed]
=======

* ``bloop.stream.shard.py::unpack_shards`` no longer raises when a Shard in the DescribeStream has a ParentId
  that is not also available in the DescribeStream response (the parent shard has been deleted).  Previously the
  code would raise while trying to link the two shard objects in memory.  Now, the shard will have a ParentId of
  ``None``.

--------------------
 2.4.0 - 2019-06-13
--------------------

The ``atomic=`` keyword for ``Engine.save`` and ``Engine.delete`` is deprecated and will be removed in 3.0.
In 2.4 your code will continue to work but will raise ``DeprecationWarning`` when you specify a value for ``atomic=``.

The ``Type._dump`` function return value is changing to ``Union[Any, bloop.Action]`` in 2.4 to prepare for the
change in 3.0 to exclusively returning a ``bloop.Action``.  For built-in types and most custom types that only
override ``dynamo_dump`` this is a no-op, but if you call ``Type._dump`` you can use ``bloop.actions.unwrap()`` on
the result to get the inner value.  If you have a custom ``Type._dump`` method it must return an action in 3.0.  For
ease of use you can use ``bloop.actions.wrap()`` which will specify either the ``SET`` or ``REMOVE`` action to match
existing behavior.  Here's an example of how you can quickly modify your code:

.. code-block:: python

    # current pre-2.4 method, continues to work until 3.0
    def _dump(self, value, **kwargs):
        value = self.dynamo_dump(value, **kwargs)
        if value is None:
            return None
        return {self.backing_type: value}

    # works in 2.4 and 3.0
    from bloop import actions
    def _dump(self, value, **kwargs):
        value = actions.unwrap(value)
        value = self.dynamo_dump(value, **kwargs)
        return actions.wrap(value)

Note that this is backwards compatible in 2.4: ``Type._dump`` will not change unless you opt to pass the new
``Action`` object to it.

[Added]
=======

* ``SearchIterator.token`` provides a way to start a new Query or Scan from a previous query/scan's state.
  See `Issue #132`_.
* ``SearchIterator.move_to`` takes a token to update the search state.  Count/ScannedCount state are lost when
  moving to a token.
* ``Engine.delete`` and ``Engine.save`` take an optional argument ``sync=`` which can be used to update objects with
  the old or new values from DynamoDB after saving or deleting.  See the `Return Values`_ section of the User Guide
  and `Issue #137`_.
* ``bloop.actions`` expose a way to manipulate atomic counters and sets.  See the `Atomic Counters`_ section of the
  User Guide and `Issue #136`_.

.. _Issue #132: https://github.com/numberoverzero/bloop/issues/132
.. _Return Values: https://bloop.readthedocs.io/en/latest/user/engine.html#return-values
.. _Issue #137: https://github.com/numberoverzero/bloop/issues/137
.. _Atomic Counters: https://bloop.readthedocs.io/en/latest/user/engine.html#actions
.. _Issue #136: https://github.com/numberoverzero/bloop/issues/136

[Changed]
=========

* The ``atomic=`` keyword for ``Engine.save`` and ``Engine.delete`` emits ``DeprecationWarning`` and will be
  removed in 3.0.
* ``Type._dump`` will return a ``bloop.action.Action`` object if one is passed in, in preparation for the
  change in 3.0.

--------------------
 2.3.3 - 2019-01-27
--------------------

``Engine.bind`` is much faster for multi-model tables.  See `Issue #130`_.

.. _Issue #130: https://github.com/numberoverzero/bloop/issues/130

[Changed]
=========

* *(internal)* ``SessionWrapper`` caches ``DescribeTable`` responses.  You can clear these with
  ``SessionWrapper.clear_cache``; mutating calls such as ``.enable_ttl`` will invalidate the cached description.
* *(internal)* Each ``Engine.bind`` will call ``CreateTable`` at most once per table.  Subsequent calls to ``bind``
  will call ``CreateTable`` again.

--------------------
 2.3.2 - 2019-01-27
--------------------

Minor bug fix.

[Fixed]
=======

* *(internal)* ``bloop.conditions.iter_columns`` no longer yields ``None`` on ``Condition()`` (or
  any other condition whose ``.column`` attribute is ``None``).

--------------------
 2.3.0 - 2019-01-24
--------------------

This release adds support for `Transactions`_ and `On-Demand Billing`_.  Transactions can include changes across
tables, and provide ACID guarantees at a 2x throughput cost and a limit of 10 items per transaction.
See the `User Guide`__ for details.

.. code-block:: python

    with engine.transaction() as tx:
        tx.save(user, tweet)
        tx.delete(event, task)
        tx.check(meta, condition=Metadata.worker_id == current_worker)

__ https://bloop.readthedocs.io/en/latest/user/transactions.html

[Added]
=======

* ``Engine.transaction(mode="w")`` returns a transaction object which can be used directly or as a context manager.
  By default this creates a ``WriteTransaction``, but you can pass ``mode="r"`` to create a read transaction.
* ``WriteTransaction`` and ``ReadTransaction`` can be prepared for committing with ``.prepare()`` which returns a
  ``PreparedTransaction`` which can be committed with ``.commit()`` some number of times.  These calls are usually
  handled automatically when using the read/write transaction as a context manager::

    # manual calls
    tx = engine.transaction()
    tx.save(user)
    p = tx.prepare()
    p.commit()

    # equivalent functionality
    with engine.transaction() as tx:
        tx.save(user)
* Meta supports `On-Demand Billing`_::

    class MyModel(BaseModel):
        id = Column(String, hash_key=True)
        class Meta:
            billing = {"mode": "on_demand"}

* *(internal)* ``bloop.session.SessionWrapper.transaction_read`` and
  ``bloop.session.SessionWrapper.transaction_write`` can be used to call TransactGetItems and TransactWriteItems
  with fully serialized request objects.  The write api requires a client request token to provide idempotency guards,
  but does not provide temporal bounds checks for those tokens.

[Changed]
=========

* ``Engine.load`` now logs at ``INFO`` instead of ``WARNING`` when failing to load some objects.
* ``Meta.ttl["enabled"]`` will now be a literal ``True`` or ``False`` after binding the model, rather than the string
  "enabled" or "disabled".
* If ``Meta.encryption`` or ``Meta.backups`` is None or missing, it will now be set after binding the model.
* ``Meta`` and GSI read/write units are not validated if billing mode is ``"on_demand"`` since they will be 0 and the
  provided setting is ignored.


.. _Transactions: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transactions.html
.. _On-Demand Billing: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html#HowItWorks.OnDemand

--------------------
 2.2.0 - 2018-08-30
--------------------

[Added]
=======
* ``DynamicList`` and ``DynamicMap`` types can store arbitrary values, although they will only be loaded as their
  primitive, direct mapping to DynamoDB backing types.  For example::

    class MyModel(BaseModel):
        id = Column(String, hash_key=True)
        blob = Column(DynamicMap)
    i = MyModel(id="i")
    i.blob = {"foo": "bar", "inner": [True, {1, 2, 3}, b""]}

* Meta supports `Continuous Backups`_ for Point-In-Time Recovery::

    class MyModel(BaseModel):
        id = Column(String, hash_key=True)
        class Meta:
            backups = {"enabled": True}

* ``SearchIterator`` exposes an ``all()`` method which eagerly loads all results and returns a single list.
  Note that the query or scan is reset each time the method is called, discarding any previously buffered state.

[Changed]
=========

* ``String`` and ``Binary`` types load ``None`` as ``""`` and ``b""`` respectively.
* Saving an empty String or Binary (``""`` or ``b""``) will no longer throw a botocore exception, and will instead
  be treated as ``None``.  This brings behavior in line with the Set, List, and Map types.

.. _Continuous Backups: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BackupRestore.html

--------------------
 2.1.0 - 2018-04-07
--------------------

Added support for `Server-Side Encryption`_.  This uses an AWS-managed Customer Master Key (CMK) stored in `KMS`_
which is `managed for free`_: "You are not charged for the following: AWS-managed CMKs, which are automatically
created on your behalf when you first attempt to encrypt a resource in a supported AWS service."

[Added]
=======

* ``Meta`` supports Server Side Encryption::

    class MyModel(BaseModel):
        id = Column(String, hash_key=True)
        class Meta:
            encryption = {"enabled": True}

.. _Server-Side Encryption: https://aws.amazon.com/blogs/aws/new-encryption-at-rest-for-dynamodb/
.. _KMS: https://console.aws.amazon.com/iam/#/encryptionKeys
.. _managed for free: https://aws.amazon.com/kms/pricing/

--------------------
 2.0.1 - 2018-02-03
--------------------

Fix a bug where the last records in a closed shard in a Stream were dropped.  See `Issue #87`_ and
`PR #112`_.

.. _Issue #111: https://github.com/numberoverzero/bloop/issues/111
.. _PR #112: https://github.com/numberoverzero/bloop/pull/112

[Fixed]
=======

* ``Stream`` no longer drops the last records from a closed Shard when moving to the child shard.

--------------------
 2.0.0 - 2017-11-27
--------------------

2.0.0 introduces 4 significant new features:

* Model inheritance and mixins
* Table name templates:  ``table_name_template="prod-{table_name}"``
* `TTL`_ support: ``ttl = {"column": "not_after"}``
* Column defaults::

    verified=Column(Boolean, default=False)
    not_after = Column(
        Timestamp,
        default=lambda: (
            datetime.datetime.now() +
            datetime.timedelta(days=30)
        )
    )

Python 3.6.0 is now the minimum required version, as Bloop takes advantage of ``__set_name__`` and
``__init_subclass__`` to avoid the need for a Metaclass.

A number of internal-only and rarely-used external methods have been removed, as the processes which required them
have been simplified:

* ``Column.get, Column.set, Column.delete`` in favor of their descriptor protocol counterparts
* ``bloop.Type._register`` is no longer necessary before using a custom Type
* ``Index._bind`` is replaced by helpers ``bind_index`` and ``refresh_index``.  You should not need to call these.
* A number of overly-specific exceptions have been removed.

[Added]
=======

* ``Engine`` takes an optional keyword-only arg ``"table_name_template"`` which takes either a string used to format
  each name, or a function which will be called with the model to get the table name of.  This removes the need to
  connect to the ``before_create_table`` signal, which also could not handle multiple table names for the same model.
  With this change ``BaseModel.Meta.table_name`` will no longer be authoritative, and the engine must be consulted to
  find a given model's table name.  An internal function ``Engine._compute_table_name`` is available, and the
  per-engine table names may be added to the model.Meta in the future.  (see `Issue #96`_)
* A new exception ``InvalidTemplate`` is raised when an Engine's table_name_template is a string but does
  not contain the required ``"{table_name}"`` formatting key.
* You can now specify a `TTL`_ (see `Issue #87`_) on a model much like a Stream::

    class MyModel(BaseModel):
        class Meta:
            ttl = {
                "column": "expire_after"
            }


        id = Column(UUID, hash_key=True)
        expire_after = Column(Timestamp)


* A new type, ``Timestamp`` was added.  This stores a ``datetime.datetime`` as a unix timestamp in whole seconds.
* Corresponding ``Timestamp`` types were added to the following extensions, mirroring the ``DateTime`` extension:
  ``bloop.ext.arrow.Timestamp``, ``bloop.ext.delorean.Timestamp``, and ``bloop.ext.pendulum.Timestamp``.
* ``Column`` takes an optional kwarg ``default``, either a single value or a no-arg function that returns a value.
  Defaults are applied only during ``BaseModel.__init__`` and not when loading objects from a Query, Scan, or Stream.
  If your function returns ``bloop.util.missing``, no default will be applied.  (see `PR #90`_, `PR #105`_
  for extensive discussion)
* *(internal)* A new abstract interface, ``bloop.models.IMeta`` was added to assist with code completion.  This
  fully describes the contents of a ``BaseModel.Meta`` instance, and can safely be subclassed to provide hints to your
  editor::

    class MyModel(BaseModel):
        class Meta(bloop.models.IMeta):
            table_name = "my-table"
        ...

* *(internal)* ``bloop.session.SessionWrapper.enable_ttl`` can be used to enable a TTL on a table.  This SHOULD NOT
  be called unless the table was just created by bloop.
* *(internal)* helpers for dynamic model inheritance have been added to the ``bloop.models`` package:

  * ``bloop.models.bind_column``
  * ``bloop.models.bind_index``
  * ``bloop.models.refresh_index``
  * ``bloop.models.unbind``

  Direct use is discouraged without a strong understanding of how binding and inheritance work within bloop.

.. _TTL: https://aws.amazon.com/about-aws/whats-new/2017/02/amazon-dynamodb-now-supports-automatic-item-expiration-with-time-to-live-ttl/
.. _Issue #96: https://github.com/numberoverzero/bloop/issues/96
.. _Issue #87: https://github.com/numberoverzero/bloop/issues/87
.. _PR #90: https://github.com/numberoverzero/bloop/pull/90
.. _PR #105: https://github.com/numberoverzero/bloop/pull/105


[Changed]
=========

* Python 3.6 is the minimum supported version.
* ``BaseModel`` no longer requires a Metaclass, which allows it to be used as a mixin to an existing class which
  may have a Metaclass.
* ``BaseModel.Meta.init`` no longer defaults to the model's ``__init__`` method, and will instead use
  ``cls.__new__(cls)`` to obtain an instance of the model.  You can still specify a custom initialization function::

    class MyModel(BaseModel):
        class Meta:
            @classmethod
            def init(_):
                instance = MyModel.__new__(MyModel)
                instance.created_from_init = True
        id = Column(...)

* ``Column`` and ``Index`` support the shallow copy method ``__copy__`` to simplify inheritance with custom subclasses.
  You may override this to change how your subclasses are inherited.
* ``DateTime`` explicitly guards against ``tzinfo is None``, since ``datetime.astimezone`` started silently allowing
  this in Python 3.6 -- you should not use a naive datetime for any reason.
* ``Column.model_name`` is now ``Column.name``, and ``Index.model_name`` is now ``Index.name``.
* ``Column(name=)`` is now ``Column(dynamo_name=)`` and ``Index(name=)`` is now ``Index(dynamo_name=)``
* The exception ``InvalidModel`` is raised instead of ``InvalidIndex``.
* The exception ``InvalidSearch`` is raised instead of the following: ``InvalidSearchMode``, ``InvalidKeyCondition``,
  ``InvalidFilterCondition``, and ``InvalidProjection``.
* *(internal)* ``bloop.session.SessionWrapper`` methods now require an explicit table name, which is not read from the
  model name.  This exists to support different computed table names per engine.  The following methods now require
  a table name: ``create_table``, ``describe_table`` *(new)*, ``validate_table``, and ``enable_ttl`` *(new)*.


[Removed]
=========

* bloop no longer supports Python versions below 3.6.0
* bloop no longer depends on declare__
* ``Column.get``, ``Column.set``, and ``Column.delete`` helpers have been removed in favor of using the Descriptor
  protocol methods directly:  ``Column.__get__``, ``Column.__set__``, and ``Column.__delete__``.
* ``bloop.Type`` no longer exposes a ``_register`` method; there is no need to register types before using them,
  and you can remove the call entirely.
* ``Column.model_name``, ``Index.model_name``, and the kwargs ``Column(name=)``, ``Index(name=)`` (see above)
* The exception ``InvalidIndex`` has been removed.
* The exception ``InvalidComparisonOperator`` was unused and has been removed.
* The exception ``UnboundModel`` is no longer raised during ``Engine.bind`` and has been removed.
* The exceptions ``InvalidSearchMode``, ``InvalidKeyCondition``, ``InvalidFilterCondition``, and ``InvalidProjection``
  have been removed.
* *(internal)* ``Index._bind`` has been replaced with the more complete solutions in ``bloop.models.bind_column`` and
  ``bloop.models.bind_index``.

__ https://pypi.python.org/pypi/declare

--------------------
 1.3.0 - 2017-10-08
--------------------

This release is exclusively to prepare users for the ``name``/``model_name``/``dynamo_name`` changes coming in 2.0;
your 1.2.0 code will continue to work as usual but will raise ``DeprecationWarning`` when accessing ``model_name`` on
a Column or Index, or when specifying the ``name=`` kwarg in the ``__init__`` method of ``Column``,
``GlobalSecondaryIndex``, or ``LocalSecondaryIndex``.

Previously it was unclear if ``Column.model_name`` was the name of this column in its model, or the name of the model
it is attached to (eg. a shortcut for ``Column.model.__name__``).  Additionally the ``name=`` kwarg actually mapped to
the object's ``.dynamo_name`` value, which was not obvious.

Now the ``Column.name`` attribute will hold the name of the column in its model, while ``Column.dynamo_name`` will
hold the name used in DynamoDB, and is passed during initialization as ``dynamo_name=``.  Accessing ``model_name`` or
passing ``name=`` during ``__init__`` will raise deprecation warnings, and bloop 2.0.0 will remove the deprecated
properties and ignore the deprecated kwargs.

[Added]
=======

* ``Column.name`` is the new home of the ``Column.model_name`` attribute.  The same is true for
  ``Index``, ``GlobalSecondaryIndex``, and ``LocalSecondaryIndex``.
* The ``__init__`` method of ``Column``, ``Index``, ``GlobalSecondaryIndex``, and ``LocalSecondaryIndex`` now takes
  ``dynamo_name=`` in place of ``name=``.

[Changed]
=========

* Accessing ``Column.model_name`` raises ``DeprecationWarning``, and the same for Index/GSI/LSI.
* Providing ``Column(name=)`` raises ``DeprecationWarning``, and the same for Index/GSI/LSI.

--------------------
 1.2.0 - 2017-09-11
--------------------

[Changed]
=========

* When a Model's Meta does not explicitly set ``read_units`` and ``write_units``, it will only default to 1/1 if the
  table does not exist and needs to be created.  If the table already exists, any throughput will be considered
  valid.  This will still ensure new tables have 1/1 iops as a default, but won't fail if an existing table has more
  than one of either.

  There is no behavior change for explicit **integer** values of ``read_units`` and ``write_units``: if the table does
  not exist it will be created with those values, and if it does exist then validation will fail if the actual values
  differ from the modeled values.

  An explicit ``None`` for either ``read_units`` or ``write_units`` is equivalent to omitting the value, but allows
  for a more explicit declaration in the model.

  Because this is a relaxing of a default only within the context of validation (creation has the same semantics) the
  only users that should be impacted are those that do not declare ``read_units`` and ``write_units`` and rely on the
  built-in validation **failing** to match on values != 1.  Users that rely on the validation to succeed on tables with
  values of 1 will see no change in behavior.  This fits within the extended criteria of a minor release since there
  is a viable and obvious workaround for the current behavior (declare 1/1 and ensure failure on other values).

* When a Query or Scan has projection type "count", accessing the ``count`` or ``scanned`` properties will
  immediately execute and exhaust the iterator to provide the count or scanned count.  This simplifies the previous
  workaround of calling ``next(query, None)`` before using ``query.count``.

[Fixed]
=======

* Fixed a bug where a Query or Scan with projection "count" would always raise KeyError (see `Issue #95`_)
* Fixed a bug where resetting a Query or Scan would cause ``__next__``
  to raise ``botocore.exceptions.ParamValidationError`` (see `Issue #95`_)

.. _Issue #95: https://github.com/numberoverzero/bloop/issues/95

--------------------
 1.1.0 - 2017-04-26
--------------------

[Added]
=======
* ``Engine.bind`` takes optional kwarg ``skip_table_setup``
  to skip CreateTable and DescribeTable calls (see `Issue #83`_)
* Index validates against a superset of the projection (see `Issue #71`_)

.. _Issue #83: https://github.com/numberoverzero/bloop/issues/83
.. _Issue #71: https://github.com/numberoverzero/bloop/issues/71


--------------------
 1.0.3 - 2017-03-05
--------------------

Bug fix.

[Fixed]
=======

* Stream orders records on the integer of SequenceNumber, not the lexicographical sorting of its string
  representation.  This is an annoying bug, because `as documented`__ we **should** be using lexicographical sorting
  on the opaque string.  However, without leading 0s that sort fails, and we must assume the string represents an
  integer to sort on.  Particularly annoying, tomorrow the SequenceNumber could start with non-numeric characters
  and still conform to the spec, but the sorting-as-int assumption breaks.  However, we can't properly sort without
  making that assumption.

__ http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_StreamRecord.html#DDB-Type-streams_StreamRecord-SequenceNumber

--------------------
 1.0.2 - 2017-03-05
--------------------

Minor bug fix.

[Fixed]
=======

* extension types in ``ext.arrow``, ``ext.delorean``, and ``ext.pendulum`` now load and dump ``None`` correctly.

--------------------
 1.0.1 - 2017-03-04
--------------------

Bug fixes.

[Changed]
=========

* The ``arrow``, ``delorean``, and ``pendulum`` extensions now have a default timezone of ``"utc"`` instead of
  ``datetime.timezone.utc``.  There are open issues for both projects to verify if that is the expected behavior.

[Fixed]
=======

* DynamoDBStreams return a Timestamp for each record's ApproximateCreationDateTime, which botocore is translating
  into a real datetime.datetime object.  Previously, the record parser assumed an int was used.  While this fix is
  a breaking change for an internal API, this bug broke the Stream iterator interface entirely, which means no one
  could have been using it anyway.

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

See the Migration Guide above for specific examples of breaking changes and how to fix them, or the
`User Guide`__ for a tour of the new Bloop.  Lastly, the Public and Internal API References are
finally available and should cover everything you need to extend or replace whole subsystems in Bloop
(if not, please open an issue).

__ https://bloop.readthedocs.io/en/latest/user/quickstart.html#user-quickstart

[Added]
=======

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
  record creation time.  Use ``engine.stream(model, "trim_horizon")`` to get started.  See the
  `User Guide`__ for details.
* New exceptions ``RecordsExpired`` and ``ShardIteratorExpired`` for errors in stream state
* New exceptions ``Invalid*`` for bad input subclass ``BloopException`` and ``ValueError``
* ``DateTime`` types for the three most common date time libraries:

    * ``bloop.ext.arrow.DateTime``
    * ``bloop.ext.delorean.DateTime``
    * ``bloop.ext.pendulum.DateTime``

* ``model.Meta`` has a new optional attribute ``stream`` which can be used to enable a stream on the model's table.
* ``model.Meta`` exposes the same ``projection`` attribute as ``Index`` so that ``(index or model.Meta).projection``
  can be used interchangeably
* New ``Stream`` class exposes DynamoDBStreams API as a single iterable with powerful seek/jump options, and simple
  json-friendly tokens for pausing and resuming iteration.
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

__ https://bloop.readthedocs.io/en/latest/user/streams.html#user-streams

[Changed]
=========

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
  For a less strict, **lossy** ``Float`` type see the `Patterns`__ section of the User Guide
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

__ https://bloop.readthedocs.io/en/latest/user/patterns.html#patterns-float

[Removed]
=========

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

[Fixed]
=======

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
  parameter.  This was mentioned in 0.9.2 and ``context`` is now required.
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
  ``context``.  This dict will become required in 0.9.6, and contains the engine
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
