.. _changelog:

Versions
^^^^^^^^

This document provides migration instructions for each major version, as well as the complete changelog for
versions dating back to :ref:`v0.9.0<changelog-v0.9.0>` from December 2015.  The migration guides provide detailed
examples and tips for migrating from the previous major version (excluding the 1.0.0 guide, which only covers
migration from 0.9.0 and newer).

====================
 Migrating to 3.0.0
====================

The 3.0.0 release includes two api changes from 2.4.0 that you may need to update your code to handle.

* The ``atomic=`` kwarg to ``Engine.save`` and ``Engine.delete`` was deprecated in 2.4.0 and is removed in 3.0.0.
* The return type of ``Type._dump`` must now be a ``bloop.action.Action`` instance, even when the value is ``None``.
  This does not impact custom types that only implement ``dynamo_load`` and ``dynamo_dump``.

--------------------
 ``atomic`` keyword
--------------------

The atomic keyword to Engine.save and Engine.delete has been removed in favor of a user pattern.  This offers a
reasonable performance improvement for users that never used the atomic keyword, and addresses ambiguity related to
per-row atomic vs transactional atomic operations.  For context on the deprecation, see `Issue #138`_.  For the
equivalent user pattern, see :ref:`patterns-snapshot`.  To migrate your existing code, you can use the following:

.. code-block:: python

    # pre-3.0 code to migrate:
    engine.load(some_object)
    some_object.some_attr = "new value"
    engine.save(some_object, atomic=True)

    # post-3.0 code:

    # https://bloop.readthedocs.io/en/latest/user/patterns.html#snapshot-condition
    from your_patterns import snapshot

    engine.load(some_object)
    condition = snapshot(some_object)
    some_object.some_attr = "new value"
    engine.save(some_object, condition=condition)

.. _Issue #138: https://github.com/numberoverzero/bloop/issues/138

------------
 Type._dump
------------

Bloop now allows users to specify how a value should be applied in an UpdateExpression by wrapping a value in a
``bloop.actions.Action`` object.  This is done transparently for raw values, which are interpreted
as either ``bloop.actions.set`` or ``bloop.actions.remove``.  With 2.4 and to support `Issue #136`_ you can also
specify an ``add`` or ``delete`` action:

.. code-block:: python

    my_user.aliases = bloop.actions.add("new_alias")
    my_website.views = bloop.actions.add(1)

To maintain flexibility the bloop ``Type`` class has the final say as to which action a value should use.  This allows
eg. the ``List`` type to take a literal ``[]`` and change the action from ``actions.set`` to ``actions.remove(None)``
to indicate that the value should be cleared.  This also means your custom type could see an ``actions.delete`` and
modify the value to instead be expressible in an ``actions.set``.

If your custom types today only override ``dynamo_dump`` or ``dynamo_load`` then you don't need to do anything for this
migration.  However if you currently override ``_dump`` then you should update your function to (1) handle input that
may be an action or not, and (2) always return an action instance.  In general, you should not modify an input
action and instead should return a new instance (possibly with the same action_type).

Here's the migration of the base ``Type._dump``:

.. code-block:: python

    # pre-3.0 code to migrate:
    def _dump(self, value, **kwargs):
        value = self.dynamo_dump(value, **kwargs)
        if value is None:
            return None
        return {self.backing_type: value}


    # post-3.0 code:
    from bloop import actions

    def _dump(self, value, **kwargs):
        wrapped = actions.wrap(value)  # [1]
        value = self.dynamo_dump(wrapped.value, **kwargs)
        if value is None:
            return actions.wrap(None)  # [2]
        else:
            value = {self.backing_type: value}
            return wrapped.type.new_action(value)  # [3]

    # [1] always wrap the input value to ensure you're working with an Action instance
    # [2] returns actions.remove(None) which will remove the value like None previously did
    # [3] new_action uses the **same action type** as the input.
    #         If you want to always return a SET action instead use: return actions.set(value)


.. _Issue #136: https://github.com/numberoverzero/bloop/issues/136

====================
 Migrating to 2.0.0
====================

The 2.0.0 release includes a number of api changes and new features.

* The largest functional change is the ability to compose models through subclassing; this is
  referred to as Abstract Inheritance and Mixins throughout the User Guide.
* Python 3.6.0 is the minimum required version.
* ``Meta.init`` now defaults to ``cls.__new__(cls)`` instead of ``cls.__init__()``; when model instances are created
  as part of ``engine.query``, ``engine.stream`` etc. these will not call your model's ``__init__`` method.  The
  default ``BaseModel.__init__`` is not meant for use outside of local instantiation.
* The ``Column`` and ``Index`` kwarg ``name`` was renamed to ``dynamo_name`` to accurately reflect how the value was
  used: ``Column(SomeType, name="foo")`` becomes ``Column(SomeType, dynamo_name="foo")``.
  Additionally, the column and index attribute ``model_name`` was renamed to ``name``; ``dynamo_name`` is unchanged
  and reflects the kwarg value, if provided.


--------
 Engine
--------

A new Engine kwarg ``table_name_template`` can be used to modify the table name used per-engine, as documented in
the new :ref:`Engine Configuration <user-engine-config>` section of the User Guide.  Previously, you may have used
the ``before_create_table`` signal as follows:

.. code-block:: python

    # Nonce table names to avoid testing collisions
    @before_create_table.connect
    def apply_table_nonce(_, model, **__):
        nonce = datetime.now().isoformat()
        model.Meta.table_name += "-test-{}".format(nonce)

This will modify the actual model's ``Meta.table_name``, whereas the new kwarg can be used to only modify the bound
table name for a single engine.  The following can be expressed for a single Engine as follows:


.. code-block:: python

    def apply_nonce(model):
        nonce = datetime.now().isoformat()
        return f"{model.Meta.table_name}-test-{nonce}"

    engine = Engine(table_name_template=apply_nonce)

-------------
 Inheritance
-------------

You can now use abstract base models to more easily compose common models.  For example, you may use the same
id structure for classes.  Previously, this would look like the following:

.. code-block:: python

    class User(BaseModel):
        id = Column(String, hash_key=True)
        version = Column(Integer, range_key=True)
        data = Column(Binary)

    class Profile(BaseModel):
        id = Column(String, hash_key=True)
        version = Column(Integer, range_key=True)
        summary = Column(String)

Now, you can define an abstract base and re-use the ``id`` and ``version`` columns in both:

.. code-block:: python

    class MyBase(BaseModel):
        class Meta:
            abstract = True
        id = Column(String, hash_key=True)
        version = Column(Integer, range_key=True)

    class User(MyBase):
        data = Column(Binary)

    class Profile(MyBase):
        summary = Column(String)


You can use multiple inheritance to compose models from multiple mixins; base classes do not need to subclass
``BaseModel``.  Here's the same two models as above, but the hash and range keys are defined across two mixins:


.. code-block:: python

    class StringHash:
        id = Column(String, hash_key=True)

    class IntegerRange:
        version = Column(Integer, range_key=True)


    class User(StringHash, IntegerRange, BaseModel):
        data = Column(Binary)

    class Profile(StringHash, IntegerRange, BaseModel):
        summary = Column(String)

Mixins may also contain ``GlobalSecondaryIndex`` and ``LocalSecondaryIndex``, even if their hash/range keys aren't
defined in that mixin:


.. code-block:: python

    class ByEmail:
        by_email = GlobalSecondaryIndex(projection="keys", hash_key="email")


    class User(StringHash, IntegerRange, ByEmail, BaseModel):
        email = Column(String)

-----------
 Meta.init
-----------

With the addition of column defaults (see below) Bloop needed to differentiate local mode instantiation from remote
model instantiation.  Local model instantiation still uses ``__init__``, as in:

.. code-block:: python

    user = User(email="me@gmail.com", verified=False)

Unlike ``Engine.load`` which takes existing model instances, all of ``Engine.query, Engine.scan, Engine.stream``
will create their own instances.  These methods use the model's ``Meta.init`` to create new instances.  Previously
this defaulted to ``__init__``.  However, with the default ``__init__`` method applying defaults in 2.0.0 this is
no longer acceptable for remote instantiation.  Instead, ``cls.__new__(cls)`` is used by default to create instances
during query/scan/stream.

This is an important distinction that Bloop should have made early on, but was forced due to defaults.  For example,
imagine querying an index that doesn't project a column with a default.  If the base ``__init__`` was still used, the
Column's default would be used for the non-projected column even if there was already a value in DynamoDB.  Here's one
model that would have the problem:

.. code-block:: python

    class User(BaseModel):
        id = Column(UUID, hash_key=True)
        created = Column(DateTime, default=datetime.datetime.now)
        email = Column(String)
        by_email = GlobalSecondaryIndex(projection="keys", hash_key=email)

    user = User(id=uuid.uuid4(), email="me@gmail.com")
    engine.save(user)
    print(user.created)  # Some datetime T1


    query = engine.Query(User.by_email, hash_key=User.email=="me@gmail.com")
    partial_user = query.first()

    partial_user.created  # This column isn't part of the index's projection!


If ``User.Meta.init`` was still ``User.__init__`` then ``partial_user.created`` would invoke the default function for
``User.created`` and give us the current datetime.  Instead, Bloop 2.0.0 will call ``User.__new__(User)`` and we'll
get an ``AttributeError`` because ``partial_user`` doesn't have a ``created`` value.


-----------------
 Column Defaults
-----------------

Many columns have the same initialization value, even across models.  For example, all but one of the following
columns will be set to the same value or using the same logic:

.. code-block:: python

    class User(BaseModel):
        email = Column(String, hash_key=True)
        id = Column(UUID)
        verified = Column(Boolean)
        created = Column(DateTime)
        followers = Column(Integer)

Previously, you might apply defaults by creating a simple function:

.. code-block:: python

    def new_user(email) -> User:
        return User(
            email=email,
            id=uuid.uuid4(),
            verified=False,
            created=datetime.datetime.now(),
            followers=0)

You'll still need a function for related initialization (eg. across fields or model instances) but for simple defaults,
you can now specify them with the Column:

.. code-block:: python

    class User(BaseModel):
        email = Column(String, hash_key=True)
        id = Column(UUID, default=uuid.uuid4)
        verified = Column(Boolean, default=False)
        created = Column(DateTime, default=datetime.datetime.now)
        followers = Column(Integer, default=0)


    def new_user(email) -> User:
        return User(email=email)

Defaults are only applied when creating new local instances inside the default ``BaseModel.__init__`` - they are not
evaluated when loading objects with ``Engine.load``, ``Engine.query``, ``Engine.stream`` etc.  If you define a custom
``__init__`` without calling ``super().__init__(...)`` they will not be applied.

In a related change, see above for the ``BaseModel.Meta.init`` change.  By default Bloop uses ``cls.__new__(cls)`` to
create new instances of your models during ``Engine.scan`` and ``Engine.query`` instead of the previous default to
``__init__``.  This is intentional, to avoid applying unnecessary defaults to partially-loaded objects.


-----
 TTL
-----

DynamoDB introduced the ability to specify a `TTL`_ column, which indicates a date (in seconds since the epoch) after
which the row may be automatically (eventually) cleaned up.  This column must be a Number, and Bloop exposes the
``Timestamp`` type which is used as a ``datetime.datetime``.  Like the DynamoDBStreams feature, the TTL is configured
on a model's Meta attribute:

.. code-block:: python

    class TemporaryPaste(BaseModel):
        class Meta:
            ttl = {
                "column": "delete_after"
            }
        id = Column(String, hash_key=True)
        s3_location = Column(String, dynamo_name="s3")
        delete_after = Column(Timestamp)

Remember that it can take up to 24 hours for the row to be deleted; you should guard your reads using the current time
against the cleanup time, or a filter with your queries:

.. code-block:: python

    # made up index
    query = engine.Query(
        TemporaryPaste.by_email,
        key=TemporaryPaste.email=="me@gmail.com",
        filter=TemporaryPaste.delete_after <= datetime.datetime.now())
    print(query.first())

Bloop still refuses to update existing tables, so TTL will only be enabled on tables if they are created by Bloop
during ``Engine.bind``.  Otherwise, the declaration exists exclusively to verify configuration.

.. _TTL: https://aws.amazon.com/about-aws/whats-new/2017/02/amazon-dynamodb-now-supports-automatic-item-expiration-with-time-to-live-ttl/

-------
 Types
-------

A new type ``Timestamp`` was added for use with the new TTL feature (see above).  This is a ``datetime.datetime`` in
Python just like the ``DateTime`` type, but is stored as an integer (whole seconds since epoch) instead of an ISO 8601
string.  As with ``DateTime``, drop-in replacements are available for ``arrow``, ``delorean``, and ``pendulum``.

------------
 Exceptions
------------

* ``InvalidIndex`` was replaced by the existing ``InvalidModel``
* ``InvalidSearchMode``, ``InvalidKeyCondition``, ``InvalidFilterCondition``, and ``InvalidProjection`` were replaced
  by ``InvalidSearch``
* ``UnboundModel`` was removed without replacement; ``Engine.bind`` was refactored so that it would never be raised.
* ``InvalidComparisonOperator`` was removed without replacement; it was never raised.

====================
 Migrating to 1.0.0
====================

The 1.0.0 release includes a number of api changes, although functionally not much has changed since 0.9.6.  The
biggest changes are to Query and Scan syntax, which has changed from a builder pattern to a single call.  The
remaining changes are mostly resolved through a different import or parameter/attribute name.

-----------------
 Session, Client
-----------------

In 1.0.0 the Engine wraps two clients: one for DynamoDB, and one for DynamoDBStreams.  Bloop will create default
clients for any missing parameters using ``boto3.client``:

.. code-block:: python

    import boto3
    from bloop import Engine

    ddb = boto3.client("dynamodb")
    streams = boto3.client("dynamodbstreams")
    engine = Engine(dynamodb=ddb, dynamodbstreams=streams)

Before 0.9.11
=============

Prior to 0.9.11, you could customize the session that an Engine used to talk to DynamoDB by creating an instance of a
:class:`boto3.session.Session` and passing it to the Engine during instantiation.  This allowed you to use a different
profile name:

.. code-block:: python

    from boto3 import Session
    from bloop import Engine

    session = Session(profile_name="my-profile")
    engine = Engine(session=session)

Now, you will need to create client instances from that session:

.. code-block:: python

    from boto3 import session
    from bloop import Engine

    session = Session(profile_name="my-profile")
    engine = Engine(
        dynamodb=session.client("dynamodb"),
        dynamodbstreams=session.client("dynamodbstreams")
    )

After 0.9.11
============

In 0.9.11, the Engine changed to take a :class:`bloop.Client` which wrapped a boto3 client.  This allowed you to
connect to a different endpoint, such as a DynamoDBLocal instance:

.. code-block:: python

    import boto3
    from bloop import Client, Engine

    boto_client = boto3.client("dynamodb", endpoint_url="http://localhost:8000")
    bloop_client = Client(boto_client=boto_client)
    engine = Engine(client=bloop_client)

The intermediate bloop Client is no longer necessary, but a dynamodbstreams client can be provided:

.. code-block:: python

    import boto3
    from bloop import Client, Engine

    ddb = boto3.client("dynamodb", endpoint_url="http://localhost:8000")
    streams = boto3.client("dynamodbstreams", endpoint_url="http://localhost:8000")
    engine = Engine(dynamodb=ddb, dynamodbstreams=streams)

--------
 Engine
--------

Config
======

Prior to 1.0.0, Engine took a number of configuration options.  These have all been removed, and baked into existing
structures, or are only specified at the operation level.  Engine no longer takes ``**config`` kwargs.

* ``atomic`` controlled the default value for ``delete`` and ``save`` operations.  If your engine had a default
  ``atomic`` of ``True``, you must now explicitly specify that with each ``delete`` and ``save``.
  The same is true for ``consistent``, which controlled the default for ``load``, ``query``, and ``scan``.
* ``prefetch`` controlled the default number of items that Bloop would fetch for a ``query`` or ``scan``.  Bloop now
  uses the built-in pagination controls, and will fetch the next page when the currently buffered page has been
  iterated.  There is no way to control the number of items loaded into the buffer at once.
* ``strict`` controlled the default setting for ``query`` and ``scan`` against an LSI.  This is now part of the
  declaration of an LSI: ``by_create = LocalSecondaryIndex(projection="all", range_key="created", strict=False)``.  By
  default an LSI is strict, which matches the default configuration option.  This change means an LSI must be accessed
  by every caller the same way.  You can't have one caller use ``strict=True`` while another uses ``strict=False``.

EngineView and ``context``
==========================

Because there are no more ``engine.config`` values, there is no point to using engines as context managers.
Previously, you could use an ``EngineView`` to change one config option of an engine for a local command, without
changing the underlying engine's configuration:

.. code-block:: python

    with engine.context(atomic=True) as atomic:
        atomic.save(...)
        # a bunch of operations that perform atomic saves

``Engine.context`` and the ``EngineView`` class have been removed since there is no longer an ``Engine.config``.

Engine.save, Engine.delete
==========================

These functions take ``*objs`` instead of ``objs``, which makes passing a small number of items more comfortable.

.. code-block:: python

    user = User(...)
    tweet = Tweet(...)

    # Old: explicit list required
    engine.save([user, tweet])

    # 1.0.0: *varargs
    engine.save(user, tweet)

    # 1.0.0: save a list
    some_users = get_modified()
    engine.save(*some_users)

-------------
 Query, Scan
-------------

Queries and Scans are now created in a single call, instead of using an ambiguous builder pattern.  This will simplify
most calls, but will be disruptive if you rely on partially building queries in different parts of your code.

Creating Queries
================

The most common issue with the builder pattern was creating multi-condition filters.  Each call would **replace** the
existing filter, not append to it.  For example:

.. code-block:: python

    # This only checks the date, NOT the count
    q = engine.query(User).key(User.id == 0)
    q = q.filter(User.friends >= 3)
    q = q.filter(User.created >= arrow.now().replace(years=-1))

    # 1.0.0 only has one filter option
    q = engine.query(
        User, key=User.id == 0,
        filter=(
            (User.friends >= 3) &
            (User.created >= ...)
        )
    )

The other query controls have been baked in, including ``projection``, ``consistent``, and ``forward``.  Previously,
you changed the ``forward`` option through the properties ``ascending`` and ``descending``.  Use ``forward=False`` to
sort descending.

Here is a query with all options before and after.  The structure is largely the same, with a lower symbolic overhead:

.. code-block:: python

    # Pre 1.0.0
    q = (
        engine.query(User)
            .key(User.id == 0)
            .projection("all")
            .descending
            .consistent
            .filter(User.name.begins_with("a"))
    )

    # 1.0.0
    q = engine.query(
        User,
        key=User.id == 0,
        projection="all",
        forward=False,
        consistent=True,
        filter=User.name.begins_with("a")
    )


The same changes apply to :func:`Engine.scan <bloop.engine.Engine.scan>`, although Scans can't be performed in
descending order.

Parallel Scans
==============

1.0.0 allows you to create a parallel scan by specifying the segment that this scan covers.  This is just a tuple of
``(Segment, TotalSegments)``.  For example, to scan ``Users`` in three pieces:

.. code-block:: python

    scans = [
        engine.scan(User, parallel=(0, 3)),
        engine.scan(User, parallel=(1, 3)),
        engine.scan(User, parallel=(2, 3))
    ]

    for worker, scan in zip(workers, scans):
        worker.process(scan)

Iteration and Properties
========================

The ``all`` method and ``prefetch`` and ``limit`` options have been removed.  Each call to :func:`Engine.query` or
:func:`Engine.scan` will create a new iterator that tracks its progress and can be reset.  To create different
iterators over the same parameters, you must call :func:`Engine.query` multiple times.

.. code-block:: pycon

    # All the same iterator
    >>> scan = engine.scan(User, filter=...)
    >>> it_one = iter(scan)
    >>> it_two = iter(scan)
    >>> it_one is it_two is scan
    True

Query and Scan no longer buffer their results, and you will need to reset the query to execute it again.

.. code-block:: python

    >>> scan = engine.scan(User)
    >>> for result in scan:
    ...     pass
    ...
    >>> scan.exhausted
    True
    >>> scan.reset()
    >>> for result in scan:
    ...     print(result.id)
    ...
    0
    1
    2

* The ``complete`` property has been renamed to ``exhausted`` to match the new ``Stream`` interface.
* The ``results`` property has been removed.
* ``count``, ``scanned``, ``one()``, and ``first()`` are unchanged.

--------
 Models
--------

Base Model and ``abstract``
===========================

Model declaration is largely unchanged, except for the model hierarchy.  Early versions tied one base model to one
engine; later versions required a function to create each new base.  In 1.0.0, every model inherits from a single
abstract model, :class:`~bloop.models.BaseModel`:

.. code-block:: python

    from bloop import BaseModel, Column, Integer


    class User(BaseModel):
        id = Column(Integer, hash_key=True)
        ...

Additionally, any model can be an abstract base for a number of other models (to simplify binding subsets of all
models) by setting the ``Meta`` attribute ``abstract`` to ``True``:

.. code-block:: python

    from bloop import BaseModel

    class AbstractUser(BaseModel):
        class Meta:
            abstract = True

        @property
        def is_email_verified(self):
            return bool(getattr(self, "verified", False))

Before 0.9.6
------------

Models were tied to a single Engine, and so the base class for any model had to come from that Engine:

.. code-block:: python

    from bloop import Engine

    primary = Engine()
    secondary = Engine()

    class User(primary.model):
        ...

    # secondary can't save or load instances of User!

Now that models are decoupled from engines, any engine can bind and load any model:

.. code-block:: python

    from bloop import BaseModel, Engine

    primary = Engine()
    secondary = Engine()

    class User(BaseModel):
        ...

    primary.bind(User)
    secondary.bind(User)

After 0.9.6
-----------

After models were decoupled from Engines, Bloop still used some magic to create base models that didn't have hash keys
but also didn't fail various model creation validation.  This meant you had to get a base model from ``new_base()``:

.. code-block:: python

    from bloop import Engine, new_base

    primary = Engine()
    secondary = Engine()

    Base = new_base()

    class User(Base):
        ...

    primary.bind(User)
    secondary.bind(User)

Now, the base model is imported directly.  You can simplify the transition using an alias import.  To adapt the above
code, we would alias ``BaseModel`` to ``Base``:

.. code-block:: python

    from bloop import Engine
    from bloop import BaseModel as Base

    primary = Engine()
    secondary = Engine()

    class User(Base):
        ...

    primary.bind(User)
    secondary.bind(User)

Binding
=======

:func:`Engine.bind <bloop.engine.Engine.bind>` has undergone a few stylistic tweaks, and started offering recursive
binding.  The parameter ``base`` is no longer keyword-only.

To bind all concrete (``Meta.abstract=False``) models from a single base, pass the base model:

.. code-block:: python

    from bloop import BaseModel, Engine

    class AbstractUser(BaseModel):
        class Meta:
            abstract = True

    class AbstractDataBlob(BaseModel):
        class Meta:
            abstract = True

    class User(AbstractUser):
        ...

    class S3Blob(AbstractDataBlob):
        ...

    engine = Engine()
    engine.bind(AbstractUser)

This will bind ``User`` but not ``S3Blob``.

---------
 Indexes
---------

Projection is Required
======================

In 1.0.0, ``projection`` is required for both :class:`~bloop.models.GlobalSecondaryIndex` and
:class:`~bloop.models.LocalSecondaryIndex`.  This is because Bloop now supports binding multiple models to the same
table, and the ``"all"`` projection is not really DynamoDB's all, but instead an ``INCLUDE`` with all columns that
the model defines.

Previously:

.. code-block:: python

    from bloop import new_base, Column, Integer, GlobalSecondaryIndex

    class MyModel(new_base()):
        id = Column(Integer, hash_key=True)
        data = Column(Integer)

        # implicit "keys"
        by_data = GlobalSecondaryIndex(hash_key="data")

Now, this must explicitly state that the projection is "keys":

.. code-block:: python

    from bloop import BaseModel, Column, Integer, GlobalSecondaryIndex

    class MyModel(BaseModel):
        id = Column(Integer, hash_key=True)
        data = Column(Integer)

        by_data = GlobalSecondaryIndex(
            projection="keys", hash_key="data")

Hash and Range Key
==================

1.0.0 also lets you use the Column object (and not just its model name) as the parameter to ``hash_key`` and
``range_key``:

.. code-block:: python

    class MyModel(BaseModel):
        id = Column(Integer, hash_key=True)
        data = Column(Integer)

        by_data = GlobalSecondaryIndex(
            projection="keys", hash_key=data)

``__set__`` and ``__del__``
===========================

Finally, Bloop disallows setting and deleting attributes on objects with the same name as an index.  Previously, it
would simply set that value on the object and silently ignore it when loading or saving.  It wasn't clear that the
value wasn't applied to the Index's hash or range key.

.. code-block:: python

    >>> class MyModel(BaseModel):
    ...     id = Column(Integer, hash_key=True)
    ...     data = Column(Integer)
    ...     by_data = GlobalSecondaryIndex(
    ...         projection="keys", hash_key=data)
    ...
    >>> obj = MyModel()
    >>> obj.by_data = "foo"
    Traceback (most recent call last):
      ...
    AttributeError: MyModel.by_data is a GlobalSecondaryIndex

-------
 Types
-------

DateTime
========

Previously, :class:`~bloop.types.DateTime` was backed by arrow.  Instead of forcing a particular library on users --
and there are a number of high-quality choices -- Bloop's built-in datetime type is now backed by the standard
library's :class:`datetime.datetime`.  This type only loads and dumps values in UTC, and uses a fixed ISO8601 format
string which always uses ``+00:00`` for the timezone.  :class:`~bloop.types.DateTime` will forcefully convert the
timezone when saving to DynamoDB with :func:`datetime.datetime.astimezone` which raises on naive datetime objects.
For this reason, you must specify a timezone when using this type.

Most users are expected to have a preferred datetime library, and so Bloop now includes implementations of DateTime
in a new extensions module ``bloop.ext`` for the three most popular datetime libraries: arrow, delorean, and pendulum.
These expose the previous interface, which allows you to specify a local timezone to apply when loading values from
DynamoDB.  It still defaults to UTC.

To swap out an existing DateTime class and continue using arrow objects:

.. code-block:: python

    # from bloop import DateTime
    from bloop.ext.arrow import DateTime

To use delorean instead:

.. code-block:: python

    # from bloop import DateTime
    from bloop.ext.delorean import DateTime

Future extensions will also be grouped by external package, and are not limited to types.  For example, an alternate
Engine implementation could be provided in ``bloop.ext.sqlalchemy`` that can bind SQLAlchemy's ORM models and
transparently maps Bloop types to SQLALchemy types.

Float
=====

Float has been renamed to :class:`~bloop.types.Number` and now takes an optional :class:`decimal.Context` to use when
translating numbers to DynamoDB's wire format.  The same context used in previous versions (which comes
from the specifications in DynamoDB's User Guide) is used as the default; existing code only needs to use the new
name or alias it on import:

.. code-block:: python

    # from bloop import Float
    from bloop import Number as Float

:ref:`A new pattern <patterns-float>` has been added that provides a less restrictive type which always loads and
dumps ``float`` instead of :class:`decimal.Decimal`.  This comes at the expense of exactness, since Float's decimal
context does not trap Rounding or Inexact signals.  This is a common request for boto3; keep its limitations in mind
when storing and loading values.  It's probably fine for a cached version of a product rating, but you're playing with
fire storing account balances with it.

String
======

A minor change, :class:`~bloop.types.String` no longer calls ``str(value)`` when dumping to DynamoDB.  This was
obscuring cases where the wrong value was provided, but the type silently coerced a string using that object's
``__str__``.  Now, you will need to manually call ``str`` on objects, or boto3 will complain of an incorrect type.

.. code-block:: pycon

    >>> from bloop import BaseModel, Column, Engine, String

    >>> class MyModel(BaseModel):
    ...     id = Column(String, hash_key=True)
    ...
    >>> engine = Engine()
    >>> engine.bind(MyModel)

    >>> not_a_str = object()
    >>> obj = MyModel(id=not_a_str)

    # previously, this would store "<object object at 0x7f92a5a2f680>"
    # since that is str(not_a_str).
    >>> engine.save(obj)

    # now, this raises (newlines for readability)
    Traceback (most recent call last):
      ..
    ParamValidationError: Parameter validation failed:
    Invalid type for
        parameter Key.id.S,
        value: <object object at 0x7f92a5a2f680>,
        type: <class 'object'>,
        valid types: <class 'str'>

------------
 Exceptions
------------

``NotModified`` was raised by :func:`Engine.load <bloop.engine.Engine.load>` when some objects were not found.  This
has been renamed to :exc:`~bloop.exceptions.MissingObjects` and is otherwise unchanged.

Exceptions for unknown or abstract models have changed slightly.  When an Engine fails to load or dump a model,
it will raise :exc:`~bloop.exceptions.UnboundModel`.  When a value fails to load or dump but isn't a subclass of
:class:`~bloop.models.BaseModel`, the engine raises :exc:`~bloop.exceptions.UnknownType`.  When you attempt to perform
a mutating operation (load, save, ...) on an abstract model, the engine raises :exc:`~bloop.exceptions.InvalidModel`.

.. include:: ../../CHANGELOG.rst
