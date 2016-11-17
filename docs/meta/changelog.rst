.. _changelog:

Versions
^^^^^^^^

This document provides migration instructions for each major version, as well as the complete changelog for
versions dating back to :ref:`v0.9.0<changelog-v0.9.0>` from December 2015.  The migration guides provide detailed
examples and tips for migrating from the previous major version (excluding the 1.0.0 guide, which only covers
migration from 0.9.0 and newer).

====================
 Migrating to 1.0.0
====================

The 1.0.0 release includes a number of api changes, although functionally not much has changed since 0.9.6.  The
biggest changes are to Query and Scan syntax, which has changed from a builder pattern to a single call.  The
remaining changes are mostly resolved through a different import or parameter/attribute name.

--------------------------
 Custom Session or Client
--------------------------

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

---------------
 Engine Config
---------------

Defaults
========

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

----------------------------
 Engine.save, Engine.delete
----------------------------

These functions take ``*objs`` instead of ``objs``, which makes passing a small number of items more comfortable.
Previously, you would save two items with:

.. code-block:: python

    user = User(...)
    tweet = Tweet(...)

    engine.save([user, tweet])

This is now:

.. code-block:: python

    user = User(...)
    tweet = Tweet(...)

    engine.save(user, tweet)

And to save a list:

.. code-block:: python

    some_list = get_users_to_save()
    engine.save(*some_list)


.. include:: ../../CHANGELOG.rst
