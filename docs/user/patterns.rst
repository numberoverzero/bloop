Bloop Patterns
^^^^^^^^^^^^^^

.. _patterns-local:

================
 DynamoDB Local
================

Connect to a local DynamoDB instance.

.. code-block:: python

    import boto3
    import bloop

    dynamodb = boto3.client("dynamodb", endpoint="http://127.0.0.1:8000")
    dynamodbstreams = boto3.client("dynamodbstreams", endpoint="http://127.0.0.1:8000")

    engine = bloop.Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)

.. note::

    DynamoDB Local has an issue with expressions and Global Secondary Indexes, and will throw errors about
    ExpressionAttributeName when you query or scan against a GSI.  For example, see
    `this issue <https://github.com/numberoverzero/bloop/issues/43>`_.

.. _patterns-if-not-exist:

========================
 Generic "if not exist"
========================

Create a condition for any model or object that fails the operation if the item already exists.

.. code-block:: python

    from bloop import Condition

    def if_not_exist(obj):
        condition = Condition()
        for key in obj.Meta.keys:
            condition &= key.is_(None)
        return condition

    tweet = Tweet(account=uuid.uuid4(), id="numberoverzero")

    engine.save(tweet, condition=if_not_exist(tweet))
    # or
    engine.save(tweet, condition=if_not_exist(Tweet))

.. _patterns-float:

============
 Float Type
============

A number type with a :class:`decimal.Context` that doesn't trap :class:`decimal.Rounded` or :class:`decimal.Inexact`.

.. code-block:: python

    import decimal
    from bloop import Number

    class Float(Number):
        def __init__(self):
            context = decimal.Context(
                Emin=-128, Emax=126, rounding=None, prec=38,
                traps=[decimal.Clamped, decimal.Overflow, decimal.Underflow])
            super().__init__(context=context)

        def dynamo_load(self, value, *, context, **kwargs):
            value = super().dynamo_load(value, context=context, **kwargs)
            # float goes in, float goes out.  You can't explain that!
            return float(value)

.. warning::

    **Do not use this pattern if you care about the accuracy of your data.**
    This will almost certainly cause duplicate and missing data.  You're probably here because dealing with
    :class:`decimal.Decimal` `can be frustrating`__, and it `doesn't play nicely`__ with the standard library.

    Think carefully before you throw away correctness guarantees in your data layer.  Before you copy and paste
    this into your secure bitcoin trading app, a brief reminder about floats:

    .. code-block:: pycon

        >>> from decimal import Decimal
        >>> d = Decimal("3.14")
        >>> f = float(d)
        >>> d2 = Decimal(f)
        >>> d == d2
        False

    __ https://github.com/boto/boto3/issues/665
    __ https://github.com/boto/boto3/issues/369

============================
 Sharing Tables and Indexes
============================

Bloop allows you to map multiple models to the same table.  You can rename columns during
init with the ``name=`` param, change column types across models, and still use conditional
operations and Bloop's atomic builder.  This flexibility extends to GSIs and LSIs as long
as a Model's Index projects a subset of the actual Index.  On shared tables, a shared index
provides tighter query validation and reduces consumed throughput.

In the following (very contrived) example, the ``employees-uk`` table is used for both employees
and managers.  Queries against ``by_level`` provide emails for Employees of a certain level, and
provides all directs for managers at a certain level.


.. code-block:: python

    class Employee(BaseModel):
        class Meta:
            table_name = "employees-uk"
        id = Column(UUID, hash_key=True)
        level = Column(Integer)
        email = Column(String)
        manager_id = Column(UUID)

        by_level = GlobalSecondaryIndex(
            projection=[email], hash_key=level)


    class Manager(BaseModel):
        class Meta:
            table_name = "employees-uk"
        id = Column(UUID, hash_key=True)
        level = Column(Integer)
        email = Column(String)
        manager_id = Column(UUID)
        directs = Column(Set(UUID))

        by_level = GlobalSecondaryIndex(
            projection=[directs], hash_key=level)


.. note::

    If you try to create these tables by binding the models, one of them will fail.
    If ``Employee`` is bound first, ``Manager`` won't see ``directs`` in the ``by_level`` GSI.
    You must create the indexes through the console, or use a dummy model.

    .. code-block:: python

        def build_indexes(engine):
            """Call before binding Employee or Manager"""
            class _(BaseModel):
                class Meta:
                    table_name = "employees-uk"
                id = Column(UUID, hash_key=True)
                level = Column(Integer)
                email = Column(String)
                manager_id = Column(UUID)
                directs = Column(Set(UUID))
                by_level = GlobalSecondaryIndex(
                    projection=[directs, email],
                    hash_key=level)
            engine.bind(_)

==========================
 Cross-region Replication
==========================

Replicating the same model across multiple regions using streams is straightforward.  We'll need one engine per region,
which can be instantiated with the following helper:

.. code-block:: python

    import boto3
    import bloop


    def engine_for_region(region):
        dynamodb = boto3.client("dynamodb", region_name=region)
        dynamodbstreams = boto3.client("dynamodbstreams", region_name=region)
        return bloop.Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)


    src_engine = engine_for_region("us-west-2")
    dst_engine = engine_for_region("us-east-1")

And here's our replication.  This assumes that the model has been bound to both engines.  Although this starts at the
trim horizon, we'd usually keep track of progress somewhere else using ``Stream.token`` to avoid replicating stale
changes (every run would start at trim_horizon).

.. code-block:: python

    stream = src_engine.stream(MyModelHere, "trim_horizon")
    while True:
        record = next(stream)
        if not record:
            continue
        old, new = record["old"], record["new"]
        if new:
            dst_engine.save(new)
        else:
            dst_engine.delete(old)

This is a simplified example; see :ref:`periodic-heartbeats` for automatically managing shard iterator expiration.