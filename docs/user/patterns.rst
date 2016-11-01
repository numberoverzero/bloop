Bloop Patterns
^^^^^^^^^^^^^^

.. _patterns-local:

==============
DynamoDB Local
==============

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

======================
Generic "if not exist"
======================

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

==========
Float Type
==========

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
