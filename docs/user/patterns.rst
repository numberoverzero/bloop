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
