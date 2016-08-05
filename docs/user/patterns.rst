Common Patterns
^^^^^^^^^^^^^^^

.. _patterns-local:

DynamoDB Local
==============

Connect to a local DynamoDB instance.

.. code-block:: python

    import boto3
    import bloop

    boto_client = boto3.client(
        "dynamodb",
        endpoint="http://127.0.0.1:8000",
        region_name="us-west-2")
    client = bloop.Client(boto_client=boto_client)
    engine = bloop.Engine(client=client)

.. note::

    DynamoDB Local has an issue with expressions and Global Secondary Indexes, and will throw errors about
    ExpressionAttributeName when you query or scan against a GSI.  For example, see
    `this issue <https://github.com/numberoverzero/bloop/issues/43>`_.

Generic "if not exist"
======================

Condition to ensure an object's hash (or hash + range) key are not set (item doesn't exist).

.. code-block:: python

    def if_not_exist(obj):
        hash_key = obj.Meta.hash_key
        range_key = obj.Meta.range_key

        condition = hash_key.is_(None)
        if range_key:
            condition &= range_key.is_(None)
        return condition


    # Usage
    tweet = Tweet(account=uuid.uuid4(), id="numberoverzero", ...)
    engine.save(tweet, condition=if_not_exist(tweet))

.. _patterns-engine-config:

Temporary Engine Config
=======================

This is a simple function to create a copy of an engine, but with slightly different config.

.. code-block:: python

    def using_config(engine, **config):
        return bloop.Engine(
            type_engine=engine.type_engine,
            client=engine.client,
            config={**engine.config, **config})

For example, let's say you use strict queries to control your consumed throughput, but some legacy code still
needs to load everything from an LSI:

.. code-block:: python

    relaxed = using_config(engine, strict=False)
    scan = relaxed.scan(Model.some_lsi).select("all").build()
    for entry in scan:
        # References to engine still use strict queries
        process(engine, entry)
