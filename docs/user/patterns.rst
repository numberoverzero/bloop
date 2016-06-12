Patterns
========

DynamoDB Local
--------------

::

    import boto3
    import bloop

    boto_client = boto3.client(
        "dynamodb", endpoint="http://127.0.0.1:8000")
    client = bloop.Client(boto_client=boto_client)
    engine = bloop.Engine(client=client)

    ...


Generic "if not exist"
----------------------

::

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
