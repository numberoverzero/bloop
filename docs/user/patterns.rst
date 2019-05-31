Bloop Patterns
^^^^^^^^^^^^^^

.. _patterns-local:

================
 DynamoDB Local
================

Connect to a local DynamoDB instance.  As of 2018-08-29 DynamoDBLocal still does not support features like TTL or
ContinuousBackups (even in a stubbed capacity) which means you will need to patch the client for local testing.

.. code-block:: python

    import boto3
    import bloop

    dynamodb = boto3.client("dynamodb", endpoint_url="http://127.0.0.1:8000")
    dynamodbstreams = boto3.client("dynamodbstreams", endpoint_url="http://127.0.0.1:8000")
    engine = bloop.Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)

To resolve missing features in DynamoDBLocal, you can patch the client (see below) or use an alternative to
DynamoDBLocal such as localstack.  Localstack isn't recommended until `Issue #728`_ is addressed.

The following code is designed to be easily copied and pasted.  When you set up your engine for local testing just
import and call ``patch_engine`` to stub responses to missing methods.  By default describe ttl and describe
backups will return "DISABLED" for every table.  You can use
``client.mock_ttl["my-table-name"] = True`` or ``client.mock_backups["my-table-name"] = True`` to instead return
"ENABLED".

The original patching code used by bloop's integration tests can be found `here`_ while historical context on
using DynamoDBLocal with bloop can be found in `Issue #117`_.

.. code-block:: python

    # patch_local.py
    import bloop


    class PatchedDynamoDBClient:
        def __init__(self, real_client):
            self.__client = real_client
            self.mock_ttl = {}
            self.mock_backups = {}

        def describe_time_to_live(self, TableName, **_):
            r = "ENABLED" if self.mock_ttl.get(TableName) else "DISABLED"
            return {"TimeToLiveDescription": {"TimeToLiveStatus": r}}

        def describe_continuous_backups(self, TableName, **_):
            r = "ENABLED" if self.mock_backups.get(TableName) else "DISABLED"
            return {"ContinuousBackupsDescription": {"ContinuousBackupsStatus": r}}

        # TODO override any other methods that DynamoDBLocal doesn't provide

        def __getattr__(self, name):
            # use the original client for everything else
            return getattr(self.__client, name)


    def patch_engine(engine):
        client = PatchedDynamoDBClient(engine.session.dynamodb_client)
        engine.session.dynamodb_client = client
        return client


And its usage, assuming you've saved the file as patch_local.py:

.. code-block:: python

    from .patch_local import patch_engine

    # same 3 lines from above
    dynamodb = boto3.client("dynamodb", endpoint_url="http://127.0.0.1:8000")
    dynamodbstreams = boto3.client("dynamodbstreams", endpoint_url="http://127.0.0.1:8000")
    engine = bloop.Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)

    client = patch_engine(engine)

    client.mock_ttl["MyTableName"] = True
    client.mock_backups["MyTableName"] = False


.. _Issue #728: https://github.com/localstack/localstack/issues/728
.. _here: https://github.com/numberoverzero/bloop/blob/4d2c967a8f74eb2b70a5ed9f90d5325449e56f8a/tests/integ/conftest.py#L18-L29
.. _Issue #117: https://github.com/numberoverzero/bloop/issues/117

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

A number type that loads values as floats but preserves the Decimal context recommended by DynamoDB when saving.
While you could specify a relaxed :class:`decimal.Context` in the constructor, that is strongly discouraged
as it will cause issues comparing values.

.. code-block:: python

    class Float(Number):
        def dynamo_load(self, *args, **kwargs):
            return float(super.dynamo_load(*args, **kwargs))

============================
 Sharing Tables and Indexes
============================

Bloop allows you to map multiple models to the same table.  You can rename columns during
init with the ``dynamo_name=`` param, change column types across models, and still use conditional
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
 Cross-Region Replication
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

.. _custom-column:

==================================
 Customizing the ``Column`` Class
==================================

As mentioned in :ref:`type-validation`, Bloop intentionally does not impose its own concept of type validation or
a nullable constraint on columns.  Instead, these can be trivially added to the existing Column class:

.. code-block:: python

    import bloop

    class Column(bloop.Column):

        def __init__(self, *args, nullable=True, check_type=True, **kwargs):
            super().__init__(*args, **kwargs)
            self.nullable = nullable
            self.check_type = check_type

        def __set__(self, obj, value):
            if value is None:
                if self.nullable:
                    return
                raise ValueError(f"{self!r} does not allow None")
            elif self.check_type and not isinstance(value, self.typedef.python_type):
                msg = "Tried to set {} with invalid type {} (expected {})"
                raise TypeError(msg.format(
                    self.name, type(value),
                    self.typedef.python_type
                ))
            super().__set__(obj, value)

Using this class, a type failure looks like:

.. code-block:: python

    >>> class Appointment(BaseModel):
    ...     id = Column(UUID, hash_key=True, nullable=False)
    ...     date = Column(DateTime)
    ...     location = Column(String, check_type=True)
    >>> engine.bind(Appointment)
    >>> appt = Appointment(id=uuid.uuid4())

    >>> appt.id = None
    ValueError: Tried to set id to None but column is not nullable
    >>> appt.location = 3
    TypeError: Tried to set location with invalid type <class 'int'> (expected <class 'str'>)


====================
 Json Serialization
====================

When you're ready to serialize your objects for use in other systems you should reach for `marshmallow`__.
Marshmallow's context-specific serialization is useful for excluding fields for different consumers,
such as internal account notes.  You can specify multiple formats and switch based on use eg. base64 to send bytes
over the wire or as raw bytes to write to disk.

But when you want to quickly send something over the wire, marshmallow can be heavy.
The following is a drop in function for the ``default`` argument to ``json.dumps``.

It is not intended for production use.  For historical discussion, see `Issue #135`_.

.. code-block:: python

    # bloop_serializer.py
    import base64
    import datetime
    import decimal
    import uuid
    from bloop import BaseModel

    def serialize(use_float: bool = True, explicit_none: bool = True):
        def default(obj):
            # bloop.Set[T]
            if isinstance(obj, set):
                return list(obj)
            # bloop.{Datetime,Timestamp}
            if isinstance(obj, datetime.datetime):
                return obj.isoformat()
            # bloop.UUID
            elif isinstance(obj, uuid.UUID):
                return str(obj)
            # bloop.Number
            elif isinstance(obj, decimal.Decimal):
                if use_float:
                    return float(obj)
                return str(obj)
            # bloop.Binary
            elif isinstance(obj, bytes):
                return base64.b64encode(obj).decode("utf-8")
            # bloop.BaseModel
            elif isinstance(obj, BaseModel):
                return {
                    c.name: getattr(obj, c.name, None)
                    for c in obj.Meta.columns
                    if hasattr(obj, c.name) or explicit_none
                }
            raise TypeError(f"Type {type(obj)} is not serializable")
        return default


To use the serializer, simply pass it to ``json.dumps``:

.. code-block:: python

    import json
    from bloop_serializer import serialize

    user = User(...)
    json.dumps(
        user,
        default=serialize(),
        indent=True, sort_keys=True
    )

    # render None/empty values as null instead of omitting
    json.dumps(
        user,
        default=serialize(explicit_none=True),
        indent=True, sort_keys=True
    )


__ https://marshmallow.readthedocs.io

.. _Issue #135: https://github.com/numberoverzero/bloop/issues/135

.. _marshmallow-pattern:

==============================
 Integrating with Marshmallow
==============================

Instead of adding your own validation layer to the Column class :ref:`as detailed above <custom-column>` you can easily
leverage powerful libraries such as `marshmallow`__ and `flask-marshmallow`__.  Here's a self-contained example that
uses flask and marshmallow to expose get and list operations for a User class:

.. code-block:: python

    from flask import Flask, jsonify
    from flask_marshmallow import Marshmallow
    from bloop import BaseModel, Column, Engine, Integer, String, DateTime
    from datetime import datetime

    app = Flask(__name__)
    ma = Marshmallow(app)
    engine = Engine()


    class User(Model):
        def __init__(self, **kwargs):
            kwargs.setdefault("date_created", datetime.now())
            super().__init__(**kwargs)

        email = Column(String, hash_key=True)
        password = Column(String)
        date_created = Column(DateTime, default=lambda: datetime.now())

    engine.bind(User)


    class UserSchema(ma.Schema):
        class Meta:
            # Fields to expose
            fields = ["_links"]
            fields += [column.name for column in User.Meta.columns]
        # Smart hyperlinking
        _links = ma.Hyperlinks({
            'self': ma.URLFor('user_detail', id='<id>'),
            'collection': ma.URLFor('users')
        })

    user_schema = UserSchema()
    users_schema = UserSchema(many=True)


    @app.route('/api/users/')
    def users():
        all_users = list(engine.scan(User))
        result = users_schema.dump(all_users)
        return jsonify(result.data)

    @app.route('/api/users/<id>')
    def user_detail(id):
        user = User(id=id)
        engine.load(user)
        return user_schema.jsonify(user)


__ https://marshmallow.readthedocs.io
__ https://flask-marshmallow.readthedocs.io
