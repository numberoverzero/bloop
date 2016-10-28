import declare

from .conditions import render
from .exceptions import (
    InvalidModel,
    MissingKey,
    MissingObjects,
    UnboundModel,
    UnknownType,
)
from .models import Index, ModelMetaclass
from .search import Search
from .session import SessionWrapper
from .signals import (
    before_create_table,
    model_bound,
    model_validated,
    object_deleted,
    object_loaded,
    object_saved,
)
from .stream import stream_for
from .util import missing, unpack_from_dynamodb, walk_subclasses


__all__ = ["Engine"]


def value_of(column):
    """value_of({'S': 'Space Invaders'}) -> 'Space Invaders'"""
    return next(iter(column.values()))


def index_for(key):
    """index_for({'id': {'S': 'foo'}, 'range': {'S': 'bar'}}) -> ('bar', 'foo')"""
    return tuple(sorted(value_of(k) for k in key.values()))


def extract_key(key_shape, item):
    """construct a key according to key_shape for building an index"""
    return {field: item[field] for field in key_shape}


def dump_key(engine, obj):
    """dump the hash (and range, if there is one) key(s) of an object into
    a dynamo-friendly format.

    returns {dynamo_name: {type: value} for dynamo_name in hash/range keys}
    """
    key = {}
    for key_column in obj.Meta.keys:
        key_value = getattr(obj, key_column.model_name, missing)
        if key_value is missing:
            raise MissingKey("{!r} is missing {}: {!r}".format(
                obj, "hash_key" if key_column.hash_key else "range_key",
                key_column.model_name
            ))
        key_value = engine._dump(key_column.typedef, key_value)
        key[key_column.dynamo_name] = key_value
    return key


def validate_not_abstract(*objs):
    for obj in objs:
        if obj.Meta.abstract:
            cls = obj if isinstance(obj, type) else obj.__class__
            raise InvalidModel("{!r} is abstract.".format(cls.__name__))


def validate_is_model(model):
    if not isinstance(model, ModelMetaclass):
        cls = model if isinstance(model, type) else model.__class__
        raise InvalidModel("{!r} does not subclass BaseModel.".format(cls.__name__))


def fail_unknown(model, from_declare):
    # Best-effort check for a more helpful message
    if isinstance(model, ModelMetaclass):
        msg = "{!r} is not bound.  Did you forget to call engine.bind?"
        raise UnboundModel(msg.format(model.__name__)) from from_declare
    else:
        msg = "{!r} is not a registered Type."
        obj = model.__name__ if hasattr(model, "__name__") else model
        raise UnknownType(msg.format(obj)) from from_declare


class Engine:
    """Primary means of interacting with DynamoDB.

    .. code-block:: pycon

        >>> from your_project.models import Game
        >>> from bloop import Engine
        >>> engine = Engine()
        >>> engine.bind(Game)
        >>> game = Game(id=101, title="Starship X")
        >>> engine.save(game)

    :param dynamodb: DynamoDB client.  Defaults to ``boto3.client("dynamodb")``.
    :param dynamodbstreams: DynamoDbStreams client.  Defaults to ``boto3.client("dynamodbstreams")``.
    """
    def __init__(self, *, dynamodb=None, dynamodbstreams=None):
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
        self.type_engine = declare.TypeEngine.unique()
        self.session = SessionWrapper(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)

    def _dump(self, model, obj, context=None, **kwargs):
        context = context or {"engine": self}
        try:
            return context["engine"].type_engine.dump(model, obj, context=context, **kwargs)
        except declare.DeclareException as from_declare:
            fail_unknown(model, from_declare)

    def _load(self, model, value, context=None, **kwargs):
        context = context or {"engine": self}
        try:
            return context["engine"].type_engine.load(model, value, context=context, **kwargs)
        except declare.DeclareException as from_declare:
            fail_unknown(model, from_declare)

    def bind(self, base):
        """Create backing tables for a model and its subclasses.

        .. code-block:: pycon

            >>> class User(BaseModel):
            ...     id = Column(UUID, hash_key=True)
            ...     email = Column(String)
            ...
            >>> engine = Engine()
            >>> engine.bind(User)

        :param base: Base model to bind.  Can be abstract.
        """
        # Make sure we're looking at models
        validate_is_model(base)

        concrete = set(filter(lambda m: not m.Meta.abstract, walk_subclasses(base)))

        # create_table doesn't block until ACTIVE or validate.
        # It also doesn't throw when the table already exists, making it safe
        # to call multiple times for the same unbound model.
        for model in concrete:
            before_create_table.send(self, engine=self, model=model)
            self.session.create_table(model)

        for model in concrete:
            self.session.validate_table(model)
            model_validated.send(self, engine=self, model=model)

            self.type_engine.register(model)
            self.type_engine.bind(context={"engine": self})
            model_bound.send(self, engine=self, model=model)

    def delete(self, *objs, condition=None, atomic=False):
        """Delete one or more objects.

        .. code-block:: pycon

            >>> user = User(id=123, email="user@domain.com")
            >>> engine.save(user)
            >>> engine.delete(user)

        Use a condition to ensure some criteria is met before deleting the object(s):

        .. code-block:: pycon

            >>> user = User(id=123, email="user@domain.com")
            # Don't delete the user if the email has changed
            same_email = User.email == user.email
            >>> engine.delete(user, condition=same_email)

        When ``atomic`` is True, the delete is only performed if the local object and the DynamoDB object
        are exactly the same.  This can be combined with other conditions:

        .. code-block:: pycon

            # Local user doesn't know its verified state,
            # so it isn't part of the atomic condition.
            >>> engine.delete(User, atomic=True,
            ...     condition=User.verified.is_(False))
        """
        objs = set(objs)
        validate_not_abstract(*objs)
        for obj in objs:
            self.session.delete_item({
                "TableName": obj.Meta.table_name,
                "Key": dump_key(self, obj),
                **render(self, obj=obj, atomic=atomic, condition=condition)
            })
            object_deleted.send(self, engine=self, obj=obj)

    def load(self, *objs, consistent=False):
        """Populate objects from DynamoDB.

        .. code-block:: pycon

            >>> user = User(id=123)
            >>> game = Game(title="Starship X")

            >>> engine.load(user, game)
            >>> user.email
            "user@domain.com"
            >>> game.rating
            3.14

        Uses `strongly consistent reads`__ when ``consistent`` is True.
        Raises :exc:`~bloop.exceptions.MissingObjects` if one or more objects aren't loaded.

        __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
        """
        objs = set(objs)
        validate_not_abstract(*objs)

        table_index, object_index, request = {}, {}, {}

        for obj in objs:
            table_name = obj.Meta.table_name
            key = dump_key(self, obj)
            index = index_for(key)

            if table_name not in object_index:
                table_index[table_name] = list(sorted(key.keys()))
                object_index[table_name] = {}
                request[table_name] = {"Keys": [], "ConsistentRead": consistent}

            if index not in object_index[table_name]:
                request[table_name]["Keys"].append(key)
                object_index[table_name][index] = set()
            object_index[table_name][index].add(obj)

        response = self.session.load_items(request)

        for table_name, list_of_attrs in response.items():
            for attrs in list_of_attrs:
                key_shape = table_index[table_name]
                key = extract_key(key_shape, attrs)
                index = index_for(key)

                for obj in object_index[table_name].pop(index):
                    unpack_from_dynamodb(
                        attrs=attrs, expected=obj.Meta.columns, engine=self, obj=obj)
                    object_loaded.send(self, engine=self, obj=obj)
                if not object_index[table_name]:
                    object_index.pop(table_name)

        if object_index:
            not_loaded = set()
            for index in object_index.values():
                for index_set in index.values():
                    not_loaded.update(index_set)
            raise MissingObjects("Failed to load some objects.", objects=not_loaded)

    def query(self, model_or_index, key, filter=None, projection="all", limit=None,
              consistent=False, forward=True):
        """Create a reusable :class:`~bloop.search.QueryIterator`.

        .. code-block:: pycon

            >>> q = engine.query(
            ...     User.by_email,
            ...     User.email == "user@domain.com",
            ...     projection="all",
            ...     filter=User.age >= 18,
            ... )
            >>> list(q)
            [User(id=0, email=...), User(id=1, email=...)]
            >>> q.first()
            User(id=0, email=...)
            >>> q.one()
            ConstraintViolation: Query found more than one result.

        A `key condition`__ must use an equality condition ``==`` against the hash key of the Model
        or Index being queried.  The condition can also include one of the following conditions
        against the range key::

            <, <=, ==, >=, >, begins_with, between

        To use a hash key and range key condition together:

        .. code-block:: pycon

            >>> in_home = File.path == "~"
            >>> start_with_a = File.name.begins_with("a")
            >>> q = engine.query(File, in_home & starts_with_a)

        :param model_or_index: A model or index to query.  For example, ``User`` or ``User.by_email``.
        :param key:
            Key condition.  This must include an equality against the hash key, and optionally one
            of a restricted set of conditions on the range key.
        :param filter: Filter condition.  Only matching objects will be included in the results.
        :param projection:
            "all", "count", a list of column names, or a list of :class:`~bloop.models.Column`.  When projection is
            "count", you must advance the iterator to retrieve the count.

        :param int limit: Maximum number of items returned.  This is not DynamoDB's `Limit`__ parameter.
        :param bool consistent: Use `strongly consistent reads`__ if True.  Default is False.
        :param bool forward:  Query in ascending or descending order.  Default is True (ascending).

        :return: A reusable query iterator with helper methods.
        :rtype: :class:`~bloop.search.QueryIterator`

        __ http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html\
           #DDB-Query-request-KeyConditionExpression
        __ http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-Limit
        __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
        """
        if isinstance(model_or_index, Index):
            model, index = model_or_index.model, model_or_index
        else:
            model, index = model_or_index, None
        validate_not_abstract(model)
        q = Search(
            mode="query", engine=self, session=self.session, model=model, index=index, key=key,
            filter=filter, projection=projection, limit=limit, consistent=consistent, forward=forward)
        return iter(q.prepare())

    def save(self, *objs, condition=None, atomic=False):
        """Save one or more objects.

        .. code-block:: pycon

            >>> user = User(id=123, email="user@domain.com")
            >>> engine.save(user)
            >>> engine.delete(user)

        Use a condition to ensure some criteria is met before saving the object(s):

        .. code-block:: pycon

            >>> user = User(id=123, email="user@domain.com")
            # Don't save the user if their account isn't verified
            # must_be_verified = User.verified.is_(True)
            >>> engine.save(user, condition=must_be_verified)

        When ``atomic`` is True, the save is only performed if the local object and the DynamoDB object
        are exactly the same.  This can be combined with other conditions:

        .. code-block:: pycon

            # Local user doesn't know its subscription level,
            # so it isn't part of the atomic condition.
            >>> engine.save(User, atomic=True,
            ...     condition=User.subscription >= 3)
        """
        objs = set(objs)
        validate_not_abstract(*objs)
        for obj in objs:
            self.session.save_item({
                "TableName": obj.Meta.table_name,
                "Key": dump_key(self, obj),
                **render(self, obj=obj, atomic=atomic, condition=condition, update=True)
            })
            object_saved.send(self, engine=self, obj=obj)

    def scan(self, model_or_index, filter=None, projection="all", limit=None, consistent=False):
        """Create a reusable :class:`~bloop.search.ScanIterator`.

        .. code-block:: pycon

            >>> recently_online = User.last_login.between(
            ...     arrow.now().replace(hours=-2),
            ...     arrow.now()
            ... )
            >>> q = engine.scan(
            ...     User,
            ...     filter=recently_online,
            ...     projection=[User.verified, User.email]
            ... )
            >>> list(q)
            [User(id=0, ...), User(id=1, ...)]
            >>> q.first()
            User(id=0, ...)
            >>> q.one()
            ConstraintViolation: Scan found more than one result.

        :param model_or_index: A model or index to scan.  For example, ``User`` or ``User.by_email``.
        :param filter: Filter condition.  Only matching objects will be included in the results.
        :param projection:
            "all", "count", a list of column names, or a list of :class:`~bloop.models.Column`.  When projection is
            "count", you must advance the iterator to retrieve the count.

        :param int limit: Maximum number of items returned.  This is not DynamoDB's `Limit`__ parameter.
        :param bool consistent: Use `strongly consistent reads`__ if True.  Default is False.

        :return: A reusable scan iterator with helper methods.
        :rtype: :class:`~bloop.search.ScanIterator`

        __ http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-Limit
        __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
        """
        if isinstance(model_or_index, Index):
            model, index = model_or_index.model, model_or_index
        else:
            model, index = model_or_index, None
        validate_not_abstract(model)
        s = Search(
            mode="scan", engine=self, session=self.session, model=model, index=index,
            filter=filter, projection=projection, limit=limit, consistent=consistent)
        return iter(s.prepare())

    def stream(self, model, position):
        """Create an iterator with approximate chronological ordering over all shards in a model's Stream.

        The stream's initial position can be:

        * Either end of the stream with "trim_horizon" and "latest"
        * At the same position as another stream with :attr:`Stream.token <bloop.stream.Stream.token>`
        * An arrow datetime.  This can be **very expensive** for high volume streams.

        .. code-block:: pycon

            >>> last_night = arrow.now().replace(hours=-6)
            >>> stream = engine.stream(User, position=last_night)
            >>> next(stream)
            {'key': None,
             'old': None,
             'new': User(id=0, email="user@domain.com"),
             'meta': {'created_at': <Arrow [2016-10-28T01:58:00-07:00]>,
                      'event': {'id': '5ad8700c0adbfad0083e44fc2e3861c0',
                                'type': 'insert',
                                'version': '1.1'},
                      'sequence_number': '100000000006486326346'}
            }
            >>> engine.save(User(id=0, email="new@email.com"))
            >>> next(stream)
            {'key': None,
             'old': User(id=0, email="user@domain.com"),
             'new': User(id=0, email="new@email.com"),
             'meta': {'created_at': <Arrow [2016-10-28T01:59:00-07:00]>,
                      'event': {'id': 'd8dfc861287b917f81bcbf3cd8a8a5b3',
                                'type': 'modify',
                                'version': '1.1'},
                      'sequence_number': '200000000006486327270'}
            }

        You can pick up where a previous stream left off:

        .. code-block:: pycon

            >>> token = previous_stream.token
            >>> token
            {'active': ['shardId-00000001477645069173-8148b80f'],
             'shards': [{'iterator_type': 'after_sequence',
               'sequence_number': '200000000006486327270',
               'shard_id': 'shardId-00000001477645069173-8148b80f'}],
             'stream_arn': 'arn:.../stream/2016-10-28T08:57:46.568'}
            >>> stream = engine.stream(User, token)

        :param model: The model to stream records from.
        :param position: "trim_horizon", "latest", a stream token, or a time.
        :return: An iterator for records in all shards.
        :rtype: :class:`~bloop.stream.Stream`
        """
        validate_not_abstract(model)
        stream = stream_for(self, model)
        stream.move_to(position=position)
        return stream
