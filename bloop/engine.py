import declare

from .conditions import render
from .exceptions import (
    InvalidModel,
    InvalidStream,
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
from .stream import Stream
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

    def bind(self, model):
        """Create backing tables for a model and its non-abstract subclasses.

        :param model: Base model to bind.  Can be abstract.
        :raises bloop.exceptions.InvalidModel: if ``model`` is not a subclass of :class:`~bloop.models.BaseModel`.
        """
        # Make sure we're looking at models
        validate_is_model(model)

        concrete = set(filter(lambda m: not m.Meta.abstract, walk_subclasses(model)))

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

        :param objs: objects to delete.
        :param condition: only perform each delete if this condition holds.
        :param bool atomic: only perform each delete if the local and DynamoDB versions of the object match.
        :raises bloop.exceptions.ConstraintViolation: if the condition (or atomic) is not met.
        """
        objs = set(objs)
        validate_not_abstract(*objs)
        for obj in objs:
            item = {
                "TableName": obj.Meta.table_name,
                "Key": dump_key(self, obj)
            }
            item.update(render(self, obj=obj, atomic=atomic, condition=condition))
            self.session.delete_item(item)
            object_deleted.send(self, engine=self, obj=obj)

    def load(self, *objs, consistent=False):
        """Populate objects from DynamoDB.

        :param objs: objects to delete.
        :param bool consistent: Use `strongly consistent reads`__ if True.  Default is False.
        :raises bloop.exceptions.MissingKey: if any object doesn't provide a value for a key column.
        :raises bloop.exceptions.MissingObjects: if one or more objects aren't loaded.

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

    def query(self, model_or_index, key, filter=None, projection="all", consistent=False, forward=True):
        """Create a reusable :class:`~bloop.search.QueryIterator`.

        :param model_or_index: A model or index to query.  For example, ``User`` or ``User.by_email``.
        :param key:
            Key condition.  This must include an equality against the hash key, and optionally one
            of a restricted set of conditions on the range key.
        :param filter: Filter condition.  Only matching objects will be included in the results.
        :param projection:
            "all", "count", a list of column names, or a list of :class:`~bloop.models.Column`.  When projection is
            "count", you must advance the iterator to retrieve the count.
        :param bool consistent: Use `strongly consistent reads`__ if True.  Default is False.
        :param bool forward:  Query in ascending or descending order.  Default is True (ascending).

        :return: A reusable query iterator with helper methods.
        :rtype: :class:`~bloop.search.QueryIterator`

        __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
        """
        if isinstance(model_or_index, Index):
            model, index = model_or_index.model, model_or_index
        else:
            model, index = model_or_index, None
        validate_not_abstract(model)
        q = Search(
            mode="query", engine=self, model=model, index=index, key=key, filter=filter,
            projection=projection, consistent=consistent, forward=forward)
        return iter(q.prepare())

    def save(self, *objs, condition=None, atomic=False):
        """Save one or more objects.

        :param objs: objects to save.
        :param condition: only perform each save if this condition holds.
        :param bool atomic: only perform each save if the local and DynamoDB versions of the object match.
        :raises bloop.exceptions.ConstraintViolation: if the condition (or atomic) is not met.
        """
        objs = set(objs)
        validate_not_abstract(*objs)
        for obj in objs:
            item = {
                "TableName": obj.Meta.table_name,
                "Key": dump_key(self, obj),
            }
            item.update(render(self, obj=obj, atomic=atomic, condition=condition, update=True))
            self.session.save_item(item)
            object_saved.send(self, engine=self, obj=obj)

    def scan(self, model_or_index, filter=None, projection="all", consistent=False, parallel=None):
        """Create a reusable :class:`~bloop.search.ScanIterator`.

        :param model_or_index: A model or index to scan.  For example, ``User`` or ``User.by_email``.
        :param filter: Filter condition.  Only matching objects will be included in the results.
        :param projection:
            "all", "count", a list of column names, or a list of :class:`~bloop.models.Column`.  When projection is
            "count", you must exhaust the iterator to retrieve the count.
        :param bool consistent: Use `strongly consistent reads`__ if True.  Default is False.
        :param tuple parallel: Perform a `parallel scan`__.  A tuple of (Segment, TotalSegments)
            for this portion the scan. Default is None.
        :return: A reusable scan iterator with helper methods.
        :rtype: :class:`~bloop.search.ScanIterator`

        __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
        __ http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#QueryAndScanParallelScan
        """
        if isinstance(model_or_index, Index):
            model, index = model_or_index.model, model_or_index
        else:
            model, index = model_or_index, None
        validate_not_abstract(model)
        s = Search(
            mode="scan", engine=self, model=model, index=index, filter=filter,
            projection=projection, consistent=consistent, parallel=parallel)
        return iter(s.prepare())

    def stream(self, model, position):
        """Create a :class:`~bloop.stream.Stream` that provides approximate chronological ordering.

        :param model: The model to stream records from.
        :param position: "trim_horizon", "latest", a stream token, or a :class:`datetime.datetime`.
        :return: An iterator for records in all shards.
        :rtype: :class:`~bloop.stream.Stream`
        :raises bloop.exceptions.InvalidStream: if the model does not have a stream.
        """
        validate_not_abstract(model)
        if not model.Meta.stream or not model.Meta.stream.get("arn"):
            raise InvalidStream("{!r} does not have a stream arn".format(model))
        stream = Stream(model=model, engine=self)
        stream.move_to(position=position)
        return stream
