import boto3
import declare

from .conditions import render
from .exceptions import (
    AbstractModelError,
    InvalidModel,
    MissingKey,
    MissingObjects,
    UnboundModel,
    UnknownType,
)
from .models import Index, ModelMetaclass
from .search import Query, Scan
from .session import SessionWrapper
from .signals import (
    before_create_table,
    model_bound,
    model_validated,
    object_deleted,
    object_loaded,
    object_saved,
)
from .stream import Stream, stream_for
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
            raise AbstractModelError("{!r} is abstract.".format(cls.__name__))


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
    """

    Basic Usage:

    .. code-block:: python

        # Declare some models
        class User(BaseModel):
            user_id = Column(Integer, hash_key=True)

        class Game(BaseModel):
            user_id = Column(Integer, hash_key=True)
            game_title = Column(String, range_key=True)

            by_title = GlobalSecondaryIndex(
                projection="keys", hash_key="game_title")

        # Bind the models to tables in DynamoDB
        engine.bind(User)
        engine.bind(Game)
        # OR: bind all subclasses with engine.bind(BaseModel)

        # Create a few instances
        user = User(user_id=101)
        game = Game(user_id=101, game_title="Starship X")

        # Persist in DynamoDB
        engine.save(user, game)

        # Load with consistent reads
        engine.load(user, consistent=True)

        # Delete an object only if it hasn't been modified
        engine.delete(game, atomic=True)
    """
    def __init__(self, session=None, type_engine=None):
        """
        Create a new eng

        :param session: An existing boto3 session, or None to use the default session
        :type session: boto3.session.Session
        :param type_engine: An existing type engine, or None to use a unique type engine.
        :type type_engine: declare.TypeEngine
        """
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
        self.type_engine = type_engine or declare.TypeEngine.unique()
        self.session = SessionWrapper(session or boto3)

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
        """Create tables for all models subclassing base"""
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

        Raises :exc:`~bloop.exceptions.MissingObjects` if objects aren't loaded.
        :attr:`MissingObjects.objects <~bloop.exceptions.MissingObjects>` includes the objects not loaded.
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

    def query(self, model_or_index, key=None, filter=None, projection="all", limit=None,
              consistent=False, forward=True):
        if isinstance(model_or_index, Index):
            model, index = model_or_index.model, model_or_index
        else:
            model, index = model_or_index, None
        validate_not_abstract(model)
        q = Query(
            engine=self, session=self.session, model=model, index=index, key=key, filter=filter,
            projection=projection, limit=limit, consistent=consistent, forward=forward)
        return iter(q.prepare())

    def save(self, *objs, condition=None, atomic=False):
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
        if isinstance(model_or_index, Index):
            model, index = model_or_index.model, model_or_index
        else:
            model, index = model_or_index, None
        validate_not_abstract(model)
        s = Scan(
            engine=self, session=self.session, model=model, index=index, filter=filter,
            projection=projection, limit=limit, consistent=consistent)
        return iter(s.prepare())

    def stream(self, model, position) -> Stream:
        validate_not_abstract(model)
        stream = stream_for(self, model)
        stream.move_to(position=position)
        return stream
