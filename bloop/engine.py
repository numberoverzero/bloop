import boto3
import declare

from .exceptions import AbstractModelError, InvalidModel, MissingKey, MissingObjects, UnboundModel, UnknownType
from .expressions import render
from .filter import Filter
from .models import Index, ModelMetaclass
from .session import SessionWrapper
from .tracking import clear, is_model_validated, sync
from .util import missing, walk_subclasses, signal, unpack_from_dynamodb

__all__ = ["Engine", "before_create_table", "model_bound", "model_validated"]

# Signals!
before_create_table = signal("before_create_table")
model_bound = signal("model_bound")
model_validated = signal("model_validated")


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
            raise MissingKey("{!r} is missing values for its keys.".format(obj))
        key_value = engine._dump(key_column.typedef, key_value)
        key[key_column.dynamo_name] = key_value
    return key


def raise_on_abstract(*objs, cls=False):
    for obj in objs:
        if obj.Meta.abstract:
            cls = obj if cls else obj.__class__
            raise AbstractModelError("{!r} is abstract.".format(cls.__name__))


def raise_on_unknown(model, from_declare):
    # Best-effort check for a more helpful message
    if isinstance(model, ModelMetaclass):
        msg = "{!r} is not bound.  Did you forget to call engine.bind?"
        raise UnboundModel(msg.format(model.__name__)) from from_declare
    else:
        msg = "{!r} is not a registered Type."
        raise UnknownType(msg.format(model.__name__)) from from_declare


class Engine:
    def __init__(self, session=None, type_engine=None):
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
        self.type_engine = type_engine or declare.TypeEngine.unique()
        self.session = SessionWrapper(session or boto3)

    def _dump(self, model, obj, context=None, **kwargs):
        context = context or {"engine": self}
        try:
            return context["engine"].type_engine.dump(model, obj, context=context, **kwargs)
        except declare.DeclareException as from_declare:
            raise_on_unknown(model, from_declare)

    def _load(self, model, value, context=None, **kwargs):
        context = context or {"engine": self}
        try:
            return context["engine"].type_engine.load(model, value, context=context, **kwargs)
        except declare.DeclareException as from_declare:
            raise_on_unknown(model, from_declare)

    def bind(self, base):
        """Create tables for all models subclassing base"""
        # Make sure we're looking at models
        if not isinstance(base, ModelMetaclass):
            raise InvalidModel("The base class must subclass BaseModel.")

        # whether the model's typedefs should be registered, and
        # whether the model should be eligible for validation
        def is_concrete(model):
            # Models that aren't explicitly abstract should be bound
            abstract = model.Meta.abstract
            return not abstract

        concrete = set(filter(is_concrete, walk_subclasses(base)))
        unvalidated = concrete - set(filter(is_model_validated, concrete))

        # create_table doesn't block until ACTIVE or validate.
        # It also doesn't throw when the table already exists, making it safe
        # to call multiple times for the same unbound model.
        for model in unvalidated:
            before_create_table.send(self, model=model)
            self.session.create_table(model)

        for model in concrete:
            if model in unvalidated:
                self.session.validate_table(model)
            # Model won't need to be verified the
            # next time its BaseModel is bound to an engine
            model_validated.send(self, model=model)

            self.type_engine.register(model)
            self.type_engine.bind(context={"engine": self})
            model_bound.send(self, model=model)

    def delete(self, *objs, condition=None, atomic=False):
        objs = set(objs)
        raise_on_abstract(*objs)
        for obj in objs:
            item = {"TableName": obj.Meta.table_name, "Key": dump_key(self, obj)}
            atomic = atomic and obj or None
            rendered = render(self, atomic=atomic, condition=condition)
            item.update(rendered)

            self.session.delete_item(item)
            clear(obj)

    def load(self, *objs, consistent=False):
        """Populate objects from dynamodb, optionally using consistent reads.

        If any objects are not found, raises MissingObjects with the attribute
        `objects` containing a list of the objects that were not loaded.

        Example
        -------
        class HashOnly(bloop.BaseModel):
            user_id = Column(NumberType, hash_key=True)

        class HashAndRange(bloop.BaseModel):
            user_id = Column(NumberType, hash_key=True)
            game_title = Column(StringType, range_key=True)

        hash_only = HashOnly(user_id=101)
        hash_and_range = HashAndRange(user_id=101, game_title="Starship X")

        # Load only one instance, with consistent reads
        engine.load(hash_only, consistent=True)

        # Load multiple instances
        engine.load([hash_only, hash_and_range])
        """
        objs = set(objs)
        raise_on_abstract(*objs)

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
                    sync(obj, self)
                if not object_index[table_name]:
                    object_index.pop(table_name)

        if object_index:
            not_loaded = set()
            for index in object_index.values():
                for index_set in index.values():
                    not_loaded.update(index_set)
            raise MissingObjects("Failed to load some objects.", objects=not_loaded)

    def query(self, model_or_index, consistent=False, strict=True):
        if isinstance(model_or_index, Index):
            model, index = model_or_index.model, model_or_index
            select = "projected"
        else:
            model, index = model_or_index, None
            select = "all"
        raise_on_abstract(model, cls=True)
        return Filter(
            engine=self, mode="query", model=model, index=index, strict=strict, select=select,
            consistent=consistent)

    def save(self, *objs, condition=None, atomic=False):
        objs = set(objs)
        raise_on_abstract(*objs)
        for obj in objs:
            item = {"TableName": obj.Meta.table_name, "Key": dump_key(self, obj)}

            atomic = atomic and obj or None
            rendered = render(self, atomic=atomic, condition=condition, update=obj)
            item.update(rendered)

            self.session.save_item(item)
            sync(obj, self)

    def scan(self, model_or_index, consistent=False, strict=True):
        if isinstance(model_or_index, Index):
            model, index = model_or_index.model, model_or_index
            select = "projected"
        else:
            model, index = model_or_index, None
            select = "all"
        raise_on_abstract(model, cls=True)
        return Filter(
            engine=self, mode="scan", model=model, index=index, strict=strict, select=select,
            consistent=consistent)
