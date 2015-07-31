import bloop.client
import bloop.condition
import bloop.exceptions
import bloop.filter
import bloop.index
import bloop.model
import bloop.tracking
import collections
import collections.abc
import declare
DEFAULT_CONFIG = {
    "atomic": False,
    "consistent": False,
    "prefetch": 0,
    "save": "update",
    "strict": True,
}


def _list_of(objs):
    """ wrap single elements in a list """
    if isinstance(objs, str):  # pragma: no cover
        return [objs]
    elif isinstance(objs, collections.abc.Iterable):
        return objs
    else:
        return [objs]


def _value_of(column):
    """ _value_of({"S": "Space Invaders"}) -> "Space Invaders" """
    return next(iter(column.values()))


def _dump_key(engine, obj):
    """
    dump the hash (and range, if there is one) key(s) of an object into
    a dynamo-friendly format.

    returns {dynamo_name: {type: value} for dynamo_name in hash/range keys}
    """
    meta = obj.Meta
    hash_key, range_key = meta.hash_key, meta.range_key
    hash_value = getattr(obj, hash_key.model_name)
    key = {
        hash_key.dynamo_name: engine._dump(hash_key.typedef, hash_value)}
    if range_key:
        range_value = getattr(obj, range_key.model_name)
        key[range_key.dynamo_name] = engine._dump(
            range_key.typedef, range_value)
    return key


class Engine:
    model = None

    def __init__(self, *, session=None, **config):
        self.client = bloop.client.Client(session=session)
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
        self.type_engine = declare.TypeEngine.unique()
        self.model = bloop.model.BaseModel(self)
        self.unbound_models = set()
        self.models = set()
        self.config = dict(DEFAULT_CONFIG)
        self.config.update(config)

    def _dump(self, model, obj):
        """ Return a dict of the obj in DynamoDB format """
        try:
            return self.type_engine.dump(model, obj)
        except declare.DeclareException:
            if model in self.unbound_models:
                raise bloop.exceptions.UnboundModel("load", model, obj)
            else:
                raise ValueError(
                    "Failed to dump unknown model {}".format(model))

    def _instance(self, model):
        """ Return an instance of a given model """
        return self._load(model, {})

    def _load(self, model, value):
        try:
            return self.type_engine.load(model, value)
        except declare.DeclareException:
            if model in self.unbound_models:
                raise bloop.exceptions.UnboundModel("load", model, None)
            else:
                raise ValueError(
                    "Failed to load unknown model {}".format(model))

    def _update(self, obj, attrs, expected):
        bloop.tracking.update(obj, attrs, expected)
        for column in expected:
            value = attrs.get(column.dynamo_name, None)
            if value is not None:
                value = self._load(column.typedef, value)
            setattr(obj, column.model_name, value)

    def bind(self):
        """ Create tables for all models that have been registered """
        # create_table doesn't block until ACTIVE or validate.
        # It also doesn't throw when the table already exists, making it safe
        # to call multiple times for the same unbound model.
        for model in self.unbound_models:
            self.client.create_table(model)

        unverified = set(self.unbound_models)
        while unverified:
            model = unverified.pop()

            self.client.validate_table(model)
            # If the call above didn't throw, everything's good to go.

            self.type_engine.register(model)
            for column in model.Meta.columns:
                self.type_engine.register(column.typedef)
            self.type_engine.bind()
            # If nothing above threw, we can mark this model bound

            self.unbound_models.remove(model)
            self.models.add(model)

    def context(self, **config):
        """
        with engine.context(atomic=True, consistent=True) as atomic:
            atomic.load(obj)
            del obj.foo
            obj.bar += 1
            atomic.save(obj)
        """
        return EngineView(self, **config)

    def delete(self, objs, *, condition=None):
        objs = _list_of(objs)
        rendering = self.config["atomic"] or condition
        for obj in objs:
            item = {"TableName": obj.Meta.table_name,
                    "Key": _dump_key(self, obj)}
            if rendering:
                renderer = bloop.condition.ConditionRenderer(self)
                item_condition = bloop.condition.Condition()
                if self.config["atomic"]:
                    item_condition &= bloop.tracking.atomic_condition(obj)
                if condition:
                    item_condition &= condition
                # Guard against an atomic delete on an object that was
                # never loaded (no expected fields)
                if item_condition:
                    renderer.render(item_condition, "condition")
                item.update(renderer.rendered)
            self.client.delete_item(item)
            bloop.tracking.clear(obj)

    def load(self, objs, *, consistent=None):
        """
        Populate objects from dynamodb, optionally using consistent reads.

        If any objects are not found, throws ObjectsNotFound with the attribute
        `missing` containing a list of the objects that were not loaded.

        Example
        -------
        engine = Engine()

        class HashOnly(engine.model):
            user_id = Column(NumberType, hash_key=True)

        class HashAndRange(engine.model):
            user_id = Column(NumberType, hash_key=True)
            game_title = Column(StringType, range_key=True)

        hash_only = HashOnly(user_id=101)
        hash_and_range = HashAndRange(user_id=101, game_title="Starship X")

        # Load only one instance, with consistent reads
        engine.load(hash_only, consistent=True)

        # Load multiple instances
        engine.load(hash_only, hash_and_range)
        """
        if consistent is None:
            consistent = self.config["consistent"]

        objs = _list_of(objs)
        # The RequestItems dictionary of table:Key(list) that will be
        # passed to client
        request_items = {}
        # table_name:dynamodb_name(list) of table keys (hash and opt range)
        # that is used to pull the correct attributes from result items
        # when mapping fields back to the input models
        table_key_shapes = {}
        # Index objects by the (table_name, _dump_key) tuple that
        # can be used to find their attributes in the results map
        objs_by_key = {}

        # Use set here to properly de-dupe list (don't load same obj twice)
        for obj in set(objs):
            table_name = obj.Meta.table_name
            if table_name not in request_items:
                request_items[table_name] = {"Keys": [],
                                             "ConsistentRead": consistent}
            key = _dump_key(self, obj)
            request_items[table_name]["Keys"].append(key)
            # Make sure we can find the key shape for this table and index
            # the object by its table name and key values for quickly loading
            # from results
            key_shape = table_key_shapes[table_name] = list(key)
            index = (table_name,
                     tuple(_value_of(key[n]) for n in key_shape))
            objs_by_key[index] = obj

        results = self.client.batch_get_items(request_items)

        for table_name, items in results.items():
            # The attributes that make up the key
            key_shape = table_key_shapes[table_name]
            for item in items:
                # Find the instance by key in the index above O(1)
                index = (table_name,
                         tuple(_value_of(item[n]) for n in key_shape))
                obj = objs_by_key.pop(index)
                self._update(obj, item, obj.Meta.columns)

        # If there are still objects, they weren't found
        if objs_by_key:
            raise bloop.exceptions.NotModified("load", objs_by_key.values())

    def query(self, obj):
        if isinstance(obj, bloop.index._Index):
            model, index = obj.model, obj
        else:
            model, index = obj, None
        return bloop.filter.Query(engine=self, model=model, index=index)

    def save(self, objs, *, condition=None):
        objs = _list_of(objs)
        atomic = self.config["atomic"]
        mode = self.config["save"]
        update = mode == "update"
        rendering = atomic or condition or update
        try:
            func = {"overwrite": self.client.put_item,
                    "update": self.client.update_item}[mode]
        except KeyError:
            raise ValueError("Unknown save mode {}".format(mode))
        for obj in objs:
            if mode == "overwrite":
                item = {"TableName": obj.Meta.table_name,
                        "Item": self._dump(obj.__class__, obj)}
            if mode == "update":
                item = {"TableName": obj.Meta.table_name,
                        "Key": _dump_key(self, obj)}
            if rendering:
                renderer = bloop.condition.ConditionRenderer(self)
                item_condition = bloop.condition.Condition()
                if update:
                    diff = bloop.tracking.diff_obj(obj, self)
                    renderer.update(diff)
                if atomic:
                    item_condition &= bloop.tracking.atomic_condition(obj)
                if condition:
                    item_condition &= condition
                if item_condition:
                    renderer.render(item_condition, "condition")
                item.update(renderer.rendered)
            func(item)
            bloop.tracking.update_current(obj, self)

    def scan(self, obj):
        if isinstance(obj, bloop.index._Index):
                model, index = obj.model, obj
        else:
            model, index = obj, None
        return bloop.filter.Scan(engine=self, model=model, index=index)


class EngineView(Engine):
    def __init__(self, engine, **config):
        self.client = engine.client
        self.config = dict(engine.config)
        self.config.update(config)
        self.model = engine.model
        self.type_engine = engine.type_engine

    def bind(self):
        raise RuntimeError("EngineViews can't modify engine types or bindings")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False
