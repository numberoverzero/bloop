import bloop.client
import bloop.condition
import bloop.filter
import bloop.index
import bloop.model
import bloop.tracking
import bloop.util
import collections
import collections.abc
import declare
MISSING = bloop.util.Sentinel('MISSING')


class ObjectsNotFound(Exception):
    ''' Thrown when batch_get fails to find some objects '''
    def __init__(self, message, objs):
        super().__init__(message)
        self.missing = list(objs)


def list_of(objs):
    ''' wrap single elements in a list '''
    # String check first since it is also an Iterable
    if isinstance(objs, str):  # pragma: no cover
        return [objs]
    elif isinstance(objs, collections.abc.Iterable):
        return objs
    else:
        return [objs]


def value_of(column):
    '''
    Return the value in a key definition

    Example
    -------
    value_of({'S': 'Space Invaders'}) -> 'Space Invaders'
    '''
    return next(iter(column.values()))


def dump_key(engine, obj):
    '''
    dump the hash (and range, if there is one) key(s) of an object into
    a dynamo-friendly format.

    returns {dynamo_name: {type: value} for dynamo_name in hash/range keys}
    '''
    meta = obj.Meta
    hash_key, range_key = meta.hash_key, meta.range_key

    hash_value = getattr(obj, hash_key.model_name)
    key = {
        hash_key.dynamo_name: engine.__dump__(
            hash_key.typedef, hash_value)
    }
    if range_key:
        range_value = getattr(obj, range_key.model_name)
        key[range_key.dynamo_name] = engine.__dump__(
            range_key.typedef, range_value)
    return key


class Engine(object):
    model = None

    def __init__(self, session=None, prefetch=0, persist_mode="overwrite"):
        self.client = bloop.client.Client(session=session)
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
        self.type_engine = declare.TypeEngine.unique()
        self.model = bloop.model.BaseModel(self)
        self.unbound_models = set()
        self.models = set()

        # Control how many pages are loaded at once during scans/queries.
        #   "all": the full query will be executed at once.
        #   = 0: Pages will be loaded on demand.
        #   > 0: that number of pages will be fetched at a time.
        self.prefetch = prefetch

        # Control how objects are persisted.  PutItem will completely overwrite
        # an existing item, including deleting fields not set on the
        # local item.  A query against a GSI with projection KEYS_ONLY will
        # not load non-key attributes, and saving it back with PutItem would
        # clear all non-key attributes.

        # Options: "update", "overwrite"
        self.persist_mode = persist_mode

    @property
    def prefetch(self):
        return self._prefetch

    @prefetch.setter
    def prefetch(self, value):
        self._prefetch = bloop.filter.validate_prefetch(value)

    @property
    def persist_mode(self):
        return self._persist_mode

    @persist_mode.setter
    def persist_mode(self, value):
        if value not in ("overwrite", "update"):
            raise ValueError("persist_mode must be `overwrite` or `update`")
        self._persist_mode = value

    def register(self, model):
        if model not in self.models:
            self.unbound_models.add(model)

    def __load__(self, model, value):
        try:
            return self.type_engine.load(model, value)
        except declare.DeclareException:
            if model in self.unbound_models:
                raise RuntimeError("Must call `engine.bind()` before loading")
            else:
                raise ValueError(
                    "Failed to load unknown model {}".format(model))

    def __dump__(self, model, obj):
        ''' Return a dict of the obj in DynamoDB format '''
        try:
            return self.type_engine.dump(model, obj)
        except declare.DeclareException:
            if model in self.unbound_models:
                raise RuntimeError("Must call `engine.bind()` before dumping")
            else:
                raise ValueError(
                    "Failed to dump unknown model {}".format(model))

    def __instance__(self, model):
        ''' Return an instance of a given model '''
        return self.__load__(model, {})

    def __update__(self, obj, attrs, expected):
        for column in expected:
            value = attrs.get(column.dynamo_name, MISSING)
            # Missing expected column - try to remove the existing
            # value.  If the value didn't exist on the obj, it's
            # already in the expected state.
            if value is MISSING:
                try:
                    delattr(obj, column.model_name)
                except AttributeError:
                    pass
            # Load the value through the column's typedef into the obj
            else:
                value = self.__load__(column.typedef, value)
                setattr(obj, column.model_name, value)
        bloop.tracking.update(obj, attrs, expected)

    def bind(self):
        ''' Create tables for all models that have been registered '''
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
            columns = model.Meta.columns
            for column in columns:
                self.type_engine.register(column.typedef)
            self.type_engine.bind()
            # If nothing above threw, we can mark this model bound

            self.unbound_models.remove(model)
            self.models.add(model)

    def load(self, objs, *, consistent=False):
        '''
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
        hash_and_range = HashAndRange(user_id=101, game_title='Starship X')

        # Load only one instance, with consistent reads
        engine.load(hash_only, consistent=True)

        # Load multiple instances
        engine.load(hash_only, hash_and_range)
        '''
        objs = list_of(objs)

        # The RequestItems dictionary of table:Key(list) that will be
        # passed to client
        request_items = {}

        # table_name:dynamodb_name(list) of table keys (hash and opt range)
        # that is used to pull the correct attributes from result items
        # when mapping fields back to the input models
        table_key_shapes = {}

        # Index objects by the (table_name, dump_key) tuple that
        # can be used to find their attributes in the results map
        objs_by_key = {}

        # Use set here to properly de-dupe list (don't load same obj twice)
        for obj in set(objs):
            table_name = obj.Meta.table_name
            if table_name not in request_items:
                request_items[table_name] = {
                    "Keys": [],
                    "ConsistentRead": consistent
                }
            key = dump_key(self, obj)
            request_items[table_name]["Keys"].append(key)
            # Make sure we can find the key shape for this table and index
            # the object by its table name and key values for quickly loading
            # from results
            key_shape = table_key_shapes[table_name] = list(key)

            index = (
                table_name,
                tuple(value_of(key[n]) for n in key_shape)
            )
            objs_by_key[index] = obj

        results = self.client.batch_get_items(request_items)

        for table_name, items in results.items():
            # The attributes that make up the key
            key_shape = table_key_shapes[table_name]
            for item in items:
                # Find the instance by key in the index above O(1)
                index = (
                    table_name,
                    tuple(value_of(item[n]) for n in key_shape)
                )
                obj = objs_by_key.pop(index)
                self.__update__(obj, item, obj.Meta.columns)

        # If there are still objects, they weren't found
        if objs_by_key:
            raise ObjectsNotFound("Failed to load some objects",
                                  objs_by_key.values())

    def save(self, objs, *, condition=None):
        objs = list_of(objs)
        if len(objs) > 1 and condition:
            raise ValueError("condition is only usable with a single object")
        if self.persist_mode == "update":
            for obj in objs:
                # Safe to forward condition since it's None for lists
                self._save_update(obj, condition=condition)
        elif self.persist_mode == "overwrite":
            self._save_overwrite(objs, condition=condition)
        else:  # pragma: no cover
            raise ValueError(
                "Unknown persist mode {}".format(self.persist_mode))

    def _save_update(self, obj, *, condition=None):
        '''
        Don't need to check len(objs) if condition, since it's verified above
        '''
        model = obj.__class__
        # Load the tracking diff, dump into an UpdateExpression
        diff = bloop.tracking.diff_obj(obj, self)
        renderer = bloop.condition.ConditionRenderer(self)
        renderer.update(diff)
        if condition:
            renderer.render(condition, 'condition')

        item = {
            "TableName": model.Meta.table_name,
            "Key": dump_key(self, obj)
        }
        item.update(renderer.rendered)
        self.client.update_item(item)
        # Mark all columns of the item as tracked
        bloop.tracking.update_current(obj, self)
        return

    def _save_overwrite(self, objs, *, condition=None):
        if len(objs) == 1:
            obj = objs[0]
            model = obj.__class__
            renderer = bloop.condition.ConditionRenderer(self)
            if condition:
                renderer.render(condition, 'condition')
            item = {
                "TableName": model.Meta.table_name,
                "Item": self.__dump__(model, obj),
            }
            item.update(renderer.rendered)
            self.client.put_item(item)
            # Mark all columns of the item as tracked
            bloop.tracking.update_current(obj, self)
        else:
            request_items = collections.defaultdict(list)
            # Use set here to properly de-dupe list (don't save same obj twice)
            for obj in set(objs):
                put_item = {
                    "PutRequest": {
                        "Item": self.__dump__(obj.__class__, obj)
                    }
                }
                table_name = obj.Meta.table_name
                request_items[table_name].append(put_item)
            self.client.batch_write_items(request_items)
            # TODO: update tracking for each object as its batch
            # successfully writes.  Otherwise we could fail to update the
            # tracking for an object if a subsequent batch fails to write
            for obj in set(objs):
                bloop.tracking.update_current(obj, self)

    def delete(self, objs, *, condition=None):
        objs = list_of(objs)
        if len(objs) > 1 and condition:
            raise ValueError("condition is only usable with a single object")

        elif len(objs) == 1 and condition:
            obj = objs[0]
            model = obj.__class__
            renderer = bloop.condition.ConditionRenderer(self)
            renderer.render(condition, 'condition')
            item = {
                "TableName": model.Meta.table_name,
                "Key": dump_key(self, obj)
            }
            item.update(renderer.rendered)
            self.client.delete_item(item)
            bloop.tracking.clear(obj)

        else:
            request_items = collections.defaultdict(list)
            # Use set here to properly de-dupe list (don't save same obj twice)
            for obj in set(objs):
                del_item = {
                    "DeleteRequest": {
                        "Key": dump_key(self, obj)
                    }
                }
                table_name = obj.Meta.table_name
                request_items[table_name].append(del_item)

            self.client.batch_write_items(request_items)
            # TODO: clear tracking for each object as its batch
            # successfully deletes.  Otherwise we could fail to clear the
            # tracking for an object if a subsequent batch fails to delete
            for obj in set(objs):
                bloop.tracking.clear(obj)

    def query(self, obj):
        if bloop.index.is_index(obj):
            model, index = obj.model, obj
        else:
            model, index = obj, None
        return bloop.filter.Query(engine=self, model=model, index=index)

    def scan(self, obj):
        if bloop.index.is_index(obj):
                model, index = obj.model, obj
        else:
            model, index = obj, None
        return bloop.filter.Scan(engine=self, model=model, index=index)
