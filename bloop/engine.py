import bloop.condition
import bloop.dynamo_client
import bloop.filter
import bloop.model
import collections
import collections.abc
import declare
missing = object()


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


class Engine(object):
    model = None

    def __init__(self, session=None):
        self.client = bloop.dynamo_client.DynamoClient(session=session)
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
        self.type_engine = declare.TypeEngine.unique()
        self.model = bloop.model.BaseModel(self)
        self.unbound_models = set()
        self.models = set()

        # Control how many pages are loaded at once during scans/queries.
        #   < 0: the full query will be executed at once.
        #   = 0: Pages will be loaded on demand.
        #   > 0: that number of pages will be fetched at a time.
        self.prefetch = {
            "query": 0,
            "scan": 0
        }

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

    def __dump__(self, model, value):
        try:
            return self.type_engine.dump(model, value)
        except declare.DeclareException:
            if model in self.unbound_models:
                raise RuntimeError("Must call `engine.bind()` before dumping")
            else:
                raise ValueError(
                    "Failed to dump unknown model {}".format(model))

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
            key = bloop.dynamo_client.dump_key(self, obj)
            # Add the key to the request
            request_items[table_name]["Keys"].append(key)
            # Make sure we can find the key shape for this table
            key_shape = table_key_shapes[table_name] = list(key)
            # Index the object by its table name and key
            # values for quickly loading from results
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
                # Build the index so we can find the object to load in O(1)
                index = (
                    table_name,
                    tuple(value_of(item[n]) for n in key_shape)
                )
                obj = objs_by_key.pop(index)

                # Not using self.__load__(obj, item) because we don't want to
                # go through meta['bloop.init'] - we want to populate the
                # existing model instance
                columns = obj.Meta.columns
                for column in columns:
                    value = item.get(column.dynamo_name, missing)
                    # Missing expected column
                    if value is not missing:
                        value = self.__load__(column.typedef, value)
                        setattr(obj, column.model_name, value)

        # If there are still objects, they weren't found
        if objs_by_key:
            raise ObjectsNotFound("Failed to load some objects",
                                  objs_by_key.values())

    def save(self, objs, *, condition=None):
        objs = list_of(objs)
        if len(objs) > 1 and condition:
            raise ValueError("condition is only usable with a single object")

        elif len(objs) == 1 and condition:
            obj = objs[0]
            model = obj.__class__
            item = {
                "TableName": model.Meta.table_name,
                "Item": self.__dump__(model, obj),
            }
            item.update(bloop.condition.render(
                self, condition, mode="condition"))
            self.client.put_item(item)

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

    def delete(self, objs, *, condition=None):
        objs = list_of(objs)
        if len(objs) > 1 and condition:
            raise ValueError("condition is only usable with a single object")

        elif len(objs) == 1 and condition:
            obj = objs[0]
            model = obj.__class__
            item = {
                "TableName": model.Meta.table_name,
                "Key": bloop.dynamo_client.dump_key(self, obj)
            }
            item.update(bloop.condition.render(
                self, condition, mode="condition"))
            self.client.delete_item(item)

        else:
            request_items = collections.defaultdict(list)
            # Use set here to properly de-dupe list (don't save same obj twice)
            for obj in set(objs):
                del_item = {
                    "DeleteRequest": {
                        "Key": bloop.dynamo_client.dump_key(self, obj)
                    }
                }
                table_name = obj.Meta.table_name
                request_items[table_name].append(del_item)

            self.client.batch_write_items(request_items)

    def query(self, model, index=None):  # pragma: no cover
        return bloop.filter.Query(engine=self, model=model, index=index)

    def scan(self, model, index=None):  # pragma: no cover
        return bloop.filter.Scan(engine=self, model=model, index=index)
