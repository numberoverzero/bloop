import bloop.model
from bloop.filter import Query, Scan
from bloop.condition import render
from bloop.dynamo_client import DynamoClient, dump_key
import declare
import collections
import collections.abc
missing = object()


class ObjectsNotFound(Exception):
    ''' Thrown when batch_get fails to find some objects '''
    def __init__(self, message, objs):
        super().__init__(message)
        self.missing = list(objs)


class ConstraintViolation(Exception):
    ''' Thrown when a condition is not met during save/delete '''
    def __init__(self, message, obj):
        super().__init__(message)
        self.obj = obj


def list_of(objs):
    ''' wrap single elements in a list '''
    # String check first since it is also an Iterable
    if isinstance(objs, str):
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
        self.dynamo_client = DynamoClient(session=session)
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
        self.type_engine = declare.TypeEngine.unique()
        self.model = bloop.model.BaseModel(self)
        self.unbound_models = set()
        self.models = []

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
        return self.type_engine.load(model, value)

    def __dump__(self, model, value):
        return self.type_engine.dump(model, value)

    def bind(self):
        ''' Create tables for all models that have been registered '''
        # If any model's table fails to create, we want to push it back into
        # the unbound_models set so any error can be handled and retried.
        while self.unbound_models:
            model = self.unbound_models.pop()
            try:
                self.dynamo_client.create_table(model)
            except Exception as exception:
                self.unbound_models.add(model)
                raise exception
            else:
                self.type_engine.register(model)
                columns = model.Meta.columns
                for column in columns:
                    self.type_engine.register(column.typedef)
                self.type_engine.bind()

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
        # passed to dynamo_client
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

        results = self.dynamo_client.batch_get_items(request_items)

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
            table_name = model.Meta.table_name
            item = self.__dump__(model, obj)
            expression = render(self, model, condition, mode="condition")
            self.dynamo_client.put_item(table_name, item, expression)

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

            self.dynamo_client.batch_write_items(request_items)

    def delete(self, objs, *, condition=None):
        objs = list_of(objs)
        if len(objs) > 1 and condition:
            raise ValueError("condition is only usable with a single object")

        elif len(objs) == 1 and condition:
            obj = objs[0]
            model = obj.__class__
            table_name = model.Meta.table_name
            key = dump_key(self, obj)
            expression = render(self, model, condition, mode="condition")
            self.dynamo_client.delete_item(table_name, key, expression)

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

            self.dynamo_client.batch_write_items(request_items)

    def query(self, model, index=None):
        return Query(engine=self, model=model, index=index)

    def scan(self, model, index=None):
        return Scan(engine=self, model=model, index=index)
