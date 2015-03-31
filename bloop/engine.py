import bloop.model
from bloop.expression import render, Filter
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

    def __init__(self, namespace=None):
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
        self.dynamodb_client = DynamoClient()
        self.type_engine = declare.TypeEngine.unique()
        self.plugins = collections.defaultdict(list)
        self.model = bloop.model.BaseModel(self)
        self.models = []

    def register(self, model):
        self.models.append(model)
        self.type_engine.register(model)
        columns = model.__meta__['dynamo.columns']
        for column in columns:
            self.type_engine.register(column.typedef)
        self.type_engine.bind()

    def on(self, event):
        '''
        Decorate a function to be invoked when the given `event` occurs.

        Valid events are:
            - before_load
            - before_dump
            - after_load
            - after_dump

        function signature should match:
            (event name, model instance, *args, **kwargs)
        '''
        def wrap_function(func):
            self.plugins[event].append(func)
            return func
        return wrap_function

    def __trigger__(self, event, model, *args, **kwargs):
        plugins = self.plugins[event]
        for plugin in plugins:
            plugin(event, model, *args, **kwargs)

    def __load__(self, model, value):
        return self.type_engine.load(model, value)

    def __dump__(self, model, value):
        return self.type_engine.dump(model, value)

    def bind(self):
        ''' Create tables for all models that have been registered '''
        for model in self.models:
            self.dynamodb_client.create_table(model)

    def load(self, objs, *, consistent_read=False):
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
        engine.load(hash_only, consistent_read=True)

        # Load multiple instances
        engine.load(hash_only, hash_and_range)
        '''
        objs = list_of(objs)

        # The RequestItems dictionary of table:Key(list) that will be
        # passed to dynamodb_client
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
            meta = obj.__meta__
            table_name = meta['dynamo.table.name']
            if table_name not in request_items:
                request_items[table_name] = {
                    "Keys": [],
                    "ConsistentRead": consistent_read
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

        results = self.dynamodb_client.batch_get_items(request_items)

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

                # Let plugins know we're going to load the object
                self.__trigger__('before_load', obj)

                columns = obj.__meta__["dynamo.columns"]
                for column in columns:
                    value = item.get(column.dynamo_name, missing)
                    # Missing expected column
                    if value is not missing:
                        value = self.__load__(column.typedef, value)
                        setattr(obj, column.model_name, value)

                # Let plugins clean up or validate the object after loading
                self.__trigger__('after_load', obj)

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
            table_name = model.__meta__['dynamo.table.name']
            item = self.__dump__(model, obj)
            expression = render(self, model, condition)
            self.dynamodb_client.put_item(table_name, item, expression)

        else:
            request_items = collections.defaultdict(list)
            # Use set here to properly de-dupe list (don't save same obj twice)
            for obj in set(objs):
                put_item = {
                    "PutRequest": {
                        "Item": self.__dump__(obj.__class__, obj)
                    }
                }
                table_name = obj.__meta__['dynamo.table.name']
                request_items[table_name].append(put_item)

            self.dynamodb_client.batch_write_items(request_items)

    def delete(self, objs, *, condition=None):
        objs = list_of(objs)
        if len(objs) > 1 and condition:
            raise ValueError("condition is only usable with a single object")

        elif len(objs) == 1 and condition:
            obj = objs[0]
            model = obj.__class__
            table_name = model.__meta__['dynamo.table.name']
            key = dump_key(self, obj)
            expression = render(self, model, condition)
            self.dynamodb_client.delete_item(table_name, key, expression)

        else:
            request_items = collections.defaultdict(list)
            # Use set here to properly de-dupe list (don't save same obj twice)
            for obj in set(objs):
                del_item = {
                    "DeleteRequest": {
                        "Key": dump_key(self, obj)
                    }
                }
                table_name = obj.__meta__['dynamo.table.name']
                request_items[table_name].append(del_item)

            self.dynamodb_client.batch_write_items(request_items)

    def query(self, model, index=None):
        return Filter(engine=self, mode='query', model=model, index=index)

    def scan(self, model, index=None):
        return Filter(engine=self, mode='scan', model=model, index=index)
