import bloop.model
import bloop.dynamo
from bloop.expression import render
import declare
import botocore
import boto3
missing = object()


def dump_key(engine, column, key):
    '''
    key is a {model_name: value} dict

    returns {dynamo_name: {type: value}} or {}
    '''
    if not column:
        return {}

    typedef = column.typedef
    key_value = key[column.model_name]
    dynamo_value = engine.type_engine.dump(typedef, key_value)
    return {column.dynamo_name: dynamo_value}


class Engine(object):
    model = None

    def __init__(self, namespace=None):
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
        self.dynamodb_client = boto3.client("dynamodb")
        self.type_engine = declare.TypeEngine.unique()
        self.model = bloop.model.BaseModel(self)
        self.models = []

    def register(self, model):
        self.models.append(model)
        self.type_engine.register(model)
        columns = model.__meta__['dynamo.columns']
        for column in columns:
            self.type_engine.register(column.typedef)
        self.type_engine.bind()

    def __load__(self, model, value):
        return self.type_engine.load(model, value)

    def __dump__(self, model, value):
        return self.type_engine.dump(model, value)

    def bind(self):
        ''' Create tables for all models that have been registered '''
        for model in self.models:
            try:
                table = bloop.dynamo.describe_model(model)
                self.dynamodb_client.create_table(**table)
            except botocore.exceptions.ClientError as error:
                # Raise unless the table already exists
                error_code = error.response['Error']['Code']
                if error_code != 'ResourceInUseException':
                    raise error

    def get(self, model, consistent_read=False, **key):
        '''
        Return a single instance of the model, or raise KeyError

        key must specify values for the hash key and,
        if the model has one, the range key.

        Example
        -------
        engine = Engine()

        class HashOnly(engine.model):
            user_id = Column(NumberType, hash_key=True)

        class HashAndRange(engine.model):
            user_id = Column(NumberType, hash_key=True)
            game_title = Column(StringType, range_key=True)

        engine.get(HashOnly, user_id=101)
        engine.get(HashOnly, user_id=101, game='Starship X')
        '''
        meta = model.__meta__
        table_name = meta['dynamo.table.name']

        hash_key = meta['dynamo.table.hash_key']
        range_key = meta['dynamo.table.range_key']

        dynamo_key = {}
        dynamo_key.update(dump_key(self, hash_key, key))
        dynamo_key.update(dump_key(self, range_key, key))

        dynamo_item = self.dynamodb_client.get_item(
            TableName=table_name, Key=dynamo_key,
            ConsistentRead=consistent_read).get("Item", missing)

        if dynamo_item is missing:
            raise KeyError("No item found for {}".format(key))
        return self.__load__(model, dynamo_item)

    def save(self, item, overwrite=False):
        model = item.__class__
        meta = model.__meta__
        table_name = meta['dynamo.table.name']
        dynamo_item = self.__dump__(model, item)

        # Blow away any existing item
        if overwrite:
            self.dynamodb_client.put_item(
                TableName=table_name, Item=dynamo_item)

        # Assert that the hash (and range, if there is one) keys are None
        else:
            hash_key = meta['dynamo.table.hash_key']
            range_key = meta['dynamo.table.range_key']

            condition = hash_key.is_(None)
            if range_key:
                condition &= range_key.is_(None)
            expression = render(self, model, condition)

            self.dynamodb_client.put_item(
                TableName=table_name, Item=dynamo_item, **expression)
