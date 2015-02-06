import bloop.model
import bloop.dynamo
import bloop.expression
import declare
import botocore
import boto3


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
