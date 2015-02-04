import bloop.model
import declare


class Engine(object):
    model = None

    def __init__(self, namespace=None):
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
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

    def load(self, model, value):
        return self.type_engine.load(model, value)

    def dump(self, model, value):
        return self.type_engine.dump(model, value)

    def bind(self):
        ''' Create tables for all models that have been registered '''
        for model in self.models:
            model.bind()
