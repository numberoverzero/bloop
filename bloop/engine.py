import declare
import uuid


class ModelMetaclass(declare.ModelMetaclass):
    def __new__(metaclass, name, bases, attrs):
        model = super().__new__(metaclass, name, bases, attrs)
        engine = model.__meta__["bloop_engine"]
        # If the engine already has a base, register this model
        if engine.model:
            engine.register(model)
        # Otherwise this IS the engine's base model
        return model


def _unique_base(engine):
    class Model(object, metaclass=ModelMetaclass):
        __meta__ = {
            'type_engine': engine.type_engine,
            'bloop_engine': engine
        }
    return Model


class Engine(object):
    model = None

    def __init__(self, namespace=None):
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
        namespace = namespace or uuid.uuid4()
        self.type_engine = declare.TypeEngine(namespace=namespace)
        self.models = []
        self.model = _unique_base(self)

    def register(self, model):
        self.models.append(model)


if __name__ == "__main__":
    e = Engine()

    class Person(e.model):
        pass
