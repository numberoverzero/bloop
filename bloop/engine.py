import declare
import uuid


class ModelMetaclass(declare.ModelMetaclass):
    # TODO: Use __meta__["bloop_engine"] to call engine.register
    pass


def _unique_base(engine):
    class Model(object, metaclass=ModelMetaclass):
        __meta__ = {
            'type_engine': engine.type_engine,
            'bloop_engine': engine
        }
    return Model


class Engine(object):
    def __init__(self):
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
        namespace = uuid.uuid4()
        self.type_engine = declare.TypeEngine(namespace=namespace)
        self._base_model = _unique_base(self)

    @property
    def model(self):
        return self._base_model

    def register(self, model):
        self.models.append(model)
