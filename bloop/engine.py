import declare
import uuid


def _unique_base(namespace):
    class Model(declare.Model):
        __meta__ = {
            'namespace': namespace
        }
    return Model


class Engine(object):
    def __init__(self):
        # Unique namespace so the type engine for multiple bloop Engines
        # won't have the same TypeDefinitions
        namespace = uuid.uuid4()
        self.type_engine = declare.TypeEngine(namespace=namespace)
        self._base_model = _unique_base(namespace)

    @property
    def model(self):
        return self._base_model
