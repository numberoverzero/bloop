class BloopException(Exception):
    """An unexpected exception occurred."""


class AbstractModelError(BloopException, ValueError):
    """There is no way to load or save an abstract model."""


class ConstraintViolation(BloopException):
    """A required condition was not met."""


class MissingObjects(BloopException):
    """Some objects were not found."""
    def __init__(self, *args, objects=None):
        super().__init__(*args)
        self.objects = list(objects) if objects else []


class TableMismatch(BloopException):
    """The expected and actual tables for a model do not match."""


class UnboundModel(BloopException, ValueError):
    """There is no way to load or save instances of an unbound model."""


class UnknownType(BloopException, ValueError):
    """The provided type has not been registered with the type engine."""
