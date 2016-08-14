"""
Exceptions raised during normal use of bloop which can be programatically
responded to.

There are no exceptions for things like specifying an invalid
key when constructing a Query, for example, because there is no way to
automatically recover from that failure.

"""

__all__ = [
    "AbstractModelError", "BloopException", "ConstraintViolation",
    "MissingObjects", "TableMismatch", "UnboundModel"]

_CONSTRAINT_FAILURE = "Failed to meet required condition during {}"
_NOT_LOADED = "Failed to load some objects"
_TABLE_MISMATCH = "Existing table for model {} does not match expected"
_UNBOUND = "Failed to {} unbound model.  Did you forget to call engine.bind()?"


class BloopException(Exception):
    """An unexpected exception occurred."""


class AbstractModelError(BloopException, ValueError):
    """There is no way to load or save an abstract model."""


class ConstraintViolation(BloopException):
    """Raised when a condition is not met.

     This is thrown when an explicit condition passed to load/save/delete fails, an atomic operation fails,
     or an implicit query/scan condition fails (one/first)

    Attributes:
        obj (dict): The dict that was sent to dynamodb and failed some
            conditional operation

    """
    def __init__(self, operation, obj):
        super().__init__(_CONSTRAINT_FAILURE.format(operation), obj)
        self.obj = obj


class MissingObjects(BloopException):
    """Some objects were not found."""
    def __init__(self, *args, objects=None):
        super().__init__(*args)
        self.objects = list(objects) if objects else []


class TableMismatch(BloopException):
    """Raised when binding a model to an existing table with the wrong schema.

    Attributes:
        model (:class:`bloop.models.BaseModel`):
            The model that was trying to bind
        expected (dict): The expected schema for the table
        actual (dict): The actual schema of the table
    """
    def __init__(self, model, expected, actual):
        super().__init__(_TABLE_MISMATCH.format(model), expected, actual)
        self.model = model
        self.expected = expected
        self.actual = actual


class UnboundModel(BloopException):
    """Raised when loading or dumping on a model before binding it to an engine

    Attributes:
        model (:class:`bloop.models.BaseModel`):
            The model of the object being loaded, or dumped
        obj (object or None): The instance of the model that was being dumped,
            or loaded into.  If a new instance of the model was being created,
            this will be None

    """
    def __init__(self, operation, model, obj):
        super().__init__(_UNBOUND.format(operation), model, obj)
        self.model = model
        self.obj = obj
