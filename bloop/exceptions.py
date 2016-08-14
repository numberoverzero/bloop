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
    """The expected and actual tables for a model do not match."""


class UnboundModel(BloopException, ValueError):
    """There is no way to load or save instances of an unbound model."""


class UnknownType(BloopException, ValueError):
    """The provided type has not been registered with the type engine."""
