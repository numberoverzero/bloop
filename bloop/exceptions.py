"""
Exceptions raised during normal use of bloop which can be programatically
responded to.

There are no exceptions for things like specifying an invalid
key when constructing a Query, for example, because there is no way to
automatically recover from that failure.

"""

_CONSTRAINT_FAILURE = "Failed to meet expected condition during {}"
_NOT_MODIFIED = "Failed to modify some obects during {}"
_TABLE_MISMATCH = "Existing table for model {} does not match expected"
_UNBOUND = "Failed to {} unbound model.  Did you forget to call engine.bind()?"


class BloopException(Exception):
    """Base exception for blanket catching"""
    pass


class ConstraintViolation(BloopException):
    """Raised when a condition is not met during put/update/delete.

    Attributes:
        obj (dict): The dict that was sent to dynamodb and failed some
            conditional operation

    """
    def __init__(self, operation, obj):
        super().__init__(_CONSTRAINT_FAILURE.format(operation))
        self.obj = obj


class NotModified(BloopException):
    """Raised when some objects are not loaded, saved, or deleted.

    Attributes:
        objects (list): the objects not modified

    """
    def __init__(self, operation, objects):
        super().__init__(_NOT_MODIFIED.format(operation))
        self.objects = list(objects)


class TableMismatch(BloopException):
    """Raised when binding a model to an existing table with the wrong schema.

    Attributes:
        model (:class:`bloop.model.BaseModel`):
            The model that was trying to bind
        expected (dict): The expected schema for the table
        actual (dict): The actual schema of the table
    """
    def __init__(self, model, expected, actual):
        super().__init__(_TABLE_MISMATCH.format(model))
        self.model = model
        self.expected = expected
        self.actual = actual


class UnboundModel(BloopException):
    """Raised when loading or dumping on a model before binding it to an engine

    Attributes:
        model (:class:`bloop.model.BaseModel`):
            The model of the object being loaded, or dumped
        obj (object or None): The instance of the model that was being dumped,
            or loaded into.  If a new instance of the model was being created,
            this will be None

    """
    def __init__(self, operation, model, obj):
        super().__init__(_UNBOUND.format(operation))
        self.model = model
        self.obj = obj
