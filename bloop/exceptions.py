CONSTRAINT_FAILURE = "Failed to meet expected condition during {}"
NOT_MODIFIED = "Failed to modify some obects during {}"
TABLE_MISMATCH = "Existing table for model {} does not match expected"
UNBOUND = "Failed to {} unbound model.  Did you forget to call engine.bind()?"


class ConstraintViolation(Exception):
    """ Raised when a condition is not met during put/update/delete """
    def __init__(self, operation, obj):
        super().__init__(CONSTRAINT_FAILURE.format(operation))
        self.obj = obj


class NotModified(Exception):
    """
    Raised when some objects are not loaded, saved, or deleted.
    self.objects contains the objects not modified
    """
    def __init__(self, operation, objects):
        super().__init__(NOT_MODIFIED.format(operation))
        self.objects = objects


class TableMismatch(Exception):
    """
    Raised when trying to bind a model against an existing table that
    doesn't match the required attributes for the model's definition
    """
    def __init__(self, model, expected, actual):
        super().__init__(TABLE_MISMATCH.format(model))
        self.model = model
        self.expected = expected
        self.actual = actual


class UnboundModel(Exception):
    """
    Raised when attempting to load/dump a model before binding it to an engine
    """
    def __init__(self, operation, model, obj):
        super().__init__(UNBOUND.format(operation))
        self.model = model
        self.obj = obj
