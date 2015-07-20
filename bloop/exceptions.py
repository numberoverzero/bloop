CONSTRAINT_FAILURE = "Failed to meet expected condition during {}"
NOT_MODIFIED = "Failed to modify some obects during {}"


class ConstraintViolation(Exception):
    ''' Thrown when a condition is not met during put/update/delete '''
    def __init__(self, operation, obj):
        super().__init__(CONSTRAINT_FAILURE.format(operation))
        self.obj = obj


class NotModified(Exception):
    '''
    Thrown when some objects are not loaded, saved, or deleted.
    self.objects contains the objects not modified
    '''
    def __init__(self, operation, objects):
        super().__init__(NOT_MODIFIED.format(operation))
        self.objects = objects
