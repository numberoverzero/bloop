class BloopException(Exception):
    """An unexpected exception occurred."""


class ConstraintViolation(BloopException):
    """A required condition was not met."""


class MissingObjects(BloopException):
    """Some objects were not found."""
    def __init__(self, *args, objects=None):
        super().__init__(*args)
        self.objects = list(objects) if objects else []


class TableMismatch(BloopException):
    """The expected and actual tables for this Model do not match."""


class UnboundModel(BloopException, ValueError):
    """This Model has not been bound to the Engine."""


class UnknownType(BloopException, ValueError):
    """This Type has not been registered with the type engine."""


class InvalidSearchMode(BloopException, ValueError):
    """Search mode must be 'scan' or 'query'."""


class MissingKey(BloopException, ValueError):
    """The instance must provide values for its key columns."""


class RecordsExpired(BloopException):
    """The requested stream records are beyond the trim horizon."""


class ShardIteratorExpired(BloopException):
    """The shard iterator is past its expiration date."""


class InvalidModel(BloopException, ValueError):
    """This is not a valid Model."""


class InvalidIndex(BloopException, ValueError):
    """This is not a valid Index."""


class InvalidStream(BloopException, ValueError):
    """This is not a valid stream definition."""


class InvalidShardIterator(BloopException, ValueError):
    """This is not a valid shard iterator."""


class InvalidComparisonOperator(BloopException, ValueError):
    """This is not a valid Comparison operator."""


class InvalidCondition(BloopException, ValueError):
    """This is not a valid Condition."""


class InvalidKeyCondition(BloopException, ValueError):
    """This is not a valid key condition for the Model and Index."""


class InvalidFilterCondition(BloopException, ValueError):
    """This is not a valid filter condition for the Model and Index."""


class InvalidProjection(BloopException, ValueError):
    """This is not a valid projection option for the Model and Index."""


class InvalidPosition(BloopException, ValueError):
    """This is not a valid position for a Stream."""
