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


class UnknownType(BloopException, ValueError):
    """This does not support the Type interface."""


class InvalidSearch(BloopException, ValueError):
    """The search was malformed"""


class MissingKey(BloopException, ValueError):
    """The instance must provide values for its key columns."""


class RecordsExpired(BloopException):
    """The requested stream records are beyond the trim horizon."""


class ShardIteratorExpired(BloopException):
    """The shard iterator is past its expiration date."""


class InvalidModel(BloopException, ValueError):
    """This is not a valid Model."""


class InvalidTemplate(BloopException, ValueError):
    """This is not a valid template string."""


class InvalidStream(BloopException, ValueError):
    """This is not a valid stream definition."""


class InvalidShardIterator(BloopException, ValueError):
    """This is not a valid shard iterator."""


class InvalidCondition(BloopException, ValueError):
    """This is not a valid Condition."""


class InvalidPosition(BloopException, ValueError):
    """This is not a valid position for a Stream."""
