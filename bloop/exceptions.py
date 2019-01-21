class BloopException(Exception):
    """An unexpected exception occurred."""


class ConstraintViolation(BloopException):
    """A required condition was not met."""


class TransactionCanceled(BloopException):
    """The transaction was canceled.

    A WriteTransaction is canceled when:
        * A condition in one of the condition expressions is not met.
        * A table in the TransactWriteItems request is in a different account or region.
        * More than one action in the TransactWriteItems operation targets the same item.
        * There is insufficient provisioned capacity for the transaction to be completed.
        * An item size becomes too large (larger than 400 KB), or a local secondary index (LSI)
          becomes too large, or a similar validation error occurs because of changes made by the transaction.

    A ReadTransaction is canceled when:
        * There is an ongoing TransactGetItems operation that conflicts with a concurrent PutItem,
          UpdateItem, DeleteItem or TransactWriteItems request.
        * A table in the TransactGetItems request is in a different account or region.
        * There is insufficient provisioned capacity for the transaction to be completed.
        * There is a user error, such as an invalid data format.

    .. seealso::

        The API reference for `TransactionCanceledException`_

        .. _TransactionCanceledException: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactGetItems.html#API_TransactGetItems_Errors
    """  # noqa: E501


class TransactionTokenExpired(BloopException):
    """The transaction's tx_id (ClientRequestToken) was first used more than 10 minutes ago"""


class MissingObjects(BloopException):
    """Some objects were not found."""

    #: The objects that failed to load
    objects: list

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
