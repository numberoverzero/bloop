import enum
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, List, NamedTuple, Optional

from .conditions import render
from .exceptions import MissingObjects, TransactionTokenExpired
from .models import unpack_from_dynamodb
from .signals import object_deleted, object_loaded, object_saved
from .util import dump_key, get_table_name


__all__ = [
    "PreparedTransaction",
    "ReadTransaction",
    "Transaction",
    "TxItem", "TxType",
    "WriteTransaction",
]
logger = logging.getLogger("bloop.transactions")

MAX_TRANSACTION_ITEMS = 10
# per docs this is 10 minutes, minus a bit for clock skew guard
MAX_TOKEN_LIFETIME = timedelta(minutes=9, seconds=30)


class TxType(enum.Enum):
    """Enum whose value is the wire format of its name"""
    Get = "Get"
    Check = "CheckCondition"
    Delete = "Delete"
    Update = "Update"

    @classmethod
    def by_alias(cls, name: str) -> "TxType":
        """get a type by the common bloop operation name: get/check/delete/save"""
        return {
            "get": TxType.Get,
            "check": TxType.Check,
            "delete": TxType.Delete,
            "save": TxType.Update,
        }[name]


class TxItem(NamedTuple):
    """
    Includes the type, an object, and its condition/atomic settings.

    The common way to construct an item is through the ``new`` method:

    .. code-block:: pycon

        >>> get_item = TxItem.new("get", some_obj)
        >>> save_item = TxItem.new("save", some_obj, atomic=True)
    """

    #: How this item will be used in a transaction
    type: TxType

    #: The object that will be modified, persisted, or referenced in a transaction
    obj: Any

    #: An optional condition that constrains an update
    condition: Optional[Any]

    #: Whether the object must be identical locally to Dynamo before the commit takes place.
    atomic: bool

    @classmethod
    def new(cls, type_alias, obj, condition=None, atomic=False) -> "TxItem":
        return TxItem(type=TxType.by_alias(type_alias), obj=obj, condition=condition, atomic=atomic)

    @property
    def is_update(self):
        """Whether this should render an "UpdateExpression" in the TransactItem"""
        return self.type is TxType.Update

    @property
    def should_render_obj(self):
        """Whether the object values should be rendered in the TransactItem"""
        return self.type not in {TxType.Check, TxType.Get}


# hack to get around NamedTuple field docstrings renaming:
# https://stackoverflow.com/a/39320627
TxItem.type.__doc__ = """How this item will be used in a transaction"""
TxItem.obj.__doc__ = """The object that will be modified, persisted, or referenced in a transaction"""
TxItem.condition.__doc__ = """An optional condition that constrains an update"""
TxItem.atomic.__doc__ = """Whether the object must be identical locally to Dynamo before the commit takes place."""


class Transaction:
    """
    Holds a collection of transaction items to be rendered into a PreparedTransaction.

    If used as a context manager, calls prepare() and commit() when the outermost context exits.

    .. code-block:: pycon

        >>> engine = Engine()
        >>> tx = Transaction(engine)
        >>> tx.mode = "w"
        >>> p1 = tx.prepare()
        >>> p2 = tx.prepare()  # different instances

        >>> with tx:
        ...     pass
        >>> #  tx.prepare().commit() is called here
    """
    mode: str
    _items: List[TxItem]

    def __init__(self, engine):
        self.engine = engine
        self._items = []
        self._ctx_depth = 0

    def __enter__(self):
        self._ctx_depth += 1
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._ctx_depth -= 1
        if exc_type:
            return
        if self._ctx_depth == 0:
            self.prepare().commit()

    def _extend(self, items):
        if len(self._items) + len(items) > MAX_TRANSACTION_ITEMS:
            raise RuntimeError(f"transaction cannot exceed {MAX_TRANSACTION_ITEMS} items.")
        self._items += items

    def prepare(self):
        """
        Create a new PreparedTransaction that can be committed.

        This is called automatically when exiting the transaction as a context:

        .. code-block:: python

            >>> engine = Engine()
            >>> tx = WriteTransaction(engine)
            >>> prepared = tx.prepare()
            >>> prepared.commit()

            # automatically calls commit when exiting
            >>> with WriteTransaction(engine) as tx:
            ...     # modify the transaction here
            ...     pass
            >>> # tx commits here

        :return:
        """
        tx = PreparedTransaction()
        tx.prepare(
            engine=self.engine,
            mode=self.mode,
            items=self._items,
        )
        return tx


class PreparedTransaction:
    """
    Transaction that can be committed once or more.

    Usually created from a :class:`~bloop.transactions.Transaction` instance.
    """
    mode: str
    items: List[TxItem]

    #: Unique id used as the "ClientRequestToken" for write transactions.  This is
    #: generated but not sent with a read transaction, since reads are not idempotent.
    tx_id: str

    #: When the transaction was first committed at.  A prepared write transaction can only call commit
    #: again within 10 minutes of its first commit.  This is ``None`` until commit() is called at least once.
    first_commit_at: Optional[datetime] = None

    def __init__(self):
        self.engine = None
        self._request = None

    def prepare(self, engine, mode, items) -> None:
        """
        Create a unique transaction id and dumps the items into a cached request object.
        """
        self.tx_id = str(uuid.uuid4()).replace("-", "")
        self.engine = engine
        self.mode = mode
        self.items = items
        self._prepare_request()

    def _prepare_request(self):
        self._request = [
            {
                item.type.value: {
                    "Key": dump_key(self.engine, item.obj),
                    "TableName": get_table_name(self.engine, item.obj),
                    **render(
                        self.engine,
                        obj=item.obj if item.should_render_obj else None,
                        atomic=item.atomic,
                        condition=item.condition,
                        update=item.is_update),
                }
            }
            for item in self.items
        ]

    def commit(self) -> None:
        """
        Commit the transaction with a fixed transaction id.

        A read transaction can call commit() any number of times, while a write transaction can only use the
        same tx_id for 10 minutes from the first call.
        """
        now = datetime.now(timezone.utc)
        if self.first_commit_at is None:
            self.first_commit_at = now

        if self.mode == "r":
            response = self.engine.session.transaction_read(self._request)
        elif self.mode == "w":
            if now - self.first_commit_at > MAX_TOKEN_LIFETIME:
                raise TransactionTokenExpired
            response = self.engine.session.transaction_write(self._request, self.tx_id)
        else:
            raise ValueError(f"unrecognized mode {self.mode}")

        self._handle_response(response)

    def _handle_response(self, response: dict) -> None:
        if self.mode == "w":
            for item in self.items:
                obj = item.obj
                if item.type is TxType.Delete:
                    object_deleted.send(self.engine, engine=self.engine, obj=obj)
                elif item.type is TxType.Update:
                    object_saved.send(self.engine, engine=self.engine, obj=obj)
        else:
            blobs = response["Responses"]
            not_loaded = set()
            if len(self.items) != len(blobs):
                raise RuntimeError("malformed response from DynamoDb")
            for item, blob in zip(self.items, blobs):
                obj = item.obj
                if not blob:
                    not_loaded.add(obj)
                    continue
                unpack_from_dynamodb(attrs=blob["Item"], expected=obj.Meta.columns, engine=self.engine, obj=obj)
                object_loaded.send(self.engine, engine=self.engine, obj=obj)
            if not_loaded:
                logger.info("loaded {} of {} objects".format(len(self.items) - len(not_loaded), len(self.items)))
                raise MissingObjects("Failed to load some objects.", objects=not_loaded)
            logger.info("successfully loaded {} objects".format(len(self.items)))


class ReadTransaction(Transaction):
    """
    Loads all items in the same transaction.  Items can be from different models and tables.
    """
    mode = "r"

    def load(self, *objs) -> "ReadTransaction":
        """
        Add one or more objects to be loaded in this transaction.

        At most 10 items can be loaded in the same transaction.  All objects will be loaded each time you
        call commit().


        :param objs: Objects to add to the set that are loaded in this transaction.
        :return: this transaction for chaining
        :raises bloop.exceptions.MissingObjects: if one or more objects aren't loaded.
        """
        self._extend([TxItem.new("get", obj) for obj in objs])
        return self


class WriteTransaction(Transaction):
    """
    Applies all updates in the same transaction.  Items can be from different models and tables.

    As with an engine, you can apply conditions to each object that you save or delete, or a condition for the entire
    transaction that won't modify the specified object:

    .. code-block:: python

        # condition on some_obj
        >>> tx.save(some_obj, condition=SomeModel.name.begins_with("foo"))
        # condition on the tx, based on the values of some_other_obj
        >>> tx.check(some_other_obj, condition=ThatModel.capacity >= 100)

    """
    mode = "w"

    def check(self, obj, condition) -> "WriteTransaction":
        """
        Add a condition which must be met for the transaction to commit.

        While the condition is checked against the provided object, that object will not be modified.  It is only
        used to provide the hash and range key to apply the condition to.

        At most 10 items can be checked, saved, or deleted in the same transaction.  The same idempotency token will
        be used for a single prepared transaction, which allows you to safely call commit on the PreparedCommit object
        multiple times.


        :param obj: The object to use for the transaction condition.  This object will not be modified.
        :param condition: A condition on an object which must hold for the transaction to commit.
        :return: this transaction for chaining
        """
        self._extend([TxItem.new("check", obj, condition)])
        return self

    def save(self, *objs, condition=None, atomic=False) -> "WriteTransaction":
        """
        Add one or more objects to be saved in this transaction.

        At most 10 items can be checked, saved, or deleted in the same transaction.  The same idempotency token will
        be used for a single prepared transaction, which allows you to safely call commit on the PreparedCommit object
        multiple times.

        :param objs: Objects to add to the set that are updated in this transaction.
        :param condition: A condition for these objects which must hold for the transaction to commit.
        :param bool atomic: only commit the transaction if the local and DynamoDB versions of the object match.
        :return: this transaction for chaining
        """
        self._extend([TxItem.new("save", obj, condition, atomic) for obj in objs])
        return self

    def delete(self, *objs, condition=None, atomic=False) -> "WriteTransaction":
        """
        Add one or more objects to be deleted in this transaction.

        At most 10 items can be checked, saved, or deleted in the same transaction.  The same idempotency token will
        be used for a single prepared transaction, which allows you to safely call commit on the PreparedCommit object
        multiple times.

        :param objs: Objects to add to the set that are deleted in this transaction.
        :param condition: A condition for these objects which must hold for the transaction to commit.
        :param bool atomic: only commit the transaction if the local and DynamoDB versions of the object match.
        :return: this transaction for chaining
        """
        self._extend([TxItem.new("delete", obj, condition, atomic) for obj in objs])
        return self
