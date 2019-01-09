import enum
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, List, NamedTuple, Optional, Union

from .conditions import render
from .exceptions import TransactionTokenExpired
from .util import dump_key, get_table_name


__all__ = ["PreparedTransaction", "ReadTransaction", "Transaction", "WriteTransaction", "new_tx"]

MAX_TRANSACTION_ITEMS = 10
# per docs this is 10 minutes, minus a bit for clock skew guard
MAX_TOKEN_LIFETIME = timedelta(minutes=9, seconds=30)


class _TxType(enum.Enum):
    Get = "Get"
    Check = "CheckCondition"
    Delete = "Delete"
    Update = "Update"


class _TxWriteItem(NamedTuple):
    type: _TxType
    obj: Any
    condition: Optional[Any]
    atomic: bool

    @property
    def is_update(self):
        """Whether this should render an "UpdateExpression" in the TransactItem"""
        return self.type is _TxType.Update

    @property
    def should_render_obj(self):
        """Whether the object values should be rendered in the TransactItem"""
        return self.type not in {_TxType.Check, _TxType.Get}


class Transaction:
    mode: str
    items: List[_TxWriteItem]

    def __init__(self, engine):
        self.engine = engine
        self.items = []
        self._ctx_depth = 0

    def __enter__(self):
        self._ctx_depth += 1
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._ctx_depth -= 1
        if exc_type:
            return
        self.commit()

    def commit(self) -> "PreparedTransaction":
        if self._ctx_depth > 0:
            raise ValueError("cannot call commit within a context manager")
        tx = PreparedTransaction()
        tx.prepare(
            engine=self.engine,
            mode=self.mode,
            items=self.items,
        )
        tx.commit()
        return tx

    def _extend(self, items):
        if len(self.items) + len(items) > MAX_TRANSACTION_ITEMS:
            raise ValueError(f"transaction cannot exceed {MAX_TRANSACTION_ITEMS} items.")
        self.items += items


class PreparedTransaction:
    mode: str
    items: List[_TxWriteItem]
    tx_id: str
    first_commit_at: Optional[datetime] = None

    def __init__(self):
        self.engine = None
        self._request = None

    def prepare(self, engine, mode, items) -> None:
        self.tx_id = str(uuid.uuid4()).replace("-", "")
        self.engine = engine
        self.mode = mode
        self.items = items
        self._prepare_request()

    def _prepare_request(self):
        self._request = [
            {
                item.type.value: {
                    "Key": dump_key(self.engine, item),
                    "TableName": get_table_name(self.engine, item),
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

        self.handle_response(response)

    def handle_response(self, response: dict) -> None:
        # TODO
        pass


class ReadTransaction(Transaction):
    mode = "r"

    def load(self, *objs) -> "ReadTransaction":
        self._extend([
            _TxWriteItem(type=_TxType.Get, obj=obj, condition=None, atomic=False)
            for obj in objs
        ])
        return self


class WriteTransaction(Transaction):
    mode = "w"

    def check(self, obj, condition) -> "WriteTransaction":
        self._extend([
            _TxWriteItem(type=_TxType.Check, obj=obj, condition=condition, atomic=False)
        ])
        return self

    def save(self, *objs, condition=None, atomic=False) -> "WriteTransaction":
        self._extend([
            _TxWriteItem(type=_TxType.Update, obj=obj, condition=condition, atomic=atomic)
            for obj in objs
        ])
        return self

    def delete(self, *objs, condition=None, atomic=False) -> "WriteTransaction":
        self._extend([
            _TxWriteItem(type=_TxType.Delete, obj=obj, condition=condition, atomic=atomic)
            for obj in objs
        ])
        return self


def new_tx(engine, mode) -> Union[ReadTransaction, WriteTransaction]:
    if mode == "r":
        cls = ReadTransaction
    elif mode == "w":
        cls = WriteTransaction
    else:
        raise ValueError(f"unknown mode {mode}")
    return cls(engine)
