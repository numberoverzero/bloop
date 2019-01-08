import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple, Optional, Union

from .exceptions import TransactionTokenExpired


__all__ = ["PreparedTransaction", "ReadTransaction", "Transaction", "WriteTransaction", "new_tx"]

MAX_TRANSACTION_ITEMS = 10
# per docs this is 10 minutes, minus a bit for clock skew guard
MAX_TOKEN_LIFETIME = timedelta(minutes=9, seconds=30)


class _TxWriteItem(NamedTuple):
    mode: str
    obj: Any
    condition: Optional[Any]
    atomic: bool


class Transaction:
    mode: str

    def __init__(self, engine):
        self.engine = engine
        self.objs = []
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
            objs=self.objs,
            mode=self.mode
        )
        tx.commit()
        return tx


class PreparedTransaction:
    tx_id: str
    mode: str
    objs: list
    first_commit_at: datetime

    def __init__(self):
        self.engine = None
        self.first_commit_at = None
        self._items = None

    def prepare(self, engine, objs, mode) -> None:
        self.tx_id = str(uuid.uuid4()).replace("-", "")
        self.mode = mode
        self.objs = objs
        self.engine = engine
        self.prepare_request()

    def prepare_request(self):
        # TODO
        pass

    def commit(self) -> None:
        now = datetime.now(timezone.utc)
        if self.first_commit_at is None:
            self.first_commit_at = now

        if self.mode == "r":
            response = self.engine.session.transaction_read(self._items)
        elif self.mode == "w":
            if now - self.first_commit_at > MAX_TOKEN_LIFETIME:
                raise TransactionTokenExpired
            response = self.engine.session.transaction_write(self._items, self.tx_id)
        else:
            raise ValueError(f"unrecognized mode {self.mode}")
        self._handle_response(response)

    def _handle_response(self, response: dict) -> None:
        # TODO
        pass


class ReadTransaction(Transaction):
    mode = "r"

    def load(self, *objs) -> "ReadTransaction":
        if len(self.objs) + len(objs) > MAX_TRANSACTION_ITEMS:
            raise ValueError(f"transaction cannot exceed {MAX_TRANSACTION_ITEMS} items.")
        self.objs += objs
        return self


class WriteTransaction(Transaction):
    mode = "w"

    def check(self, obj, condition) -> "WriteTransaction":
        self._extend([
            _TxWriteItem(mode="check", obj=obj, condition=condition, atomic=False)
        ])
        return self

    def save(self, *objs, condition=None, atomic=False) -> "WriteTransaction":
        self._extend([
            _TxWriteItem(mode="update", obj=obj, condition=condition, atomic=atomic)
            for obj in objs
        ])
        return self

    def delete(self, *objs, condition=None, atomic=False) -> "WriteTransaction":
        self._extend([
            _TxWriteItem(mode="delete", obj=obj, condition=condition, atomic=atomic)
            for obj in objs
        ])
        return self

    def _extend(self, items):
        if len(self.objs) + len(items) > MAX_TRANSACTION_ITEMS:
            raise ValueError(f"transaction cannot exceed {MAX_TRANSACTION_ITEMS} items.")
        self.objs += items


def new_tx(engine, mode) -> Union[ReadTransaction, WriteTransaction]:
    if mode == "r":
        cls = ReadTransaction
    elif mode == "w":
        cls = WriteTransaction
    else:
        raise ValueError(f"unknown mode {mode}")
    return cls(engine)
