import datetime
from typing import Union


__all__ = ["Transaction", "ReadTransaction", "WriteTransaction", "new_tx"]


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
    first_commit_at: datetime.datetime
    request: dict

    def prepare(self, engine, objs, mode) -> None:
        pass
        # TODO

    def commit(self) -> None:
        pass
        # TODO

    def _handle_response(self, resp: dict) -> None:
        pass
        # TODO


class ReadTransaction(Transaction):
    mode = "r"

    def load(self, *objs) -> "ReadTransaction":
        # TODO
        return self


class WriteTransaction(Transaction):
    mode = "w"

    def check(self, obj, condition) -> "WriteTransaction":
        # TODO
        return self

    def save(self, *objs, condition=None, atomic=False) -> "WriteTransaction":
        # TODO
        return self

    def delete(self, *objs, condition=None, atomic=False) -> "WriteTransaction":
        # TODO
        return self


def new_tx(engine, mode) -> Union[ReadTransaction, WriteTransaction]:
    if mode == "r":
        cls = ReadTransaction
    elif mode == "w":
        cls = WriteTransaction
    else:
        raise ValueError(f"unknown mode {mode}")
    return cls(engine)
