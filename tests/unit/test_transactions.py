from unittest.mock import Mock

import pytest

from bloop.transactions import (
    MAX_TRANSACTION_ITEMS,
    PreparedTransaction,
    ReadTransaction,
    Transaction,
    TxItem,
    TxType,
    WriteTransaction,
    new_tx,
)


class NoopTransaction(Transaction):
    committed = False

    def __init__(self, engine):
        super().__init__(engine)
        self.prepared = Mock(spec=PreparedTransaction)

    def commit(self):
        super().commit()
        self.committed = True

    def _prepare(self):
        return self.prepared


@pytest.mark.parametrize("type, expected", [
    (TxType.Get, False),
    (TxType.Check, False),
    (TxType.Delete, False),
    (TxType.Update, True),
])
def test_item_is_update(type, expected):
    item = TxItem(type=type, obj=None, condition=None, atomic=False)
    assert item.is_update is expected


@pytest.mark.parametrize("type, expected", [
    (TxType.Get, False),
    (TxType.Check, False),
    (TxType.Delete, True),
    (TxType.Update, True),
])
def test_item_should_render_obj(type, expected):
    item = TxItem(type=type, obj=None, condition=None, atomic=False)
    assert item.should_render_obj is expected


def test_new_tx_unknown(engine):
    with pytest.raises(ValueError):
        new_tx(engine, "unknown")


@pytest.mark.parametrize("mode, cls", [
    ("r", ReadTransaction),
    ("w", WriteTransaction),
])
def test_new_tx(mode, cls, engine):
    tx = new_tx(engine, mode)
    assert isinstance(tx, cls)


def test_tx_nested_ctx(engine):
    """Transaction.__exit__ should only call commit on the outer context"""
    tx = NoopTransaction(engine)

    with tx:
        with tx:
            with tx:
                pass
            assert not tx.committed
        assert not tx.committed
    assert tx.committed
    tx.prepared.commit.assert_called_once()


def test_tx_commit_in_ctx(engine):
    """Calling transaction.commit() within a context manager raises"""
    tx = NoopTransaction(engine)
    # This test setup is a little weird. We want to see tx.commit raise an exception and assert that the commit
    # didn't happen, but we *also* want to see that Transaction.__exit__ doesn't commit when an exception is
    # bubbling up. The outer pytest.raises verified __exit__ while a manual try/except lets us assert
    # on Transaction.commit() and re-raise so the __exit__ won't commit.
    with pytest.raises(RuntimeError):
        with tx:
            try:
                tx.commit()
            except RuntimeError:
                assert not tx.committed
                raise
        assert not tx.committed


def test_tx_extend(engine):
    """Each Transaction can hold transactions.MAX_TRANSACTION_ITEMS items"""
    tx = Transaction(engine)
    for _ in range(MAX_TRANSACTION_ITEMS):
        tx._extend([object()])
    with pytest.raises(RuntimeError):
        tx._extend([object()])
