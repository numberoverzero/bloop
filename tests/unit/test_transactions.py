from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from bloop.exceptions import MissingObjects, TransactionTokenExpired
from bloop.transactions import (
    MAX_TOKEN_LIFETIME,
    MAX_TRANSACTION_ITEMS,
    PreparedTransaction,
    ReadTransaction,
    Transaction,
    TxItem,
    TxType,
    WriteTransaction,
    new_tx,
)

from tests.helpers.models import User


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


@pytest.fixture
def wx(engine):
    """prepared write tx with one item"""
    user = User(id="numberoverzero")
    items = [TxItem.new("update", user, condition=User.id.is_(None))]
    tx = PreparedTransaction()
    tx.prepare(engine, "w", items)
    return tx


@pytest.fixture
def rx(engine):
    """prepared read tx with one item"""
    user = User(id="numberoverzero")
    items = [TxItem.new("get", user)]
    tx = PreparedTransaction()
    tx.prepare(engine, "r", items)
    return tx


@pytest.mark.parametrize("type, expected", [
    (TxType.Get, False),
    (TxType.Check, False),
    (TxType.Delete, False),
    (TxType.Update, True),
])
def test_is_update(type, expected):
    item = TxItem(type=type, obj=None, condition=None, atomic=False)
    assert item.is_update is expected


@pytest.mark.parametrize("type, expected", [
    (TxType.Get, False),
    (TxType.Check, False),
    (TxType.Delete, True),
    (TxType.Update, True),
])
def test_should_render_obj(type, expected):
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
    assert tx.mode == mode


def test_nested_ctx(engine):
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


def test_commit_in_ctx(engine):
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


def test_extend(engine):
    """Each Transaction can hold transactions.MAX_TRANSACTION_ITEMS items"""
    tx = Transaction(engine)
    for _ in range(MAX_TRANSACTION_ITEMS):
        tx._extend([object()])
    with pytest.raises(RuntimeError):
        tx._extend([object()])


def test_read_item(engine):
    engine.bind(User)
    user = User(id="numberoverzero")
    tx = ReadTransaction(engine)

    tx.load(user)
    p = tx._prepare()

    expected_items = [TxItem.new("get", user, None, False)]
    assert tx._items == expected_items
    assert p.items == expected_items
    assert p.first_commit_at is None


def test_check_complex_item(engine):
    engine.bind(User)
    user = User(id="numberoverzero")
    tx = WriteTransaction(engine)

    condition = User.id.begins_with("foo")
    tx.check(user, condition=condition)
    p = tx._prepare()

    expected_items = [TxItem.new("check", user, condition, False)]
    assert tx._items == expected_items
    assert p.items == expected_items
    assert p.first_commit_at is None
    assert len(p._request) == 1
    entry = p._request[0]["CheckCondition"]
    expected_fields = {
        "Key", "TableName",
        "ConditionExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues"
    }
    assert set(entry.keys()) == expected_fields


def test_save_complex_item(engine):
    engine.bind(User)
    user = User(id="numberoverzero")
    tx = WriteTransaction(engine)

    condition = User.id.begins_with("foo")
    tx.save(user, condition=condition, atomic=True)
    p = tx._prepare()

    expected_items = [TxItem.new("update", user, condition, True)]
    assert tx._items == expected_items
    assert p.items == expected_items
    assert p.first_commit_at is None
    assert len(p._request) == 1
    entry = p._request[0]["Update"]
    expected_fields = {
        "Key", "TableName",
        "ConditionExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues"
    }
    assert set(entry.keys()) == expected_fields


def test_delete_complex_item(engine):
    engine.bind(User)
    user = User(id="numberoverzero")
    tx = WriteTransaction(engine)

    condition = User.id.begins_with("foo")
    tx.delete(user, condition=condition, atomic=True)
    p = tx._prepare()

    expected_items = [TxItem.new("delete", user, condition, True)]
    assert tx._items == expected_items
    assert p.items == expected_items
    assert p.first_commit_at is None
    assert len(p._request) == 1
    entry = p._request[0]["Delete"]
    expected_fields = {
        "Key", "TableName",
        "ConditionExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues"
    }
    assert set(entry.keys()) == expected_fields


def test_commit_bad_mode(rx):
    rx.mode = "j"
    with pytest.raises(ValueError):
        rx.commit()


def test_write_commit_expired(wx, session):
    now = datetime.now(timezone.utc)
    offset = MAX_TOKEN_LIFETIME + timedelta(seconds=1)
    wx.first_commit_at = now - offset

    with pytest.raises(TransactionTokenExpired):
        wx.commit()

    session.transaction_write.assert_not_called()


def test_read_commit(rx, session):
    """read commits don't expire"""
    session.transaction_read.return_value = {
        "Responses": [
            {
                "Item": {
                    "id": {"S": "numberoverzero"},
                    "age": {"N": "3"}
                }
            }
        ]
    }

    now = datetime.now(timezone.utc)
    offset = MAX_TOKEN_LIFETIME + timedelta(seconds=1)
    rx.first_commit_at = now - offset
    rx.commit()

    assert rx.items[0].obj.age == 3
    session.transaction_read.assert_called_once_with(rx._request)


def test_write_commit(wx, session):
    now = datetime.now(timezone.utc)
    wx.commit()
    assert (wx.first_commit_at - now) <= timedelta(seconds=1)
    session.transaction_write.assert_called_once_with(wx._request, wx.tx_id)


def test_malformed_read_response(rx, session):
    session.transaction_read.return_value = {"Responses": []}
    with pytest.raises(RuntimeError):
        rx.commit()


def test_read_missing_object(rx, session):
    session.transaction_read.return_value = {"Responses": [{}]}
    with pytest.raises(MissingObjects) as excinfo:
        rx.commit()

    obj = rx.items[0].obj
    assert excinfo.value.objects == [obj]
