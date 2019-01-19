from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from tests.helpers.models import User

from bloop.exceptions import MissingObjects, TransactionTokenExpired
from bloop.signals import object_deleted, object_loaded, object_saved
from bloop.transactions import (
    MAX_TOKEN_LIFETIME,
    MAX_TRANSACTION_ITEMS,
    PreparedTransaction,
    ReadTransaction,
    Transaction,
    TxItem,
    TxType,
    WriteTransaction,
)


class NoopTransaction(Transaction):

    def __init__(self, engine):
        super().__init__(engine)
        self.prepared = Mock(spec=PreparedTransaction)

    def prepare(self):
        return self.prepared


@pytest.fixture
def wx(engine):
    """prepared write tx with one item"""
    user = User(id="numberoverzero")
    other = User(id="other")
    items = [
        TxItem.new("save", user, condition=User.id.is_(None)),
        TxItem.new("delete", other),
        TxItem.new("check", other, condition=User.email.begins_with("foo"))
    ]
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


def test_nested_ctx(engine):
    """Transaction.__exit__ should only call commit on the outer context"""
    tx = NoopTransaction(engine)

    with tx:
        with tx:
            with tx:
                pass
    tx.prepared.commit.assert_called_once()


def test_no_commit_during_exception(engine):
    """Transaction.__exit__ shouldn't commit if the block raised an exception"""
    tx = NoopTransaction(engine)
    with pytest.raises(ZeroDivisionError):
        with tx:
            raise ZeroDivisionError
    tx.prepared.commit.assert_not_called()


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
    p = tx.prepare()

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
    p = tx.prepare()

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
    p = tx.prepare()

    expected_items = [TxItem.new("save", user, condition, True)]
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
    p = tx.prepare()

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
    calls = {"loaded": 0}

    @object_loaded.connect
    def on_loaded(*_, **__):
        calls["loaded"] += 1

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

    session.transaction_read.assert_called_once_with(rx._request)
    assert rx.items[0].obj.age == 3
    assert calls["loaded"] == 1


def test_write_commit(wx, session):
    calls = {
        "saved": 0,
        "deleted": 0
    }

    @object_saved.connect
    def on_saved(*_, **__):
        calls["saved"] += 1

    @object_deleted.connect
    def on_deleted(*_, **__):
        calls["deleted"] += 1

    now = datetime.now(timezone.utc)
    wx.commit()

    session.transaction_write.assert_called_once_with(wx._request, wx.tx_id)
    assert (wx.first_commit_at - now) <= timedelta(seconds=1)
    assert calls["saved"] == 1
    assert calls["deleted"] == 1


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
