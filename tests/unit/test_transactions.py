import pytest

from bloop.transactions import (
    ReadTransaction,
    TxItem,
    TxType,
    WriteTransaction,
    new_tx,
)


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
