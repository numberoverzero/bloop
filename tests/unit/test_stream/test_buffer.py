import pytest
import arrow
from unittest.mock import Mock
from bloop.stream.buffer import RecordBuffer, heap_item
from bloop.stream.shard import Shard


def new_clock():
    x = 0

    def call():
        nonlocal x
        x += 1
        return x
    return call


def new_record(created_at, sequence_number):
    return {
        "meta": {
            "created_at": created_at,
            "sequence_number": sequence_number
        }
    }


def new_shard() -> Shard:
    return Mock(spec=Shard)


@pytest.mark.parametrize("created_at", [None, arrow.now()])
@pytest.mark.parametrize("sequence_number", [None, 2])
def test_heap_item_clock(created_at, sequence_number):
    """heap_item guarantees total ordering, even for identical items."""
    shard = new_shard()
    clock = new_clock()

    record = new_record(created_at, sequence_number)

    first_item = heap_item(clock, record, shard)
    second_item = heap_item(clock, record, shard)

    assert first_item < second_item
    # Same payload - heap_item returns tuple of (ordering, payload)
    assert first_item[1] == second_item[1]

    # Called twice before this
    assert clock() == 3


@pytest.mark.parametrize("created_at", [None, arrow.now()])
@pytest.mark.parametrize("sequence_number", [None, 2])
def test_heap_item_broken_clock(created_at, sequence_number):
    """When the clock can return the same value, total ordering is lost."""
    shard = new_shard()
    broken_clock = lambda: 4

    record = new_record(created_at, sequence_number)

    first_item = heap_item(broken_clock, record, shard)
    second_item = heap_item(broken_clock, record, shard)
    assert first_item == second_item


def test_empty_buffer():
    """Trying to access an empty buffer raises IndexError"""
    buffer = RecordBuffer()

    assert not buffer
    with pytest.raises(IndexError):
        buffer.pop()
    with pytest.raises(IndexError):
        buffer.peek()


def test_single_record():
    """Push a record, peek at it, then get the same thing back"""
    record = new_record(arrow.now(), 1)
    shard = new_shard()
    buffer = RecordBuffer()

    buffer.push(record, shard)
    assert buffer

    same_record, same_shard = buffer.peek()

    also_same_record, also_same_shard = buffer.pop()
    assert not buffer

    assert record is same_record is also_same_record
    assert shard is same_shard is also_same_shard


def test_sort_every_push():
    """Push high to low, retrieve low to high"""
    now = arrow.now()
    records = [new_record(now, i) for i in reversed(range(5))]
    shard = new_shard()
    buffer = RecordBuffer()

    for record in records:
        buffer.push(record, shard)
        # inserting high to low, every record should be at the front
        assert buffer.peek()[0] is record

    same_records = [
        buffer.pop()[0]
        for _ in range(len(records))
    ]
    same_records.reverse()
    assert records == same_records


def test_push_all():
    """Bulk push is slightly more efficient"""
    now = arrow.now()
    records = [new_record(now, i) for i in reversed(range(5))]
    shard = new_shard()
    buffer = RecordBuffer()

    pairs = [(record, shard) for record in records]
    buffer.push_all(pairs)

    same_records = [
        buffer.pop()[0]
        for _ in range(len(records))
    ]
    same_records.reverse()
    assert records == same_records


def test_clear():
    record = new_record(arrow.now(), 1)
    shard = new_shard()
    buffer = RecordBuffer()

    buffer.push(record, shard)
    assert buffer

    buffer.clear()
    assert not buffer


def test_buffer_heap():
    """RecordBuffer directly exposes its heap"""
    record = new_record(arrow.now(), 1)
    shard = new_shard()
    buffer = RecordBuffer()

    buffer.push(record, shard)

    #  [(sort, (record, shard)]
    # [0]     [1]      [1]
    assert buffer.heap[0][1][1] is shard
