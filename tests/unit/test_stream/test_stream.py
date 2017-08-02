import datetime
from unittest.mock import MagicMock

import pytest

from bloop.models import BaseModel, Column
from bloop.stream.coordinator import Coordinator
from bloop.stream.stream import Stream
from bloop.types import Integer, String
from bloop.util import ordered

from . import build_shards


@pytest.fixture
def coordinator():
    # MagicMock because we're testing __next__
    return MagicMock(spec=Coordinator)


@pytest.fixture
def stream(coordinator, engine):
    stream = Stream(model=Email, engine=engine)
    stream.coordinator = coordinator
    return stream


class Email(BaseModel):
    class Meta:
        stream = {
            "include": {"new", "old"},
            "arn": "stream-arn"
        }
    id = Column(Integer, hash_key=True)
    data = Column(String)


def test_repr(stream):
    assert repr(stream) == "<Stream[Email]>"


def test_iter(stream):
    """stream is both an Iterable and an Iterator"""
    assert iter(stream) is stream


def test_token(engine):
    engine.bind(Email)
    shards = build_shards(3, {0: [1, 2]}, stream_arn=Email.Meta.stream["arn"])
    shards[1].iterator_type = "latest"
    shards[2].iterator_type = "at_sequence"
    shards[2].sequence_number = "sequence-number"

    stream = Stream(model=Email, engine=engine)
    stream.coordinator.roots.append(shards[0])
    stream.coordinator.active.extend(shards[1:])

    assert ordered(stream.token) == ordered({
        "stream_arn": "stream-arn",
        "active": ["shard-id-1", "shard-id-2"],
        "shards": [
            {"shard_id": "shard-id-0"},
            {"shard_id": "shard-id-1", "parent": "shard-id-0", "iterator_type": "latest"},
            {"shard_id": "shard-id-2", "parent": "shard-id-0",
             "iterator_type": "at_sequence", "sequence_number": "sequence-number"},
        ]
    })


def test_heartbeat(stream, coordinator):
    stream.heartbeat()
    coordinator.heartbeat.assert_called_once_with()


def test_move_to(stream, coordinator):
    stream.move_to("latest")
    coordinator.move_to.assert_called_once_with("latest")


def test_next_no_record(stream, coordinator):
    coordinator.__next__.return_value = None
    # Explicit marker so we don't get next's default value
    missing = object()

    record = next(stream, missing)
    assert record is None


def test_next_unpacks(stream, coordinator):
    now = datetime.datetime.now(datetime.timezone.utc)
    meta = {
        "created_at": now,
        "sequence_number": "sequence-number",
        "event": {
            "id": "event-id",
            "type": "event-type",
            "version": "event-version"
        }
    }
    coordinator.__next__.return_value = {
        # Impossible to have old and key, but for the sake of testing
        # an object that's partially/fully loaded
        "old": {
            "id": {"N": "0"},
            "data": {"S": "some-data"}
        },
        "key": {
            # Omitted because the model only includes "new"
            "id": {"N": "343"}
        },
        "new": None,
        "meta": meta
    }

    record = next(stream)

    assert record["old"].id == 0
    assert record["old"].data == "some-data"

    assert record["new"] is None

    assert record["key"] is None
    assert not hasattr(record["key"], "data")
