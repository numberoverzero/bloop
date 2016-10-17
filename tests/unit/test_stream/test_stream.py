import arrow
import pytest
from bloop.models import BaseModel, Column
from bloop.exceptions import InvalidStream
from bloop.stream.stream import stream_for, Stream
from bloop.stream.coordinator import Coordinator
from bloop.types import Integer, String
from bloop.util import ordered
from unittest.mock import MagicMock
from ...helpers.models import User
from . import build_shards


@pytest.fixture
def coordinator():
    # MagicMock because we're testing __next__
    return MagicMock(spec=Coordinator)


@pytest.fixture
def stream(coordinator, engine):
    return Stream(model=Email, engine=engine, coordinator=coordinator)


class Email(BaseModel):
    class Meta:
        stream = {
            "include": {"new"},
            "arn": "stream-arn"
        }
    id = Column(Integer, hash_key=True)
    data = Column(String)


def test_no_stream_arn(engine):
    """Can't create a stream for a model that doesn't have an arn"""
    with pytest.raises(InvalidStream):
        stream_for(engine, User)


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

    stream = stream_for(engine, Email)
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
    now = arrow.now()
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
            "id": {"N": "1"}
        },
        "new": None,
        "meta": meta
    }

    record = next(stream)

    assert record["old"].id == 0
    assert record["old"].data == "some-data"

    assert record["new"] is None

    assert record["key"].id == 1
    assert not hasattr(record["key"], "data")
