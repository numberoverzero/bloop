import pytest
from bloop.stream.shard import Shard
from bloop.stream.coordinator import Coordinator


@pytest.fixture
def shard(session):
    return Shard(stream_arn="stream-arn", shard_id="shard-id", session=session)


@pytest.fixture
def coordinator(engine, session):
    return Coordinator(engine=engine, session=session, stream_arn="stream-arn")
