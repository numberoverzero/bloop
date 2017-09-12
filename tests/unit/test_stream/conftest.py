import pytest

from bloop.stream.coordinator import Coordinator
from bloop.stream.shard import Shard


@pytest.fixture
def stream_arn():
    return "stream-arn"


@pytest.fixture
def shard_id():
    return "shard-id"


@pytest.fixture
def shard(session, stream_arn, shard_id):
    return Shard(stream_arn=stream_arn, shard_id=shard_id, session=session)


@pytest.fixture
def coordinator(session, stream_arn):
    return Coordinator(session=session, stream_arn=stream_arn)
