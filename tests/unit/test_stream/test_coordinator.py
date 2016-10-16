import pytest
import functools
from bloop.stream.shard import Shard
from bloop.util import ordered
from . import build_get_records_responses, build_shards, local_record


def test_coordinator_repr(coordinator):
    coordinator.stream_arn = "repr-stream-arn"
    assert repr(coordinator) == "<Coordinator[repr-stream-arn]>"


def test_heartbeat(coordinator, session):
    find_records_id = "id-find-records"
    no_records_id = "id-no-records"
    has_sequence_id = "id-has-sequence"

    # When "id-finds-records" gets a response, it should only advance once and return 3 records.
    records = build_get_records_responses(3, 1)[0]

    def mock_get_records(iterator_id):
        return {
            find_records_id: records,
            no_records_id: {},
            has_sequence_id: {}
        }[iterator_id]
    session.get_stream_records.side_effect = mock_get_records

    make_shard = functools.partial(Shard, stream_arn=coordinator.stream_arn, shard_id="shard-id", session=session)
    coordinator.active.extend([
        # Has a sequence number, should not be called during a heartbeat
        make_shard(iterator_id=has_sequence_id, iterator_type="at_sequence", sequence_number="sequence-number"),
        # No sequence number, should find records during a heartbeat
        make_shard(iterator_id=find_records_id, iterator_type="trim_horizon"),
        # No sequence number, should not find records during a heartbeat
        make_shard(iterator_id=no_records_id, iterator_type="latest"),
    ])

    coordinator.heartbeat()

    assert session.get_stream_records.call_count == 2
    session.get_stream_records.assert_any_call(find_records_id)
    session.get_stream_records.assert_any_call(no_records_id)

    assert len(coordinator.buffer) == 3
    pairs = [coordinator.buffer.pop() for _ in range(len(coordinator.buffer))]
    sequence_numbers = [record["meta"]["sequence_number"] for (record, _) in pairs]
    assert sequence_numbers == [0, 1, 2]


def test_heartbeat_until_sequence_number(coordinator, session):
    """After heartbeat() finds records for a shard, the shard doens't check during the next heartbeat."""
    shard = Shard(stream_arn=coordinator.stream_arn, shard_id="shard-id", session=session,
                  iterator_id="iterator-id", iterator_type="latest")
    coordinator.active.append(shard)

    session.get_stream_records.side_effect = build_get_records_responses(1)

    # First call fetches records from DynamoDB
    coordinator.heartbeat()
    assert coordinator.buffer
    assert shard.sequence_number is not None
    session.get_stream_records.assert_called_once_with("iterator-id")

    # Second call ships the shard, since it now has a sequence_number.
    coordinator.heartbeat()
    assert session.get_stream_records.call_count == 1


def test_token(coordinator):
    coordinator.stream_arn = "token-arn"
    # Two roots, each with 3 descendants.
    shards = build_shards(8, {0: 2, 1: [3, 4], 2: [5, 6], 3: 7}, session=coordinator.session, stream_arn="token-arn")

    # First two are roots to the rest of the trees
    coordinator.roots = [shards[0], shards[1]]
    # Only the leaves are active
    coordinator.active = [shards[4], shards[5], shards[6], shards[7]]

    expected_token = {
        "stream_arn": "token-arn",
        "active": [shard.shard_id for shard in coordinator.active],
        "shards": [shard.token for shard in shards]
    }
    # stream_arn is the same for all shards, so it's not stored per-shard.
    for shard_token in expected_token["shards"]:
        del shard_token["stream_arn"]

    assert ordered(expected_token) == ordered(coordinator.token)


@pytest.mark.parametrize("is_active", [True, False])
@pytest.mark.parametrize("is_root", [True, False])
@pytest.mark.parametrize("has_buffered", [True, False])
def test_remove_shard(is_active, is_root, has_buffered, coordinator):
    shard = Shard(stream_arn=coordinator.stream_arn, shard_id="shard-id",
                  iterator_type="at_sequence", sequence_number="sequence-number")
    # Always has a buffered record
    other = Shard(stream_arn=coordinator.stream_arn, shard_id="other-shard-id",
                  iterator_type="after_sequence", sequence_number="other-sequence-number")
    children = [Shard(stream_arn="child-arn", shard_id="child-" + str(i)) for i in range(4)]
    shard.children.extend(children)

    if is_active:
        coordinator.active.append(shard)
    if is_root:
        coordinator.roots.append(shard)
    if has_buffered:
        records = [local_record(sequence_number=i) for i in range(7)]
        coordinator.buffer.push_all((r, shard) for r in records)
    coordinator.buffer.push(local_record(sequence_number="other-record"), other)

    coordinator.remove_shard(shard)

    if is_active:
        assert all(child in coordinator.active for child in children)
    if is_root:
        assert all(child in coordinator.roots for child in children)

    # Any records that were buffered from the removed shard are gone.
    while coordinator.buffer:
        record, record_shard = coordinator.buffer.pop()
        assert record_shard is not shard
