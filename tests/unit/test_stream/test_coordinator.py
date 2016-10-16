import functools
from bloop.stream.shard import Shard
from . import build_get_records_responses


def test_coordinator_repr(coordinator):
    coordinator.stream_arn = "repr-stream-arn"
    assert repr(coordinator) == "<Coordinator[repr-stream-arn]>"


def test_heartbeat_latest(coordinator, session):
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
