import collections
import datetime
import functools
import logging
from unittest.mock import call

import pytest

from bloop.exceptions import InvalidPosition, InvalidStream, RecordsExpired
from bloop.stream.shard import CALLS_TO_REACH_HEAD, Shard, last_iterator
from bloop.util import ordered

from . import (
    build_get_records_responses,
    build_shards,
    dynamodb_record_with,
    local_record,
    stream_description,
)


def test_iter(coordinator):
    assert iter(coordinator) is coordinator


def test_coordinator_repr(coordinator):
    coordinator.stream_arn = "repr-stream-arn"
    assert repr(coordinator) == "<Coordinator[repr-stream-arn]>"


def test_next_no_records_or_shards(coordinator):
    """next(coordinator) returns None when the buffer is empty and active shards find nothing"""
    result = next(coordinator)
    assert result is None


def test_next_from_buffer(coordinator, shard, session):
    """next(coordinator) doesn't call GetRecords when the buffer has records"""
    shard.iterator_type = "latest"
    shard.sequence_number = None
    coordinator.active.append(shard)

    record = local_record(sequence_number="25")
    coordinator.buffer.push(record, shard)

    returned_record = next(coordinator)

    assert returned_record is record
    assert shard.iterator_type == "after_sequence"
    assert shard.sequence_number == "25"
    assert not coordinator.buffer
    # No outbound calls
    session.get_stream_records.assert_not_called()
    session.get_shard_iterator.assert_not_called()
    session.describe_stream.assert_not_called()

    assert shard in coordinator.active


def test_next_advances_all_shards(coordinator, session):
    """next(coordinator) advances all active shards, and removes exhausted shards"""
    [has_records, will_expire] = build_shards(2, session=session, stream_arn=coordinator.stream_arn)
    coordinator.active = [has_records, will_expire]
    has_records.iterator_id = "has-records-id"
    will_expire.iterator_id = "will-expire"

    def mock_get_stream_records(iterator_id):
        if iterator_id == "will-expire":
            return {"Records": [], "NextShardIterator": last_iterator}
        return {
            "Records": [dynamodb_record_with(key=True)],
            "NextShardIterator": "next-iterator-id"}
    session.get_stream_records.side_effect = mock_get_stream_records
    # Don't find children for will_expire
    session.describe_stream.return_value = {"StreamArn": coordinator.stream_arn, "Shards": []}

    result = next(coordinator)

    assert result
    assert has_records in coordinator.active
    assert will_expire not in coordinator.active


def test_advance_shards_with_buffer(coordinator, shard, session):
    """The coordinator always drains the buffer before pulling from active shards"""
    coordinator.active.append(shard)
    record = local_record()
    coordinator.buffer.push(record, shard)

    coordinator.advance_shards()
    session.get_stream_records.assert_not_called()


def test_advance_pulls_from_all_active_shards(coordinator, session):
    """The coordinator checks all active shards, and doesn't stop on the first active one."""
    [has_records, no_records] = build_shards(2, session=session, stream_arn=coordinator.stream_arn)
    has_records.iterator_id = "has-records-id"
    no_records.iterator_id = "no-records-id"
    coordinator.active = [has_records, no_records]

    def mock_get_stream_records(iterator_id):
        response = {
            "Records": [dynamodb_record_with(key=True)],
            "NextShardIterator": "next-iterator-id"
        }
        if iterator_id != "has-records-id":
            response["Records"].clear()
        return response
    session.get_stream_records.side_effect = mock_get_stream_records

    assert not coordinator.buffer
    coordinator.advance_shards()

    assert coordinator.buffer
    # 1 from has-records-id, and CALLS_TO_REACH_HEAD from no-records-id since it contains no records
    assert session.get_stream_records.call_count == 1 + CALLS_TO_REACH_HEAD
    session.get_stream_records.assert_any_call("has-records-id")
    session.get_stream_records.assert_any_call("no-records-id")
    session.get_stream_records.assert_any_call("next-iterator-id")

    assert [has_records, no_records] == coordinator.active


def test_buffer_closed_records(coordinator, session):
    """
    When a shard is closed, the last set of records is still buffered even though the shard is no longer tracked.
    https://github.com/numberoverzero/bloop/issues/111
    """
    closed_shard = Shard(
        stream_arn=coordinator.stream_arn,
        shard_id="closed-shard-id",
        iterator_id="closed-iter-id",
        session=session)
    coordinator.active = [closed_shard]

    session.get_stream_records.return_value = {
        "Records": [
            dynamodb_record_with(sequence_number=123, key=True),
            dynamodb_record_with(sequence_number=456, key=True),
            dynamodb_record_with(sequence_number=789, key=True)
        ]
        # last records so no NextShardIterator
    }

    # called when the coordinator
    session.describe_stream.return_value = {
        "Shards": [],
        "StreamArn": coordinator.stream_arn
    }

    assert not coordinator.closed

    record = next(coordinator)
    assert not coordinator.active
    assert record["meta"]["sequence_number"] == "123"
    assert len(coordinator.buffer) == coordinator.closed[closed_shard] == 2

    record = next(coordinator)
    assert record["meta"]["sequence_number"] == "456"
    assert len(coordinator.buffer) == coordinator.closed[closed_shard] == 1

    record = next(coordinator)
    assert record["meta"]["sequence_number"] == "789"
    assert not coordinator.buffer
    assert not coordinator.closed


@pytest.mark.parametrize("has_children, loads_children", [(True, False), (False, False), (False, True)])
def test_advance_removes_exhausted(has_children, loads_children, coordinator, shard, session):
    """Exhausted shards are removed; any children are promoted, and reset to trim_horizon"""
    shard.iterator_id = last_iterator
    shard.iterator_type = "latest"

    coordinator.active.append(shard)

    if has_children:
        # Already loaded, doesn't need to call DescribeStream
        child = Shard(
            stream_arn=coordinator.stream_arn, shard_id="child-id", parent=shard,
            iterator_type="at_sequence", sequence_number="sequence-number",
            session=session)
        shard.children.append(child)
    elif loads_children:
        # Child exists, but isn't known locally
        session.describe_stream.return_value = {
            "Shards": [{
                "SequenceNumberRange": {
                    "EndingSequenceNumber": "820400000000000001192334",
                    "StartingSequenceNumber": "820400000000000001192334"
                },
                "ShardId": "child-id",
                "ParentShardId": "shard-id"
            }],
            "StreamArn": coordinator.stream_arn
        }
    else:
        # No children
        session.describe_stream.return_value = {
            "Shards": [],
            "StreamArn": coordinator.stream_arn
        }

    coordinator.advance_shards()

    # No records found
    assert not coordinator.buffer
    # No longer active
    assert shard not in coordinator.active

    if has_children:
        # Children are already loaded, no need to DescribeStream
        session.describe_stream.assert_not_called()
    else:
        # No children locally, DescribeStream tried to find some
        session.describe_stream.assert_called_once_with(
            stream_arn=coordinator.stream_arn,
            first_shard=shard.shard_id)

    # Children (pre-existing or found in DescribeStream) are active
    if has_children or loads_children:
        assert len(coordinator.active) == 1
        assert coordinator.active[0].parent is shard

        # Part of promoting the child is resetting it to trim_horizon
        session.get_shard_iterator.assert_called_once_with(
            stream_arn=coordinator.stream_arn,
            shard_id="child-id",
            iterator_type="trim_horizon",
            sequence_number=None
        )
    # Without a child, there's no need to get a new iterator
    else:
        session.get_shard_iterator.assert_not_called()


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
    # None of the shards have children
    session.describe_stream.return_value = {"StreamArn": coordinator.stream_arn, "Shards": []}

    make_shard = functools.partial(Shard, stream_arn=coordinator.stream_arn, shard_id="shard-id", session=session)
    coordinator.active = [
        # Has a sequence number, should not be called during a heartbeat
        make_shard(iterator_id=has_sequence_id, iterator_type="at_sequence", sequence_number="sequence-number"),
        # No sequence number, should find records during a heartbeat
        make_shard(iterator_id=find_records_id, iterator_type="trim_horizon"),
        # No sequence number, should not find records during a heartbeat
        make_shard(iterator_id=no_records_id, iterator_type="latest"),
    ]

    coordinator.heartbeat()

    assert session.get_stream_records.call_count == 2
    session.get_stream_records.assert_any_call(find_records_id)
    session.get_stream_records.assert_any_call(no_records_id)

    assert len(coordinator.buffer) == 3
    pairs = [coordinator.buffer.pop() for _ in range(len(coordinator.buffer))]
    sequence_numbers = [record["meta"]["sequence_number"] for (record, _) in pairs]
    assert sequence_numbers == ["0", "1", "2"]


def test_heartbeat_until_sequence_number(coordinator, session):
    """After heartbeat() finds records for a shard, the shard doesn't check during the next heartbeat."""
    shard = Shard(stream_arn=coordinator.stream_arn, shard_id="shard-id", session=session,
                  iterator_id="iterator-id", iterator_type="latest")
    coordinator.active.append(shard)

    session.get_stream_records.side_effect = build_get_records_responses(1, 0)

    # First call fetches records from DynamoDB
    coordinator.heartbeat()
    assert coordinator.buffer
    assert shard.sequence_number is not None
    session.get_stream_records.assert_called_once_with("iterator-id")

    # Second call skips the shard, since it now has a sequence_number.
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


def test_token_closed_records(coordinator, session):
    """
    When a shard is closed, the last set of records is still buffered even though the shard is no longer tracked.
    The token must include the closed shard until its buffered records are consumed.

    https://github.com/numberoverzero/bloop/issues/111
    """
    closed_shard = Shard(
        stream_arn=coordinator.stream_arn,
        shard_id="closed-shard-id",
        iterator_id="closed-iter-id",
        session=session)
    coordinator.active = [closed_shard]

    session.get_stream_records.return_value = {
        "Records": [
            dynamodb_record_with(sequence_number=123, key=True),
            dynamodb_record_with(sequence_number=456, key=True),
            dynamodb_record_with(sequence_number=789, key=True)
        ]
        # last records so no NextShardIterator
    }

    # called when the coordinator
    session.describe_stream.return_value = {
        "Shards": [],
        "StreamArn": coordinator.stream_arn
    }

    initial_token = coordinator.token
    assert initial_token == {
        "stream_arn": "stream-arn",
        "active": ["closed-shard-id"],
        "shards": []
    }

    record = next(coordinator)
    assert record["meta"]["sequence_number"] == "123"
    assert coordinator.closed[closed_shard] == len(coordinator.buffer) == 2

    # the token should still include the shard in "active", and the "shards"
    # list should contain a pointer to the sequence number 123
    token = coordinator.token
    assert token == {
        "stream_arn": "stream-arn",
        "active": ["closed-shard-id"],
        "shards": [{
            "iterator_type": "after_sequence",
            "sequence_number": "123",
            "shard_id": "closed-shard-id",
        }]
    }


@pytest.mark.parametrize("is_active", [True, False])
@pytest.mark.parametrize("is_root", [True, False])
@pytest.mark.parametrize("has_buffered", [True, False])
def test_remove_shard(is_active, is_root, has_buffered, coordinator):
    shard = Shard(stream_arn=coordinator.stream_arn, shard_id="shard-id",
                  iterator_type="at_sequence", sequence_number="13")
    # Always has a buffered record
    other = Shard(stream_arn=coordinator.stream_arn, shard_id="other-shard-id",
                  iterator_type="after_sequence", sequence_number="200")
    children = [Shard(stream_arn="child-arn", shard_id="child-" + str(i)) for i in range(4)]
    shard.children.extend(children)

    if is_active:
        coordinator.active.append(shard)
    if is_root:
        coordinator.roots.append(shard)
    if has_buffered:
        records = [local_record(sequence_number=str(i)) for i in range(7)]
        coordinator.buffer.push_all((r, shard) for r in records)
    coordinator.buffer.push(local_record(sequence_number="200"), other)

    coordinator.remove_shard(shard, drop_buffered_records=True)

    if is_active:
        assert all(child in coordinator.active for child in children)
    if is_root:
        assert all(child in coordinator.roots for child in children)

    # Any records that were buffered from the removed shard are gone.
    while coordinator.buffer:
        record, record_shard = coordinator.buffer.pop()
        assert record_shard is not shard


def test_move_to_old_token(coordinator, shard, session, caplog):
    """Can't rebuild from a token with shards that have no connection to the current generation"""
    root = Shard(stream_arn=coordinator.stream_arn, shard_id="parent-shard")
    shard.parent = root
    root.children.append(shard)
    coordinator.active.append(shard)
    coordinator.roots.append(root)
    token = coordinator.token

    # There is no lineage that connects the shard_id from the token to the shards in the stream description.
    session.describe_stream.return_value = stream_description(1)

    with pytest.raises(InvalidStream):
        coordinator.move_to(token)

    assert caplog.record_tuples == [
        ("bloop.stream", logging.INFO, "Unknown or expired shard \"parent-shard\" - pruning from stream token"),
        ("bloop.stream", logging.INFO, "Unknown or expired shard \"shard-id\" - pruning from stream token"),
    ]


def test_move_to_valid_token(coordinator, session, caplog):
    """Moving to a token validates and clears unknown shards, and gets iterators for active shards"""
    # +--------------+
    # | <TOKEN>      |       # 0: token root no longer exists; was active
    # |   __0__      |       # 1: branch no longer exists
    # |  / +---\-----|----+  # 2: first shard in token that exists
    # | 1  |    2    |    |  #    new Coordinator root
    # |    |     3   |    |  # 3: last shard the token knows exists
    # +----|----/-\--+    |  #
    #      |   4   5      |  # 4: unknown descendant of token
    #      |    <CURRENT> |  # 5: unknown descendant of token
    #      +--------------+
    token_shards = build_shards(4, {0: [1, 2], 2: 3}, stream_arn=coordinator.stream_arn)
    token_root = token_shards[0]
    coordinator.roots = coordinator.active = [token_root]
    token_root.iterator_type = "at_sequence"
    token_root.sequence_number = "root-sequence-number"
    token = coordinator.token

    # Easier to build the whole shard and then pop the token-only shards
    description = stream_description(6, {0: [1, 2], 2: 3, 3: [4, 5]}, stream_arn=coordinator.stream_arn)
    description["Shards"][2].pop("ParentShardId")
    description["Shards"] = description["Shards"][2:]

    session.describe_stream.return_value = description
    session.get_shard_iterator.return_value = "current-root-iterator"

    coordinator.move_to(token)

    session.describe_stream.assert_called_once_with(stream_arn=coordinator.stream_arn)
    session.get_shard_iterator.assert_called_once_with(
        stream_arn=coordinator.stream_arn,
        shard_id="shard-id-2",
        iterator_type="trim_horizon",
        sequence_number=None
    )


def test_move_to_token_with_old_sequence_number(coordinator, session, caplog):
    """If a token shard's sequence_number is past the trim_horizon, it moves to trim_horizon."""
    description = stream_description(1)
    stream_arn = coordinator.stream_arn
    shard_id = description["Shards"][0]["ShardId"]
    token = {
        "stream_arn": stream_arn,
        "active": [shard_id],
        "shards": [{"shard_id": shard_id,
                    "sequence_number": "beyond-trim-horizon",
                    "iterator_type": "at_sequence"}]
    }

    def mock_get_shard_iterator(sequence_number, **kwargs):
        if sequence_number == "beyond-trim-horizon":
            raise RecordsExpired
        else:
            return "at-trim-horizon"
    session.get_shard_iterator.side_effect = mock_get_shard_iterator
    session.describe_stream.return_value = description

    coordinator.move_to(token)

    session.describe_stream.assert_called_once_with(stream_arn=stream_arn)
    session.get_shard_iterator.assert_has_calls([
        call(stream_arn=stream_arn, shard_id=shard_id,
             sequence_number="beyond-trim-horizon", iterator_type="at_sequence"),
        call(stream_arn=stream_arn, shard_id=shard_id,
             sequence_number=None, iterator_type="trim_horizon")
    ])

    assert caplog.record_tuples == [
        ("bloop.stream", logging.INFO,
         "SequenceNumber \"beyond-trim-horizon\" in shard \"shard-id-0\" beyond trim horizon: jumping to trim_horizon")
    ]


def test_move_to_future_time(coordinator, session):
    """Moving to a time in the future simply moves to Stream latest."""
    # -----------
    # 0 -> 1 -> 2
    #        -> 3
    # -----------
    # 4 -> 5
    #   -> 6 -> 7
    # -----------
    session.describe_stream.return_value = stream_description(
        8, {0: 1, 1: [2, 3], 4: [5, 6], 6: 7},
        stream_arn=coordinator.stream_arn)
    expected_active_ids = ["shard-id-{}".format(i) for i in [2, 3, 5, 7]]
    session.get_shard_iterator.return_value = "child-iterator-id"

    coordinator.move_to(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1))

    # Remote calls - no new records fetched, loaded child shards and jumped to their latest
    session.get_stream_records.assert_not_called()
    session.describe_stream.assert_called_once_with(stream_arn=coordinator.stream_arn)
    assert session.get_shard_iterator.call_count == len(expected_active_ids)
    for expected_shard_id in expected_active_ids:
        session.get_shard_iterator.assert_any_call(
            stream_arn=coordinator.stream_arn,
            shard_id=expected_shard_id,
            iterator_type="latest",
            sequence_number=None
        )

    assert set(s.shard_id for s in coordinator.active) == set(expected_active_ids)
    assert not coordinator.buffer


def test_move_to_datetime(coordinator, session):
    """Move to a time somewhere in the middle of a stream"""
    position = datetime.datetime.now(datetime.timezone.utc)

    # 0 -> 1
    #   -> 2 -> 3
    #           ^ Never searched for a record, since record is found in 2
    description = stream_description(4, {0: [1, 2], 2: 3}, stream_arn=coordinator.stream_arn)
    # No shards in the root, and it's exhausted
    root_id = "shard-id-0"
    # No shards in one child, but it's still open
    fail_to_seek = "shard-id-1"
    # Shards in the other child, and it's still open
    find_records = "shard-id-2"

    # Hand back the same iterator id for fail_to_seek to simplify responses table
    continue_response = {"Records": [], "NextShardIterator": fail_to_seek}
    record = dynamodb_record_with(
        key=True,
        sequence_number="345",
        creation_time=position + datetime.timedelta(hours=1))

    responses = {
        # Shard 0 will immediately exhaust without finding records
        root_id: [{"Records": [], "NextShardIterator": last_iterator}],
        # Shard 1 will not find any records, but stay open (always has a NextShardIterator)
        fail_to_seek: [continue_response] * CALLS_TO_REACH_HEAD,
        # Shard 2 will find a record, and stay open.
        find_records: [{"Records": [record], "NextShardIterator": "not-followed-iterator-id"}],
    }

    # Fixed description, should be called once
    session.describe_stream.return_value = description
    # For simplicity, iterator ids are shard ids
    session.get_shard_iterator.side_effect = lambda shard_id, **kwargs: shard_id
    # Responses are iterator in the same order as their list in `responses`
    session.get_stream_records.side_effect = lambda iterator_id: responses[iterator_id].pop(0)

    coordinator.move_to(position)

    # Records from seeking are pushed into the buffer.  Only one record from second child.
    actual_record, source_shard = coordinator.buffer.pop()
    assert not coordinator.buffer
    assert actual_record["meta"]["sequence_number"] == "345"
    assert source_shard.shard_id == find_records

    # Both child shards are active, even though only one found a child.
    # Since root shard was exhausted, its children are the current roots.
    assert {shard.shard_id for shard in coordinator.active} == {fail_to_seek, find_records}
    assert {shard.shard_id for shard in coordinator.roots} == {fail_to_seek, find_records}

    # Remote calls: 1 DescribeStream at the beginning: stream moves to trim_horizon
    #               4 GetShardIterator: 2 for root shard (stream jump to trim_horizon, shard jump to trim_horizon)
    #                                   1 for find_records (seek_to jumps to trim_horizon)
    #                                   1 for fail_to_seek (seek_to jumps to trim_horizon)
    #               7 GetRecords: 1 for exhausted root (when the stream moves to trim_horizon)
    #                             1 for find_records shard
    #                             5 for fail_to_seek, which is still open
    session.describe_stream.assert_called_once_with(stream_arn=coordinator.stream_arn)
    assert session.get_shard_iterator.call_count == 4
    for shard_id in {root_id, fail_to_seek, find_records}:
        session.get_shard_iterator.assert_any_call(
            stream_arn=coordinator.stream_arn,
            shard_id=shard_id,
            iterator_type="trim_horizon",
            sequence_number=None
        )
    # This odd construct is because we can't use assert_has_calls (doesn't check number of each call)
    # and we can't use call_args_list directly, because we don't know the order the root's children will
    # have when they're unpacked (dict -> list in stream.shard.unpack_shards).
    # Instead, we pull the args (c[0]) out of each call in call_args_list, and grab the first element (c[0][0]).
    # We don't need to check the length of call args or call kwargs, because the side_effect only takes one arg.
    args = [c[0][0] for c in session.get_stream_records.call_args_list]
    calls_with_counts = collections.Counter(args)
    # Doesn't contain the child shard of find_records, because it was never searched.
    assert calls_with_counts == {
        root_id: 1,
        find_records: 1,
        fail_to_seek: CALLS_TO_REACH_HEAD
    }


def test_move_to_trim_horizon(coordinator, session):
    """Moving to the trim_horizon clears existing state and adds new shards"""
    # All of these should be cleaned up entirely
    previous_shards = build_shards(3, {0: 1},
                                   session=session,
                                   stream_arn=coordinator.stream_arn,
                                   shard_id_prefix="previous")
    coordinator.roots.extend((previous_shards[0], previous_shards[2]))
    coordinator.active.append(previous_shards[1])
    coordinator.buffer.push(local_record(), previous_shards[1])

    # -----------
    # 0 -> 1 -> 2
    #        -> 3
    # -----------
    # 4 -> 5
    #   -> 6 -> 7
    # -----------
    session.describe_stream.return_value = stream_description(
        8, {0: 1, 1: [2, 3], 4: [5, 6], 6: 7},
        stream_arn=coordinator.stream_arn)
    expected_root_ids = ["shard-id-0", "shard-id-4"]
    expected_active_ids = expected_root_ids
    session.get_shard_iterator.return_value = "child-iterator-id"

    coordinator.move_to("trim_horizon")

    # Remote calls - no new records fetched, loaded child shards and jumped to their trim_horizons
    session.get_stream_records.assert_not_called()
    session.describe_stream.assert_called_once_with(stream_arn=coordinator.stream_arn)
    assert session.get_shard_iterator.call_count == len(expected_active_ids)
    for expected_shard_id in expected_active_ids:
        session.get_shard_iterator.assert_any_call(
            stream_arn=coordinator.stream_arn,
            shard_id=expected_shard_id,
            iterator_type="trim_horizon",
            sequence_number=None
        )

    # Previous local state is cleared
    assert all(shard not in coordinator.active for shard in previous_shards)
    assert all(shard not in coordinator.roots for shard in previous_shards)
    assert not coordinator.buffer

    # Current local state; trim_horizon sets roots to active
    assert set(s.shard_id for s in coordinator.active) == set(expected_active_ids)
    assert set(s.shard_id for s in coordinator.roots) == set(expected_root_ids)


def test_move_to_latest(coordinator, session):
    """Moving to the trim_horizon clears existing state and adds new shards"""
    # All of these should be cleaned up entirely
    previous_shards = build_shards(3, {0: 1},
                                   session=session,
                                   stream_arn=coordinator.stream_arn,
                                   shard_id_prefix="previous")
    coordinator.roots.extend((previous_shards[0], previous_shards[2]))
    coordinator.active.append(previous_shards[1])
    coordinator.buffer.push(local_record(), previous_shards[1])

    # -----------
    # 0 -> 1 -> 2
    #        -> 3
    # -----------
    # 4 -> 5
    #   -> 6 -> 7
    # -----------
    session.describe_stream.return_value = stream_description(
        8, {0: 1, 1: [2, 3], 4: [5, 6], 6: 7},
        stream_arn=coordinator.stream_arn)
    expected_root_ids = ["shard-id-0", "shard-id-4"]
    expected_active_ids = ["shard-id-{}".format(i) for i in [2, 3, 5, 7]]
    session.get_shard_iterator.return_value = "child-iterator-id"

    coordinator.move_to("latest")

    # Remote calls - no new records fetched, loaded child shards and jumped to their latest
    session.get_stream_records.assert_not_called()
    session.describe_stream.assert_called_once_with(stream_arn=coordinator.stream_arn)
    assert session.get_shard_iterator.call_count == len(expected_active_ids)
    for expected_shard_id in expected_active_ids:
        session.get_shard_iterator.assert_any_call(
            stream_arn=coordinator.stream_arn,
            shard_id=expected_shard_id,
            iterator_type="latest",
            sequence_number=None
        )

    # Previous local state is cleared
    assert all(shard not in coordinator.active for shard in previous_shards)
    assert all(shard not in coordinator.roots for shard in previous_shards)
    assert not coordinator.buffer

    # Current local state: latest sets shards without children to active
    assert set(s.shard_id for s in coordinator.active) == set(expected_active_ids)
    assert set(s.shard_id for s in coordinator.roots) == set(expected_root_ids)


@pytest.mark.parametrize("position", [
    None,
    "start", "end", "head", "front", "back",
    "at_sequence", ("after_sequence", "sequence-number"),
])
def test_move_to_unknown(position, coordinator):
    with pytest.raises(InvalidPosition):
        coordinator.move_to(position)
