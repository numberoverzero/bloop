import arrow
from typing import Dict, List, Mapping, Optional
from ..session import SessionWrapper
from .models import Coordinator, Shard, new_shard
from .stream_utils import get_with_catchup, is_exhausted, walk_shards
from .tokens import load_coordinator


def move_coordinator(coordinator: Coordinator, position) -> None:
    if position in {"latest", "trim_horizon"}:
        coordinator.roots.clear()
        coordinator.active.clear()
        coordinator.buffer.clear()

        coordinator.roots.extend(
            load_all_shards(coordinator.session, coordinator.stream_arn))

        if position == "trim_horizon":
            for shard in coordinator.roots:
                jump_to(coordinator, shard, "trim_horizon")
            coordinator.active.extend(coordinator.roots)
        else:
            # latest
            leaf_shards = [
                shard for shard in walk_shards(*coordinator.root_shards)
                if not shard.children]
            for shard in leaf_shards:
                jump_to(coordinator, shard, "latest")
            coordinator.active.extend(leaf_shards)

    elif isinstance(position, arrow.Arrow):
        # TODO
        ...
    elif isinstance(position, Mapping):
        load_coordinator(coordinator, position)
        # TODO prune, validate iterators, get new ids, etc.

    else:
        raise ValueError("Don't know how to move to position {!r}".format(position))


def heartbeat(coordinator: Coordinator) -> None:
    # Try to keep active shards with ``latest`` and ``trim_horizon`` iterators alive.
    # Ideally, this will find records and make them ``at_sequence`` or ``after_sequence`` iterators.
    for shard in coordinator.active:
        if shard.sequence_number is None:
            records = advance_shard(coordinator, shard)
            # Success!  This shard now has an ``at_sequence`` iterator
            if records:
                coordinator.buffer.push_all((record, shard) for record in records)


def advance_coordinator(coordinator: Coordinator) -> Optional[Dict]:
    # Try to get the next record from each shard and push it into the buffer.
    if not coordinator.buffer:
        record_shard_pairs = []
        for shard in coordinator.active:
            records = advance_shard(coordinator, shard)
            if records:
                record_shard_pairs.extend((record, shard) for record in records)
        coordinator.buffer.push_all(record_shard_pairs)

        # Clean up exhausted Shards.
        # Can't modify the active list while iterating it.
        to_remove = [shard for shard in coordinator.active if is_exhausted(shard)]
        for shard in to_remove:
            # Remove shard from active[, roots]
            # Fetch children, add children to active[, roots]
            # Jump children to trim_horizon
            remove_shard(coordinator, shard)

    # Still have buffered records from a previous call, or we just refilled the buffer above
    if coordinator.buffer:
        record, shard = coordinator.buffer.pop()

        # Now that the record is "consumed", advance the shard's checkpoint
        shard.sequence_number = record["dynamodb"]["SequenceNumber"]
        shard.iterator_type = "after_sequence"
        return record

    # No records :(
    return None


def advance_shard(coordinator: Coordinator, shard: Shard) -> List[Dict]:
    # TODO handle RecordsExpired
    # TODO handle ShardIteratorExpired -> session.get_shard_iterator and retry
    return get_with_catchup(coordinator.session, shard)


def remove_shard(coordinator: Coordinator, shard: Shard) -> None:
    # Try to load children.  If we find any, the Coordinator needs to
    # remove this Shard from the active set and add the children.
    children = fetch_children(coordinator.session, shard)

    # This was a root; remove it from the root set
    # and if necessary, promote the children to roots
    if shard in coordinator.roots:
        coordinator.roots.remove(shard)
        if children:
            coordinator.roots.extend(children)

    # If the Shard was active, remove it and set its children to active.
    if shard in coordinator.active:
        coordinator.active.remove(shard)
        if children:
            coordinator.active.extend(children)
            for child in children:
                # Pick up right where the removed Shard left off
                jump_to(coordinator, child, "trim_horizon")


def fetch_children(session: SessionWrapper, shard: Shard) -> List[Shard]:
    """If a shard doesn't have children, fetches them (if they exist)."""
    # If a Shard has children, that number will never change.
    # Children are the result of exactly one event:
    #   increased throughput -> exactly 2 children
    #         open for ~4hrs -> at most 1 child
    if shard.children:
        return shard.children
    children = [
        s for s in session.describe_stream(
            stream_arn=shard.stream_arn,
            first_shard=shard.shard_id)["Shards"]
        if s.get("ParentShardId") == shard.shard_id]
    for child in children:
        child = new_shard(
            stream_arn=shard.stream_arn,
            shard_id=child.get("ShardId"),
            parent=shard)
        shard.children.append(child)
    return shard.children


def load_all_shards(session: SessionWrapper, stream_arn: str) -> Dict[str, Shard]:
    """Return Shards indexed by Shard.shard_id"""
    shards = [
        new_shard(stream_arn=stream_arn, shard_id=shard["ShardId"], parent=shard.get("ParentShardId"))
        for shard in session.describe_stream(stream_arn=stream_arn)["Shards"]
    ]
    by_id = {shard.shard_id: shard for shard in shards}
    for shard in shards:
        if shard.parent:
            parent = shard.parent = by_id[shard.parent]
            parent.children.append(shard)
    return by_id


def jump_to(coordinator: Coordinator, shard: Shard, iterator_type: str) -> None:
    """Move the Shard to ``trim_horizon`` or ``latest``, resetting all internal state."""
    # Don't need to handle RecordsExpired; ``latest`` isn't beyond the trim horizon,
    # and ``trim_horizon`` is *at* the trim horizon, not beyond it.
    iterator_id = coordinator.session.get_shard_iterator(
        stream_arn=shard.stream_arn,
        shard_id=shard.shard_id,
        iterator_type=iterator_type)
    shard.iterator_id = iterator_id
    # Drop all previous tracking state; the previous position
    # is irrelevant to the new iterator position of the Shard
    shard.empty_responses = 0
    shard.iterator_type = iterator_type
    shard.sequence_number = None
    remove_buffered_records(coordinator, shard)


def seek_to(coordinator: Coordinator, shard: Shard, position: arrow.Arrow) -> None:
    """Expensive - move the shard to the earliest record that is after the given position."""
    # TODO
    return None


def remove_buffered_records(coordinator: Coordinator, shard: Shard) -> None:
    """Remove all records from the Shard in the coordinator's buffer"""
    heap = coordinator.buffer.heap
    # ( create_time,  ( record,  shard ) )
    #                 |----- x[1] -----|
    #           shard = x[1][1] ---^
    to_remove = [x for x in heap if x[1][1] is shard]
    for x in to_remove:
        heap.remove(x)
