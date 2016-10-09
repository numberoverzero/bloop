import arrow
import collections
from typing import Dict, List, Mapping, Optional
from ..exceptions import InvalidStream, RecordsExpired, ShardIteratorExpired
from ..session import SessionWrapper
from .models import Coordinator, Shard, unpack_shards
from .stream_utils import get_with_catchup, walk_shards
from .tokens import load_coordinator


def move_coordinator(coordinator: Coordinator, position) -> None:
    if position in {"latest", "trim_horizon"}:
        coordinator.roots.clear()
        coordinator.active.clear()
        coordinator.buffer.clear()

        latest_shards = unpack_shards(
            coordinator.session.describe_stream(
                stream_arn=coordinator.stream_arn)["Shards"])
        coordinator.roots.extend(
            shard
            for shard in latest_shards.values()
            if not shard.parent)

        if position == "trim_horizon":
            for shard in coordinator.roots:
                jump_to(coordinator, shard, "trim_horizon")
            coordinator.active.extend(coordinator.roots)
        # latest
        else:
            for root in coordinator.roots:
                for shard in walk_shards(root):
                    if not shard.children:
                        jump_to(coordinator, shard, "latest")
                        coordinator.active.append(shard)

    elif isinstance(position, arrow.Arrow):
        # TODO
        raise NotImplementedError
    elif isinstance(position, Mapping):
        update_coordinator_from_token(coordinator, position)
    else:
        raise ValueError("Don't know how to move to position {!r}".format(position))


def update_coordinator_from_token(coordinator: Coordinator, token: Mapping) -> None:
    # 0) Load the token into the coordinator so we can re-use the normal processing utilities to
    #    prune and add Shards as necessary.
    load_coordinator(coordinator, token)

    # 1) Load the Stream's actual Shards from DynamoDBStreams for validation and updates.
    #    (this is a mapping of {shard_id: shard})
    by_id = unpack_shards(coordinator.session.describe_stream(stream_arn=coordinator.stream_arn)["Shards"])

    # 2) Walk each root shard's tree, to find an intersection with the actual shards that exist.
    #    If there's any Shard with no children AND it's not part of the returned shards from DynamoDBStreams,
    #    there's no way to relate the token structure to the existing structure so we bail.  If it has children,
    #    prune the Shard from the Coordinator and try to find its children.
    unverified = collections.deque(coordinator.roots)
    while unverified:
        shard = unverified.popleft()
        # Found an intersection; no need to keep searching this branch.
        if shard.shard_id in by_id:
            continue

        # No worries, we'll just try to verify the children
        if shard.children:
            unverified.extend(shard.children)
            # This shard doesn't exist, so prune it and promote its children to its position(s)
            remove_shard(coordinator, shard)
            continue
        raise InvalidStream("The Stream token contains an unresolvable Shard.")

    # 3) Now that everything's verified, grab new iterators for the coordinator's active Shards.
    for shard in coordinator.active:
        try:
            jump_to(coordinator, shard, shard.iterator_type, shard.sequence_number)
        except RecordsExpired:
            # TODO logger.info "SequenceNumber from token was past trim_horizon, moving to trim_horizon instead"
            jump_to(coordinator, shard, "trim_horizon", None)
        # If the Shard has a sequence_number, it may be beyond trim_horizon.  The records between
        # [sequence_number, current trim_horizon) can never be retrieved, so we can ignore that
        # they exist, and simply jump to the current trim_horizon.


def heartbeat(coordinator: Coordinator) -> None:
    # Try to keep active shards with ``latest`` and ``trim_horizon`` iterators alive.
    # Ideally, this will find records and make them ``at_sequence`` or ``after_sequence`` iterators.
    for shard in coordinator.active:
        if shard.iterator_type in {"latest", "trim_horizon"}:

            # There's no safe default when advance_shard raises ShardIteratorExpired
            # because resetting to the new trim_horizon/latest could miss records.
            # Had the user called Stream.heartbeat() within 15 minutes, this wouldn't happen.

            # Don't need to handle RecordsExpired because only sequence_number-based
            # iterators can fall behind the trim_horizon.
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
        to_remove = [shard for shard in coordinator.active if shard.exhausted]
        for shard in to_remove:
            # 0) Fetch Shard's children if they haven't been loaded
            #    (perhaps the Shard just closed?)
            fetch_children(coordinator.session, shard)

            # 1) Remove the shard from the Coordinator.  If the Shard has
            #    children and was active, those children are added to the active list
            #    If the Shard was a root, those children become roots.
            was_active = shard in coordinator.active
            remove_shard(coordinator, shard)

            # 2) If the shard was active, now its children are active.
            #    Move each child Shard to its trim_horizon.
            if was_active:
                for child in shard.children:
                    # Pick up right where the removed Shard left off
                    jump_to(coordinator, child, "trim_horizon")
                    # The child's previous empty responses have no
                    # bearing on its new position at the trim_horizon.
                    child.empty_responses = 0

    # Still have buffered records from a previous call, or we just refilled the buffer above
    if coordinator.buffer:
        record, shard = coordinator.buffer.pop()

        # Now that the record is "consumed", advance the shard's checkpoint
        shard.sequence_number = record["dynamodb"]["SequenceNumber"]
        shard.iterator_type = "after_sequence"
        return reformat(record)

    # No records :(
    return None


def reformat(record: Dict) -> Dict:
    """Repack a record into a cleaner structure for consumption."""
    # Unwrap the inner structure, since most of it comes from here
    return {
        "key": record["dynamodb"].get("Keys", None),
        "new": record["dynamodb"].get("NewImage", None),
        "old": record["dynamodb"].get("OldImage", None),

        "meta": {
            "created_at": arrow.get(record["dynamodb"]["ApproximateCreationDateTime"]),
            "event": {
                "id": record["eventID"],
                "type": record["eventName"].lower(),
                "version": record["eventVersion"]
            },
            "sequence_number": record["dynamodb"]["SequenceNumber"],
        }
    }


def advance_shard(coordinator: Coordinator, shard: Shard) -> List[Dict]:
    try:
        return get_with_catchup(coordinator.session, shard)
    except ShardIteratorExpired:
        # Refreshing a sequence_number-based Shard iterator is deterministic;
        # if the iterator type is latest or trim_horizon, it's up to the caller to
        # decide how to proceed.
        if shard.iterator_type in {"trim_horizon", "latest"}:
            raise

    # Since the expired iterator has a sequence_number, try to refresh automatically.
    # This could still raise RecordsExpired, if the desired position fell behind the
    # the trim_horizon since it expired.
    jump_to(coordinator, shard, shard.iterator_type, shard.sequence_number)

    # If it didn't expire, let's try returning records once more.
    return get_with_catchup(coordinator.session, shard)


def remove_shard(coordinator: Coordinator, shard: Shard) -> List[Shard]:
    # try/catch avoids the O(N) search of using `if shard in ...`

    try:
        # If the Shard was a root, remove it and promote its children to roots
        coordinator.roots.remove(shard)
        if shard.children:
            coordinator.roots.extend(shard.children)
    except ValueError:
        pass

    try:
        # If the Shard was active, remove it and set its children to active.
        coordinator.active.remove(shard)
        if shard.children:
            coordinator.active.extend(shard.children)
    except ValueError:
        pass

    # Remove any records in the buffer that came from the shard
    heap = coordinator.buffer.heap
    # ( ordering,  ( record,  shard ) )
    #               |----- x[1] -----|
    #        shard = x[1][1] ---^
    # TODO can this be improved?  Gets expensive for high-volume streams with large buffers
    to_remove = [x for x in heap if x[1][1] is shard]
    for x in to_remove:
        heap.remove(x)


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
        child = Shard(
            stream_arn=shard.stream_arn,
            shard_id=child.get("ShardId"),
            parent=shard)
        shard.children.append(child)
    return shard.children


def jump_to(coordinator: Coordinator, shard: Shard, iterator_type: str, sequence_number: str=None) -> None:
    # Just a simple wrapper; let the caller handle RecordsExpired
    shard.iterator_id = coordinator.session.get_shard_iterator(
        stream_arn=shard.stream_arn,
        shard_id=shard.shard_id,
        iterator_type=iterator_type,
        sequence_number=sequence_number)
    shard.iterator_type = iterator_type
    shard.sequence_number = sequence_number


def seek_to(coordinator: Coordinator, shard: Shard, position: arrow.Arrow) -> bool:
    """Move the Shard's iterator to the earliest record that after the given time.

    Returns whether a record matching the criteria was found.
    """
    # TODO
    return None
