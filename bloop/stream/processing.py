import arrow
import collections
from typing import Mapping, Any
from ..exceptions import InvalidStream, RecordsExpired
from .coordinator import Coordinator, remove_shard
from .shard import Shard, unpack_shards


def move_coordinator(coordinator: Coordinator, position) -> None:
    if position in {"latest", "trim_horizon"}:
        coordinator.roots.clear()
        coordinator.active.clear()
        coordinator.buffer.clear()

        latest_shards = unpack_shards(
            coordinator.session.describe_stream(
                stream_arn=coordinator.stream_arn)["Shards"],
            coordinator.stream_arn,
            coordinator.session)
        coordinator.roots.extend(
            shard
            for shard in latest_shards.values()
            if not shard.parent)

        if position == "trim_horizon":
            for shard in coordinator.roots:
                shard.jump_to(iterator_type="trim_horizon")
            coordinator.active.extend(coordinator.roots)
        # latest
        else:
            for root in coordinator.roots:
                for shard in root.walk_tree():
                    if not shard.children:
                        shard.jump_to(iterator_type="latest")
                        coordinator.active.append(shard)

    elif isinstance(position, arrow.Arrow):
        # TODO
        raise NotImplementedError
    elif isinstance(position, Mapping):
        update_coordinator_from_token(coordinator, position)
    else:
        raise ValueError("Don't know how to move to position {!r}".format(position))


def update_coordinator_from_token(coordinator: Coordinator, token: Mapping[str, Any]) -> None:
    stream_arn = coordinator.stream_arn = token["stream_arn"]

    # 0) Load the token into the coordinator so we can re-use the normal
    #    processing utilities to prune and add Shards as necessary.
    coordinator.roots.clear()
    coordinator.active.clear()
    coordinator.buffer.clear()

    by_id = unpack_shards(token["shards"], stream_arn, coordinator.session)
    coordinator.roots = [shard for shard in by_id.values() if not shard.parent]
    coordinator.active.extend(by_id[shard_id] for shard_id in token["active"])

    # 1) Load the Stream's actual Shards from DynamoDBStreams for
    #    validation and updates. (this is a mapping of {shard_id: shard})
    by_id = unpack_shards(coordinator.session.describe_stream(stream_arn)["Shards"], stream_arn, coordinator.session)

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
            shard.jump_to(iterator_type=shard.iterator_type, sequence_number=shard.sequence_number)
        except RecordsExpired:
            # TODO logger.info "SequenceNumber from token was past trim_horizon, moving to trim_horizon instead"
            shard.jump_to(iterator_type="trim_horizon")
        # If the Shard has a sequence_number, it may be beyond trim_horizon.  The records between
        # [sequence_number, current trim_horizon) can never be retrieved, so we can ignore that
        # they exist, and simply jump to the current trim_horizon.


def seek_to(coordinator: Coordinator, shard: Shard, position: arrow.Arrow) -> bool:
    """Move the Shard's iterator to the earliest record that after the given time.

    Returns whether a record matching the criteria was found.
    """
    # TODO
    return None
