from typing import Dict, Mapping
from .models import Coordinator, Shard
from .stream_utils import walk_shards


def tokenize_coordinator(coordinator: Coordinator) -> Dict:
    """Clean up temporary fields, tokenize active, roots"""
    # Can't do anything with engine, session, buffer.
    return {
        "stream_arn": coordinator.stream_arn,
        "shard_trees": [tokenize_shard(shard) for shard in coordinator.roots],
        # All the shards are captured in "shard_trees", just list the shard ids
        "active_shard_ids": [shard.shard_id for shard in coordinator.active]
    }


def tokenize_shard(shard: Shard) -> Dict:
    """Clean up temporary fields, recurse through children"""
    # Don't need stream_arn, coordinator will have that
    # Don't need empty_responses, will have to seek on ``trim_horizon``, ``latest`` anyway.
    return {
        "shard_id": shard.shard_id,
        "iterator_id": shard.iterator_id,
        "iterator_type": shard.iterator_type,
        "sequence_number": shard.sequence_number,
        "parent": shard.parent["shard_id"] if shard.parent else None,
        "children": [tokenize_shard(child) for child in shard.children]
    }


def load_shard_state(stream_arn: str, shard_dict: Mapping) -> Shard:
    shard = Shard(
        stream_arn=stream_arn,
        shard_id=shard_dict["shard_id"],
        iterator_id=shard_dict["iterator_id"],
        iterator_type=shard_dict["iterator_type"],
        sequence_number=shard_dict["sequence_number"],
        parent=None,
        children=[load_shard_state(stream_arn, child_dict) for child_dict in shard_dict["children"]],
        empty_responses=0)
    for child in shard.children:
        child.parent = shard

    return shard


def load_coordinator_state(coordinator: Coordinator, token: Mapping) -> None:
    """Load the state described by the token into the Coordinator."""
    stream_arn = coordinator.stream_arn = token["stream_arn"]

    # Clear out state - we're not replacing the lists in case users keep references to them
    coordinator.roots.clear()
    coordinator.active.clear()
    coordinator.buffer.clear()

    coordinator.roots.extend(load_shard_state(stream_arn, shard) for shard in token["shard_trees"])

    # Build shard index in O(N), N = number of shards in all trees
    all_shards = {
        shard.shard_id: shard
        for root_shard in coordinator.roots
        for shard in walk_shards(root_shard)}

    # Associate active shards in O(A), A = number of active shards
    coordinator.active.extend(all_shards[shard_id] for shard_id in token["active_shard_ids"])

    # TODO try to get iterators and describe the stream to fail early on missing/expired objects
    return
