from typing import Dict, Mapping
from .models import Coordinator, Shard
from .stream_utils import walk_shards


def tokenize_coordinator(coordinator: Coordinator) -> Dict:
    """Clean up temporary fields, tokenize active, roots"""
    # 0) Flatten each Shard into a dict with ids for parent/children.
    #    The returned dict has a single key, the Shard's shard_id.
    #    This lets us unpack into a dict.  The final structure:
    #        {
    #            "shard-id-1": {
    #                "iterator_type": "at_sequence",
    #                "sequence_number": None,
    #                "parent": "shard-id-2"
    #            },
    #            "shard-id-2": {
    #                "iterator_type": "latest",
    #                "parent": None,
    #            }
    #        }
    shards = {}
    for root in coordinator.roots:
        for shard in walk_shards(root):
            token = shard.token
            # Redundant fields, the coordinator token will contain these
            token.pop("shard_id")
            token.pop("stream_arn")
            shards.update({shard.shard_id: token})
    active = [shard.shard_id for shard in coordinator.active]

    return {
        "stream_arn": coordinator.stream_arn,
        "shards": shards,
        "active": active
    }


def load_shard(stream_arn: str, shard_dict: Mapping) -> Shard:
    # Does not populate Shard.shard_id
    # Does not expand parent, children from shard_ids into Shards
    return Shard(
        stream_arn=stream_arn,
        # Will be set by whatever unpacks each shard dict
        shard_id=None,
        iterator_id=None,
        iterator_type=shard_dict["iterator_type"],
        sequence_number=shard_dict["sequence_number"],
        parent=shard_dict["parent"],
        children=shard_dict["children"]
    )


def load_coordinator(coordinator: Coordinator, token: Mapping) -> None:
    """Load the state described by the token into the Coordinator. Does not prune against real stream state."""
    stream_arn = coordinator.stream_arn = token["stream_arn"]

    # Clear out state - we're not replacing the lists in case users keep references to them
    coordinator.roots.clear()
    coordinator.active.clear()
    coordinator.buffer.clear()

    by_id = {
        shard_id: load_shard(stream_arn, shard_dict)
        for shard_id, shard_dict in token["shards"].items()}

    for shard_id, shard in by_id.items():
        shard.shard_id = shard_id
        shard.parent = by_id[shard.parent] if shard.parent else None
        shard.children = [by_id[child_id] for child_id in shard.children]
        # Take care of coordinator roots while we're here; avoid another list allocation
        if not shard.parent:
            coordinator.roots.append(shard)

    coordinator.active.extend(by_id[shard_id] for shard_id in token["active"])
