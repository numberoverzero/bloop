import collections
from ..session import SessionWrapper
from ..util import Sentinel
from .models import Shard

from typing import Dict, List

# Approximate number of calls to fully traverse an empty shard
CALLS_TO_REACH_HEAD = 5

last_iterator = Sentinel("LastIterator")


def walk_shards(*shards: Shard):
    """Generator that visits all shards in a shard tree"""
    shards = collections.deque([shards])
    while shards:
        shard = shards.popleft()
        yield shard
        shards.extend(shard.children)


def is_exhausted(shard: Shard) -> bool:
    """True if there is no next iterator_id"""
    return shard.iterator_id is last_iterator


def get_with_catchup(session: SessionWrapper, shard: Shard) -> List[Dict]:
    """Call GetRecords and apply catch-up logic.  Updates shard.iterator_id.  No exception handling."""
    # Won't be able to find new records.
    if shard.iterator_id is last_iterator:
        return []

    # Already caught up, just the one call please.
    if shard.empty_responses >= CALLS_TO_REACH_HEAD:
        return _apply_response(shard, session.get_stream_records(shard.iterator_id))

    # Up to 5 calls to try and find a result
    while shard.empty_responses < CALLS_TO_REACH_HEAD and not is_exhausted(shard):
        records = _apply_response(shard, session.get_stream_records(shard.iterator_id))
        if records:
            # Stop working the first time we find results.
            return records
        # Keep looking.
        shard.empty_responses += 1

    # Failed after 5 calls
    return []


def _apply_response(shard: Shard, response: Dict) -> List[Dict]:
    records = response.get("Records", [])
    shard.iterator_id = response.get("NextShardIterator", last_iterator)

    # The iterator state should ONLY be updated if there's no sequence_number already.
    # This ensures we can refresh from a fixed point, which is unnecessary if we have a number.
    # If sequence_number is set, we're risking data loss by skipping the existing checkpoint.
    if records and shard.sequence_number is None:
        shard.sequence_number = records[0]["dynamodb"]["SequenceNumber"]
        shard.iterator_type = "at_sequence"
    return records
