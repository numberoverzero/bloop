import collections
from typing import Optional, Dict, Any, Iterator, List, Mapping

from ..exceptions import ShardIteratorExpired
from ..session import SessionWrapper
from ..util import Sentinel

# Approximate number of calls to fully traverse an empty shard
CALLS_TO_REACH_HEAD = 5

last_iterator = Sentinel("LastIterator")


class Shard:
    def __init__(self, *, stream_arn: str, shard_id: str,
                 iterator_id: Optional[str]=None, iterator_type: Optional[str]=None,
                 sequence_number: Optional[str]=None, parent: Optional["Shard"]=None,
                 session: Optional[SessionWrapper]=None):
        # Set once on creation, never changes
        self.stream_arn = stream_arn
        # Set once on creation, never changes
        self.shard_id = shard_id
        # Changes frequently, not set initially
        # Iterators have a 15 minute lifetime, and need to be refreshed before then.
        # If they expire, a new one can be created deterministically IFF the Shard has
        # both a sequence_number and iterator_type of "at_sequence" or "after_sequence"
        # Iterators also change on nearly every GetRecords call.
        # When the end of a closed Shard is reached, this becomes None.
        self.iterator_id = iterator_id
        # Changes infrequently, not set initially
        # This will change on seek/jump, and the first time a record is encountered
        # (usually, from "trim_horizon" or "latest" to "at_sequence")
        self.iterator_type = iterator_type
        # Changes frequently, not set initially
        # See iterator_type
        self.sequence_number = sequence_number
        # Changes very infrequently, set initially
        # This will only change when the parent Shard previously existed, but has
        # since passed the 24 hour trim horizon.
        # After the initial set, this will never go from None -> Shard,
        # since Shards do not re-parent.
        self.parent = parent
        # Changes infrequently, set initially
        # Unlike parent, new children are added periodically (~4hrs) and as throughput
        # requires splitting a Shard.  A Shard will have exactly 0, 1, or 2 children.
        # A Shard with 0 children may be open, or there was a reduction in throughput.
        # A Shard with 1 child is closed, and did not split.
        # A Shard with 2 children is closed, due to an increase in throughput.  Updates to
        #   records that were in this Shard may be in either child Shard, but will consistently
        #   be written to the same child Shard (updates to record A will only go to one child, and not
        #   bounce back and forth).
        self.children = []
        # Changes infrequently, 0 initially
        # Tracks how many times a GetRecords call has returned no results, but a next iterator_id.
        # After ~5 empty responses, we can reasonably expect that the iterator is near the HEAD of an open Shard.
        # This dictates how hard we need to work to "catch up" a new iterator, in the face of empty results
        # (which provide no SequenceNumber or ApproximateCreationDateTime to approximate our location in the Stream).
        self.empty_responses = 0

        # Convenience so the Coordinator's session isn't explicitly needed for every call
        self.session = session

    def __iter__(self):
        return self

    def __next__(self) -> List[Dict[str, Any]]:
        try:
            return self.get_records()
        except ShardIteratorExpired:
            # Refreshing a sequence_number-based Shard iterator is deterministic;
            # if the iterator type is latest or trim_horizon, it's up to the caller to
            # decide how to proceed.
            if self.iterator_type in {"trim_horizon", "latest"}:
                raise

        # Since the expired iterator has a sequence_number, try to refresh automatically.
        # This could still raise RecordsExpired, if the desired position fell behind the
        # the trim_horizon since it expired.
        self.jump_to(iterator_type=self.iterator_type, sequence_number=self.sequence_number)

        # If it didn't expire, let's try returning records once more.
        return self.get_records()

    @property
    def exhausted(self) -> bool:
        return self.iterator_id is last_iterator

    @property
    def token(self) -> Dict[str, Any]:
        """Does not recursively tokenize children.

        Returns fields that may be redundant for generating a Stream token,
        such as stream_arn and shard_id.
        """
        return {
            "stream_arn": self.stream_arn,
            "shard_id": self.shard_id,
            "iterator_type": self.iterator_type,
            "sequence_number": self.sequence_number,
            "parent": self.parent.shard_id if self.parent else None
        }

    def walk_tree(self) -> Iterator["Shard"]:
        """Generator that visits all shards in a shard tree"""
        shards = collections.deque(self)
        while shards:
            shard = shards.popleft()
            yield shard
            shards.extend(shard.children)

    def jump_to(self, *, iterator_type: str, sequence_number: Optional[str]=None) -> None:
        # Just a simple wrapper; let the caller handle RecordsExpired
        self.iterator_id = self.session.get_shard_iterator(
            stream_arn=self.stream_arn,
            shard_id=self.shard_id,
            iterator_type=iterator_type,
            sequence_number=sequence_number)
        self.iterator_type = iterator_type
        self.sequence_number = sequence_number

    def load_children(self) -> None:
        """Try to load the shard's children from DynamoDB if it doesn't have any."""
        # If a Shard has children, that number will never change.
        # Children are the result of exactly one event:
        #   increased throughput -> exactly 2 children
        #         open for ~4hrs -> at most 1 child
        if self.children:
            return self.children
        children = [
            s for s in self.session.describe_stream(
                stream_arn=self.stream_arn,
                first_shard=self.shard_id)["Shards"]
            if s.get("ParentShardId") == self.shard_id]
        for child in children:
            child = Shard(
                stream_arn=self.stream_arn,
                shard_id=child.get("ShardId"),
                parent=self,
                session=self.session)
            self.children.append(child)
        return self.children

    def get_records(self) -> List[Dict[str, Any]]:
        """Call GetRecords and apply catch-up logic.  Updates shard.iterator_id.  No exception handling."""
        # Won't be able to find new records.
        if self.exhausted:
            return []

        # Already caught up, just the one call please.
        if self.empty_responses >= CALLS_TO_REACH_HEAD:
            return self._apply_get_records_response(self.session.get_stream_records(self.iterator_id))

        # Up to 5 calls to try and find a result
        while self.empty_responses < CALLS_TO_REACH_HEAD and not self.exhausted:
            records = self._apply_get_records_response(self.session.get_stream_records(self.iterator_id))
            if records:
                # Stop working the first time we find results.
                return records
            # Keep looking.
            self.empty_responses += 1

        # Failed after 5 calls
        return []

    def _apply_get_records_response(self, response: Mapping[str, Any]) -> List[Dict[str, Any]]:
        records = response.get("Records", [])
        self.iterator_id = response.get("NextShardIterator", last_iterator)

        # The iterator state should ONLY be updated if there's no sequence_number already.
        # This ensures we can refresh from a fixed point, which is unnecessary if we have a number.
        # If sequence_number is set, we're risking data loss by skipping the existing checkpoint.
        if records and self.sequence_number is None:
            self.sequence_number = records[0]["dynamodb"]["SequenceNumber"]
            self.iterator_type = "at_sequence"
        return records


def unpack_shards(shards: List[Mapping[str, Any]], stream_arn: str, session: SessionWrapper) -> Dict[str, Shard]:
    """List[Dict] -> Dict[shard_id, Shard].

    Each Shards' parent/children are hooked up with the other Shards in the list.
    """
    by_id = {shard_token["shard_id"]:
             Shard(stream_arn=stream_arn, shard_id=shard_token["shard_id"],
                   iterator_type=shard_token["iterator_type"], sequence_number=shard_token["sequence_number"],
                   session=session)
             for shard_token in shards}

    for shard_token in shards:
        shard = by_id[shard_token["shard_id"]]
        parent_id = shard_token.get("parent")
        if parent_id:
            shard.parent = by_id[parent_id]
            shard.parent.children.append(shard)
    return by_id
