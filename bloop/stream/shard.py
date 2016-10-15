import arrow
import collections
from typing import Optional, Dict, Any, List, Mapping, Iterable

from ..exceptions import ShardIteratorExpired
from ..session import SessionWrapper
from ..util import Sentinel

# Approximate number of calls to fully traverse an empty shard
CALLS_TO_REACH_HEAD = 5

last_iterator = Sentinel("LastIterator")
missing = Sentinel("missing")


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

    def __repr__(self):
        if self.exhausted:
            # <Shard[exhausted, id='shardId-00000001414562045508-2bac9cd2']>
            details = "exhausted"
        elif self.iterator_type == "at_sequence":
            # <Shard[at=300000000000000499659, id='shardId-00000001414562045508-2bac9cd2']>
            details = "at_seq=" + repr(self.sequence_number)
        elif self.iterator_type == "after_sequence":
            # <Shard[after=300000000000000499659, id='shardId-00000001414562045508-2bac9cd2']>
            details = "after_seq=" + repr(self.sequence_number)
        elif self.iterator_type in ["trim_horizon", "latest"]:
            # <Shard[latest, id='shardId-00000001414562045508-2bac9cd2']>
            # <Shard[trim_horizon, id='shardId-00000001414562045508-2bac9cd2']>
            details = self.iterator_type
        else:
            # <Shard[id='shardId-00000001414562045508-2bac9cd2']>
            details = ""
        if details:
            details += ", "
        return "<{}[{}id={!r}]>".format(self.__class__.__name__, details, self.shard_id)

    def __next__(self) -> List[Dict[str, Any]]:
        try:
            return self.get_records()
        except ShardIteratorExpired:
            # Refreshing a sequence_number-based Shard iterator is deterministic;
            # if the iterator type is latest or trim_horizon, it's up to the caller to
            # decide how to proceed.
            if self.iterator_type in ["trim_horizon", "latest"]:
                raise

        # Since the expired iterator has a sequence_number, try to refresh automatically.
        # This could still raise RecordsExpired, if the desired position fell behind the
        # the trim_horizon since it expired.
        self.jump_to(iterator_type=self.iterator_type, sequence_number=self.sequence_number)

        # If it didn't expire, let's try returning records once more.
        return self.get_records()

    def __eq__(self, other):
        try:
            return (
                (self.token == other.token) and
                (self.iterator_id == other.iterator_id) and
                ({child.shard_id for child in self.children} == {child.shard_id for child in other.children})
            )
        except (AttributeError, TypeError):
            return False

    @property
    def exhausted(self) -> bool:
        return self.iterator_id is last_iterator

    @property
    def token(self) -> Dict[str, Any]:
        """Does not recursively tokenize children.

        Returns fields that may be redundant for generating a Stream token,
        such as stream_arn and shard_id.
        """
        # TODO logger.info when iterator_type is "trim_horizon" or "latest"
        return {
            "stream_arn": self.stream_arn,
            "shard_id": self.shard_id,
            "iterator_type": self.iterator_type,
            "sequence_number": self.sequence_number,
            "parent": self.parent.shard_id if self.parent else None
        }

    def walk_tree(self) -> Iterable["Shard"]:
        """Generator that visits all shards in a shard tree"""
        shards = collections.deque([self])
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
        self.empty_responses = 0

    def seek_to(self, position: arrow.Arrow) -> List[Dict[str, Any]]:
        """Move the Shard's iterator to the earliest record that after the given time.

        Returns the first records at or past ``position``.  If the list is empty,
        the seek failed to find records, either because the Shard is exhausted or it
        reached the HEAD of an open Shard.
        """
        # 0) We have no way to associate the date with a position, so we simply have to go through
        #    the entire Shard until we find a set of records with at least one with ApproxCreateDate >= position.
        self.jump_to(iterator_type="trim_horizon")

        # Stop once the Shard is exhausted (can't possibly find the record)
        # or we've somewhat confidently caught the HEAD of an open Shard.
        while (not self.exhausted) and (self.empty_responses < CALLS_TO_REACH_HEAD):
            # Don't need to worry about RecordsExpired, since we're starting at trim_horizon.
            # Don't need to worry about ShardIteratorExpired, since we just created this one at trim_horizon.
            records = self.get_records()
            # Shortcut: we need AT LEAST one record to be after the position, so check the last record.
            # if it's before the position, all of the records in this response are.
            if records and records[-1]["meta"]["created_at"] >= position:
                # Reverse search is faster (on average; they're still both O(n) worst),
                # since we're looking for the first number *below* the position.
                for index, record in enumerate(reversed(records)):
                    if record["meta"]["created_at"] < position:
                        return records[len(records) - index:]
                # If the loop above finished, it means ALL the records are after the position.
                return records

        # Either exhausted the Shard, or caught up to HEAD.
        # It's only a failure to seek if the Shard is exhausted.
        return []

    def load_children(self) -> None:
        """Try to load the shard's children from DynamoDB if it doesn't have any.

        Loads all shards that have this shard as an ancestor.
        """
        # If a Shard has children, that number will never change.
        # Children are the result of exactly one event:
        #   increased throughput -> exactly 2 children
        #         open for ~4hrs -> at most 1 child
        if self.children:
            return self.children

        # ParentShardId -> [Shard, ...]
        by_parent = collections.defaultdict(list)
        # ShardId -> Shard
        by_id = {}
        for shard in self.session.describe_stream(
                stream_arn=self.stream_arn,
                first_shard=self.shard_id)["Shards"]:
            parent_list = by_parent[shard.get("ParentShardId")]
            shard = Shard(
                stream_arn=self.stream_arn,
                shard_id=shard["ShardId"],
                parent=shard.get("ParentShardId"),
                session=self.session)
            parent_list.append(shard)
            by_id[shard.shard_id] = shard

        # Find this shard when looking up shards by ParentShardId
        by_id[self.shard_id] = self

        # Insert this shard's children, then handle its child's descendants etc.
        to_insert = collections.deque(by_parent[self.shard_id])
        while to_insert:
            shard = to_insert.popleft()
            # ParentShardId -> Shard
            shard.parent = by_id[shard.parent]
            shard.parent.children.append(shard)
            # Continue for any shards that have this shard as their parent
            to_insert.extend(by_parent[shard.shard_id])

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

        # Failed after 5 calls
        return []

    def _apply_get_records_response(self, response: Mapping[str, Any]) -> List[Dict[str, Any]]:
        records = response.get("Records", [])
        records = [reformat_record(record) for record in records]
        self.iterator_id = response.get("NextShardIterator", last_iterator)

        # The iterator state should ONLY be updated if there's no sequence_number already.
        # This ensures we can refresh from a fixed point, which is unnecessary if we have a number.
        # If sequence_number is set, we're risking data loss by skipping the existing checkpoint.
        if records and self.sequence_number is None:
            self.sequence_number = records[0]["meta"]["sequence_number"]
            self.iterator_type = "at_sequence"
        elif not records:
            self.empty_responses += 1
        return records


def reformat_record(record: Mapping[str, Any]) -> Dict[str, Any]:
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


def unpack_shards(shards: List[Mapping[str, Any]], stream_arn: str, session: SessionWrapper) -> Dict[str, Shard]:
    """List[Dict] -> Dict[shard_id, Shard].

    Each Shards' parent/children are hooked up with the other Shards in the list.
    """
    if not shards:
        return {}

    # When unpacking tokens, shard id key is "shard_id"
    # When unpacking DescribeStream responses, shard id key is "ShardId"
    if "ShardId" in shards[0]:
        shards = _translate_shards(shards)

    by_id = {shard_token["shard_id"]:
             Shard(stream_arn=stream_arn, shard_id=shard_token["shard_id"],
                   iterator_type=shard_token["iterator_type"], sequence_number=shard_token["sequence_number"],
                   parent=shard_token.get("parent"), session=session)
             for shard_token in shards}

    for shard in by_id.values():
        if shard.parent:
            shard.parent = by_id[shard.parent]
            shard.parent.children.append(shard)
    return by_id


def _translate_shards(shards: List[Mapping[str, Any]]) -> Iterable[Mapping[str, Any]]:
    """Converts the dicts from DescribeStream to the internal Shard format."""
    for shard in shards:
        yield {
            "shard_id": shard["ShardId"],
            "iterator_type": None,
            "sequence_number": None,
            "iterator_id": None,
            "parent": shard.get("ParentShardId")
        }
