import heapq
from typing import Dict, List, Optional, Tuple, Any

from .stream_utils import walk_shards

from ..session import SessionWrapper
from ..util import Sentinel

last_iterator = Sentinel("LastIterator")

__all__ = ["Coordinator", "RecordBuffer", "Shard"]


def heap_item(clock: int, record: Dict, shard: "Shard") -> Tuple[int, Tuple[Dict, "Shard"]]:
    # Primary ordering is by event creation time.
    # However, creation time is *approximate* and has whole-second resolution.
    # This means two events in the same shard within one second can't be ordered.
    ordering = record["dynamodb"]["ApproximateCreationDateTime"]
    # From testing, SequenceNumber isn't a guaranteed ordering either.  However,
    # it is guaranteed to be unique within a shard.  This will be tie-breaker
    # for multiple records within the same shard, within the same second.
    second_ordering = record["dynamodb"]["SequenceNumber"]
    # It's possible though unlikely, that sequence numbers will collide across
    # multiple shards, within the same second.  The final tie-breaker is
    # a monotonically increasing integer from the buffer.
    return (ordering, second_ordering, clock), (record, shard)


class RecordBuffer:
    def __init__(self):
        # (total_ordering, (record, shard))
        #  ^--sort         ^--data  ^--src
        self._heap = []

        # Used by the total ordering clock
        self.__monotonic_integer = 0

    def push(self, record: Dict, shard: "Shard") -> None:
        heapq.heappush(self._heap, heap_item(self.clock, record, shard))

    def push_all(self, record_shard_pairs: List[Tuple[Dict, "Shard"]]) -> None:
        # Faster than inserting one at a time, just dump them in the list
        # and then heapify the whole thing.
        for pair in record_shard_pairs:
            self._heap.append(heap_item(self.clock, *pair))
        heapq.heapify(self._heap)

    def pop(self) -> Tuple[Dict, "Shard"]:
        return heapq.heappop(self._heap)[1]

    def peek(self) -> Tuple[Dict, "Shard"]:
        return self._heap[0][1]

    def clear(self) -> None:
        self._heap.clear()

    @property
    def heap(self) -> List[Tuple[Dict, "Shard"]]:
        return self._heap

    def __len__(self) -> int:
        return len(self._heap)

    @property
    def clock(self):
        """A monotonically increasing integer."""
        # The return value is in between previous and new, so that __monotonic_integer
        # is never set to a tie-breaking value.  This tries to avoid collisions when
        # someone directly manipulates the underlying int.
        self.__monotonic_integer += 2
        return self.__monotonic_integer - 1


class Shard:
    def __init__(self, *, stream_arn: str, shard_id: str,
                 iterator_id: Optional[str]=None, iterator_type: Optional[str]=None,
                 sequence_number: Optional[str]=None, parent: Optional["Shard"]=None):
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

    @property
    def exhausted(self):
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
            "parent": self.parent.shard_id if self.parent else None,
            "children": [child.shard_id for child in self.children]
        }


class Coordinator:
    def __init__(self, *, engine, session: SessionWrapper, stream_arn: str):
        # Set once on creation, never changes
        self.engine = engine

        # Set once on creation, never changes
        self.session = session

        # Set once on creation, never changes
        self.stream_arn = stream_arn

        # Changes infrequently, set initially
        self.roots = []

        # Changes infrequently, set initially
        self.active = []

        # Single buffer for the lifetime of the Coordinator, but mutates frequently
        # Records in the buffer aren't considered read.  When a Record popped from the buffer is
        # consumed, the Coordinator MUST notify the Shard by updating the sequence_number and iterator_type.
        # The new values should be:
        #   shard.sequence_number = record["dynamodb"]["SequenceNumber"]
        #   shard.iterator_type = "after_record"
        self.buffer = RecordBuffer()

    @property
    def token(self) -> Dict[str, Any]:
        shard_tokens = []
        for root in self.roots:
            for shard in walk_shards(root):
                token = shard.token
                token.pop("stream_arn")
                shard_tokens.append(token)
        return {
            "stream_arn": self.stream_arn,
            "active": [shard.shard_id for shard in self.active],
            "shards": shard_tokens
        }
