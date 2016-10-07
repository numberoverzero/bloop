import heapq
from typing import Dict, List, NamedTuple, Optional, Tuple, Union

from ..session import SessionWrapper

__all__ = ["Coordinator", "RecordBuffer", "Shard"]


def heap_item(record: Dict, shard: "Shard") -> Tuple[int, [Dict, "Shard"]]:
    key = record["dynamodb"]["ApproximateCreationDateTime"]
    return key, (record, shard)


class RecordBuffer:
    def __init__(self):
        # (create_time,  (record, shard))
        #   ^--sort key    ^--data  ^--data src
        self._heap = []

    def push(self, record: Dict, shard: "Shard") -> None:
        heapq.heappush(self._heap, heap_item(record, shard))

    def push_all(self, record_shard_pairs: Tuple[Dict, "Shard"]) -> None:
        # Faster than inserting one at a time, just dump them in the list
        # and then heapify the whole thing.
        for pair in record_shard_pairs:
            self._heap.append(heap_item(*pair))
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

Shard = NamedTuple("Shard", [
    # Set once on creation, never changes
    ("stream_arn", str),

    # Set once on creation, never changes
    ("shard_id", str),

    # Changes frequently, not set initially
    # Iterators have a 15 minute lifetime, and need to be refreshed before then.
    # If they expire, a new one can be created deterministically IFF the Shard has
    # both a sequence_number and iterator_type of "at_sequence" or "after_sequence"
    # Iterators also change on nearly every GetRecords call.
    # When the end of a closed Shard is reached, this becomes None.
    ("iterator_id", Optional[str]),

    # Changes infrequently, not set initially
    # This will change on seek/jump, and the first time a record is encountered
    # (usually, from "trim_horizon" or "latest" to "at_sequence")
    ("iterator_type", Optional[str]),

    # Changes frequently, not set initially
    # See iterator_type
    ("sequence_number", Optional[str]),

    # Changes very infrequently, set initially
    # This will only change when the parent Shard previously existed, but has
    # since passed the 24 hour trim horizon.
    # After the initial set, this will never go from None -> Shard,
    # since Shards do not re-parent.
    ("parent", Optional["Shard"]),

    # Changes infrequently, set initially
    # Unlike parent, new children are added periodically (~4hrs) and as throughput
    # requires splitting a Shard.  A Shard will have exactly 0, 1, or 2 children.
    # A Shard with 0 children may be open, or there was a reduction in throughput.
    # A Shard with 1 child is closed, and did not split.
    # A Shard with 2 children is closed, due to an increase in throughput.  Updates to
    #   records that were in this Shard may be in either child Shard, but will consistently
    #   be written to the same child Shard (updates to record A will only go to one child, and not
    #   bounce back and forth).
    ("children", List["Shard"]),

    # Changes infrequently, 0 initially
    # Tracks how many times a GetRecords call has returned no results, but a next iterator_id.
    # After ~5 empty responses, we can reasonably expect that the iterator is near the HEAD of an open Shard.
    # This dictates how hard we need to work to "catch up" a new iterator, in the face of empty results
    # (which provide no SequenceNumber or ApproximateCreationDateTime to approximate our location in the Stream).
    ("empty_responses", int)
])

Coordinator = NamedTuple("Coordinator", [
    # Set once on creation, never changes
    ("engine", "bloop.Engine"),

    # Set once on creation, never changes
    ("session", SessionWrapper),

    # Set once on creation, never changes
    ("stream_arn", str),

    # Changes infrequently, set initially
    ("roots", List["Shard"]),

    # Changes infrequently, set initially
    ("active", List["Shard"]),

    # Single buffer for the lifetime of the Coordinator, but mutates frequently
    # Records in the buffer aren't considered read.  When a Record popped from the buffer is
    # consumed, the Coordinator MUST notify the Shard by updating the sequence_number and iterator_type.
    # The new values should be:
    #   shard.sequence_number = record["dynamodb"]["SequenceNumber"]
    #   shard.iterator_type = "after_record"
    ("buffer", RecordBuffer),
])


def new_coordinator(engine, session: SessionWrapper, stream_arn: str) -> Coordinator:
    return Coordinator(
        engine=engine,
        session=session,
        stream_arn=stream_arn,
        roots=[],
        active=[],
        buffer=RecordBuffer()
    )


def new_shard(stream_arn: str, shard_id: str, parent: Optional[Union[str, Shard]]=None):
    return Shard(
        stream_arn=stream_arn,
        shard_id=shard_id,
        iterator_id=None,
        iterator_type=None,
        sequence_number=None,
        parent=parent,
        children=[],
        empty_responses=0
    )
