import heapq


def heap_item(clock, record, shard):
    """Create a tuple of (ordering, (record, shard)) for use in a RecordBuffer."""
    # Primary ordering is by event creation time.
    # However, creation time is *approximate* and has whole-second resolution.
    # This means two events in the same shard within one second can't be ordered.
    ordering = record["meta"]["created_at"]
    # From testing, SequenceNumber isn't a guaranteed ordering either.  However,
    # it is guaranteed to be unique within a shard.  This will be tie-breaker
    # for multiple records within the same shard, within the same second.
    second_ordering = record["meta"]["sequence_number"]
    # It's possible though unlikely, that sequence numbers will collide across
    # multiple shards, within the same second.  The final tie-breaker is
    # a monotonically increasing integer from the buffer.
    total_ordering = (ordering, second_ordering, clock())
    return total_ordering, record, shard


class RecordBuffer:
    def __init__(self):
        self._heap = []

        # Used by the total ordering clock
        self.__monotonic_integer = 0

    def push(self, record, shard):
        heapq.heappush(self._heap, heap_item(self.clock, record, shard))

    def push_all(self, record_shard_pairs):
        # Faster than inserting one at a time; the heap is sorted once after all inserts.
        for record, shard in record_shard_pairs:
            item = heap_item(self.clock, record, shard)
            self._heap.append(item)
        heapq.heapify(self._heap)

    def pop(self):
        return heapq.heappop(self._heap)[1:]

    def peek(self):
        return self._heap[0][1:]

    def clear(self):
        self._heap.clear()

    @property
    def heap(self):
        return self._heap

    def __len__(self):
        return len(self._heap)

    def clock(self):
        """Returns a monotonically increasing integer."""
        # Try to avoid collisions from someone accessing the underlying int.
        self.__monotonic_integer += 2
        return self.__monotonic_integer - 1
