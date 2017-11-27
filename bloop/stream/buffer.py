import heapq
import random


def jitter():
    """Used to advance a monotonic clock by a small amount"""
    return random.randint(1, 5)


def heap_item(clock, record, shard):
    """Create a tuple of (ordering, (record, shard)) for use in a RecordBuffer."""
    # Primary ordering is by event creation time.
    # However, creation time is *approximate* and has whole-second resolution.
    # This means two events in the same shard within one second can't be ordered.
    ordering = record["meta"]["created_at"]
    # From testing, SequenceNumber isn't a guaranteed ordering either.  However,
    # it is guaranteed to be unique within a shard.  This will be tie-breaker
    # for multiple records within the same shard, within the same second.
    second_ordering = int(record["meta"]["sequence_number"])
    # It's possible though unlikely, that sequence numbers will collide across
    # multiple shards, within the same second.  The final tie-breaker is
    # a monotonically increasing integer from the buffer.
    total_ordering = (ordering, second_ordering, clock())
    return total_ordering, record, shard


class RecordBuffer:
    """Maintains a total ordering for records across any number of shards.

    Methods are thin wrappers around :mod:`heapq`.  Buffer entries have the form:

    .. code-block: python

        (total_ordering, record, shard)

    where ``total_ordering`` is a tuple of ``(created_at, sequence_number, monotonic_clock)`` created from each
    record as it is inserted.
    """
    def __init__(self):
        self.heap = []

        # Used by the total ordering clock
        self.__monotonic_integer = 0

    def push(self, record, shard):
        """Push a new record into the buffer

        :param dict record: new record
        :param shard: Shard the record came from
        :type shard: :class:`~bloop.stream.shard.Shard`
        """
        heapq.heappush(self.heap, heap_item(self.clock, record, shard))

    def push_all(self, record_shard_pairs):
        """Push multiple (record, shard) pairs at once, with only one :meth:`heapq.heapify` call to maintain order.

        :param record_shard_pairs: list of ``(record, shard)`` tuples
            (see :func:`~bloop.stream.buffer.RecordBuffer.push`).
        """
        # Faster than inserting one at a time; the heap is sorted once after all inserts.
        for record, shard in record_shard_pairs:
            item = heap_item(self.clock, record, shard)
            self.heap.append(item)
        heapq.heapify(self.heap)

    def pop(self):
        """Pop the oldest (lowest total ordering) record and the shard it came from.

        :return: Oldest ``(record, shard)`` tuple.
        """
        return heapq.heappop(self.heap)[1:]

    def peek(self):
        """A :func:`~bloop.stream.buffer.RecordBuffer.pop` without removing the (record, shard) from the buffer.

        :return: Oldest ``(record, shard)`` tuple.
        """
        return self.heap[0][1:]

    def clear(self):
        """Drop the entire buffer."""
        self.heap.clear()

    def __len__(self):
        return len(self.heap)

    def clock(self):
        """Returns a monotonically increasing integer.

        **Do not rely on the clock using a fixed increment.**

        .. code-block:: python

            >>> buffer = RecordBuffer()
            >>> buffer.clock()
            3
            >>> buffer.clock()
            40
            >>> buffer.clock()
            41
            >>> buffer.clock()
            300

        :return: A unique clock value guaranteed to be larger than every previous value
        :rtype: int
        """
        # Try to prevent collisions from someone accessing the underlying int.
        # This offset ensures _RecordBuffer__monotonic_integer will never have
        # the same value as any call to clock().
        value = self.__monotonic_integer + jitter()
        self.__monotonic_integer = value + jitter()
        return value
