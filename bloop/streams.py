import arrow
import collections

from typing import Dict, List, Optional

from .exceptions import InvalidShardIterator, RecordsExpired, SeekFailed, ShardIteratorExpired
from .session import SessionWrapper
from .signals import object_loaded
from .util import unpack_from_dynamodb


CALLS_TO_REACH_HEAD = 5
"""The approximate number of consecutive empty calls required to fully iterate a single shard.

This is the upper limit of advance_iterator calls to ensure an iterator is caught up to HEAD.
HEAD means "probably caught up to whatever changes are occurring within the shard".  It's not *really* caught up,
since we can't really do that with a moving target and no idea of "where" the iterator is in time.

We want to advance differently when we could be in the middle of empty time or we're roughly caught up.
Starting from TRIM_HORIZON and hitting multiple empty records, we should try to advance the iterator a
few times to clear any possible gap.

This number is an unofficial approximation based on multiple rounds of testing. As an implementation detail of
DynamoDB Streams, it is subject to change without notice, which could have bad consequences for keeping iterators
roughly in sync by time.  If the time delta of each empty call suddenly covers half of the possible shard time,
we may think we're at the head of an open shard when we've only moved halfway through empty space.

Benchmarking: https://gist.github.com/numberoverzero/8bde1089b5def6cc8c6d5fba61866702
"""


def first_overlap(records: List[Dict], position: arrow.Arrow) -> int:
    """Return the index of the first value with a time after position.

    """
    i = len(records)
    while i > 0:
        i -= 1
        record = records[i]
        create_time = arrow.get(record["dynamodb"]["ApproximateCreationDateTime"])
        if create_time < position:
            # This is the first record that was before position; return the index after this
            return i + 1
    # If every record has a create time after the position, return 0.
    # It's not _before_ the position, but it's the earliest in the list that's after the position.
    return 0


def list_shards(session: SessionWrapper, stream: Dict[str, str], first_shard: str=None) -> List[Dict]:
    """Flat list of shards with unknown sort stability"""
    return session.describe_stream(
        stream_arn=stream["stream_arn"],
        first_shard=first_shard
    )["Shards"]


def rebuild_shard_forest(session: SessionWrapper, stream: Dict[str, str]) -> None:
    """Clean up shards that rolled off, unpack flat list into existing (or empty) shard forest"""
    list_shards(session=session, stream=stream)
    # TODO unpack shards
    pass


class Shard:
    def __init__(self, session: SessionWrapper, *,
                 stream_arn: str, shard_id: str, iterator_type: str,
                 sequence_number: str=None, iterator_id: str=None):
        """Call ``shard.refresh()`` before ``next(shard)`` or the shard will immediately be exhausted"""
        self.session = session
        self.stream_arn = stream_arn
        self.shard_id = shard_id
        self.iterator_id = iterator_id
        self.iterator_type = iterator_type
        self.sequence_number = sequence_number

        # True when GetRecords didn't return an iterator_id and the buffer is empty
        # Can't use iterator_id is None, since that will be true before the first iterator is created
        self.exhausted = False

        # popleft <- [foo, bar, baz] <- append[extend]
        self.buffer = collections.deque()

    def heartbeat(self) -> None:
        # No way to keep the iterator alive, there's nothing
        # to do after it becomes exhausted
        if self.exhausted:
            return

        # Don't need to heartbeat an iterator that can be recreated deterministically
        if self.iterator_type not in {"trim_horizon", "latest"}:
            return

        # can raise RecordsExpired, ShardIteratorExpired
        response = self.session.get_stream_records(self.iterator_id)
        records = response.get("Records", [])
        self.iterator_id = response.get("NextShardIterator", None)
        if records:
            # Success!  We've got a sequence number
            self.sequence_number = records[0]["dynamodb"]["SequenceNumber"]
            self.iterator_type = "at_record"
            self.buffer.extend(records)

    def jump(self, position: str) -> None:
        """Jump to ``trim_horizon`` or ``latest``"""
        if position not in {"trim_horizon", "latest"}:
            raise InvalidShardIterator("Can only jump to trim_horizon or latest.")
        self.iterator_id = None
        self.iterator_type = position
        self.sequence_number = None
        self.exhausted = False
        self.refresh()

    def seek(self, position: arrow.Arrow) -> None:
        self.iterator_id = None
        self.iterator_type = "trim_horizon"
        self.sequence_number = None
        self.exhausted = False
        self.refresh()

        # This doesn't iter over self because the per-record overhead of manipulating the
        # shard state is huge in the cases where iterating the Shard is already taxing
        # (that is, a Shard with millions of records).  Instead, we can shortcut by
        # checking records[-1] and then iterating the first record set that
        # has a record created after the position.
        total_empty_responses = 0
        while total_empty_responses < CALLS_TO_REACH_HEAD:
            try:
                response = self.session.get_stream_records(self.iterator_id)
                records = response.get("Records", [])
                self.iterator_id = response.get("NextShardIterator", None)

                if records:
                    last_create_time = records[-1]["dynamodb"]["ApproximateCreationDateTime"]
                    if arrow.get(last_create_time) >= position:
                        # Found a record set that overlaps position!
                        # Now to find the earliest record that overlaps.
                        first_after = first_overlap(records, position)
                        self.buffer.extend(records[first_after:])
                        self.iterator_type = "at_sequence"
                        self.sequence_number = records[first_after]["dynamodb"]["SequenceNumber"]
                        return
                else:
                    # No results. increment count so we can stop searching an open shard after a reasonable
                    # effort. total_empty_responses is never reset, because after we reach CALLS_TO_REACH_HEAD,
                    # we would be at the head of a shard even in the pathological case of a closed, empty shard.
                    # (A closed empty shard takes approximately CALLS_TO_REACH_HEAD calls to fully traverse)
                    total_empty_responses += 1

                # This MUST be after the if records: block above.  Otherwise, we might break on the last
                # record set, when the desired position was within that record set.
                if not self.iterator_id:
                    # There isn't another record set to check after this, so just bail
                    break

            # The standard exception re-wrap to provide more info
            except RecordsExpired as records_expired:
                raise RecordsExpired.for_iterator(self) from records_expired
            except ShardIteratorExpired as shard_expired:
                raise ShardIteratorExpired.for_iterator(self) from shard_expired

        raise SeekFailed.for_iterator(self)

    def refresh(self) -> None:
        if self.exhausted:
            # There's no point to refreshing an exhausted iterator, since
            # there won't be another iterator id to get records from.
            return
        # can raise RecordsExpired
        self.iterator_id = self.session.get_shard_iterator(
            stream_arn=self.stream_arn,
            shard_id=self.shard_id,
            iterator_type=self.iterator_type,
            sequence_number=self.sequence_number)
        self.buffer.clear()

    def __iter__(self):
        return self

    def __next__(self):
        # Can't get more elements and buffer is empty
        if self.exhausted:
            return None

        record = self._yield_from_buffer()
        if record:
            return record

        try:
            response = self.session.get_stream_records(self.iterator_id)
            self.buffer.extend(response.get("Records", []))
            self.iterator_id = response.get("NextShardIterator", None)
        except RecordsExpired as records_expired:
            raise RecordsExpired.for_iterator(self) from records_expired
        except ShardIteratorExpired as shard_expired:
            # We could call refresh, but it's up to the caller to decide.
            # It's also not clear what to do when a trim_horizon/latest iterator expires.

            # Most likely, the caller will check self.iterator_type and self.refresh()
            # on at/after sequence, and raise on trim_horizon/latest.
            raise ShardIteratorExpired.for_iterator(self) from shard_expired

        return self._yield_from_buffer()

    def _yield_from_buffer(self) -> Optional[Dict]:
        """Store sequence_number of yielded record so we can rebuild the iterator."""
        record = None
        if self.buffer:
            record = self.buffer.popleft()

        # Can't do this check first in case the popleft above removed the last record
        if not self.iterator_id and not self.buffer:
            self.exhausted = True

        # Track last yielded state
        if record:
            self.sequence_number = record["dynamodb"]["SequenceNumber"]
            self.iterator_type = "after_sequence"
        return record


class Coordinator:
    def __init__(self, session: SessionWrapper):
        self.session = session
        self.buffer = collections.deque()

    def __iter__(self):
        return self

    def __next__(self):
        """
        Returns
            Optional[{
                "keys": Optional[Dict],
                "new": Optional[Dict],
                "old": Optional[Dict],
                "event_type": Union["insert", "modify", "delete"]
            }]
        """
        # TODO THE MAGIC GOES HERE
        # Buffer where there's more results
        return {}

    def heartbeat(self) -> None:
        """Force refresh iterators without sequence numbers"""
        # TODO get_records on shard iterators without a sequence_number
        pass

    def jump(self, endpoint: str) -> None:
        """Jump to ``trim_horizon`` or ``latest``.

        This is a fast operation that will jump to either end of the stream.  This does not mean
        that every shard iterator will move to ``trim_horizon`` or ``latest``.  For example, only the root
        shards (oldest in each tree) will be part of the StreamIterator after jumping to ``trim_horizon``.
        When jumping to ``latest``, only the leaf shards (newest in each tree) will be part of the
        StreamIterator.  Leaf shards aren't necessarily open (ie. a disabled stream, or a reduction in provisioned
        throughput).
        """
        # TODO
        return

    def seek(self, position: arrow.Arrow) -> None:
        """Seek through the stream for the desired position in time.

        This is an *expensive* operation.  Seeking to an arbitrary position in time will require partially
        or fully iterating most (or all) Shards in the Stream.  At the moment, seek is O(N) for GetRecords calls
        over both open and closed shards.  A more clever algorithm could use probing to cut down the search space,
        but still has worst case O(N) performance and in practice won't save that many calls.  Open shards
        """
        # TODO
        return

    def load(self, token: collections.Mapping) -> None:
        """Update the stream to match the token's state as closely as possible.

        When loading an old token the stream may not be in the same state as when the token was created.  The following
        edge cases are handled automatically:
          - When the token references a non-existent (moved past trim_horizon) shard, that shard is ignored
          - When a sequence number is past that shard's trim_horizon, the iterator is set to trim_horizon
          - When the stream includes a shard not referenced in the token, its iterator is set to trim_horizon
        """
        # TODO
        return

    @property
    def token(self) -> Dict:
        """Dict of current state"""
        # TODO the whole all of it

        # TODO warn when including ``trim_horizon`` or ``latest`` iterators, since they
        # TODO   represent an abstract time; ``latest`` when the token is created would be
        # TODO   very different from ``latest`` when the token is re-used a day later.
        return {}


class Stream:
    """Provides an approximate iterator over all Records in all Shards in a Stream.

    There are no guarantees or bounds on ordering (you can't order records in different shards, in general) but
    in practice, this will provide a close approximation of the order that changes occurred across an entire Model's
    table.

    Examples
    ========

    # This could be part of a replication process, where
    # most of the table has been copied (+/- the last 12 hours).
    # Now, this should catch up changes missed during the bulk move,
    # and apply new changes as they come in.

    stream = engine.stream(Model, position="trim_horizon")

    # Heartbeat slightly more often than the lifetime of an iterator
    next_heartbeat = lambda: arrow.now().replace(12 * 60)

    heartbeat_at = next_heartbeat()
    for records in stream:
        if records:
            for record in records:
                replicate(record)
        if arrow.now() > next_heartbeat:
            next_heartbeat = calculate_next_heartbeat()
            stream.heartbeat()
    """
    def __init__(self, *, engine, model, session: SessionWrapper):
        self.engine = engine
        self.model = model
        self.coordinator = Coordinator(session)

    def __repr__(self):
        # <Stream[User]>
        return "<{}[{}]>".format(self.__class__, self.model.__name__)

    def __iter__(self):
        return self

    def __next__(self):
        record = next(self.coordinator, None)
        if record is None:
            return record
        meta = self.model.Meta
        self._unpack(record, "new", meta.columns)
        self._unpack(record, "old", meta.columns)
        self._unpack(record, "key", meta.keys)

    @property
    def token(self) -> Dict:
        """Dict that can be used to reconstruct the current progress of the iterator.

        Example
        =======

        with open(".stream-state", "w") as f:
            json.dump(stream.token, f)

        # Some time later
        ...

        with open(".stream-state", "r") as f:
            token = json.load(f)

        stream = engine.stream(MyModel, at=token)
        """
        return self.coordinator.token

    def heartbeat(self) -> None:
        """Call periodically to ensure iterators without a fixed sequence number don't expire.

        You should call this once every ~12 minutes so that your "latest" and "trim_horizon" shard iterators don't
        expire.  While iterators have an advertised lifetime of 15 minutes, it would be good to call more frequently
        so you aren't caught by clock skew expiring an iterator.

        When an iterator with a sequence_number expires, it can be re-created deterministically (although that
        create may raise ``RecordsExpired`` if the iterator is now beyond the trim horizon).  However, "latest" and
        "trim_horizon" iterators don't refer to a fixed point in a Shard and so can't be re-created without either:
            (1) possibly missing records between "latest" at 15 minutes ago and "latest" now
            (2) re-iterating the entire stream from trim_horizon until the approximate iterator creation time is
                reached.  This may also fail, if "15 minutes ago" has passed the trim_horizon.

        A periodic call to heartbeat removes the ambiguity without incurring the massive performance impact of
        trying to rebuild an expired "latest" or "trim_horizon" iterator from the beginning of the shard.

        Examples
        ========

        stream = engine.stream(User, position="latest")
        start_heartbeats(stream, freq=12)
        start_processing(stream, processor)

        # heartbeat.py
        def start_heartbeats(stream, freq=12):
            # Inside a thread, or whatever your io model supports
            while True:
                stream.heartbeat()
                time.sleep(freq * 60)

        # process.py
        def start_processing(stream, processor)
            # Inside a thread, or whatever your io model supports
            while True:
                records = next(stream)
                if records:
                    # No sleep when we find records, try to get the next
                    # set as fast as possible
                    processor(records)
                else:
                    # No records on any shard iterators, sleep
                    # a bit so we're not busy polling past the
                    # throttling limit
                    time.sleep(NO_RECORDS_BACKOFF)

        """
        self.coordinator.heartbeat()

    def move_to(self, position) -> None:
        """Updates the StreamIterator to point to the endpoint, time, or token provided.

        - Moving to ``trim_horizon`` or ``latest`` is very fast.
        - Moving to a specific time is slow.
        - Moving to a previous stream's token is somewhere in the middle.
        """
        if position in {"latest", "trim_horizon"}:
            move = self.coordinator.jump
        elif isinstance(position, arrow.Arrow):
            move = self.coordinator.seek
        elif isinstance(position, collections.Mapping):
            move = self.coordinator.load
        else:
            raise ValueError("Don't know how to move to position {!r}".format(position))
        move(position)

    def _unpack(self, record: Dict, key: str, expected: List) -> None:
        """Replaces the attr dict at the given key with an instance of a Model"""
        attrs = record[key]
        if attrs is None:
            return
        obj = unpack_from_dynamodb(
            attrs=attrs,
            expected=expected,
            model=self.model,
            engine=self.engine
        )
        object_loaded.send(self.engine, obj=obj)
        record[key] = obj
