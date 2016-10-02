import arrow
import collections

from typing import Dict, List

from .exceptions import RecordsExpired, ShardIteratorExpired
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


def list_shards(session: SessionWrapper, stream: Dict[str, str], first_shard: Dict[str, str]=None) -> List[Dict]:
    """Flat list of shards with unknown sort stability"""
    if first_shard:
        first_shard = first_shard["shard_id"]
    description = session.describe_stream(stream_arn=stream["stream_arn"], first_shard=first_shard)
    return description["Shards"]


def rebuild_shard_forest(session: SessionWrapper, stream: Dict[str, str]) -> None:
    """Clean up shards that rolled off, unpack flat list into existing (or empty) shard forest"""
    list_shards(session=session, stream=stream)
    # TODO unpack shards
    pass


def get_iterator(
        session: SessionWrapper, *,
        shard: Dict[str, str], iterator_type: str,
        sequence_number: str=None) -> Dict[str, str]:
    """Create an iterator dict for a shard at a given location (or trim_horizon, or latest)"""
    iterator_id = session.get_shard_iterator(
        stream_arn=shard["stream_arn"], shard_id=shard["shard_id"],
        iterator_type=iterator_type, sequence_number=sequence_number)
    # TODO should be a class so users can call iterator.move_to(...) when they get an
    return {
        "stream_arn": shard["stream_arn"],
        "shard_id": shard["shard_id"],
        "iterator_id": iterator_id,
        "type": iterator_type,
        "sequence_number": sequence_number,
    }


def advance_iterator(session: SessionWrapper, iterator: Dict[str, str]) -> List[Dict]:
    """Updates the iterator's state, and returns any records."""
    try:
        response = session.get_stream_records(iterator["iterator_id"])
    except ShardIteratorExpired as exception:
        # iterator is more than 15 minutes old
        raise ShardIteratorExpired.for_iterator(iterator) from exception
    except RecordsExpired as exception:
        # this section of the shard is beyond the trim horizon
        raise RecordsExpired.for_iterator(iterator) from exception
    records = response.get("Records", [])

    # When the shard is closed and we reach the end, NextShardIterator will be None
    iterator["iterator_id"] = response.get("NextShardIterator", None)

    # Record the last state this iterator saw,
    # so we can rebuild an expired iterator or create a stream token
    if records:
        # TODO this should be based on advancing the iterator's buffer, and only updated after an
        # TODO object is read from the buffer
        iterator["sequence_number"] = records[-1]["dynamodb"]["SequenceNumber"]
        iterator["type"] = "after_sequence"
    return records


def refresh_iterator(session: SessionWrapper, *, iterator: Dict[str, str], shard: Dict[str, str]) -> None:
    """Update the iterator in place to use a new iterator_id"""
    # Can't deterministically refresh an iterator without a sequence number
    if iterator["type"] in {"trim_horizon", "latest"}:
        raise ShardIteratorExpired.for_iterator(iterator)
    same_iterator = get_iterator(
        session, shard=shard,
        iterator_type=iterator["type"],
        sequence_number=iterator["sequence_number"]
    )
    iterator["iterator_id"] = same_iterator["iterator_id"]


class StreamIterator:
    def __init__(self, *, session, **kwargs):
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
        # TODO get_records on shard iterators without a sequence_number
        pass

    def move_to(self, position) -> None:
        """Updates the StreamIterator to point to the endpoint, time, or token provided.

        - Moving to ``trim_horizon`` or ``latest`` is very fast.
        - Moving to a specific time is slow.
        - Moving to a previous stream's token is somewhere in the middle.
        """
        if position in {"latest", "trim_horizon"}:
            move = self._jump
        elif isinstance(position, arrow.Arrow):
            move = self._seek
        elif isinstance(position, collections.Mapping):
            move = self._load
        else:
            raise ValueError("Don't know how to move to position {!r}".format(position))
        move(position)

    def _jump(self, endpoint: str) -> None:
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

    def _seek(self, position: arrow.Arrow) -> None:
        """Seek through the stream for the desired position in time.

        This is an *expensive* operation.  Seeking to an arbitrary position in time will require partially
        or fully iterating most (or all) Shards in the Stream.  At the moment, seek is O(N) for GetRecords calls
        over both open and closed shards.  A more clever algorithm could use probing to cut down the search space,
        but still has worst case O(N) performance and in practice won't save that many calls.  Open shards
        """
        # TODO
        return

    def _load(self, token: collections.Mapping) -> None:
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
    def token(self):
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
        # TODO the whole all of it

        # TODO warn when including ``trim_horizon`` or ``latest`` iterators, since they
        # TODO   represent an abstract time; ``latest`` when the token is created would be
        # TODO   very different from ``latest`` when the token is re-used a day later.
        return {}


class Stream(StreamIterator):
    def __init__(self, *, engine, model, **kwargs):
        self.engine = engine
        self.model = model
        super().__init__(**kwargs)

    def __repr__(self):
        # <Stream[User]>
        return "<{}[{}]>".format(self.__class__, self.model.__name__)

    def __next__(self):
        record = super().__next__()
        if record is None:
            return record
        meta = self.model.Meta
        self._unpack(record, "new", meta.columns)
        self._unpack(record, "old", meta.columns)
        self._unpack(record, "key", meta.keys)

    def _unpack(self, record, key, expected):
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
