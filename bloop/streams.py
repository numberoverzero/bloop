import arrow
import collections

from typing import Dict, List

from .session import SessionWrapper
from .signals import object_loaded
from .util import unpack_from_dynamodb


CALLS_TO_REACH_HEAD = 5
"""The approximate number of consecutive empty calls required to reach the HEAD of an open shard.
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
    return {
        "stream_arn": shard["stream_arn"],
        "shard_id": shard["shard_id"],
        "iterator_id": iterator_id,
        "type": iterator_type,
        "sequence_number": sequence_number,
        "consecutive_empty_responses": 0
    }


def advance_iterator(session: SessionWrapper, iterator: Dict[str, str]) -> List[Dict]:
    """Updates the iterator's state, and returns any records."""
    response = session.get_stream_records(iterator["iterator_id"])
    records = response.get("Records", [])

    # When the shard is closed and we reach the end, NextShardIterator will be None
    iterator["iterator_id"] = response.get("NextShardIterator", None)

    # Record the last state this iterator saw,
    # so we can rebuild an expired iterator or create a stream token
    if records:
        iterator["consecutive_empty_responses"] = 0
        iterator["sequence_number"] = records[-1]["dynamodb"]["SequenceNumber"]
        iterator["type"] = "after_sequence"
    else:
        # Yet another empty response :(
        iterator["consecutive_empty_responses"] += 1
    return records


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

    def move_to(self, position, strict: bool=False) -> None:
        """Updates the StreamIterator to point to the endpoint, time, or token provided.

        - Moving to ``trim_horizon`` or ``latest`` is very fast.
        - Moving to a time is slow.
        - Moving to a token from a previous stream is somewhere in the middle.

        When ``strict`` is True, moving to a token will raise when:
          - the token includes expired shards
          - the token includes sequence_numbers beyond the trim_horizon
          - the stream includes shards not in the token
        """
        if position in {"latest", "trim_horizon"}:
            self._jump(position)
        elif isinstance(position, arrow.Arrow):
            self._seek(position)
        elif isinstance(position, collections.Mapping):
            self._load(position, strict=strict)
        else:
            # TODO subclass BloopException
            raise ValueError("Unknown position <p>")

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

    def _load(self, token: collections.Mapping, strict: bool) -> None:
        """Update the stream to match the token's state as closely as possible.

        When strict is True, any of the following will cause the stream to stop loading the token:
          - Any shard in the token no longer exists
          - Any sequence_number is beyond it's shards' trim_horizon
          - The stream contains new shards not included in the token
        When strict is False, the above are handled as follows:
          - Non-existent shards are ignored
          - Sequence numbers beyond trim_horizon are set to trim_horizon instead
          - New shards are included in the stream and seek to the approximate time of the existing shards
        """

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
