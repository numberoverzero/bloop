from typing import Dict, List, MutableMapping, Any, Mapping, Optional

from ..session import SessionWrapper
from ..signals import object_loaded
from ..util import unpack_from_dynamodb

from .coordinator import Coordinator

__all__ = ["Stream"]


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
        self.coordinator = Coordinator(engine=engine, session=session, stream_arn=model.Meta.stream["arn"])

    def __repr__(self):
        # <Stream[User]>
        return "<{}[{}]>".format(self.__class__.__name__, self.model.__name__)

    def __iter__(self):
        return self

    def __next__(self) -> Optional[Mapping[str, Any]]:
        record = next(self.coordinator)
        if record:
            meta = self.model.Meta
            self._unpack(record, "new", meta.columns)
            self._unpack(record, "old", meta.columns)
            self._unpack(record, "key", meta.keys)
        return record

    @property
    def token(self) -> Dict[str, Any]:
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
        self.coordinator.move_to(position)

    def _unpack(self, record: MutableMapping[str, Any], key: str, expected: List) -> None:
        """Replaces the attr dict at the given key with an instance of a Model"""
        attrs = record.get(key)
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
