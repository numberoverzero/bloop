import collections
import collections.abc
import datetime
import logging

from ..exceptions import InvalidPosition, InvalidStream, RecordsExpired
from .buffer import RecordBuffer
from .shard import unpack_shards


logger = logging.getLogger("bloop.stream")


class Coordinator:
    """Encapsulates the shard-level management for a whole Stream.

    :param session: Used to make DynamoDBStreams calls.
    :type session: :class:`~bloop.session.SessionWrapper`
    :param str stream_arn: Stream arn, usually from the model's ``Meta.stream["arn"]``.
    """
    def __init__(self, *, session, stream_arn):

        self.session = session

        # The stream that's being coordinated
        self.stream_arn = stream_arn

        # The oldest shards in each shard tree (no parents)
        self.roots = []

        # Shards being iterated right now
        self.active = []

        # Single buffer for the lifetime of the Coordinator, but mutates frequently
        # Records in the buffer aren't considered read.  When a Record popped from the buffer is
        # consumed, the Coordinator MUST notify the Shard by updating the sequence_number and iterator_type.
        # The new values should be:
        #   shard.sequence_number = record["meta"]["sequence_number"]
        #   shard.iterator_type = "after_record"

        # Holds records from advancing all active shard iterators.
        # Shards aren't advanced again until the buffer drains completely.
        self.buffer = RecordBuffer()

    def __repr__(self):
        # <Coordinator[.../StreamCreation-travis-661.2/stream/2016-10-03T06:17:12.741]>
        return "<{}[{}]>".format(self.__class__.__name__, self.stream_arn)

    def __iter__(self):
        return self

    def __next__(self):
        if not self.buffer:
            self.advance_shards()

        if self.buffer:
            record, shard = self.buffer.pop()

            # Now that the record is "consumed", advance the shard's checkpoint
            shard.sequence_number = record["meta"]["sequence_number"]
            shard.iterator_type = "after_sequence"
            return record

        # No records :(
        return None

    def advance_shards(self):
        """Poll active shards for records and insert them into the buffer.  Rotate exhausted shards.

        Returns immediately if the buffer isn't empty.
        """
        # Don't poll shards when there are pending records.
        if self.buffer:
            return

        # 0) Collect new records from all active shards.
        record_shard_pairs = []
        for shard in self.active:
            records = next(shard)
            if records:
                record_shard_pairs.extend((record, shard) for record in records)
        self.buffer.push_all(record_shard_pairs)

        self._handle_exhausted()

    def heartbeat(self):
        """Keep active shards with "trim_horizon", "latest" iterators alive by advancing their iterators."""
        for shard in self.active:
            if shard.sequence_number is None:
                records = next(shard)
                # Success!  This shard now has an ``at_sequence`` iterator
                if records:
                    self.buffer.push_all((record, shard) for record in records)
        self._handle_exhausted()

    def _handle_exhausted(self):
        # 1) Clean up exhausted Shards.  Can't modify the active list while iterating it.
        to_remove = [shard for shard in self.active if shard.exhausted]
        for shard in to_remove:
            shard.load_children()
            # Also promotes children to the shard's previous roles
            self.remove_shard(shard)
            for child in shard.children:
                child.jump_to(iterator_type="trim_horizon")

    @property
    def token(self):
        """JSON-serializable representation of the current Stream state.

        Use :func:`Engine.stream(YourModel, token) <bloop.engine.Engine.stream>` to create an identical stream,
        or :func:`stream.move_to(token) <bloop.stream.Stream.move_to>` to move an existing stream to this position.

        :returns: Stream state as a json-friendly dict
        :rtype: dict
        """
        shard_tokens = []
        for root in self.roots:
            for shard in root.walk_tree():
                shard_tokens.append(shard.token)
                shard_tokens[-1].pop("stream_arn")
        return {
            "stream_arn": self.stream_arn,
            "active": [shard.shard_id for shard in self.active],
            "shards": shard_tokens
        }

    def remove_shard(self, shard):
        """Remove a Shard from the Coordinator.  Drops all buffered records from the Shard.

        If the Shard is active or a root, it is removed and any children promoted to those roles.

        :param shard: The shard to remove
         :type shard: :class:`~bloop.stream.shard.Shard`
        """
        try:
            self.roots.remove(shard)
        except ValueError:
            # Wasn't a root Shard
            pass
        else:
            self.roots.extend(shard.children)

        try:
            self.active.remove(shard)
        except ValueError:
            # Wasn't an active Shard
            pass
        else:
            self.active.extend(shard.children)

        # TODO can this be improved?  Gets expensive for high-volume streams with large buffers
        heap = self.buffer.heap
        # Clear buffered records from the shard.  Each record is (ordering, record, shard)
        to_remove = [x for x in heap if x[2] is shard]
        for x in to_remove:
            heap.remove(x)

    def move_to(self, position):
        """Set the Coordinator to a specific endpoint or time, or load state from a token.

        :param position: "trim_horizon", "latest", :class:`~datetime.datetime`, or a
            :attr:`Coordinator.token <bloop.stream.coordinator.Coordinator.token>`
        """
        if isinstance(position, collections.abc.Mapping):
            move = _move_stream_token
        elif hasattr(position, "timestamp") and callable(position.timestamp):
            move = _move_stream_time
        elif isinstance(position, str) and position.lower() in ["latest", "trim_horizon"]:
            move = _move_stream_endpoint
        else:
            raise InvalidPosition("Don't know how to move to position {!r}".format(position))
        move(self, position)


def _move_stream_endpoint(coordinator, position):
    """Move to the "trim_horizon" or "latest" of the entire stream."""
    # 0) Everything will be rebuilt from DescribeStream.
    stream_arn = coordinator.stream_arn
    coordinator.roots.clear()
    coordinator.active.clear()
    coordinator.buffer.clear()

    # 1) Build a Dict[str, Shard] of the current Stream from a DescribeStream call
    current_shards = coordinator.session.describe_stream(stream_arn=stream_arn)["Shards"]
    current_shards = unpack_shards(current_shards, stream_arn, coordinator.session)

    # 2) Roots are any shards without parents.
    coordinator.roots.extend(shard for shard in current_shards.values() if not shard.parent)

    # 3.0) Stream trim_horizon is the combined trim_horizon of all roots.
    if position == "trim_horizon":
        for shard in coordinator.roots:
            shard.jump_to(iterator_type="trim_horizon")
        coordinator.active.extend(coordinator.roots)
    # 3.1) Stream latest is the combined latest of all shards without children.
    else:
        for root in coordinator.roots:
            for shard in root.walk_tree():
                if not shard.children:
                    shard.jump_to(iterator_type="latest")
                    coordinator.active.append(shard)


def _move_stream_time(coordinator, time):
    """Scan through the *entire* Stream for the first record after ``time``.

    This is an extremely expensive, naive algorithm that starts at trim_horizon and simply
    dumps records into the void until the first hit.  General improvements in performance are
    tough; we can use the fact that Shards have a max life of 24hr to pick a pretty-good starting
    point for any Shard trees with 6 generations.  Even then we can't know how close the oldest one
    is to rolling off so we either hit trim_horizon, or iterate an extra Shard more than we need to.

    The corner cases are worse; short trees, recent splits, trees with different branch heights.
    """
    if time > datetime.datetime.now(datetime.timezone.utc):
        _move_stream_endpoint(coordinator, "latest")
        return

    _move_stream_endpoint(coordinator, "trim_horizon")
    shard_trees = collections.deque(coordinator.roots)
    while shard_trees:
        shard = shard_trees.popleft()
        records = shard.seek_to(time)

        # Success!  This section of some Shard tree is at the desired time.
        if records:
            coordinator.buffer.push_all((record, shard) for record in records)

        # Closed shard, keep searching its children.
        elif shard.exhausted:
            coordinator.remove_shard(shard)
            shard_trees.extend(shard.children)


def _move_stream_token(coordinator, token):
    """Move to the Stream position described by the token.

    The following rules are applied when interpolation is required:
    - If a shard does not exist (past the trim_horizon) it is ignored.  If that
      shard had children, its children are also checked against the existing shards.
    - If none of the shards in the token exist, then InvalidStream is raised.
    - If a Shard expects its iterator to point to a SequenceNumber that is now past
      that Shard's trim_horizon, the Shard instead points to trim_horizon.
    """
    stream_arn = coordinator.stream_arn = token["stream_arn"]
    # 0) Everything will be rebuilt from the DescribeStream masked by the token.
    coordinator.roots.clear()
    coordinator.active.clear()
    coordinator.buffer.clear()

    # Injecting the token gives us access to the standard shard management functions
    token_shards = unpack_shards(token["shards"], stream_arn, coordinator.session)
    coordinator.roots = [shard for shard in token_shards.values() if not shard.parent]
    coordinator.active.extend(token_shards[shard_id] for shard_id in token["active"])

    # 1) Build a Dict[str, Shard] of the current Stream from a DescribeStream call
    current_shards = coordinator.session.describe_stream(stream_arn=stream_arn)["Shards"]
    current_shards = unpack_shards(current_shards, stream_arn, coordinator.session)

    # 2) Trying to find an intersection with the actual Stream by walking each root shard's tree.
    #    Prune any Shard with no children that's not part of the actual Stream.
    #    Raise InvalidStream if the entire token is pruned.
    unverified = collections.deque(coordinator.roots)
    while unverified:
        shard = unverified.popleft()
        if shard.shard_id not in current_shards:
            logger.info("Unknown or expired shard \"{}\" - pruning from stream token".format(shard.shard_id))
            coordinator.remove_shard(shard)
            unverified.extend(shard.children)

    # 3) Everything was pruned, so the token describes an unknown stream.
    if not coordinator.roots:
        raise InvalidStream("This token has no relation to the actual Stream.")

    # 4) Now that everything's verified, grab new iterators for the coordinator's active Shards.
    for shard in coordinator.active:
        try:
            if shard.iterator_type is None:
                # Descendant of an unknown shard
                shard.iterator_type = "trim_horizon"
            # Move back to the token's specified position
            shard.jump_to(iterator_type=shard.iterator_type, sequence_number=shard.sequence_number)
        except RecordsExpired:
            # This token shard's sequence_number is beyond the trim_horizon.
            # The next closest record is at trim_horizon.
            msg = "SequenceNumber \"{}\" in shard \"{}\" beyond trim horizon: jumping to trim_horizon"
            logger.info(msg.format(shard.sequence_number, shard.shard_id))
            shard.jump_to(iterator_type="trim_horizon")
