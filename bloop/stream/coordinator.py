import arrow
import collections
from typing import Dict, List, Optional, Any, Mapping  # noqa

from .buffer import RecordBuffer
from .shard import Shard, unpack_shards
from ..exceptions import InvalidStream, RecordsExpired, InvalidPosition
from ..session import SessionWrapper


class Coordinator:
    def __init__(self, *, engine, session: SessionWrapper, stream_arn: str):
        # Set once on creation, never changes
        self.engine = engine
        # Set once on creation, never changes
        self.session = session
        # Set once on creation, never changes
        self.stream_arn = stream_arn
        # Changes infrequently, set initially
        self.roots = []  # type: List[Shard]
        # Changes infrequently, set initially
        self.active = []  # type: List[Shard]
        # Single buffer for the lifetime of the Coordinator, but mutates frequently
        # Records in the buffer aren't considered read.  When a Record popped from the buffer is
        # consumed, the Coordinator MUST notify the Shard by updating the sequence_number and iterator_type.
        # The new values should be:
        #   shard.sequence_number = record["meta"]["sequence_number"]
        #   shard.iterator_type = "after_record"
        self.buffer = RecordBuffer()

    def __repr__(self):
        # <Coordinator[.../StreamCreation-travis-661.2/stream/2016-10-03T06:17:12.741]>
        return "<{}[{}]>".format(self.__class__.__name__, self.stream_arn)

    def __next__(self) -> Optional[Dict[str, Any]]:
        # Try to get the next record from each shard and push it into the buffer.
        if not self.buffer:
            self.advance_shards()

        # Still have buffered records from a previous call,
        # or we just refilled the buffer above.
        if self.buffer:
            record, shard = self.buffer.pop()

            # Now that the record is "consumed", advance the shard's checkpoint
            shard.sequence_number = record["meta"]["sequence_number"]
            shard.iterator_type = "after_sequence"
            return record

        # No records :(
        return None

    def advance_shards(self) -> None:
        """Tries to refill the buffer by collecting records from the active shards.

        Rotates exhausted shards.
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

    def heartbeat(self) -> None:
        # Try to keep active shards with ``latest`` and ``trim_horizon`` iterators alive.
        # Ideally, this will find records and make them ``at_sequence`` or ``after_sequence`` iterators.
        for shard in self.active:
            if shard.sequence_number is None:

                # There's no safe default when advance_shard raises ShardIteratorExpired
                # because resetting to the new trim_horizon/latest could miss records.
                # Had the user called Stream.heartbeat() within 15 minutes, this wouldn't happen.

                # Don't need to handle RecordsExpired because only sequence_number-based
                # iterators can fall behind the trim_horizon.
                records = next(shard)
                # Success!  This shard now has an ``at_sequence`` iterator
                if records:
                    self.buffer.push_all((record, shard) for record in records)
        self._handle_exhausted()

    def _handle_exhausted(self):
        # 1) Clean up exhausted Shards.
        #    Can't modify the active list while iterating it.
        to_remove = [shard for shard in self.active if shard.exhausted]
        for shard in to_remove:
            # A) Fetch Shard's children if they haven't been loaded
            #    (perhaps the Shard just closed?)
            shard.load_children()

            # B) Remove the shard from the Coordinator.  If the Shard has
            #    children, those children are now active.
            #    If the Shard was a root, those children become roots.
            self.remove_shard(shard)

            # C) Move each child Shard to its trim_horizon.
            for child in shard.children:
                child.jump_to(iterator_type="trim_horizon")

    @property
    def token(self) -> Dict[str, Any]:
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

    def remove_shard(self, shard: Shard) -> None:
        # try/catch avoids the O(N) search of using `if shard in ...`

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

        # Remove any records in the buffer that came from the shard
        # ( ordering,  ( record,  shard ) )
        #               |----- x[1] -----|
        #        shard = x[1][1] ---^
        # TODO can this be improved?  Gets expensive for high-volume streams with large buffers
        heap = self.buffer.heap
        to_remove = [x for x in heap if x[1][1] is shard]
        for x in to_remove:
            heap.remove(x)

    def move_to(self, position) -> None:
        if isinstance(position, Mapping):
            move = _move_stream_token
        elif isinstance(position, arrow.Arrow):
            move = _move_stream_time
        elif isinstance(position, str) and position.lower() in ["latest", "trim_horizon"]:
            move = _move_stream_endpoint
        else:
            raise InvalidPosition("Don't know how to move to position {!r}".format(position))
        move(self, position)


def _move_stream_endpoint(coordinator: Coordinator, position: str) -> None:
    """Move to the whole stream's ``trim_horizon`` or ``latest``."""
    # 0) Clear everything; all shards will be rebuilt from the most
    #    recent values in DynamoDBStreams, so there's no
    #    need to keep any previous state.
    stream_arn = coordinator.stream_arn
    coordinator.roots.clear()
    coordinator.active.clear()
    coordinator.buffer.clear()

    # 1) Build a Dict[str, Shard] of the current Stream from a DescribeStream call
    current_shards = coordinator.session.describe_stream(stream_arn=stream_arn)["Shards"]
    current_shards = unpack_shards(current_shards, stream_arn, coordinator.session)

    # 2) Extract the roots, which are any shards without parents
    coordinator.roots.extend(shard for shard in current_shards.values() if not shard.parent)

    # 3) The Stream's trim_horizon is the combined trim_horizon of all root Shards.
    #    Move all the roots to their trim_horizon, and promote them all to active.
    if position == "trim_horizon":
        for shard in coordinator.roots:
            shard.jump_to(iterator_type="trim_horizon")
        coordinator.active.extend(coordinator.roots)
    # 4) Similarly, the combined ``latest`` of all Shards without
    #    children defines the Stream ``latest``.
    else:
        for root in coordinator.roots:
            for shard in root.walk_tree():
                if not shard.children:
                    shard.jump_to(iterator_type="latest")
                    coordinator.active.append(shard)


def _move_stream_time(coordinator: Coordinator, time: arrow.Arrow) -> None:
    """Scan through the *entire* Stream for the first record after ``time``.

    This is an extremely expensive, naive algorithm that starts at trim_horizon and simply
    dumps records into the void until the first hit.  General improvements in performance are
    tough; we can use the fact that Shards have a max life of 24hr to pick a pretty-good starting
    point for any Shard trees with 6 generations.  Even then we can't know how close the oldest one
    is to rolling off so we either hit trim_horizon, or iterate an extra Shard more than we need to.

    The corner cases are worse; short trees, recent splits, trees with different branch heights.
    """
    # This isn't a general "wait until time" method,
    # it's for jumping to a time on the range [-24h, now].
    if time > arrow.now():
        _move_stream_endpoint(coordinator, "latest")
        return

    # Start at the beginning
    _move_stream_endpoint(coordinator, "trim_horizon")
    shard_trees = collections.deque(coordinator.roots)
    while shard_trees:
        shard = shard_trees.popleft()
        records = shard.seek_to(time)

        # Success!  This section of some Shard tree is at the desired time.
        if records:
            coordinator.buffer.push_all((record, shard) for record in records)

        # Closed shard, we need to go through its children next.
        elif shard.exhausted:
            coordinator.remove_shard(shard)
            shard_trees.extend(shard.children)

        # Nothing to do if the shard didn't seek to the time AND isn't exhausted,
        # since that means it's open and the target could still appear in its future.


def _move_stream_token(coordinator: Coordinator, token: Mapping[str, Any]) -> None:
    """Move to the Stream position described by the token.

    The following rules are applied when interpolation is required:
    - If a shard does not exist (past the trim_horizon) it is ignored.  If that
      shard had children, its children are also checked against the existing shards.
    - If none of the shards in the token exist, then InvalidStream is raised.
    - If a Shard expects its iterator to point to a SequenceNumber that is now past
      that Shard's trim_horizon, the Shard instead points to trim_horizon.
    """
    stream_arn = coordinator.stream_arn = token["stream_arn"]
    coordinator.roots.clear()
    coordinator.active.clear()
    coordinator.buffer.clear()

    # 0) Load the token into the coordinator so we can re-use the normal
    #    processing utilities to prune and add Shards as necessary.
    token_shards = unpack_shards(token["shards"], stream_arn, coordinator.session)
    coordinator.roots = [shard for shard in token_shards.values() if not shard.parent]
    coordinator.active.extend(token_shards[shard_id] for shard_id in token["active"])

    # 1) Build a Dict[str, Shard] of the current Stream from a DescribeStream call
    current_shards = coordinator.session.describe_stream(stream_arn=stream_arn)["Shards"]
    current_shards = unpack_shards(current_shards, stream_arn, coordinator.session)

    # 2) Walk each the Shard tree of each root shard from the token, to find an intersection with the actual
    #    shards that exist. If there's any Shard with no children AND it's not part of the returned shards
    #    from DynamoDBStreams, that branch of the shard tree is pruned.  If none of the token was recognized,
    #    (all the shards were pruned) then InvalidStream is raised; there's no way to associate that token
    #    with the current Stream.
    unverified = collections.deque(coordinator.roots)
    while unverified:
        shard = unverified.popleft()
        if shard.shard_id not in current_shards:
            # TODO: log at WARNING for unrecognized shard id
            coordinator.remove_shard(shard)
            # Keep trying to verify the children; maybe one of them links to the current Stream?
            unverified.extend(shard.children)

    # 3) If *none* of the shard ids in the token exist, then the coordinator won't have any roots.
    #    We can guess that dead branches should be ignored (Provisioned Throughput decreases) but
    #    there needs to be *some* link to the current Stream.
    if not coordinator.roots:
        raise InvalidStream("This token has no relation to the actual Stream.")

    # 4) Now that everything's verified, grab new iterators for the coordinator's active Shards.
    for shard in coordinator.active:
        try:
            # If an active shard from the token doesn't exist but its descendant does,
            # that descendant becomes active.  The descendant's token wouldn't have an iterator type.
            # The descendant's trim_horizon is the closest to the ancestor's point that we can get.
            if shard.iterator_type is None:
                shard.iterator_type = "trim_horizon"
            # Move right back to where we left off.
            shard.jump_to(iterator_type=shard.iterator_type, sequence_number=shard.sequence_number)
        except RecordsExpired:
            # The sequence_number that this iterator was on is beyond the trim_horizon.
            # There's no way to recover the records between the expected sequence_number and the
            # trim_horizon, so just jump to the trim_horizon.  This is the same interpolation
            # performed for descendants of unknown Shards, just applied at the SequenceNumber level.
            shard.jump_to(iterator_type="trim_horizon")
            # TODO logger.info "SequenceNumber from token was past trim_horizon, moving to trim_horizon instead"

    # Done!  Each shard is now at its last state
