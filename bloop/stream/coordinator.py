import arrow
import collections
from typing import Dict, List, Optional, Any, Mapping  # noqa

from .buffer import RecordBuffer
from .shard import Shard, unpack_shards
from ..exceptions import InvalidStream, RecordsExpired
from ..session import SessionWrapper


def reformat_record(record: Mapping[str, Any]) -> Dict[str, Any]:
    """Repack a record into a cleaner structure for consumption."""
    # Unwrap the inner structure, since most of it comes from here
    return {
        "key": record["dynamodb"].get("Keys", None),
        "new": record["dynamodb"].get("NewImage", None),
        "old": record["dynamodb"].get("OldImage", None),

        "meta": {
            "created_at": arrow.get(record["dynamodb"]["ApproximateCreationDateTime"]),
            "event": {
                "id": record["eventID"],
                "type": record["eventName"].lower(),
                "version": record["eventVersion"]
            },
            "sequence_number": record["dynamodb"]["SequenceNumber"],
        }
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
        self.roots = []  # type: List[Shard]
        # Changes infrequently, set initially
        self.active = []  # type: List[Shard]
        # Single buffer for the lifetime of the Coordinator, but mutates frequently
        # Records in the buffer aren't considered read.  When a Record popped from the buffer is
        # consumed, the Coordinator MUST notify the Shard by updating the sequence_number and iterator_type.
        # The new values should be:
        #   shard.sequence_number = record["dynamodb"]["SequenceNumber"]
        #   shard.iterator_type = "after_record"
        self.buffer = RecordBuffer()

    def __iter__(self):
        return self

    def __next__(self) -> Optional[Dict[str, Any]]:
        # Try to get the next record from each shard and push it into the buffer.
        if not self.buffer:
            record_shard_pairs = []
            for shard in self.active:
                records = next(shard)
                if records:
                    record_shard_pairs.extend((record, shard) for record in records)
            self.buffer.push_all(record_shard_pairs)

            # Clean up exhausted Shards.
            # Can't modify the active list while iterating it.
            to_remove = [shard for shard in self.active if shard.exhausted]
            for shard in to_remove:
                # 0) Fetch Shard's children if they haven't been loaded
                #    (perhaps the Shard just closed?)
                shard.load_children()

                # 1) Remove the shard from the Coordinator.  If the Shard has
                #    children and was active, those children are added to the active list
                #    If the Shard was a root, those children become roots.
                was_active = shard in self.active
                self.remove_shard(shard)

                # 2) If the shard was active, now its children are active.
                #    Move each child Shard to its trim_horizon.
                if was_active:
                    for child in shard.children:
                        # Pick up right where the removed Shard left off
                        child.jump_to(self.session, iterator_type="trim_horizon")
                        # The child's previous empty responses have no
                        # bearing on its new position at the trim_horizon.
                        child.empty_responses = 0

        # Still have buffered records from a previous call, or we just refilled the buffer above
        if self.buffer:
            record, shard = self.buffer.pop()

            # Now that the record is "consumed", advance the shard's checkpoint
            shard.sequence_number = record["dynamodb"]["SequenceNumber"]
            shard.iterator_type = "after_sequence"
            return reformat_record(record)

        # No records :(
        return None

    def heartbeat(self) -> None:
        # Try to keep active shards with ``latest`` and ``trim_horizon`` iterators alive.
        # Ideally, this will find records and make them ``at_sequence`` or ``after_sequence`` iterators.
        for shard in self.active:
            if shard.iterator_type in {"latest", "trim_horizon"}:

                # There's no safe default when advance_shard raises ShardIteratorExpired
                # because resetting to the new trim_horizon/latest could miss records.
                # Had the user called Stream.heartbeat() within 15 minutes, this wouldn't happen.

                # Don't need to handle RecordsExpired because only sequence_number-based
                # iterators can fall behind the trim_horizon.
                records = next(shard)
                # Success!  This shard now has an ``at_sequence`` iterator
                if records:
                    self.buffer.push_all((record, shard) for record in records)

    @property
    def token(self) -> Dict[str, Any]:
        shard_tokens = []
        for root in self.roots:
            for shard in root.walk_tree():
                token = shard.token
                token.pop("stream_arn")
                shard_tokens.append(token)
        return {
            "stream_arn": self.stream_arn,
            "active": [shard.shard_id for shard in self.active],
            "shards": shard_tokens
        }

    def remove_shard(self, shard: Shard) -> None:
        # try/catch avoids the O(N) search of using `if shard in ...`

        try:
            # If the Shard was a root, remove it and promote its children to roots
            self.roots.remove(shard)
            if shard.children:
                self.roots.extend(shard.children)
        except ValueError:
            pass

        try:
            # If the Shard was active, remove it and set its children to active.
            self.active.remove(shard)
            if shard.children:
                self.active.extend(shard.children)
        except ValueError:
            pass

        # Remove any records in the buffer that came from the shard
        heap = self.buffer.heap
        # ( ordering,  ( record,  shard ) )
        #               |----- x[1] -----|
        #        shard = x[1][1] ---^
        # TODO can this be improved?  Gets expensive for high-volume streams with large buffers
        to_remove = [x for x in heap if x[1][1] is shard]
        for x in to_remove:
            heap.remove(x)

    def move_to(self, position) -> None:
        if position in {"latest", "trim_horizon"}:
            self.roots.clear()
            self.active.clear()
            self.buffer.clear()

            latest_shards = unpack_shards(
                self.session.describe_stream(
                    stream_arn=self.stream_arn)["Shards"],
                self.stream_arn,
                self.session)
            self.roots.extend(
                shard
                for shard in latest_shards.values()
                if not shard.parent)

            if position == "trim_horizon":
                for shard in self.roots:
                    shard.jump_to(iterator_type="trim_horizon")
                    self.active.extend(self.roots)
            # latest
            else:
                for root in self.roots:
                    for shard in root.walk_tree():
                        if not shard.children:
                            shard.jump_to(iterator_type="latest")
                            self.active.append(shard)

        elif isinstance(position, arrow.Arrow):
            # TODO
            raise NotImplementedError
        elif isinstance(position, Mapping):
            update_coordinator_from_token(self, position)
        else:
            raise ValueError("Don't know how to move to position {!r}".format(position))


# TODO move this somewhere
def update_coordinator_from_token(coordinator: Coordinator, token: Mapping[str, Any]) -> None:
    stream_arn = coordinator.stream_arn = token["stream_arn"]

    # 0) Load the token into the coordinator so we can re-use the normal
    #    processing utilities to prune and add Shards as necessary.
    coordinator.roots.clear()
    coordinator.active.clear()
    coordinator.buffer.clear()

    by_id = unpack_shards(token["shards"], stream_arn, coordinator.session)
    coordinator.roots = [shard for shard in by_id.values() if not shard.parent]
    coordinator.active.extend(by_id[shard_id] for shard_id in token["active"])

    # 1) Load the Stream's actual Shards from DynamoDBStreams for
    #    validation and updates. (this is a mapping of {shard_id: shard})
    by_id = unpack_shards(coordinator.session.describe_stream(stream_arn)["Shards"], stream_arn, coordinator.session)

    # 2) Walk each root shard's tree, to find an intersection with the actual shards that exist.
    #    If there's any Shard with no children AND it's not part of the returned shards from DynamoDBStreams,
    #    there's no way to relate the token structure to the existing structure so we bail.  If it has children,
    #    prune the Shard from the Coordinator and try to find its children.
    unverified = collections.deque(coordinator.roots)
    while unverified:
        shard = unverified.popleft()
        # Found an intersection; no need to keep searching this branch.
        if shard.shard_id in by_id:
            continue

        # No worries, we'll just try to verify the children
        if shard.children:
            unverified.extend(shard.children)
            # This shard doesn't exist, so prune it and promote its children to its position(s)
            coordinator.remove_shard(shard)
            continue
        raise InvalidStream("The Stream token contains an unresolvable Shard.")

    # 3) Now that everything's verified, grab new iterators for the coordinator's active Shards.
    for shard in coordinator.active:
        try:
            shard.jump_to(iterator_type=shard.iterator_type, sequence_number=shard.sequence_number)
        except RecordsExpired:
            # TODO logger.info "SequenceNumber from token was past trim_horizon, moving to trim_horizon instead"
            shard.jump_to(iterator_type="trim_horizon")
        # If the Shard has a sequence_number, it may be beyond trim_horizon.  The records between
        # [sequence_number, current trim_horizon) can never be retrieved, so we can ignore that
        # they exist, and simply jump to the current trim_horizon.
