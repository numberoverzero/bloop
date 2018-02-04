import collections
import logging

from ..exceptions import ShardIteratorExpired
from ..util import Sentinel


# Approximate number of calls to fully traverse an empty shard
CALLS_TO_REACH_HEAD = 5

EXACT_ITERATORS = {"at_sequence", "after_sequence"}
RELATIVE_ITERATORS = {"trim_horizon", "latest"}

logger = logging.getLogger("bloop.stream")
last_iterator = Sentinel("LastIterator")
missing = Sentinel("missing")


class Shard:
    """Encapsulates the record-level iterator management for a single Shard.

    :param str stream_arn: Stream arn, usually from the model's ``Meta.stream["arn"]``.
    :param str shard_id: Shard id, usually from a DescribeStream call.
    :param str iterator_id: *(Optional)* An existing Shard iterator id.  Default is None.
    :param str iterator_type: *(Optional)* The shard's iterator type, usually when loading from a token.
        One of "trim_horizon", "at_sequence", "after_sequence", or "latest". Default is None.
    :param str sequence_number: *(Optional)* SequenceNumber for an "at_sequence" or "after_sequence" iterator
        type.  Default is None.
    :param parent: *(Optional)* This shard's parent.  Default is None.
    :type parent: :class:`~bloop.stream.shard.Shard`
    :param session: Used to make DynamoDBStreams calls.
    :type session: :class:`~bloop.session.SessionWrapper`
    """
    def __init__(self, *, stream_arn, shard_id, iterator_id=None,
                 iterator_type=None, sequence_number=None, parent=None, session=None):

        # Set once on creation, never changes
        self.stream_arn = stream_arn

        # Set once on creation, never changes
        self.shard_id = shard_id

        # ID of the current iterator for this shard.
        # Changes with every call to :func:`~Shard.get_records`.
        self.iterator_id = iterator_id

        # One of "trim_horizon", "latest", "at_sequence", or "after_sequence".
        # Changes as the shard jumps around or when the Coordinator
        # pops a record from this shard from the buffer.
        self.iterator_type = iterator_type

        # Changes when records are consumed.  Used with :attr:`~.iterator_type`.
        self.sequence_number = sequence_number

        # The :class:`Shard` that this one spawned off of.  This will become None
        # if a Coordinator is pruning expired parents.  It is usually set as part
        # of rebuilding a shard tree, soon after the shard is instantiated.
        self.parent = parent

        # Shards have 0, 1, or 2 children.  A shard will have 0 children
        # when the shard is still open; if the stream is closed; or the table
        # throughput has decreased.
        self.children = []

        # Tracks how many times a GetRecords call has an iterator_id without any results.
        # After :data:`CALLS_TO_REACH_HEAD` empty responses, we can assume the shard is still open.
        # This dictates how hard the shard works to "catch up" a new iterator.
        self.empty_responses = 0

        self.session = session

    def __repr__(self):
        if self.exhausted:
            # <Shard[exhausted, id='shardId-00000001414562045508-2bac9cd2']>
            details = "exhausted"
        elif self.iterator_type == "at_sequence":
            # <Shard[at=300000000000000499659, id='shardId-00000001414562045508-2bac9cd2']>
            details = "at_seq=" + repr(self.sequence_number)
        elif self.iterator_type == "after_sequence":
            # <Shard[after=300000000000000499659, id='shardId-00000001414562045508-2bac9cd2']>
            details = "after_seq=" + repr(self.sequence_number)
        elif self.iterator_type in RELATIVE_ITERATORS:
            # <Shard[latest, id='shardId-00000001414562045508-2bac9cd2']>
            # <Shard[trim_horizon, id='shardId-00000001414562045508-2bac9cd2']>
            details = self.iterator_type
        else:
            # <Shard[id='shardId-00000001414562045508-2bac9cd2']>
            details = ""
        if details:
            details += ", "
        return "<{}[{}id={!r}]>".format(self.__class__.__name__, details, self.shard_id)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.get_records()
        except ShardIteratorExpired:
            # Refreshing a latest or trim_horizon iterator could lose data.
            if self.iterator_type in ["trim_horizon", "latest"]:
                raise

        # Automatically refresh at (or after) :attr:`~.sequence_number`. This can still
        # raise :exc:`RecordsExpired` if the iterator fell behind the the trim_horizon.
        self.jump_to(iterator_type=self.iterator_type, sequence_number=self.sequence_number)
        return self.get_records()

    def __eq__(self, other):
        try:
            return (
                (self.token == other.token) and
                (self.iterator_id == other.iterator_id) and
                ({child.shard_id for child in self.children} == {child.shard_id for child in other.children})
            )
        except (AttributeError, TypeError):
            return False

    __hash__ = object.__hash__

    @property
    def exhausted(self):
        """True if the shard is closed and there are no additional records to get."""
        return self.iterator_id is last_iterator

    @property
    def token(self):
        """JSON-serializable representation of the current Shard state.

        The token is enough to rebuild the Shard as part of rebuilding a Stream.

        :returns: Shard state as a json-friendly dict
        :rtype: dict
        """
        if self.iterator_type in RELATIVE_ITERATORS:
            logger.warning("creating shard token at non-exact location \"{}\"".format(self.iterator_type))
        token = {
            "stream_arn": self.stream_arn,
            "shard_id": self.shard_id,
            "iterator_type": self.iterator_type,
            "sequence_number": self.sequence_number,
        }
        if self.parent:
            token["parent"] = self.parent.shard_id
        if not self.iterator_type:
            del token["iterator_type"]
        if not self.sequence_number:
            del token["sequence_number"]
        return token

    def walk_tree(self):
        """Generator that yields each :class:`~bloop.stream.shard.Shard` by walking the shard's children in order."""
        shards = collections.deque([self])
        while shards:
            shard = shards.popleft()
            yield shard
            shards.extend(shard.children)

    def jump_to(self, *, iterator_type, sequence_number=None):
        """Move to a new position in the shard using the standard parameters to GetShardIterator.

        :param str iterator_type: "trim_horizon", "at_sequence", "after_sequence", "latest"
        :param str sequence_number: *(Optional)* Sequence number to use with at/after sequence.  Default is None.
        """
        # Just a simple wrapper; let the caller handle RecordsExpired
        self.iterator_id = self.session.get_shard_iterator(
            stream_arn=self.stream_arn,
            shard_id=self.shard_id,
            iterator_type=iterator_type,
            sequence_number=sequence_number)
        self.iterator_type = iterator_type
        self.sequence_number = sequence_number
        self.empty_responses = 0

    def seek_to(self, position):
        """Move the Shard's iterator to the earliest record after the :class:`~datetime.datetime` time.

        Returns the first records at or past ``position``.  If the list is empty,
        the seek failed to find records, either because the Shard is exhausted or it
        reached the HEAD of an open Shard.

        :param position: The position in time to move to.
        :type position: :class:`~datetime.datetime`
        :returns: A list of the first records found after ``position``.  May be empty.
        """
        # 0) We have no way to associate the date with a position,
        #    so we have to scan the shard from the beginning.
        self.jump_to(iterator_type="trim_horizon")

        position = int(position.timestamp())

        while (not self.exhausted) and (self.empty_responses < CALLS_TO_REACH_HEAD):
            records = self.get_records()
            # We can skip the whole record set if the newest (last) record isn't new enough.
            if records and records[-1]["meta"]["created_at"].timestamp() >= position:
                # Looking for the first number *below* the position.
                for offset, record in enumerate(reversed(records)):
                    if record["meta"]["created_at"].timestamp() < position:
                        index = len(records) - offset
                        return records[index:]
                return records

        # Either exhausted the Shard or caught up to HEAD.
        return []

    def load_children(self):
        """If the Shard doesn't have any children, tries to find some from DescribeStream.

        If the Shard is open this won't find any children, so an empty response doesn't
        mean the Shard will **never** have children.
        """
        # Child count is fixed the first time any of the following happen:
        # 0 :: stream closed or throughput decreased
        # 1 :: shard was open for ~4 hours
        # 2 :: throughput increased

        if self.children:
            return self.children

        # ParentShardId -> [Shard, ...]
        by_parent = collections.defaultdict(list)
        # ShardId -> Shard
        by_id = {}

        for shard in self.session.describe_stream(
                stream_arn=self.stream_arn,
                first_shard=self.shard_id)["Shards"]:
            parent_list = by_parent[shard.get("ParentShardId")]
            shard = Shard(
                stream_arn=self.stream_arn,
                shard_id=shard["ShardId"],
                parent=shard.get("ParentShardId"),
                session=self.session)
            parent_list.append(shard)
            by_id[shard.shard_id] = shard

        # Find this shard when looking up shards by ParentShardId
        by_id[self.shard_id] = self

        # Insert this shard's children, then handle its child's descendants etc.
        to_insert = collections.deque(by_parent[self.shard_id])
        while to_insert:
            shard = to_insert.popleft()
            # ParentShardId -> Shard
            shard.parent = by_id[shard.parent]
            shard.parent.children.append(shard)
            # Continue for any shards that have this shard as their parent
            to_insert.extend(by_parent[shard.shard_id])

        return self.children

    def get_records(self):
        """Get the next set of records in this shard.  An empty list doesn't guarantee the shard is exhausted.

        :returns: A list of reformatted records.  May be empty.
        """
        # Won't be able to find new records.
        if self.exhausted:
            return []

        # Already caught up, just the one call please.
        if self.empty_responses >= CALLS_TO_REACH_HEAD:
            return self._apply_get_records_response(self.session.get_stream_records(self.iterator_id))

        # Up to 5 calls to try and find a result
        while self.empty_responses < CALLS_TO_REACH_HEAD and not self.exhausted:
            records = self._apply_get_records_response(self.session.get_stream_records(self.iterator_id))
            if records:
                return records

        return []

    def _apply_get_records_response(self, response):
        records = response.get("Records", [])
        records = [reformat_record(record) for record in records]
        self.iterator_id = response.get("NextShardIterator", last_iterator)

        if records and self.sequence_number is None:
            # ONLY update these if there's no sequence_number.  Overwriting risks data loss.
            self.sequence_number = records[0]["meta"]["sequence_number"]
            self.iterator_type = "at_sequence"
        elif not records:
            self.empty_responses += 1
        return records


def reformat_record(record):
    """Repack a record into a cleaner structure for consumption."""
    return {
        "key": record["dynamodb"].get("Keys", None),
        "new": record["dynamodb"].get("NewImage", None),
        "old": record["dynamodb"].get("OldImage", None),

        "meta": {
            "created_at": record["dynamodb"]["ApproximateCreationDateTime"],
            "event": {
                "id": record["eventID"],
                "type": record["eventName"].lower(),
                "version": record["eventVersion"]
            },
            "sequence_number": record["dynamodb"]["SequenceNumber"],
        }
    }


def unpack_shards(shards, stream_arn, session):
    """List[Dict] -> Dict[shard_id, Shard].

    Each Shards' parent/children are hooked up with the other Shards in the list.
    """
    if not shards:
        return {}

    # When unpacking tokens, shard id key is "shard_id"
    # When unpacking DescribeStream responses, shard id key is "ShardId"
    if "ShardId" in shards[0]:
        shards = _translate_shards(shards)

    by_id = {shard_token["shard_id"]:
             Shard(stream_arn=stream_arn, shard_id=shard_token["shard_id"],
                   iterator_type=shard_token.get("iterator_type"), sequence_number=shard_token.get("sequence_number"),
                   parent=shard_token.get("parent"), session=session)
             for shard_token in shards}

    for shard in by_id.values():
        if shard.parent:
            shard.parent = by_id[shard.parent]
            shard.parent.children.append(shard)
    return by_id


def _translate_shards(shards):
    """Converts the dicts from DescribeStream to the internal Shard format."""
    for shard in shards:
        yield {
            "shard_id": shard["ShardId"],
            "iterator_type": None,
            "sequence_number": None,
            "iterator_id": None,
            "parent": shard.get("ParentShardId")
        }
