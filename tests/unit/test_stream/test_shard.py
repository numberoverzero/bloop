import pytest
import random
import string
import arrow
from unittest.mock import Mock
from bloop.session import SessionWrapper
from bloop.stream.buffer import RecordBuffer, heap_item
from bloop.stream.shard import Shard, reformat_record, unpack_shards
from typing import Dict, List, Union, Any


def random_str(prefix="", length=8):
    return prefix + "".join(random.choice(string.ascii_lowercase) for _ in range(length))


def build_shards(n: int, shape: Dict[int, Union[int, List[int]]],
                 session: SessionWrapper=None, stream_arn=None) -> List[Shard]:
    """Shape describes the parent/child relationships.

    a -> b -> c -> d
           -> e -> f

    is expressed as:

    build_shards(session, 6, {0: 1, 1: [2, 3], 2: 4, 3: 5})
    """
    shard_id = lambda i: random_str("shard_id-{}-".format(i), 4)
    shards = [
        Shard(stream_arn=stream_arn, shard_id=shard_id(i), session=session)
        for i in range(n)
    ]
    for shard_index, child_indexes in shape.items():
        if isinstance(child_indexes, int):
            child_indexes = [child_indexes]
        for child_index in child_indexes:
            shards[shard_index].children.append(shards[child_index])
            shards[child_index].parent = shards[shard_index]

    return shards


def stream_description(n: int, shape: Dict[int, Union[int, List[int]]], stream_arn=None) -> Dict[str, Any]:
    """Build a DsecribeStream response with the given number of shards"""
    shard_ids = [random_str("shard_id-{}-".format(i), 4) for i in range(n)]
    template = {
        "SequenceNumberRange": {
            "EndingSequenceNumber": "820400000000000001192334",
            "StartingSequenceNumber": "820400000000000001192334"
        }
    }
    shards = [{**template, "ShardId": shard_id} for shard_id in shard_ids]

    for shard_index, child_indexes in shape.items():
        if isinstance(child_indexes, int):
            child_indexes = [child_indexes]
        for child_index in child_indexes:
            shards[child_index]["ParentShardId"] = shard_ids[shard_index]
    return {
        "Shards": shards,
        "StreamArn": stream_arn
    }


keys = ("Keys", {
    "ForumName": {"S": "DynamoDB"},
    "Subject": {"S": "DynamoDB Thread 1"}})
new = ("NewImage", {
    "ForumName": {"S": "DynamoDB"},
    "Subject": {"S": "DynamoDB Thread 1"}})
old = ("OldImage", {
    "ForumName": {"S": "DynamoDB"},
    "Subject": {"S": "DynamoDB Thread 1"}})


@pytest.mark.parametrize("include", [(new,), (old,), (new, old), (keys,)])
def test_reformat_record(include):
    raw = {
            "awsRegion": "us-west-2",
            "dynamodb": {
                "ApproximateCreationDateTime": 1.46480527E9,
                "SequenceNumber": "400000000000000499660",
                "SizeBytes": 41,
                "StreamViewType": "KEYS_ONLY"
            },
            "eventID": "4b25bd0da9a181a155114127e4837252",
            "eventName": "MODIFY",
            "eventSource": "aws:dynamodb",
            "eventVersion": "1.0"
        }
    for key, obj in include:
        raw["dynamodb"][key] = obj

    record = reformat_record(raw)
    renames = {
        "NewImage": "new",
        "OldImage": "old",
        "Keys": "key"
    }

    for key, obj in include:
        assert record[renames[key]] == obj


def test_unpack_empty_shards_list(session):
    assert unpack_shards([], "stream-arn", session) == {}


def test_unpack_shards_from_token(session):
    # multiple roots, 1:1 and 1:2 relations
    shards = build_shards(5, {0: 1, 2: [3, 4]}, session, stream_arn="stream_arn")
    by_id = {shard.shard_id: shard for shard in shards}

    # unpacking shouldn't rely on ordering over the wire
    tokens = [shard.token for shard in shards]
    random.shuffle(tokens)
    unpacked = unpack_shards(tokens, "stream_arn", session)

    assert unpacked == by_id


def test_unpack_shards_from_describe_stream(session):
    # multiple roots, 1:1 and 1:2 relations
    shards = stream_description(5, {0: 1, 2: [3, 4]})["Shards"]
    by_id = {shard["ShardId"]: shard for shard in shards}

    # unpacking shouldn't rely on ordering over the wire
    random.shuffle(shards)
    unpacked = unpack_shards(shards, "stream_arn", session=session)

    for shard_id, shard in unpacked.items():
        if shard.parent is None:
            assert "ParentShardId" not in by_id[shard_id]
        else:
            assert shard.parent.shard_id == by_id[shard_id].get("ParentShardId")
