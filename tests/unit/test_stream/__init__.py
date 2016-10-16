import random
import string
from bloop.session import SessionWrapper
from bloop.stream.shard import Shard
from typing import Union, List, Dict, Any, Optional


def random_str(prefix="", length=8):
    return prefix + "".join(random.choice(string.ascii_lowercase) for _ in range(length))


def build_shards(n: int, shape: Optional[Dict[int, Union[int, List[int]]]]=None,
                 session: SessionWrapper=None, stream_arn=None) -> List[Shard]:
    """Shape describes the parent/child relationships.

    a -> b -> c -> d
           -> e -> f

    is expressed as:

    build_shards(session, 6, {0: 1, 1: [2, 3], 2: 4, 3: 5})
    """
    # Default to flat shards, no heirarchy
    shape = shape or {}
    shard_id = lambda i: random_str("shard-id-{}-".format(i), 4)
    shards = [
        Shard(stream_arn=stream_arn, shard_id=shard_id(i), session=session)
        for i in range(n)
    ]
    for shard_index, child_indexes in shape.items():
        if isinstance(child_indexes, int):
            shards[shard_index].children.append(shards[child_indexes])
            shards[child_indexes].parent = shards[shard_index]
        else:
            for child_index in child_indexes:
                shards[shard_index].children.append(shards[child_index])
                shards[child_index].parent = shards[shard_index]

    return shards


def stream_description(n: int, shape: Dict[int, Union[int, List[int]]], stream_arn=None) -> Dict[str, Any]:
    """Build a DescribeStream response with the given number of shards"""
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
            shards[child_indexes]["ParentShardId"] = shard_ids[shard_index]
        else:
            for child_index in child_indexes:
                shards[child_index]["ParentShardId"] = shard_ids[shard_index]
    return {
        "Shards": shards,
        "StreamArn": stream_arn
    }


def record_with(key=False, new=False, old=False, sequence_number=None):
    template = {
        "awsRegion": "us-west-2",
        "dynamodb": {
            "ApproximateCreationDateTime": 1.46480527E9,
            "SequenceNumber": sequence_number if sequence_number is not None else "400000000000000499660",
            "SizeBytes": 41,
            "StreamViewType": "KEYS_ONLY",

            "Keys": {
                "ForumName": {"S": "DynamoDB"},
                "Subject": {"S": "DynamoDB Thread 1"}},
            "NewImage": {
                "ForumName": {"S": "DynamoDB"},
                "Subject": {"S": "DynamoDB Thread 1"}},
            "OldImage": {
                "ForumName": {"S": "DynamoDB"},
                "Subject": {"S": "DynamoDB Thread 1"}}
        },
        "eventID": "4b25bd0da9a181a155114127e4837252",
        "eventName": "MODIFY",
        "eventSource": "aws:dynamodb",
        "eventVersion": "1.0"
    }
    if not key:
        del template["dynamodb"]["Keys"]
    if not new:
        del template["dynamodb"]["NewImage"]
    if not old:
        del template["dynamodb"]["OldImage"]
    return template


def build_get_records_responses(*chain):
    """Return an iterable of responses for session.get_stream_records calls.

    Chain is the number of results to include in each page.
    For example: [0, 2, 1] expands into (0 results, proceed) -> (2 results, proceed) -> (1 result, stop).
    Very similar to the build_responses helper in test_search.py
    """
    sequence_number = 0
    responses = []
    for i, count in enumerate(chain):
        responses.append({
            "Records": [record_with(key=True, sequence_number=sequence_number + offset) for offset in range(count)],
            "NextShardIterator": "continue-from-response-{}".format(i)
        })
        sequence_number += count
    # Last response doesn't point to a new iterator
    del responses[-1]["NextShardIterator"]

    return responses
