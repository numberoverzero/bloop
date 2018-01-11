import datetime
import random

from bloop.stream.shard import Shard
from bloop.util import Sentinel


missing = Sentinel("missing")


def build_shards(n, shape=None, session=None, stream_arn=None, shard_id_prefix=""):
    """Shape describes the parent/child relationships.

    a -> b -> c -> d
           -> e -> f

    is expressed as:

    build_shards(session, 6, {0: 1, 1: [2, 3], 2: 4, 3: 5})
    """
    # Default to flat shards, no hierarchy
    shape = shape or {}
    shard_id = lambda i: "{}shard-id-{}".format(shard_id_prefix + "-" if shard_id_prefix else "", i)
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


def stream_description(n, shape=None, stream_arn=None):
    """Build a DescribeStream response with the given number of shards"""
    # Default to flat shards, no hierarchy
    shape = shape or {}

    shard_ids = ["shard-id-{}".format(i) for i in range(n)]
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


def dynamodb_record_with(key=False, new=False, old=False, sequence_number=None, creation_time=None):
    if creation_time is None:
        creation_time = 1.46480527E9
    else:
        creation_time = creation_time.timestamp()
    if sequence_number is None:
        sequence_number = "400000000000000499660"
    sequence_number = str(sequence_number)
    creation_time = datetime.datetime.fromtimestamp(int(creation_time))
    template = {
        "awsRegion": "us-west-2",
        "dynamodb": {
            "ApproximateCreationDateTime": creation_time,
            "SequenceNumber": sequence_number,
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


def local_record(created_at=missing, sequence_number=None):
    if created_at is missing:
        created_at = datetime.datetime.now(datetime.timezone.utc)
    if sequence_number is None:
        sequence_number = str(random.randint(-100, 100))
    return {
        "meta": {
            "created_at": created_at,
            "sequence_number": sequence_number
        }
    }


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
            "Records": [
                dynamodb_record_with(key=True, sequence_number=sequence_number + offset)
                for offset in range(count)],
            "NextShardIterator": "continue-from-response-{}".format(i)
        })
        sequence_number += count
    # Last response doesn't point to a new iterator
    del responses[-1]["NextShardIterator"]

    return responses
