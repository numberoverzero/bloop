import boto3
import pendulum

from bloop import (
    BaseModel,
    Column,
    Engine,
    UUID,
)
from bloop.ext.pendulum import DateTime


def engine_for_region(region, table_name_template="{table_name}"):
    dynamodb = boto3.client("dynamodb", region_name=region)
    dynamodbstreams = boto3.client("dynamodbstreams", region_name=region)
    return Engine(
        dynamodb=dynamodb,
        dynamodbstreams=dynamodbstreams,
        table_name_template=table_name_template
    )


primary = engine_for_region("us-west-2", table_name_template="primary.{table_name}")
replica = engine_for_region("us-east-1", table_name_template="replica.{table_name}")


class SomeDataBlob(BaseModel):
    class Meta:
        stream = {
            "include": {"new", "old"}
        }

    id = Column(UUID, hash_key=True)
    uploaded = Column(DateTime, range_key=True)


primary.bind(SomeDataBlob)
replica.bind(SomeDataBlob)


def scan_replicate():
    """Bulk replicate existing data"""
    for obj in primary.scan(SomeDataBlob):
        replica.save(obj)


def stream_replicate():
    """Monitor changes in approximately real-time and replicate them"""
    stream = primary.stream(SomeDataBlob, "trim_horizon")
    next_heartbeat = pendulum.now()
    while True:
        now = pendulum.now()
        if now >= next_heartbeat:
            stream.heartbeat()
            next_heartbeat = now.add(minutes=10)

        record = next(stream)
        if record is None:
            continue
        if record["new"] is not None:
            replica.save(record["new"])
        else:
            replica.delete(record["old"])

