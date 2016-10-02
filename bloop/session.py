import collections
import boto3.session
import botocore.exceptions

from .exceptions import (
    BloopException,
    ConstraintViolation,
    InvalidShardIterator,
    InvalidStream,
    RecordsExpired,
    ShardIteratorExpired,
    TableMismatch,
    UnknownSearchMode,
)
from .signals import table_validated
from .util import Sentinel, ordered


missing = Sentinel("missing")
ready = Sentinel("ready")

__all__ = ["SessionWrapper"]
# https://boto3.readthedocs.io/en/latest/reference/services/dynamodb.html#DynamoDB.Client.batch_get_item
BATCH_GET_ITEM_CHUNK_SIZE = 100

SHARD_ITERATOR_TYPES = {
    "at_sequence": "AT_SEQUENCE_NUMBER",
    "after_sequence": "AFTER_SEQUENCE_NUMBER",
    "trim_horizon": "TRIM_HORIZON",
    "latest": "LATEST"
}


class SessionWrapper:
    def __init__(self, session: boto3.session.Session):
        self._dynamodb_client = session.client("dynamodb")
        self._stream_client = session.client("dynamodbstreams")

    def save_item(self, item):
        try:
            self._dynamodb_client.update_item(**item)
        except botocore.exceptions.ClientError as error:
            handle_constraint_violation(error)

    def delete_item(self, item):
        try:
            self._dynamodb_client.delete_item(**item)
        except botocore.exceptions.ClientError as error:
            handle_constraint_violation(error)

    def load_items(self, items):
        loaded_items = {}
        requests = collections.deque(create_batch_get_chunks(items))
        while requests:
            request = requests.pop()
            try:
                response = self._dynamodb_client.batch_get_item(RequestItems=request)
            except botocore.exceptions.ClientError as error:
                raise BloopException("Unexpected error while loading items.") from error

            # Accumulate results
            for table_name, table_items in response.get("Responses", {}).items():
                loaded_items.setdefault(table_name, []).extend(table_items)

            # Push additional request onto the deque.
            # "UnprocessedKeys" is {} if this request is done
            if response["UnprocessedKeys"]:
                requests.append(response["UnprocessedKeys"])
        return loaded_items

    def query_items(self, request):
        return self.search_items("query", request)

    def scan_items(self, request):
        return self.search_items("scan", request)

    def search_items(self, mode, request):
        validate_search_mode(mode)
        method = getattr(self._dynamodb_client, mode)
        try:
            response = method(**request)
        except botocore.exceptions.ClientError as error:
            raise BloopException("Unexpected error during {}.".format(mode)) from error
        standardize_query_response(response)
        return response

    def create_table(self, model):
        table = create_table_request(model)
        try:
            self._dynamodb_client.create_table(**table)
        except botocore.exceptions.ClientError as error:
            handle_table_exists(error, model)

    def validate_table(self, model):
        table_name = model.Meta.table_name
        status, actual = None, {}
        while status is not ready:
            try:
                actual = self._dynamodb_client.describe_table(TableName=table_name)["Table"]
            except botocore.exceptions.ClientError as error:
                raise BloopException("Unexpected error while describing table.") from error
            status = simple_table_status(actual)
        expected = expected_table_description(model)
        if not compare_tables(model, actual, expected):
            raise TableMismatch("The expected and actual tables for {!r} do not match.".format(model.__name__))
        table_validated.send(self, model=model, actual_description=actual, expected_description=expected)

    def describe_stream(self, stream_arn, first_shard=None):
        description = {"Shards": []}
        next_shard = first_shard.shard_id if first_shard else None
        while next_shard is not missing:
            try:
                response = self._stream_client.describe_stream(
                    StreamArn=stream_arn,
                    ExclusiveStartShardId=next_shard
                )["StreamDescription"]
            except botocore.exceptions.ClientError as error:
                if error.response["Error"]["Code"] == "ResourceNotFoundException":
                    raise InvalidStream("The stream arn {!r} does not exist.".format(stream_arn))
                raise BloopException("Unexpected error while describing stream.") from error
            next_shard = response.pop("LastEvaluatedShardId", missing)
            description["Shards"].extend(response.pop("Shards", []))
            description.update(response)
        return description

    def get_shard_iterator(self, stream_arn, shard_id, iterator_type, sequence_number):
        iterator_type = validate_stream_iterator_type(iterator_type)
        try:
            return self._stream_client.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType=iterator_type,
                SequenceNumber=sequence_number
            )["ShardIterator"]
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == "TrimmedDataAccessException":
                raise RecordsExpired("Sequence number {!r} is beyond the trim horizon.".format(sequence_number))
            raise BloopException("Unexpected error while creating shard iterator") from error

    def get_stream_records(self, iterator_id):
        try:
            return self._stream_client.get_records(ShardIterator=iterator_id)
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == "TrimmedDataAccessException":
                raise RecordsExpired(
                    "The iterator {!r} requested records beyond the trim horizon.".format(iterator_id))
            elif error.response["Error"]["Code"] == "ExpiredIteratorException":
                raise ShardIteratorExpired("The iterator {!r} expired.".format(iterator_id))
            raise BloopException("Unexpected error while getting records.") from error


def validate_search_mode(mode):
    if mode not in {"query", "scan"}:
        raise UnknownSearchMode("{!r} is not a valid search mode.".format(mode))


def validate_stream_iterator_type(iterator_type):
    try:
        return SHARD_ITERATOR_TYPES[iterator_type]
    except KeyError:
        raise InvalidShardIterator("Unknown iterator type {!r}".format(iterator_type))


def handle_constraint_violation(error):
    error_code = error.response["Error"]["Code"]
    if error_code == "ConditionalCheckFailedException":
        raise ConstraintViolation("The condition was not met.") from error
    else:
        raise BloopException("Unexpected error while modifying item.") from error


def handle_table_exists(error, model):
    error_code = error.response["Error"]["Code"]
    if error_code != "ResourceInUseException":
        raise BloopException("Unexpected error while creating table {!r}.".format(model.__name__)) from error
    # Don't raise if the table already exists


# MODEL HELPERS ======================================================================================== MODEL HELPERS


def standardize_query_response(response):
    count = response.setdefault("Count", 0)
    response["ScannedCount"] = response.get("ScannedCount", count)


def create_batch_get_chunks(items):
    buffer, count = {}, 0
    for table_name, table_attrs in items.items():
        consistent_read = table_attrs["ConsistentRead"]
        for key in table_attrs["Keys"]:
            # New table name?
            table = buffer.get(table_name, None)
            if table is None:
                # PERF: overhead using setdefault is (n-1)
                #       for n items in the same table in this chunk
                table = buffer[table_name] = {"ConsistentRead": consistent_read, "Keys": []}

            table["Keys"].append(key)
            count += 1
            if count >= BATCH_GET_ITEM_CHUNK_SIZE:
                yield buffer
                buffer, count = {}, 0

    # Last chunk, less than batch_size items
    if buffer:
        yield buffer

# TABLE HELPERS ======================================================================================== TABLE HELPERS


def compare_tables(model, actual, expected):
    sanitized_actual = sanitize_table_description(actual)
    # 1. If the table doesn't specify an expected stream type,
    #    don't inspect the StreamSpecification at all.
    if not model.Meta.stream:
        sanitized_actual.pop("StreamSpecification", None)
    # 2. If the table backs multiple models, the AttributeDefinitions,
    #    GlobalSecondaryIndexes, and LocalSecondaryIndexes may contain
    #    additional entries that this model doesn't care about.
    #    Drop any values in the sanitized table that aren't expected.
    subset_only = ["AttributeDefinitions", "GlobalSecondaryIndexes", "LocalSecondaryIndexes"]
    for section_name in subset_only:
        if section_name not in sanitized_actual:
            continue
        possible_superset = sanitized_actual[section_name]
        # Ordering because some inner values are lists, and we don't care about their order
        expected_values = ordered(expected.get(section_name, []))
        filtered_superset = [x for x in possible_superset if ordered(x) in expected_values]
        sanitized_actual[section_name] = filtered_superset
    return ordered(sanitized_actual) == ordered(expected)


def attribute_definitions(model):
    dedupe_attrs = set()
    attrs = []

    def add_column(column):
        if column is None:
            return
        if column in dedupe_attrs:
            return
        dedupe_attrs.add(column)
        attrs.append({
            "AttributeType": column.typedef.backing_type,
            "AttributeName": column.dynamo_name
        })

    add_column(model.Meta.hash_key)
    add_column(model.Meta.range_key)

    for index in model.Meta.indexes:
        add_column(index.hash_key)
        add_column(index.range_key)
    return attrs


def index_projection(index):
    projection_type = {
        "all": "ALL",
        "keys": "KEYS_ONLY",
        "include": "INCLUDE"
    }[index.projection["mode"]]

    projection = {"ProjectionType": projection_type}
    if index.projection["mode"] == "include":
        projection["NonKeyAttributes"] = [
            column.dynamo_name
            for column in index.projection["included"]
        ]
    return projection


def key_schema(*, index=None, model=None):
    if index:
        hash_key = index.hash_key
        range_key = index.range_key
    else:
        hash_key = model.Meta.hash_key
        range_key = model.Meta.range_key
    schema = [{
        "AttributeName": hash_key.dynamo_name,
        "KeyType": "HASH"
    }]
    if range_key:
        schema.append({
            "AttributeName": range_key.dynamo_name,
            "KeyType": "RANGE"
        })
    return schema


def global_secondary_index(index):
    return {
        "IndexName": index.dynamo_name,
        "KeySchema": key_schema(index=index),
        "Projection": index_projection(index),
        "ProvisionedThroughput": {
            "WriteCapacityUnits": index.write_units,
            "ReadCapacityUnits": index.read_units
        },
    }


def local_secondary_index(index):
    return {
        "IndexName": index.dynamo_name,
        "KeySchema": key_schema(index=index),
        "Projection": index_projection(index),
    }


def create_table_request(model):
    table = {
        "AttributeDefinitions": attribute_definitions(model),
        "KeySchema": key_schema(model=model),
        "ProvisionedThroughput": {
            "WriteCapacityUnits": model.Meta.write_units,
            "ReadCapacityUnits": model.Meta.read_units
        },
        "TableName": model.Meta.table_name,
    }
    if model.Meta.gsis:
        table["GlobalSecondaryIndexes"] = [
            global_secondary_index(index) for index in model.Meta.gsis]
    if model.Meta.lsis:
        table["LocalSecondaryIndexes"] = [
            local_secondary_index(index) for index in model.Meta.lsis]
    if model.Meta.stream:
        include = model.Meta.stream["include"]
        view = {
            ("keys",): "KEYS_ONLY",
            ("new",): "NEW_IMAGE",
            ("old",): "OLD_IMAGE",
            ("new", "old"): "NEW_AND_OLD_IMAGES"
        }[tuple(sorted(include))]

        table["StreamSpecification"] = {
            "StreamEnabled": True,
            "StreamViewType": view
        }
    return table


def expected_table_description(model):
    # Right now, we expect the exact same thing as create_table_request
    # This doesn't include statuses (table, indexes) since that's
    # pulled out by the polling mechanism
    table = create_table_request(model)
    return table


def sanitize_table_description(description):
    # We don't need to match most of what comes back from describe_table
    # This monster structure carefully extracts the exact fields that bloop
    # will compare against, without picking up any new fields that
    # describe_table may start returning.

    # Without this, describe_table could return a new piece of metadata
    # and break all table verification because our expected table doesn't
    # include the new field.

    # This also simplifies the post-processing logic by inserting empty lists
    # for missing values from the wire.
    table = {
        "AttributeDefinitions": [
            {"AttributeName": attr_definition["AttributeName"], "AttributeType": attr_definition["AttributeType"]}
            for attr_definition in description.get("AttributeDefinitions", [])
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": gsi["IndexName"],
                "KeySchema": [
                    {"AttributeName": gsi_key["AttributeName"], "KeyType": gsi_key["KeyType"]}
                    for gsi_key in gsi["KeySchema"]],
                "Projection": {
                    "NonKeyAttributes": gsi["Projection"].get("NonKeyAttributes", []),
                    "ProjectionType": gsi["Projection"]["ProjectionType"]},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": gsi["ProvisionedThroughput"]["ReadCapacityUnits"],
                    "WriteCapacityUnits": gsi["ProvisionedThroughput"]["WriteCapacityUnits"]}}
                for gsi in description.get("GlobalSecondaryIndexes", [])
        ],
        "KeySchema": [
            {"AttributeName": table_key["AttributeName"], "KeyType": table_key["KeyType"]}
            for table_key in description.get("KeySchema", [])
        ],
        "LocalSecondaryIndexes": [
            {
                "IndexName": lsi["IndexName"],
                "KeySchema": [
                    {"AttributeName": lsi_key["AttributeName"], "KeyType": lsi_key["KeyType"]}
                    for lsi_key in lsi["KeySchema"]],
                "Projection": {
                    "NonKeyAttributes": lsi["Projection"].get("NonKeyAttributes", []),
                    "ProjectionType": lsi["Projection"]["ProjectionType"]}}
                for lsi in description.get("LocalSecondaryIndexes", [])
        ],
        "ProvisionedThroughput": {
            "ReadCapacityUnits":
                description.get("ProvisionedThroughput", {"ReadCapacityUnits": None})["ReadCapacityUnits"],
            "WriteCapacityUnits":
                description.get("ProvisionedThroughput", {"WriteCapacityUnits": None})["WriteCapacityUnits"]
        },
        "StreamSpecification": description.get("StreamSpecification", None),
        "TableName": description.get("TableName", None)
    }

    indexes = table["GlobalSecondaryIndexes"] + table["LocalSecondaryIndexes"]
    for index in indexes:
        if not index["Projection"]["NonKeyAttributes"]:
            index["Projection"].pop("NonKeyAttributes")
    for possibly_empty in ["GlobalSecondaryIndexes", "LocalSecondaryIndexes", "StreamSpecification"]:
        if not table[possibly_empty]:
            table.pop(possibly_empty)

    return table


def simple_table_status(description):
    status = ready
    if description.get("TableStatus") != "ACTIVE":
        status = None
    for index in description.get("GlobalSecondaryIndexes", []):
        if index.get("IndexStatus") != "ACTIVE":
            status = None
    return status
