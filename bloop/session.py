import collections

import boto3
import botocore.exceptions

from .exceptions import (
    BloopException,
    ConstraintViolation,
    InvalidSearchMode,
    InvalidShardIterator,
    InvalidStream,
    RecordsExpired,
    ShardIteratorExpired,
    TableMismatch,
)
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
    """Provides a consistent interface to DynamoDb and DynamoDbStreams clients.

    If either client is None, that client is built using :func:`boto3.client`.

    :param dynamodb: A boto3 client for DynamoDB.  Defaults to ``boto3.client("dynamodb")``.
    :param dynamodbstreams: A boto3 client for DynamoDbStreams.  Defaults to ``boto3.client("dynamodbstreams")``.
    """
    def __init__(self, dynamodb=None, dynamodbstreams=None):
        dynamodb = dynamodb or boto3.client("dynamodb")
        dynamodbstreams = dynamodbstreams or boto3.client("dynamodbstreams")

        self.dynamodb_client = dynamodb
        self.stream_client = dynamodbstreams

    def save_item(self, item):
        """Save an object to DynamoDB.

        :param item: Unpacked into kwargs for :func:`boto3.DynamoDB.Client.update_item`.
        :raises bloop.exceptions.ConstraintViolation: if the condition (or atomic) is not met.
        """
        try:
            self.dynamodb_client.update_item(**item)
        except botocore.exceptions.ClientError as error:
            handle_constraint_violation(error)

    def delete_item(self, item):
        """Delete an object in DynamoDB.

        :param item: Unpacked into kwargs for :func:`boto3.DynamoDB.Client.delete_item`.
        :raises bloop.exceptions.ConstraintViolation: if the condition (or atomic) is not met.
        """
        try:
            self.dynamodb_client.delete_item(**item)
        except botocore.exceptions.ClientError as error:
            handle_constraint_violation(error)

    def load_items(self, items):
        """Loads any number of items in chunks, handling continuation tokens.

        :param items: Unpacked in chunks into "RequestItems" for :func:`boto3.DynamoDB.Client.batch_get_item`.
        """
        loaded_items = {}
        requests = collections.deque(create_batch_get_chunks(items))
        while requests:
            request = requests.pop()
            try:
                response = self.dynamodb_client.batch_get_item(RequestItems=request)
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
        """Wraps :func:`boto3.DynamoDB.Client.query`.

        Response always includes "Count" and "ScannedCount"

        :param request: Unpacked into :func:`boto3.DynamoDB.Client.query`
        """
        return self.search_items("query", request)

    def scan_items(self, request):
        """Wraps :func:`boto3.DynamoDB.Client.scan`.

        Response always includes "Count" and "ScannedCount"

        :param str mode: "query" or "scan"
        :param request: Unpacked into :func:`boto3.DynamoDB.Client.scan`
        """
        return self.search_items("scan", request)

    def search_items(self, mode, request):
        """Invoke query/scan by name.

        Response always includes "Count" and "ScannedCount"

        :param str mode: "query" or "scan"
        :param request: Unpacked into :func:`boto3.DynamoDB.Client.query` or :func:`boto3.DynamoDB.Client.scan`
        """
        validate_search_mode(mode)
        method = getattr(self.dynamodb_client, mode)
        try:
            response = method(**request)
        except botocore.exceptions.ClientError as error:
            raise BloopException("Unexpected error during {}.".format(mode)) from error
        standardize_query_response(response)
        return response

    def create_table(self, model):
        """Create the model's table.

        Does not wait for the table to create, and does not validate an existing table.
        Will not raise "ResourceInUseException" if the table exists or is being created.

        :param model: The :class:`~bloop.models.BaseModel` to create the table for.
        """
        table = create_table_request(model)
        try:
            self.dynamodb_client.create_table(**table)
        except botocore.exceptions.ClientError as error:
            handle_table_exists(error, model)

    def validate_table(self, model):
        """Polls until a creating table is ready, then verifies the description against the model's requirements.

        The model may have a subset of all GSIs and LSIs on the table, but the key structure must be exactly
        the same.  The table must have a stream if the model expects one, but not the other way around.

        :param model: The :class:`~bloop.models.BaseModel` to validate the table of.
        :raises bloop.exceptions.TableMismatch: When the table does not meet the constraints of the model.
        """
        table_name = model.Meta.table_name
        status, actual = None, {}
        while status is not ready:
            try:
                actual = self.dynamodb_client.describe_table(TableName=table_name)["Table"]
            except botocore.exceptions.ClientError as error:
                raise BloopException("Unexpected error while describing table.") from error
            status = simple_table_status(actual)
        expected = expected_table_description(model)
        if not compare_tables(model, actual, expected):
            raise TableMismatch("The expected and actual tables for {!r} do not match.".format(model.__name__))
        if model.Meta.stream:
            model.Meta.stream["arn"] = actual["LatestStreamArn"]

    def describe_stream(self, stream_arn, first_shard=None):
        """Wraps :func:`boto3.DynamoDBStreams.Client.describe_stream`, handling continuation tokens.

        :param str stream_arn: Stream arn, usually from the model's ``Meta.stream["arn"]``.
        :param str first_shard: *(Optional)* If provided, only shards after this shard id will be returned.
        :return: All shards in the stream, or a subset if ``first_shard`` is provided.
        :rtype: list
        """
        description = {"Shards": []}

        request = {"StreamArn": stream_arn, "ExclusiveStartShardId": first_shard}
        # boto3 isn't down with literal Nones.
        if first_shard is None:
            request.pop("ExclusiveStartShardId")

        while request.get("ExclusiveStartShardId") is not missing:
            try:
                response = self.stream_client.describe_stream(**request)["StreamDescription"]
            except botocore.exceptions.ClientError as error:
                if error.response["Error"]["Code"] == "ResourceNotFoundException":
                    raise InvalidStream("The stream arn {!r} does not exist.".format(stream_arn)) from error
                raise BloopException("Unexpected error while describing stream.") from error
            # Docs aren't clear if the terminal value is null, or won't exist.
            # Since we don't terminate the loop on None, the "or missing" here
            # will ensure we stop on a falsey value.
            request["ExclusiveStartShardId"] = response.pop("LastEvaluatedShardId", None) or missing
            description["Shards"].extend(response.pop("Shards", []))
            description.update(response)
        return description

    def get_shard_iterator(self, *, stream_arn, shard_id, iterator_type, sequence_number=None):
        """Wraps :func:`boto3.DynamoDBStreams.Client.get_shard_iterator`.

        :param str stream_arn: Stream arn.  Usually :data:`Shard.stream_arn <bloop.stream.shard.Shard.stream_arn>`.
        :param str shard_id: Shard identifier.  Usually :data:`Shard.shard_id <bloop.stream.shard.Shard.shard_id>`.
        :param str iterator_type: "sequence_at", "sequence_after", "trim_horizon", or "latest"
        :param sequence_number:
        :return: Iterator id, valid for 15 minutes.
        :rtype: str
        :raises bloop.exceptions.RecordsExpired: Tried to get an iterator beyond the Trim Horizon.
        """
        real_iterator_type = validate_stream_iterator_type(iterator_type)
        request = {
            "StreamArn": stream_arn,
            "ShardId": shard_id,
            "ShardIteratorType": real_iterator_type,
            "SequenceNumber": sequence_number
        }
        # boto3 isn't down with literal Nones.
        if sequence_number is None:
            request.pop("SequenceNumber")
        try:
            return self.stream_client.get_shard_iterator(**request)["ShardIterator"]
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == "TrimmedDataAccessException":
                raise RecordsExpired from error
            raise BloopException("Unexpected error while creating shard iterator") from error

    def get_stream_records(self, iterator_id):
        """Wraps :func:`boto3.DynamoDBStreams.Client.get_records`.

        :param iterator_id: Iterator id.  Usually :data:`Shard.iterator_id <bloop.stream.shard.Shard.iterator_id>`.
        :return: Dict with "Records" list (may be empty) and "NextShardIterator" str (may not exist).
        :rtype: dict
        :raises bloop.exceptions.RecordsExpired: The iterator moved beyond the Trim Horizon since it was created.
        :raises bloop.exceptions.ShardIteratorExpired: The iterator was created more than 15 minutes ago.
        """
        try:
            return self.stream_client.get_records(ShardIterator=iterator_id)
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == "TrimmedDataAccessException":
                raise RecordsExpired from error
            elif error.response["Error"]["Code"] == "ExpiredIteratorException":
                raise ShardIteratorExpired from error
            raise BloopException("Unexpected error while getting records.") from error


def validate_search_mode(mode):
    if mode not in {"query", "scan"}:
        raise InvalidSearchMode("{!r} is not a valid search mode.".format(mode))


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
