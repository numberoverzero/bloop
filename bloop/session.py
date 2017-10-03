import collections
import json
import logging

import boto3
import botocore.exceptions

from .exceptions import (
    BloopException,
    ConstraintViolation,
    InvalidSearch,
    InvalidShardIterator,
    InvalidStream,
    RecordsExpired,
    ShardIteratorExpired,
    TableMismatch,
)
from .util import Sentinel, ordered


logger = logging.getLogger("bloop.session")
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

    def create_table(self, table_name, model):
        """Create the model's table.  Returns True if the table is being created, False otherwise.

        Does not wait for the table to create, and does not validate an existing table.
        Will not raise "ResourceInUseException" if the table exists or is being created.

        :param str table_name: The name of the table to create for the model.
        :param model: The :class:`~bloop.models.BaseModel` to create the table for.
        :return: True if the table is being created, False if the table exists
        :rtype: bool
        """
        table = create_table_request(table_name, model)
        try:
            self.dynamodb_client.create_table(**table)
            is_creating = True
        except botocore.exceptions.ClientError as error:
            handle_table_exists(error, model)
            is_creating = False
        return is_creating

    def describe_table(self, table_name):
        """
        Polls until the table is ready, then returns the first result when the table was ready.
        :param table_name: The name of the table to describe
        :return: The result of DescribeTable["Table"]
        :rtype: dict
        """
        status, description = None, {}
        calls = 0
        while status is not ready:
            calls += 1
            try:
                description = self.dynamodb_client.describe_table(TableName=table_name)["Table"]
            except botocore.exceptions.ClientError as error:
                raise BloopException("Unexpected error while describing table.") from error
            status = simple_table_status(description)
        logger.debug("describe_table: table \"{}\" was in ACTIVE state after {} calls".format(table_name, calls))
        return description

    def validate_table(self, table_name, model):
        """Polls until a creating table is ready, then verifies the description against the model's requirements.

        The model may have a subset of all GSIs and LSIs on the table, but the key structure must be exactly
        the same.  The table must have a stream if the model expects one, but not the other way around.  When read or
        write units are not specified for the model or any GSI, the existing values will always pass validation.

        :param str table_name: The name of the table to validate the model against.
        :param model: The :class:`~bloop.models.BaseModel` to validate the table of.
        :raises bloop.exceptions.TableMismatch: When the table does not meet the constraints of the model.
        """
        actual = self.describe_table(table_name)
        if model.Meta.ttl:
            ttl = self.dynamodb_client.describe_time_to_live(TableName=table_name)
            actual["TimeToLiveDescription"] = ttl["TimeToLiveDescription"]
        expected = expected_table_description(table_name, model)
        if not compare_tables(model, actual, expected):
            raise TableMismatch("The expected and actual tables for {!r} do not match.".format(model.__name__))
        if model.Meta.stream:
            stream_arn = model.Meta.stream["arn"] = actual["LatestStreamArn"]
            logger.debug(
                "Set {}.Meta.stream[\"arn\"] to \"{}\" from DescribeTable response".format(
                    model.__name__, stream_arn
                )
            )
        if model.Meta.ttl:
            model.Meta.ttl["enabled"] = actual["TimeToLiveDescription"]["TimeToLiveStatus"].lower()
        if model.Meta.read_units is None:
            read_units = model.Meta.read_units = actual["ProvisionedThroughput"]["ReadCapacityUnits"]
            logger.debug(
                "{}.Meta does not specify read_units, set to {} from DescribeTable response".format(
                    model.__name__, read_units)
            )
        if model.Meta.write_units is None:
            write_units = model.Meta.write_units = actual["ProvisionedThroughput"]["WriteCapacityUnits"]
            logger.debug(
                "{}.Meta does not specify write_units, set to {} from DescribeTable response".format(
                    model.__name__, write_units)
            )
        # Replace any ``None`` values for read_units, write_units in GSIs with their actual values
        gsis = {index["IndexName"]: index for index in actual.pop("GlobalSecondaryIndexes", [])}
        for index in model.Meta.gsis:
            read_units = gsis[index.dynamo_name]["ProvisionedThroughput"]["ReadCapacityUnits"]
            write_units = gsis[index.dynamo_name]["ProvisionedThroughput"]["WriteCapacityUnits"]
            if index.read_units is None:
                index.read_units = read_units
                logger.debug(
                    "{}.{} does not specify read_units, set to {} from DescribeTable response".format(
                        model.__name__, index.name, read_units)
                )
            if index.write_units is None:
                index.write_units = write_units
                logger.debug(
                    "{}.{} does not specify write_units, set to {} from DescribeTable response".format(
                        model.__name__, index.name, write_units)
                )

    def enable_ttl(self, table_name, model):
        """Calls UpdateTimeToLive on the table according to model.Meta["ttl"]

        :param table_name: The name of the table to enable the TTL setting on
        :param model: The model to get TTL settings from
        """
        ttl_name = model.Meta.ttl["column"].dynamo_name
        request = {
            "TableName": table_name,
            "TimeToLiveSpecification": {
                "AttributeName": ttl_name,
                "Enabled": True
            }
        }
        try:
            self.dynamodb_client.update_time_to_live(**request)
        except botocore.exceptions.ClientError as error:
            raise BloopException("Unexpected error while setting TTL.") from error

    def describe_stream(self, stream_arn, first_shard=None):
        """Wraps :func:`boto3.DynamoDBStreams.Client.describe_stream`, handling continuation tokens.

        :param str stream_arn: Stream arn, usually from the model's ``Meta.stream["arn"]``.
        :param str first_shard: *(Optional)* If provided, only shards after this shard id will be returned.
        :return: All shards in the stream, or a subset if ``first_shard`` is provided.
        :rtype: dict
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
        raise InvalidSearch("{!r} is not a valid search mode.".format(mode))


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
    # returns a new dict so we can safely modify before using ordered(..) == ordered(..)
    # without losing access to attributes in the table that we'll apply back to the model
    # (eg. ignoring the stream arn if stream is None for validation but then preserving the
    # value in the model post-validation)
    actual = sanitize_table_description(actual)

    # 1. If the table doesn't specify an expected stream type or ttl,
    #    don't inspect the StreamSpecification or TimeToLiveDescription at all.
    if not model.Meta.stream:
        actual.pop("StreamSpecification", None)
    if not model.Meta.ttl:
        actual.pop("TimeToLiveDescription", None)
    if model.Meta.read_units is None:
        actual["ProvisionedThroughput"].pop("ReadCapacityUnits")
        expected["ProvisionedThroughput"].pop("ReadCapacityUnits")
    if model.Meta.write_units is None:
        actual["ProvisionedThroughput"].pop("WriteCapacityUnits")
        expected["ProvisionedThroughput"].pop("WriteCapacityUnits")

    # 2. Check indexes.  Actual projections must be a superset of the expected,
    #    and additional indexes are allowed.
    #    GSIs/LSIs are popped so that ordered comparison succeeds with projection supersets.
    for model_indexes, index_type in zip(
            (model.Meta.gsis, model.Meta.lsis),
            ("GlobalSecondaryIndexes", "LocalSecondaryIndexes")):
        if not model_indexes:
            continue
        actual_indexes = {index["IndexName"]: index for index in actual.pop(index_type, [])}
        expected_indexes = {index["IndexName"]: index for index in expected.pop(index_type, [])}
        for index in model_indexes:
            actual_index = actual_indexes.get(index.dynamo_name, None)
            expected_index = expected_indexes[index.dynamo_name]
            if not actual_index:
                logger.debug("compare_tables: table is missing expected index \"{}\"".format(index.name))
                return False
            index_type = actual_index["Projection"]["ProjectionType"]
            # "ALL" will always include the model's projection
            if index_type == "ALL":
                continue
            elif index_type == "KEYS_ONLY":
                projected = {
                    *[k.dynamo_name for k in model.Meta.keys],
                    *[k.dynamo_name for k in index.keys]
                }
            elif index_type == "INCLUDE":
                projected = set(actual_index["Projection"]["NonKeyAttributes"])
            else:
                logger.debug("compare_tables: unknown index projection type \"{}\"".format(index_type))
                return False
            index_includes = {column.dynamo_name for column in index.projection["included"]}
            missing = index_includes - projected
            if missing:
                logger.debug(
                    "compare_tables: actual projection for index \"{}\" is missing expected columns {}".format(
                        index.name, missing))
                return False
            if ordered(expected_index["KeySchema"]) != ordered(actual_index["KeySchema"]):
                logger.debug("compare_tables: key schema mismatch for \"{}\": {} != {}".format(
                    index.name,
                    json.dumps(ordered(actual_index["KeySchema"]), sort_keys=True),
                    json.dumps(ordered(expected_index["KeySchema"]), sort_keys=True),
                ))
                return False
            if "ProvisionedThroughput" not in expected_index:
                # LSI
                continue
            if index.read_units is None:
                actual_index["ProvisionedThroughput"].pop("ReadCapacityUnits")
                expected_index["ProvisionedThroughput"].pop("ReadCapacityUnits")
            if index.write_units is None:
                actual_index["ProvisionedThroughput"].pop("WriteCapacityUnits")
                expected_index["ProvisionedThroughput"].pop("WriteCapacityUnits")
            if ordered(expected_index["ProvisionedThroughput"]) != ordered(actual_index["ProvisionedThroughput"]):
                logger.debug("compare_tables: GSI ProvisionedThroughput mismatch: {} != {}".format(
                    json.dumps(ordered(actual_index["ProvisionedThroughput"]), sort_keys=True),
                    json.dumps(ordered(expected_index["ProvisionedThroughput"]), sort_keys=True),
                ))
                return False
    # 3. AttributeNames expected are a subset of actual (ie. an unknown index's hash key)
    # Ignore order for inner lists
    all_attributes = ordered(actual.pop("AttributeDefinitions"))
    required_attributes = ordered(expected.pop("AttributeDefinitions"))
    missing_attributes = [x for x in required_attributes if x not in all_attributes]
    if missing_attributes:
        logger.debug(
            "compare_tables: the following attributes are missing for model \"{}\": {}".format(
                model.__name__, missing_attributes))
        return False
    matches = ordered(actual) == ordered(expected)
    if not matches:
        logger.debug(
            "compare_tables: expected and actual table descriptions for model \"{}\" do not match: {} != {}".format(
                model.__name__,
                json.dumps(ordered(actual), sort_keys=True),
                json.dumps(ordered(expected), sort_keys=True))
        )
    return matches


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
            # On create when not specified, use minimum values instead of None
            "WriteCapacityUnits": index.write_units or 1,
            "ReadCapacityUnits": index.read_units or 1
        },
    }


def local_secondary_index(index):
    return {
        "IndexName": index.dynamo_name,
        "KeySchema": key_schema(index=index),
        "Projection": index_projection(index),
    }


def create_table_request(table_name, model):
    table = {
        "AttributeDefinitions": attribute_definitions(model),
        "KeySchema": key_schema(model=model),
        "ProvisionedThroughput": {
            # On create when not specified, use minimum values instead of None
            "WriteCapacityUnits": model.Meta.write_units or 1,
            "ReadCapacityUnits": model.Meta.read_units or 1,
        },
        "TableName": table_name,
    }
    if model.Meta.gsis:
        table["GlobalSecondaryIndexes"] = [
            global_secondary_index(index) for index in model.Meta.gsis]
    if model.Meta.lsis:
        table["LocalSecondaryIndexes"] = [
            local_secondary_index(index) for index in model.Meta.lsis]
    if model.Meta.stream:
        include = model.Meta.stream["include"]
        # noinspection PyTypeChecker
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


def expected_table_description(table_name, model):
    table = create_table_request(table_name, model)
    if model.Meta.ttl:
        table["TimeToLiveDescription"] = {
            # For now we ignore TimeToLiveStatus because enabling can take up to an hour;
            # we don't really care if it's on, so long as it's specified.
            "AttributeName": model.Meta.ttl["column"].dynamo_name
        }
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
        "TableName": description.get("TableName", None),
        "TimeToLiveDescription": {
            "AttributeName": description.get("TimeToLiveDescription", {"AttributeName": None})["AttributeName"],
        },
    }

    indexes = table["GlobalSecondaryIndexes"] + table["LocalSecondaryIndexes"]
    for index in indexes:
        if not index["Projection"]["NonKeyAttributes"]:
            index["Projection"].pop("NonKeyAttributes")
    if not table["TimeToLiveDescription"]["AttributeName"]:
        table.pop("TimeToLiveDescription")
    allowed_empty = ["GlobalSecondaryIndexes", "LocalSecondaryIndexes", "StreamSpecification"]
    for possibly_empty in allowed_empty:
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
