import collections
import botocore.exceptions

from .exceptions import BloopException, ConstraintViolation, TableMismatch
from .util import Sentinel, ordered
from .validation import validate_search_mode
ready = Sentinel("ready")

__all__ = ["SessionWrapper"]
# https://boto3.readthedocs.io/en/latest/reference/services/dynamodb.html#DynamoDB.Client.batch_get_item
BATCH_GET_ITEM_CHUNK_SIZE = 100


class SessionWrapper:
    def __init__(self, session):
        self._dynamodb_client = session.client("dynamodb")

    def save_item(self, item):
        wrapped_update_item(self._dynamodb_client, item)

    def delete_item(self, item):
        wrapped_delete_item(self._dynamodb_client, item)

    def load_items(self, items):
        loaded_items = {}
        requests = collections.deque(create_batch_get_chunks(items))
        while requests:
            request = requests.pop()
            response = wrapped_batch_get_item(self._dynamodb_client, request)
            # Accumulate results
            for table_name, table_items in response.get("Responses", {}).items():
                loaded_items.setdefault(table_name, []).extend(table_items)
            # Push additional requests onto the deque.
            # "UnprocessedKeys" is {} if this request is done
            if response["UnprocessedKeys"]:
                requests.append(response["UnprocessedKeys"])
        return loaded_items

    def query_items(self, request):
        return self.search_items("query", request)

    def scan_items(self, request):
        return self.search_items("scan", request)

    def search_items(self, mode, request):
        response = wrapped_search(self._dynamodb_client, mode, request)
        standardize_query_response(response)
        return response

    def create_table(self, model):
        table = create_table_request(model)
        wrapped_create_table(self._dynamodb_client, table, model)

    def validate_table(self, model):
        table_name = model.Meta.table_name
        status, description = None, {}
        while status is not ready:
            description = wrapped_describe_table(self._dynamodb_client, table_name)
            status = simple_table_status(description)
        if not compare_tables(description, model):
            raise TableMismatch("The expected and actual tables for {!r} do not match.".format(model.__name__))


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

# WRAPPERS ================================================================================================== WRAPPERS


def wrapped_batch_get_item(dynamodb_client, request):
    try:
        return dynamodb_client.batch_get_item(RequestItems=request)
    except botocore.exceptions.ClientError as error:
        raise BloopException("Unexpected error while loading items.") from error


def wrapped_update_item(dynamodb_client, item):
    try:
        dynamodb_client.update_item(**item)
    except botocore.exceptions.ClientError as error:
        handle_constraint_violation(error)


def wrapped_delete_item(dynamodb_client, item):
    try:
        dynamodb_client.delete_item(**item)
    except botocore.exceptions.ClientError as error:
        handle_constraint_violation(error)


def wrapped_search(dynamodb_client, mode, request):
    validate_search_mode(mode)
    method = getattr(dynamodb_client, mode)
    try:
        return method(**request)
    except botocore.exceptions.ClientError as error:
        raise BloopException("Unexpected error during {}.".format(mode)) from error


def wrapped_create_table(dynamodb_client, table, model):
    try:
        dynamodb_client.create_table(**table)
    except botocore.exceptions.ClientError as error:
        handle_table_exists(error, model)


def wrapped_describe_table(dynamodb_client, table_name):
    try:
        return dynamodb_client.describe_table(TableName=table_name)["Table"]
    except botocore.exceptions.ClientError as error:
        raise BloopException("Unexpected error while describing table.") from error


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


def compare_tables(actual_description, model):
    expected = expected_table_description(model)
    try:
        actual = sanitized_table_description(actual_description)
    except KeyError:
        return False
    return ordered(actual) == ordered(expected)


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
    }[index.projection]

    projection = {"ProjectionType": projection_type}
    if index.projection == "include":
        projection["NonKeyAttributes"] = [
            column.dynamo_name
            for column in index.projected_columns
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
    return table


def expected_table_description(model):
    # Right now, we expect the exact same thing as create_table_request
    # This doesn't include statuses (table, indexes) since that's
    # pulled out by the polling mechanism
    table = create_table_request(model)
    return table


def sanitized_table_description(description):
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
            {"AttributeName": attr_definition["AttributeName"],
             "AttributeType": attr_definition["AttributeType"]}
            for attr_definition in description["AttributeDefinitions"]
        ],
        "GlobalSecondaryIndexes": [
            {"IndexName": gsi["IndexName"],
             "KeySchema": [
                 {"AttributeName": gsi_key["AttributeName"],
                  "KeyType": gsi_key["KeyType"]}
                 for gsi_key in gsi["KeySchema"]],
             "Projection": {
                 "NonKeyAttributes":
                     gsi["Projection"].get("NonKeyAttributes", []),
                 "ProjectionType": gsi["Projection"]["ProjectionType"]},
             "ProvisionedThroughput": {
                 "ReadCapacityUnits":
                     gsi["ProvisionedThroughput"]["ReadCapacityUnits"],
                 "WriteCapacityUnits":
                     gsi["ProvisionedThroughput"]["WriteCapacityUnits"]}}
            for gsi in description.get("GlobalSecondaryIndexes", [])
        ],
        "KeySchema": [
            {"AttributeName": table_key["AttributeName"],
             "KeyType": table_key["KeyType"]}
            for table_key in description["KeySchema"]
        ],
        "LocalSecondaryIndexes": [
            {"IndexName": lsi["IndexName"],
             "KeySchema": [
                 {"AttributeName": lsi_key["AttributeName"],
                  "KeyType": lsi_key["KeyType"]}
                 for lsi_key in lsi["KeySchema"]],
             "Projection": {
                 "NonKeyAttributes":
                     lsi["Projection"].get("NonKeyAttributes", []),
                 "ProjectionType": lsi["Projection"]["ProjectionType"]}}
            for lsi in description.get("LocalSecondaryIndexes", [])
        ],
        "ProvisionedThroughput": {
            "ReadCapacityUnits":
                description["ProvisionedThroughput"]["ReadCapacityUnits"],
            "WriteCapacityUnits":
                description["ProvisionedThroughput"]["WriteCapacityUnits"]
        },
        "TableName": description["TableName"]
    }

    # Safe to concatenate here since we won't be removing items from the
    # combined list, but modifying the mutable dicts within
    indexes = table["GlobalSecondaryIndexes"] + table["LocalSecondaryIndexes"]
    for index in indexes:
        if not index["Projection"]["NonKeyAttributes"]:
            index["Projection"].pop("NonKeyAttributes")

    if not table["GlobalSecondaryIndexes"]:
        table.pop("GlobalSecondaryIndexes")
    if not table["LocalSecondaryIndexes"]:
        table.pop("LocalSecondaryIndexes")

    return table


def simple_table_status(description):
    status = ready
    if description.get("TableStatus") != "ACTIVE":
        status = None
    for index in description.get("GlobalSecondaryIndexes", []):
        if index.get("IndexStatus") != "ACTIVE":
            status = None
    return status
