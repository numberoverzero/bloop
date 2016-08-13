import collections
import botocore.exceptions

from ..exceptions import AbstractModelException, TableMismatch
from ..util import ordered
from .models import create_batch_get_chunks, handle_constraint_violation, standardize_query_response
from .tables import (
    create_table_request,
    expected_table_description,
    ready,
    simple_table_status,
    sanitized_table_description
)
__all__ = (
    "create_table",
    "delete_item",
    "describe_table",
    "load_items",
    "query_request",
    "save_item",
    "scan_request",
    "validate_table"
)


def save_item(dynamodb_client, item):
    try:
        dynamodb_client.update_item(**item)
    except botocore.exceptions.ClientError as error:
        handle_constraint_violation(error, "save", item)


def delete_item(dynamodb_client, item):
    try:
        dynamodb_client.delete_item(**item)
    except botocore.exceptions.ClientError as error:
        handle_constraint_violation(error, "delete", item)


def load_items(dynamodb_client, items):
    loaded_items = {}
    requests = collections.deque(create_batch_get_chunks(items))
    while requests:
        request = requests.pop()
        response = dynamodb_client.batch_get_item(RequestItems=request)
        # Accumulate results
        for table_name, table_items in response.get("Responses", {}).items():
            loaded_items.setdefault(table_name, []).extend(table_items)
        # Push additional requests onto the deque.
        # "UnprocessedKeys" is {} if this request is done
        if response["UnprocessedKeys"]:
            requests.append(response["UnprocessedKeys"])
    return loaded_items


def query_request(dynamodb_client, request):
    response = dynamodb_client.query(**request)
    standardize_query_response(response)
    return response


def scan_request(dynamodb_client, request):
    response = dynamodb_client.scan(**request)
    standardize_query_response(response)
    return response


def create_table(dynamodb_client, model):
    if model.Meta.abstract:
        raise AbstractModelException(model)
    table = create_table_request(model)
    try:
        dynamodb_client.create_table(**table)
    except botocore.exceptions.ClientError as error:
        # Raise unless the table already exists
        error_code = error.response["Error"]["Code"]
        if error_code != "ResourceInUseException":
            raise error


def describe_table(dynamodb_client, model):
    return dynamodb_client.describe_table(TableName=model.Meta.table_name)["Table"]


def validate_table(dynamodb_client, model):
    expected = expected_table_description(model)
    status, description = None, {}
    while status is not ready:
        description = describe_table(dynamodb_client, model)
        status = simple_table_status(description)
    try:
        actual = sanitized_table_description(description)
    except KeyError:
        raise TableMismatch(model, expected, description)
    if ordered(actual) != ordered(expected):
        raise TableMismatch(model, expected, actual)
