import collections
import botocore.exceptions

from ..exceptions import BloopException, ConstraintViolation, TableMismatch
from .models import create_batch_get_chunks, standardize_query_response
from .tables import (
    compare_tables,
    create_table_request,
    ready,
    simple_table_status,
)
__all__ = ["SessionWrapper"]


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
            raise TableMismatch("The expected and actual tables for {!r} do not match".format(model.__name__))


def handle_constraint_violation(error):
    error_code = error.response["Error"]["Code"]
    if error_code == "ConditionalCheckFailedException":
        raise ConstraintViolation("The provided condition was not met") from error
    else:
        raise BloopException("Unexpected error while modifying item") from error


def handle_table_exists(error, model):
    error_code = error.response["Error"]["Code"]
    if error_code != "ResourceInUseException":
        raise BloopException("Unexpected error while creating table {!r}".format(model.__name__)) from error
    # Don't raise if the table already exists


def wrapped_batch_get_item(dynamodb_client, request):
    try:
        return dynamodb_client.batch_get_item(RequestItems=request)
    except botocore.exceptions.ClientError as error:
        raise BloopException("Unexpected error while loading items") from error


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
    if mode == "scan":
        method = dynamodb_client.scan
    elif mode == "query":
        method = dynamodb_client.query
    else:
        raise ValueError("Unknown search mode {!r}".format(mode))
    try:
        return method(**request)
    except botocore.exceptions.ClientError as error:
        raise BloopException("Unexpected error during {}".format(mode)) from error


def wrapped_create_table(dynamodb_client, table, model):
    try:
        dynamodb_client.create_table(**table)
    except botocore.exceptions.ClientError as error:
        handle_table_exists(error, model)


def wrapped_describe_table(dynamodb_client, table_name):
    try:
        return dynamodb_client.describe_table(TableName=table_name)["Table"]
    except botocore.exceptions.ClientError as error:
        raise BloopException("Unexpected error while loading items") from error
