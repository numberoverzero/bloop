import botocore.exceptions

from ..exceptions import AbstractModelException, TableMismatch
from ..util import ordered
from .models import handle_constraint_violation
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
    "save_item",
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


def load_items(dynamodb_client, items):  # pragma: no cover
    pass


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
    status = None
    while status is not ready:
        description = describe_table(dynamodb_client, model)
        status = simple_table_status(description)
    try:
        actual = sanitized_table_description(description)
    except KeyError:
        raise TableMismatch(model, expected, description)
    if ordered(actual) != ordered(expected):
        raise TableMismatch(model, expected, actual)
