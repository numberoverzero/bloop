import collections
import botocore.exceptions

from ..exceptions import TableMismatch
from ..util import ordered
from .models import create_batch_get_chunks, handle_constraint_violation, standardize_query_response
from .tables import (
    create_table_request,
    describe_table,
    expected_table_description,
    ready,
    simple_table_status,
    sanitized_table_description
)
__all__ = ["SessionWrapper"]


class SessionWrapper:
    def __init__(self, session):
        self._dynamodb_client = session.client("dynamodb")

    def save_item(self, item):
        try:
            self._dynamodb_client.update_item(**item)
        except botocore.exceptions.ClientError as error:
            handle_constraint_violation(error, "save", item)

    def delete_item(self, item):
        try:
            self._dynamodb_client.delete_item(**item)
        except botocore.exceptions.ClientError as error:
            handle_constraint_violation(error, "delete", item)

    def load_items(self, items):
        loaded_items = {}
        requests = collections.deque(create_batch_get_chunks(items))
        while requests:
            request = requests.pop()
            response = self._dynamodb_client.batch_get_item(RequestItems=request)
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
        if mode == "scan":
            response = self._dynamodb_client.scan(**request)
        elif mode == "query":
            response = self._dynamodb_client.query(**request)
        else:
            raise ValueError("Unknown search mode {!r}".format(mode))
        standardize_query_response(response)
        return response

    def create_table(self, model):
        table = create_table_request(model)
        try:
            self._dynamodb_client.create_table(**table)
        except botocore.exceptions.ClientError as error:
            # Raise unless the table already exists
            error_code = error.response["Error"]["Code"]
            if error_code != "ResourceInUseException":
                raise error

    def validate_table(self, model):
        expected = expected_table_description(model)
        status, description = None, {}
        while status is not ready:
            description = describe_table(self._dynamodb_client, model)
            status = simple_table_status(description)
        try:
            actual = sanitized_table_description(description)
        except KeyError:
            raise TableMismatch(model, expected, description)
        if ordered(actual) != ordered(expected):
            raise TableMismatch(model, expected, actual)
