import bloop.column
import bloop.exceptions
import bloop.tables
import bloop.util
import boto3
import botocore
import functools
import time

__all__ = ["Client"]

DEFAULT_BACKOFF_COEFF = 50.0
DEFAULT_MAX_ATTEMPTS = 4
MAX_BATCH_SIZE = 25
RETRYABLE_ERRORS = [
    "InternalServerError",
    "ProvisionedThroughputExceededException"
]


def default_backoff_func(attempts):
    """
    Exponential backoff helper.

    attempts is the number of calls so far that have failed
    """
    if attempts == DEFAULT_MAX_ATTEMPTS:
        raise RuntimeError("Failed after {} attempts".format(attempts))
    return (DEFAULT_BACKOFF_COEFF * (2 ** attempts)) / 1000.0


def partition_batch_get_input(batch_size, items):
    """ Takes a batch_get input and partitions into 25 object chunks """
    chunk = {}
    count = 0
    for table_name, table_attrs in items.items():
        consistent_read = table_attrs.get("ConsistentRead", False)
        for key in table_attrs["Keys"]:
            # This check needs to be in the inner loop, in case the chunk
            # clears in the middle of iterating this table's keys.
            table = chunk.get(table_name, None)
            if table is None:
                table = chunk[table_name] = {
                    "ConsistentRead": consistent_read,
                    "Keys": []}
            table["Keys"].append(key)
            count += 1
            if count >= batch_size:
                yield chunk
                count = 0
                chunk = {}
    # Last chunk, less than batch_size items
    if chunk:
        yield chunk


class Client(object):
    """Intermediate client that wraps a ``boto3.client('dynamodb')``.

    Client simplifies the particularly tedious and low-level tasks when
    interfacing with DynamoDB, such as retries with exponential backoff,
    batching requests, and following continuation tokens.

    Except where model classes are taken as arguments, the function signatures
    match those of their boto3 client counterparts.

    See Also:
        * `boto3 DynamoDB Client`_
        * `DynamoDB API Reference`_

    Attributes:
        boto_client (boto3.client): Low-level client to communicate
            with DynamoDB
        backoff_func (func<int>): Calculates the duration to wait between
            retries.  By default, an exponential backoff function is used.
        batch_size (int): The maximum number of items to include in a batch
            request to DynamoDB.  Default value is 25, a lower limit may be
            useful to constrain per-request sizes.

    .. _boto3 DynamoDB Client:
        http://boto3.readthedocs.org/en/latest/reference/services/dynamodb.html#client
    .. _DynamoDB API Reference:
        http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Operations.html
    """
    def __init__(self, boto_client=None, backoff_func=None,
                 batch_size=MAX_BATCH_SIZE):
        """
        backoff_func is an optional function that takes an int
            (attempts so far) that should either:
            - return the number of seconds to sleep
            - raise to stop
        """
        # Fall back to the global session
        self.boto_client = boto_client or boto3.client("dynamodb")
        self.backoff_func = backoff_func or default_backoff_func
        self.batch_size = batch_size

    def _call_with_retries(self, func, *args, **kwargs):
        attempts = 1
        while True:
            try:
                output = func(*args, **kwargs)
            except botocore.exceptions.ClientError as error:
                error_code = error.response["Error"]["Code"]
                if error_code not in RETRYABLE_ERRORS:
                    raise error
            else:
                # No exception, success!
                return output

            # Backoff in milliseconds
            # backoff_func will return a number of seconds to wait, or raise
            delay = self.backoff_func(attempts)
            time.sleep(delay)
            attempts += 1

    def _filter(self, client_func, request):
        # Wrap client function in retries
        response = self._call_with_retries(client_func, **request)

        # When updating count, ScannedCount is omitted unless it differs
        # from Count; thus we need to default to assume that the
        # ScannedCount is equal to the Count
        count = response.get("Count", 0)
        response["Count"] = count
        response["ScannedCount"] = response.get("ScannedCount", count)
        return response

    def _modify_item(self, client_func, name, item):
        try:
            self._call_with_retries(client_func, **item)
        except botocore.exceptions.ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code == "ConditionalCheckFailedException":
                raise bloop.exceptions.ConstraintViolation(name, item)
            else:
                raise error

    def batch_get_items(self, items):
        """Load objects in batches from DynamoDB.

        The dict structure is identical to the boto3 client's counterpart.
        It also handles partitioning inputs larger than 25 items (default
        DynamoDB batch size) into batches, retrying failed requests, and
        following UnprocessedKeys for each batch.

        Args:
            items (dict): See `batch_get_item (DynamoDB Client)`_

        Returns:
            dict: {table_name: [], ...}
                Where each key is a table name, and its value is the list of
                the objects loaded from that table.

        Note:
            Order between request and response lists within a table IS NOT
            preserved.  There are numerous factors that will influence ordering
            including retries, unprocessed items, and batch boundaries.

            Instead you should keep an indexable reference to each key passed
            in and look up items in the return list using it.  For performance,
            it's likely better to pop each item from the result and retrieve
            the model instance to load into from a dict, using the hashable
            representation of the item key as the dict key.

        See Also:
            * `BatchGetItem (DynamoDB API Reference)`_
            * `batch_get_item (DynamoDB Client)`_

        .. _BatchGetItem (DynamoDB API Reference):
            http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
        .. _batch_get_item (DynamoDB Client):
            http://boto3.readthedocs.org/en/latest/reference/services/dynamodb.html#DynamoDB.Client.batch_get_item

        """
        response = {}
        get_batch = functools.partial(self._call_with_retries,
                                      self.boto_client.batch_get_item)
        request_batches = partition_batch_get_input(self.batch_size, items)

        for request_batch in request_batches:
            # After the first call, request_batch is the
            # UnprocessedKeys from the first call
            while request_batch:
                batch_response = get_batch(RequestItems=request_batch)
                items = batch_response.get("Responses", {}).items()
                for table_name, table_items in items:
                    if table_name not in response:
                        response[table_name] = []
                    response[table_name].extend(table_items)

                # If there are no unprocessed keys, this will be an empty
                # list which will break the while loop, moving to the next
                # batch of items
                request_batch = batch_response.get("UnprocessedKeys",  None)
        return response

    def create_table(self, model):
        """Create a new table from the model.

        If the table already exists, 'ResourceInUseException' is suppressed.
        This operation does not wait for the table to be in the ACTIVE state,
        nor does it ensure an existing table matches the expected schema.

        To verify the table exists, is ACTIVE, and has a matching schema, use
        :meth:`.validate_table`.

        Args:
            model: subclass of an :class:`bloop.model.BaseModel`

        See Also:
            * :meth:`.describe_table`
            * :meth:`.validate_table`
            * `CreateTable (DynamoDB API Reference)`_
            * `create_table (DynamoDB Client)`_

        .. _CreateTable (DynamoDB API Reference):
            http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html
        .. _create_table (DynamoDB Client):
            https://boto3.readthedocs.org/en/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
        """
        if model.Meta.abstract:
            raise bloop.exceptions.AbstractModelException(model)
        table = bloop.tables.create_request(model)
        create = functools.partial(self._call_with_retries,
                                   self.boto_client.create_table)
        try:
            create(**table)
        except botocore.exceptions.ClientError as error:
            # Raise unless the table already exists
            error_code = error.response["Error"]["Code"]
            if error_code != "ResourceInUseException":
                raise error

    def delete_item(self, item):
        """Delete an item from DynamoDB.

        The dict structure is identical to the boto3 client's counterpart.
        It also handles retrying failed requests.

        Args:
            item (dict): See `delete_item (DynamoDB Client)`_

        Raises:
            bloop.ConstraintViolation: If the update contains a condition that
                fails on update.

        See Also:
            * `DeleteItem (DynamoDB API Reference)`_
            * `delete_item (DynamoDB Client)`_

        .. _DeleteItem (DynamoDB API Reference):
            http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html
        .. _delete_item (DynamoDB Client):
            https://boto3.readthedocs.org/en/latest/reference/services/dynamodb.html#DynamoDB.Client.delete_item
        """
        self._modify_item(self.boto_client.delete_item, "delete", item)

    def describe_table(self, model):
        """Load the schema for a model's table.

        Args:
            model: subclass of an :class:`bloop.model.BaseModel`

        Returns:
            dict: The same return value from a ``boto3.client`` with the above
                fields stripped out.

        See Also:
            * :meth:`.create_table`
            * :meth:`.validate_table`
            * `DescribeTable (DynamoDB API Reference)`_
            * `describe_table (DynamoDB Client)`_

        .. _DescribeTable (DynamoDB API Reference):
            http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html
        .. _describe_table (DynamoDB Client):
            https://boto3.readthedocs.org/en/latest/reference/services/dynamodb.html#DynamoDB.Client.describe_table

        """
        description = self._call_with_retries(
            self.boto_client.describe_table,
            TableName=model.Meta.table_name)
        return description["Table"]

    def query(self, request):
        return self._filter(self.boto_client.query, request)

    def scan(self, request):
        return self._filter(self.boto_client.scan, request)

    def update_item(self, item):
        """Update an item in DynamoDB.  Only modify given values.

        The dict structure is identical to the boto3 client's counterpart.
        It also handles retrying failed requests.

        Args:
            item (dict): See `update_item (DynamoDB Client)`_

        Raises:
            bloop.ConstraintViolation: If the update contains a condition that
                fails on update.

        See Also:
            * `UpdateItem (DynamoDB API Reference)`_
            * `update_item (DynamoDB Client)`_

        .. _UpdateItem (DynamoDB API Reference):
            http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
        .. _update_item (DynamoDB Client):
            https://boto3.readthedocs.org/en/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
        """
        self._modify_item(self.boto_client.update_item, "update", item)

    def validate_table(self, model):
        """Busy poll until table is ACTIVE.  Raises on schema mismatch.

        The table and all GSIs must be ACTIVE before the schemas will be
        compared.  If the required schema for a model doesn't match the
        existing table's schema, :exc:`bloop.TableMismatch` is raised.

        Args:
            model: subclass of an :class:`bloop.model.BaseModel`

        Raises:
            bloop.TableMismatch: If the actual schema doesn't
                match the required schema for the model.

        See Also:
            * :meth:`.create_table`
            * :meth:`.describe_table`
        """
        expected = bloop.tables.expected_description(model)
        status = "BLOOP_NOT_ACTIVE"
        while status == "BLOOP_NOT_ACTIVE":
            description = self.describe_table(model)
            status = bloop.tables.simple_status(description)
        try:
            actual = bloop.tables.sanitized_description(description)
        except KeyError:
            raise bloop.exceptions.TableMismatch(model, expected, description)
        if bloop.util.ordered(actual) != bloop.util.ordered(expected):
            raise bloop.exceptions.TableMismatch(model, expected, actual)
