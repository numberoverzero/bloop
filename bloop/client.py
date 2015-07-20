import bloop.column
import bloop.exceptions
import bloop.index
import bloop.util
import boto3
import botocore
import enum
import functools
import time


TableStatus = enum.Enum('TableStatus', ['Busy', 'Ready'])
DEFAULT_BACKOFF_COEFF = 50.0
DEFAULT_MAX_ATTEMPTS = 4
MAX_BATCH_SIZE = 25
RETRYABLE_ERRORS = [
    "InternalServerError",
    "ProvisionedThroughputExceededException"
]


def default_backoff_func(operation, attempts):
    '''
    Exponential backoff helper.

    attempts is the number of calls so far that have failed
    '''
    if attempts == DEFAULT_MAX_ATTEMPTS:
        raise RuntimeError("Failed {} after {} attempts".format(
            operation, attempts))
    return (DEFAULT_BACKOFF_COEFF * (2 ** attempts)) / 1000.0


def partition_batch_get_input(batch_size, items):
    ''' Takes a batch_get input and partitions into 25 object chunks '''
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
    def __init__(self, session=None, backoff_func=None,
                 batch_size=MAX_BATCH_SIZE):
        '''
        backoff_func is an optional function with signature
        (dynamo operation name, attempts so far) that should either:
            - return the number of seconds to sleep
            - raise to stop
        '''
        # Fall back to the global session
        self.client = (session or boto3).client("dynamodb")
        self.backoff_func = backoff_func or default_backoff_func
        self.batch_size = batch_size

    def _call_with_retries(self, func, *args, **kwargs):
        ''' Uses `self.backoff_func` to handle retries '''
        operation = func.__name__
        attempts = 1
        while True:
            try:
                output = func(*args, **kwargs)
            except botocore.exceptions.ClientError as error:
                error_code = error.response['Error']['Code']
                if error_code not in RETRYABLE_ERRORS:
                    raise error
            else:
                # No exception, success!
                return output

            # Backoff in milliseconds
            # backoff_func will return a number of seconds to wait, or raise
            delay = self.backoff_func(operation, attempts)
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
            error_code = error.response['Error']['Code']
            if error_code == 'ConditionalCheckFailedException':
                raise bloop.exceptions.ConstraintViolation(name, item)
            else:
                raise error

    def batch_get_items(self, items):
        '''
        Takes the "RequestItems" dict and returns the "Responses" dict
        documented here:
            http://docs.aws.amazon.com/amazondynamodb/latest/ \
                APIReference/API_BatchGetItem.html

        Handles batching and throttling/retry with backoff
        '''
        response = {}
        get_batch = functools.partial(self._call_with_retries,
                                      self.client.batch_get_item)
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
        '''
        Suppress ResourceInUseException (table already exists)

        Does not wait for table to be ACTIVE, or validate schema.  This allows
        multiple CreateTable calls to kick off at once, and busy polling can
        block afterwards.
        '''
        table = table_for_model(model)
        create = functools.partial(self._call_with_retries,
                                   self.client.create_table)
        try:
            create(**table)
        except botocore.exceptions.ClientError as error:
            # Raise unless the table already exists
            error_code = error.response['Error']['Code']
            if error_code != 'ResourceInUseException':
                raise error

    def delete_item(self, item):
        self._modify_item(self.client.delete_item, "delete", item)

    def describe_table(self, model):
        description = self._call_with_retries(
            self.client.describe_table,
            TableName=model.Meta.table_name)["Table"]

        # We don't care about a bunch of the returned attributes, and want to
        # massage the returned value to match `table_for_model` that's passed
        # to `Client.create_table` so we can compare them with `ordered`
        fields = ["TableName", "ProvisionedThroughput", "KeySchema",
                  "AttributeDefinitions", "GlobalSecondaryIndexes",
                  "LocalSecondaryIndexes", "TableStatus"]
        junk_index_fields = ["ItemCount", "IndexSizeBytes", "IndexArn"]
        table = {}
        for field in fields:
            value = description.get(field, None)
            if value is not None:
                table[field] = value
        table.get("ProvisionedThroughput", {}).pop(
            "NumberOfDecreasesToday", None)

        for index in table.get('GlobalSecondaryIndexes', []):
            for field in junk_index_fields:
                index.pop(field, None)
            index.get("ProvisionedThroughput", {}).pop(
                "NumberOfDecreasesToday", None)
        for index in table.get('LocalSecondaryIndexes', []):
            for field in junk_index_fields:
                index.pop(field, None)
        return table

    def put_item(self, item):
        self._modify_item(self.client.put_item, "put", item)

    def query(self, request):
        return self._filter(self.client.query, request)

    def scan(self, request):
        return self._filter(self.client.scan, request)

    def update_item(self, item):
        self._modify_item(self.client.update_item, "update", item)

    def validate_table(self, model):
        '''
        Poll table status until Table and all GSIs are ACTIVE.
        Raise bloop.exceptions.TableMismatch if actual table
        doesn't match expected
        '''
        expected = table_for_model(model)
        status = TableStatus.Busy
        while status is TableStatus.Busy:
            actual = self.describe_table(model)
            status = table_status(actual)
        if bloop.util.ordered(actual) != bloop.util.ordered(expected):
            raise bloop.exceptions.TableMismatch(model, expected, actual)


def key_schema(*, index=None, model=None):
    if index:
        hash_key = index.hash_key
        range_key = index.range_key
    # model
    else:
        hash_key = model.Meta.hash_key
        range_key = model.Meta.range_key
    schema = [{
        'AttributeName': hash_key.dynamo_name,
        'KeyType': 'HASH'
    }]
    if range_key:
        schema.append({
            'AttributeName': range_key.dynamo_name,
            'KeyType': 'RANGE'
        })
    return schema


def attribute_definitions(model):
    ''' Only include table and index hash/range keys '''
    dedupe_attrs = set()
    attrs = []

    def has_key(column):
        return column.hash_key or column.range_key

    def attribute_def(column):
        return {
            'AttributeType': column.typedef.backing_type,
            'AttributeName': column.dynamo_name
        }

    for column in filter(has_key, model.Meta.columns):
        dedupe_attrs.add(column)
        attrs.append(attribute_def(column))
    for index in filter(has_key, model.Meta.indexes):
        hash_column = index.hash_key
        if hash_column and hash_column not in dedupe_attrs:
            dedupe_attrs.add(hash_column)
            attrs.append(attribute_def(hash_column))
        range_column = index.range_key
        if range_column and range_column not in dedupe_attrs:
            dedupe_attrs.add(range_column)
            attrs.append(attribute_def(range_column))
    return attrs


def index_projection(index):
    projection = {
        'ProjectionType': index.projection,
        'NonKeyAttributes': [
            column.dynamo_name for column in index.projection_attributes
        ]
    }
    if index.projection != 'INCLUDE' or not projection['NonKeyAttributes']:
        projection.pop('NonKeyAttributes')
    return projection


def global_secondary_indexes(model):
    gsis = []
    for index in filter(bloop.index.is_global_index,
                        model.Meta.indexes):
        gsi_key_schema = key_schema(index=index)
        provisioned_throughput = {
            'WriteCapacityUnits': index.write_units,
            'ReadCapacityUnits': index.read_units
        }

        gsis.append({
            'ProvisionedThroughput': provisioned_throughput,
            'Projection': index_projection(index),
            'IndexName': index.dynamo_name,
            'KeySchema': gsi_key_schema
        })
    return gsis


def local_secondary_indexes(model):
    lsis = []
    for index in filter(bloop.index.is_local_index,
                        model.Meta.indexes):
        lsi_key_schema = key_schema(index=index)

        lsis.append({
            'Projection': index_projection(index),
            'IndexName': index.dynamo_name,
            'KeySchema': lsi_key_schema
        })
    return lsis


def table_for_model(model):
    """ Return the expected table dict for a given model. """
    table = {
        "TableName": model.Meta.table_name,
        "ProvisionedThroughput": {
            'WriteCapacityUnits': model.Meta.write_units,
            'ReadCapacityUnits': model.Meta.read_units
        },
        "KeySchema": key_schema(model=model),
        "AttributeDefinitions": attribute_definitions(model),
        "GlobalSecondaryIndexes": global_secondary_indexes(model),
        "LocalSecondaryIndexes": local_secondary_indexes(model)
    }
    if not table['GlobalSecondaryIndexes']:
        table.pop('GlobalSecondaryIndexes')
    if not table['LocalSecondaryIndexes']:
        table.pop('LocalSecondaryIndexes')
    return table


def table_status(table):
    '''
    Returns BUSY if table or any GSI is not ACTIVE, otherwise READY

    mutates table - pops status entries
    '''
    status = TableStatus.Ready
    if table.pop("TableStatus", "ACTIVE") != "ACTIVE":
        status = TableStatus.Busy
    for index in table.get('GlobalSecondaryIndexes', []):
        if index.pop("IndexStatus", "ACTIVE") != "ACTIVE":
            status = TableStatus.Busy
    return status
