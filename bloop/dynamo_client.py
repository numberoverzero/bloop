import boto3
import botocore
import collections
import time
import functools
import bloop.column


DEFAULT_BACKOFF_COEFF = 50.0
DEFAULT_MAX_ATTEMPTS = 4
MAX_BATCH_SIZE = 25
RETRYABLE_ERRORS = [
    "InternalServerError",
    "ProvisionedThroughputExceededException"
]


def default_backoff_func(operation, attempts):
    ''' attempts is the number of calls so far that have failed '''
    if attempts == DEFAULT_MAX_ATTEMPTS:
        raise RuntimeError("Failed after {} attempts".format(attempts))
    return (DEFAULT_BACKOFF_COEFF * (2 ** attempts)) / 1000.0


def partition_batch_get_input(request_items):
    ''' Takes a batch_get input and partitions into 25 object chunks '''

    def iterate_items():
        for table_name, table_attrs in request_items.items():
            consistent_read = table_attrs.get("ConsistentRead", False)
            for key in table_attrs["Keys"]:
                yield (table_name, key, consistent_read)

    def iterate_chunks():
        chunk = {}
        items = 0
        for table_name, key, consistent_read in iterate_items():
            if items == MAX_BATCH_SIZE:
                yield chunk
                items = 0
                chunk = {}
            table = chunk.get(table_name, None)
            # First occurance of the table in this chunk
            if table is None:
                table = chunk[table_name] = {
                    "ConsistentRead": consistent_read,
                    "Keys": []}
            # Dump the key into the chunk table's `Keys` list
            table["Keys"].append(key)
            items += 1
        # Last chunk, less than MAX_BATCH_SIZE items
        if chunk:
            yield chunk

    return iterate_chunks()


def partition_batch_write_input(request_items):
    ''' Takes a batch_write input and partitions into 25 object chunks '''

    def iterate_items():
        for table_name, items in request_items.items():
            for item in items:
                yield (table_name, item)

    def iterate_chunks():
        chunk = collections.defaultdict(list)
        items = 0
        for table_name, item in iterate_items():
            if items == MAX_BATCH_SIZE:
                yield chunk
                items = 0
                chunk = {}
            chunk[table_name].append(item)
            items += 1
        # Last chunk, less than MAX_BATCH_SIZE items
        if chunk:
            yield chunk

    return iterate_chunks()


class DynamoClient(object):
    def __init__(self, backoff_func=None):
        '''

        backoff_func is an optional function with signature
        (dynamo operation name, attempts so far) that should either:
            - return the number of seconds to sleep
            - raise to stop
        '''
        self.client = boto3.client("dynamodb")
        self.backoff_func = backoff_func or default_backoff_func

    def batch_get_items(self, request):
        '''
        Takes the "RequestItems" dict and returns the "Responses" dict
        documented here:
            http://docs.aws.amazon.com/amazondynamodb/latest/ \
                APIReference/API_BatchGetItem.html

        Handles batching and throttling/retry with backoff

        Example
        ------
        From the same document above, the example input would be:

        {
            "Forum": {
                "Keys": [
                    { "Name":{"S":"Amazon DynamoDB"} },
                    { "Name":{"S":"Amazon RDS"} },
                    { "Name":{"S":"Amazon Redshift"} }
                ]
            },
            "Thread": {
                "Keys": [{
                    "ForumName":{"S":"Amazon DynamoDB"},
                    "Subject":{"S":"Concurrent reads"}
                }]
            }
        }

        And the returned response would be:
        {
            "Forum": [
                {
                    "Name":{"S":"Amazon DynamoDB"},
                    "Threads":{"N":"5"},
                    "Messages":{"N":"19"},
                    "Views":{"N":"35"}
                },
                {
                    "Name":{"S":"Amazon RDS"},
                    "Threads":{"N":"8"},
                    "Messages":{"N":"32"},
                    "Views":{"N":"38"}
                },
                {
                    "Name":{"S":"Amazon Redshift"},
                    "Threads":{"N":"12"},
                    "Messages":{"N":"55"},
                    "Views":{"N":"47"}
                }
            ],
            "Thread": [
                {
                    "Tags":{"SS":["Reads","MultipleUsers"]},
                    "Message":{"S":"... Are there any limits?"}
                }
            ]
        }
        '''
        request_batches = partition_batch_get_input(request)
        response = {}

        def iterate_response(batch):
            for table_name, table_items in batch["Responses"].items():
                for item in table_items:
                    yield (table_name, item)

        # Bound ref to batch_get for retries
        get_batch = functools.partial(self.call_with_retries,
                                      self.client.batch_get_item)

        for request_batch in request_batches:
            # After the first call, request_batch is the
            # UnprocessedKeys from the first call
            while request_batch:
                batch_response = get_batch(RequestItems=request_batch)
                # Add batch results to the full results table
                for table_name, item in iterate_response(batch_response):
                    if table_name not in response:
                        response[table_name] = []
                    response[table_name].append(item)

                # If there are no unprocessed keys, this will be an empty
                # list which will break the while loop, moving to the next
                # batch of items
                request_batch = batch_response.get("UnprocessedKeys",  None)

        return response

    def batch_write_items(self, request):
        '''
        Takes the "RequestItems" dict documented here:
            http://docs.aws.amazon.com/amazondynamodb/latest/ \
                APIReference/API_BatchWriteItem.html

        Handles batching and throttling/retry with backoff

        Example
        ------
        From the same document above, the example input would be:

        {
            "RequestItems": {
                "Forum": [
                    {
                        "PutRequest": {
                            "Item": {
                                "Name": {"S": "Amazon DynamoDB"},
                                "Category": {"S": "Amazon Web Services"}
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "Name": {"S": "Amazon RDS"},
                                "Category": {"S": "Amazon Web Services"}
                            }
                        }
                    },
                    {
                        "DeleteRequest": {
                            "Key": {
                                "Name": {"S": "Amazon Redshift"}
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "Name": {"S": "Amazon ElastiCache"},
                                "Category": {"S": "Amazon Web Services"}
                            }
                        }
                    }
                ]
            }
        }
        '''
        request_batches = partition_batch_write_input(request)

        # Bound ref to batch_write for retries
        write_batch = functools.partial(self.call_with_retries,
                                        self.client.batch_write_item)

        for request_batch in request_batches:
            # After the first call, request_batch is the
            # UnprocessedKeys from the first call
            while request_batch:
                batch_response = write_batch(RequestItems=request_batch)

                # If there are no unprocessed items, this will be an empty
                # list which will break the while loop, moving to the next
                # batch of items
                request_batch = batch_response["UnprocessedItems"]

    def call_with_retries(self, func, *args, **kwargs):
        ''' Exponential backoff helper, does not partition or map results '''
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

    def create_table(self, model):
        ''' Suppress ResourceInUseException (table already exists) '''
        table = describe_model(model)
        # Bound ref to create w/retries
        create = functools.partial(self.call_with_retries,
                                   self.client.create_table)

        try:
            create(**table)
        except botocore.exceptions.ClientError as error:
            # Raise unless the table already exists
            error_code = error.response['Error']['Code']
            if error_code != 'ResourceInUseException':
                raise error

    def delete_item(self, table, key, expression):
        self.call_with_retries(self.client.delete_item,
                               TableName=table, Key=key, **expression)

    def query(self, **request):
        # Bound ref to query for retries
        query = functools.partial(self.call_with_retries, self.client.query)
        empty = []

        results = {
            "Count": 0,
            "ScannedCount": 0,
            "Items": []
        }

        while True:
            response = query(**request)

            # When updating count, ScannedCount is omitted unless it differs
            # from Count; thus we need to default to assume that the
            # ScannedCount is equal to the Count
            count = response.get("Count", 0)
            results["Count"] += count
            results["ScannedCount"] += response.get("ScannedCount", count)

            results["Items"].extend(response.get("Items", empty))

            # Done processing when LastEvaluatedKey is empty
            last_key = response.get("LastEvaluatedKey", None)
            if not last_key:
                break
            # Otherwise, update the request key and go again
            request["ExclusiveStartKey"] = last_key

        return results

    def put_item(self, table, item, expression):
        self.call_with_retries(self.client.put_item,
                               TableName=table, Item=item, **expression)


# Helpers TODO: Refactor

def has_key(column):
    return column.hash_key or column.range_key


def attribute_definitions(model):
    ''' Only include table and index hash/range keys '''
    columns = model.__meta__["dynamo.columns"]
    indexes = model.__meta__["dynamo.indexes"]
    attrs = []
    attr_columns = set()
    for column in filter(has_key, columns):
        attr_columns.add(column)
        attrs.append(attribute_def(column))
    for index in filter(has_key, indexes):
        hash_column = index.hash_key
        if hash_column and hash_column not in attr_columns:
            attr_columns.add(hash_column)
            attrs.append(attribute_def(hash_column))
        range_column = index.range_key
        if range_column and range_column not in attr_columns:
            attr_columns.add(range_column)
            attrs.append(attribute_def(range_column))
    return attrs


def attribute_def(column):
    return {
        'AttributeType': column.typedef.backing_type,
        'AttributeName': column.dynamo_name
    }


def key_schema(model):
    schema = [{
        'AttributeName': model.hash_key.dynamo_name,
        'KeyType': 'HASH'
    }]

    if model.range_key:
        schema.append({
            'AttributeName': model.range_key.dynamo_name,
            'KeyType': 'RANGE'
        })

    return schema


def provisioned_throughput(model):
    return {
        'WriteCapacityUnits': model.write_units,
        'ReadCapacityUnits': model.read_units
    }


def table_name(model):
    return model.__meta__["dynamo.table.name"]


def global_secondary_indexes(model):
    gsis = []
    for index in filter(bloop.column.is_global_index,
                        model.__meta__["dynamo.indexes"]):
        provisioned_throughput = {
            'WriteCapacityUnits': index.write_units,
            'ReadCapacityUnits': index.read_units
        }
        key_schema = [{
            'AttributeName': index.hash_key.dynamo_name,
            'KeyType': 'HASH'
        }]
        if index.range_key:
            key_schema.append({
                'AttributeName': index.range_key.dynamo_name,
                'KeyType': 'RANGE'
            })
        # TODO - handle projections other than 'ALL' and 'KEYS_ONLY'
        projection = {
            'ProjectionType': index.projection,
            # 'NonKeyAttributes': [
            #     # TODO
            # ]
        }

        gsis.append({
            'ProvisionedThroughput': provisioned_throughput,
            'Projection': projection,
            'IndexName': index.dynamo_name,
            'KeySchema': key_schema
        })

    return gsis


def local_secondary_indexes(model):
    lsis = []
    for index in filter(bloop.column.is_local_index,
                        model.__meta__["dynamo.indexes"]):
        key_schema = [
            {
                'AttributeName': index.hash_key.dynamo_name,
                'KeyType': 'HASH'
            },
            {
                'AttributeName': index.range_key.dynamo_name,
                'KeyType': 'RANGE'
            }
        ]
        # TODO - handle projections other than 'ALL' and 'KEYS_ONLY'
        projection = {
            'ProjectionType': index.projection,
            # 'NonKeyAttributes': [
            #     # TODO
            # ]
        }

        lsis.append({
            'Projection': projection,
            'IndexName': index.dynamo_name,
            'KeySchema': key_schema
        })

    return lsis


def describe_model(model):
    description = {
        "TableName": table_name(model),
        "ProvisionedThroughput": provisioned_throughput(model),
        "KeySchema": key_schema(model),
        "AttributeDefinitions": attribute_definitions(model),
        "GlobalSecondaryIndexes": global_secondary_indexes(model),
        "LocalSecondaryIndexes": local_secondary_indexes(model)
    }
    if not description['GlobalSecondaryIndexes']:
        description.pop('GlobalSecondaryIndexes')
    if not description['LocalSecondaryIndexes']:
        description.pop('LocalSecondaryIndexes')
    return description
