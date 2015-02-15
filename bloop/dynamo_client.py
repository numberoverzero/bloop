import boto3
import botocore
import collections
import time


MAX_BATCH_SIZE = 25
RETRYABLE_ERRORS = [
    "InternalServerError",
    "ProvisionedThroughputExceededException"
]
BACKOFF_COEFF = 50.0


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
    def __init__(self):
        self.client = boto3.client("dynamodb")
        self.max_attempts = 4  # 1 call + 3 retries

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
        func = self.client.batch_get_item

        for request_batch in request_batches:
            # After the first call, request_batch is the
            # UnprocessedKeys from the first call
            while request_batch:
                batch_response = self.call_with_retries(
                    func, RequestItems=request_batch)
                # Add batch results to the full results table
                for table_name, item in iterate_response(batch_response):
                    if table_name not in response:
                        response[table_name] = []
                    response[table_name].append(item)

                # If there are no unprocessed keys, this will be an empty
                # list which will break the while loop, moving to the next
                # batch of items
                request_batch = batch_response["UnprocessedKeys"]

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
        func = self.client.batch_write_item

        for request_batch in request_batches:
            # After the first call, request_batch is the
            # UnprocessedKeys from the first call
            while request_batch:
                batch_response = self.call_with_retries(
                    func, RequestItems=request_batch)

                # If there are no unprocessed items, this will be an empty
                # list which will break the while loop, moving to the next
                # batch of items
                request_batch = batch_response["UnprocessedItems"]

    def call_with_retries(self, func, *args, **kwargs):
        ''' Exponential backoff helper, does not partition or map results '''
        attempts = 0
        while attempts < self.max_attempts:
            try:
                output = func(*args, **kwargs)
            except botocore.exceptions.ClientError as error:
                error_code = error.response['Error']['Code']
                if error_code in RETRYABLE_ERRORS:
                    attempts += 1
                else:
                    raise error
            else:
                # No exception, success!
                return output
            # Backoff in milliseconds
            delay = (BACKOFF_COEFF * (2 ** attempts)) / 1000.0
            time.sleep(delay)
        raise RuntimeError("Failed after {} attempts".format(self.attempts))

    def create_table(self, *args, **kwargs):
        ''' Suppress ResourceInUseException (table already exists) '''
        try:
            self.client.create_table(*args, **kwargs)
        except botocore.exceptions.ClientError as error:
            # Raise unless the table already exists
            error_code = error.response['Error']['Code']
            if error_code != 'ResourceInUseException':
                raise error

    def delete_item(self, table, key, expression):
        self.call_with_retries(self.client.delete_item,
                               TableName=table, Key=key, **expression)

    def put_item(self, table, item, expression):
        self.call_with_retries(self.client.put_item,
                               TableName=table, Item=item, **expression)
