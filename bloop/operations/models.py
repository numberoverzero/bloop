from ..exceptions import BloopException, ConstraintViolation

__all__ = ["create_batch_get_chunks", "handle_constraint_violation", "standardize_query_response"]

# https://boto3.readthedocs.io/en/latest/reference/services/dynamodb.html#DynamoDB.Client.batch_get_item
BATCH_GET_ITEM_CHUNK_SIZE = 100


def handle_constraint_violation(error):
    error_code = error.response["Error"]["Code"]
    if error_code == "ConditionalCheckFailedException":
        raise ConstraintViolation("The provided condition was not met") from error
    else:
        raise BloopException("There was an unexpected error from DynamoDB") from error


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
