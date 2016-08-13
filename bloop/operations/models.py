from ..exceptions import ConstraintViolation

__all__ = ["create_batch_get_chunks", "handle_constraint_violation", "standardize_query_response"]


def handle_constraint_violation(error, operation, item):
    error_code = error.response["Error"]["Code"]
    if error_code == "ConditionalCheckFailedException":
        raise ConstraintViolation(operation, item)
    else:
        raise error


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
            if count >= 25:
                yield buffer
                buffer, count = {}, 0

    # Last chunk, less than batch_size items
    if buffer:
        yield buffer
