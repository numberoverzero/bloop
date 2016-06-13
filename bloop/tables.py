__all__ = ["create_request", "expected_description", "sanitize_description"]


def attribute_definitions(model):
    dedupe_attrs = set()
    attrs = []

    def add_column(column):
        if column is None:
            return
        if column in dedupe_attrs:
            return
        dedupe_attrs.add(column)
        attrs.append({
            "AttributeType": column.typedef.backing_type,
            "AttributeName": column.dynamo_name
        })

    add_column(model.Meta.hash_key)
    add_column(model.Meta.range_key)

    for index in model.Meta.indexes:
        add_column(index.hash_key)
        add_column(index.range_key)
    return attrs


def index_projection(index):
    projection = {"ProjectionType": index.projection}
    if index.projection == "INCLUDE" and index.projection_attributes:
        projection["NonKeyAttributes"] = [
            column.dynamo_name
            for column in index.projection_attributes
        ]
    return projection


def key_schema(*, index=None, model=None):
    if index:
        hash_key = index.hash_key
        range_key = index.range_key
    else:
        hash_key = model.Meta.hash_key
        range_key = model.Meta.range_key
    schema = [{
        "AttributeName": hash_key.dynamo_name,
        "KeyType": "HASH"
    }]
    if range_key:
        schema.append({
            "AttributeName": range_key.dynamo_name,
            "KeyType": "RANGE"
        })
    return schema


def global_secondary_index(index):
    return {
        "IndexName": index.dynamo_name,
        "KeySchema": key_schema(index=index),
        "Projection": index_projection(index),
        "ProvisionedThroughput": {
            "WriteCapacityUnits": index.write_units,
            "ReadCapacityUnits": index.read_units
        },
    }


def local_secondary_index(index):
    return {
        "IndexName": index.dynamo_name,
        "KeySchema": key_schema(index=index),
        "Projection": index_projection(index),
    }


def create_request(model):
    table = {
        "AttributeDefinitions": attribute_definitions(model),
        "KeySchema": key_schema(model=model),
        "ProvisionedThroughput": {
            "WriteCapacityUnits": model.Meta.write_units,
            "ReadCapacityUnits": model.Meta.read_units
        },
        "TableName": model.Meta.table_name,
    }
    if model.Meta.gsis:
        table["GlobalSecondaryIndexes"] = [
            global_secondary_index(index) for index in model.Meta.gsis]
    if model.Meta.lsis:
        table["LocalSecondaryIndexes"] = [
            local_secondary_index(index) for index in model.Meta.lsis]
    return table


def expected_description(model):
    pass


def sanitize_description(description):
    pass
