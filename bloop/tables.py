__all__ = [
    "create_request", "expected_description",
    "sanitized_description", "simple_status"]


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
    # Right now, we expect the exact same thing as create_request
    # This doesn't include statuses (table, indexes) since that's
    # pulled out by the polling mechanism
    table = create_request(model)
    return table


def sanitized_description(description):
    # We don't need to match most of what comes back from describe_table
    # This monster structure carefully extracts the exact fields that bloop
    # will compare against, without picking up any new fields that
    # describe_table may start returning.

    # Without this, describe_table could return a new piece of metadata
    # and break all table verification because our expected table doesn't
    # include the new field.

    # This also simplifies the post-processing logic by inserting empty lists
    # for missing values from the wire.
    table = {
        "AttributeDefinitions": [
            {"AttributeName": attr_definition["AttributeName"],
             "AttributeType": attr_definition["AttributeType"]}
            for attr_definition in description["AttributeDefinitions"]
        ],
        "GlobalSecondaryIndexes": [
            {"IndexName": gsi["IndexName"],
             "KeySchema": [
                 {"AttributeName": gsi_key["AttributeName"],
                  "KeyType": gsi_key["KeyType"]}
                 for gsi_key in gsi["KeySchema"]
                 ],
             "Projection": {
                 "NonKeyAttributes":
                     gsi["Projection"].get("NonKeyAttributes", []),
                 "ProjectionType": gsi["Projection"]["ProjectionType"]
             },
             "ProvisionedThroughput": {
                 "ReadCapacityUnits":
                     gsi["ProvisionedThroughput"]["ReadCapacityUnits"],
                 "WriteCapacityUnits":
                     gsi["ProvisionedThroughput"]["WriteCapacityUnits"]
             }}
            for gsi in description.get("GlobalSecondaryIndexes", [])
        ],
        "KeySchema": [
            {"AttributeName": table_key["AttributeName"],
             "KeyType": table_key["KeyType"]}
            for table_key in description["KeySchema"]
        ],
        "LocalSecondaryIndexes": [
            {"IndexName": lsi["IndexName"],
             "KeySchema": [
                 {"AttributeName": lsi_key["AttributeName"],
                  "KeyType": lsi_key["KeyType"]}
                 for lsi_key in lsi["KeySchema"]
                 ],
             "Projection": {
                 "NonKeyAttributes":
                     lsi["Projection"].get("NonKeyAttributes", []),
                 "ProjectionType": lsi["Projection"]["ProjectionType"]
             }}
            for lsi in description.get("LocalSecondaryIndexes", [])
        ],
        "ProvisionedThroughput": {
            "ReadCapacityUnits":
                description["ProvisionedThroughput"]["ReadCapacityUnits"],
            "WriteCapacityUnits":
                description["ProvisionedThroughput"]["WriteCapacityUnits"]
        },
        "TableName": description["TableName"]
    }

    # Safe to concatenate here since we won't be removing items from the
    # combined list, but modifying the mutable dicts within
    indexes = table["GlobalSecondaryIndexes"] + table["LocalSecondaryIndexes"]
    for index in indexes:
        if not index["Projection"]["NonKeyAttributes"]:
            index["Projection"].pop("NonKeyAttributes")

    if not table["GlobalSecondaryIndexes"]:
        table.pop("GlobalSecondaryIndexes")
    if not table["LocalSecondaryIndexes"]:
        table.pop("LocalSecondaryIndexes")

    return table


def simple_status(description):
    status = "ACTIVE"
    if description.get("TableStatus") != "ACTIVE":
        status = "BLOOP_NOT_ACTIVE"
    for index in description.get("GlobalSecondaryIndexes", []):
        if index.get("IndexStatus") != "ACTIVE":
            status = "BLOOP_NOT_ACTIVE"
    return status
