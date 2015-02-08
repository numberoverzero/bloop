import bloop.column


def is_lsi(index):
    return isinstance(index, bloop.column.LocalSecondaryIndex)


def is_gsi(index):
    return isinstance(index, bloop.column.GlobalSecondaryIndex)


def has_key(column):
    return column.hash_key or column.range_key


def attribute_definitions(model):
    ''' Only include table and index hash/range keys '''
    columns = model.__meta__["dynamo.columns"]
    indexed_columns = model.__meta__["dynamo.columns.by.model_name"]
    indexes = model.__meta__["dynamo.indexes"]
    attrs = []
    attr_columns = set()
    for column in filter(has_key, columns):
        attr_columns.add(column)
        attrs.append(attribute_def(column))
    for index in filter(has_key, indexes):
        if index.hash_key:
            column = indexed_columns[index.hash_key]
            if column not in attr_columns:
                attr_columns.add(column)
                attrs.append(attribute_def(column))
        if index.range_key:
            column = indexed_columns[index.range_key]
            if column not in attr_columns:
                attr_columns.add(column)
                attrs.append(attribute_def(column))
    return attrs


def attribute_def(column):
    return {
        'AttributeType': column.typedef.backing_type,
        'AttributeName': column.dynamo_name
    }


def key_schema(model):
    meta = model.__meta__
    schema = []
    hash_key = meta['dynamo.table.hash_key']
    range_key = meta['dynamo.table.range_key']

    schema.append({
        'AttributeName': hash_key.dynamo_name,
        'KeyType': 'HASH'
    })

    if range_key:
        schema.append({
            'AttributeName': range_key.dynamo_name,
            'KeyType': 'RANGE'
        })

    return schema


def provisioned_throughput(model):
    return {
        'WriteCapacityUnits': model.__meta__["dynamo.table.write_units"],
        'ReadCapacityUnits': model.__meta__["dynamo.table.read_units"]
    }


def table_name(model):
    return model.__meta__["dynamo.table.name"]


def global_secondary_indexes(model):
    columns = model.__meta__["dynamo.columns.by.model_name"]
    gsis = []
    for index in filter(is_gsi, model.__meta__["dynamo.indexes"]):
        provisioned_throughput = {
            'WriteCapacityUnits': index.write_units,
            'ReadCapacityUnits': index.read_units
        }
        key_schema = [{
            'AttributeName': columns[index.hash_key].dynamo_name,
            'KeyType': 'HASH'
        }]
        if index.range_key:
            key_schema.append({
                'AttributeName': columns[index.range_key].dynamo_name,
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
    columns = model.__meta__["dynamo.columns.by.model_name"]
    lsis = []
    for index in filter(is_lsi, model.__meta__["dynamo.indexes"]):
        key_schema = [
            {
                'AttributeName': columns[index.hash_key].dynamo_name,
                'KeyType': 'HASH'
            },
            {
                'AttributeName': columns[index.range_key].dynamo_name,
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
