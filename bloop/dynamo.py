import bloop.index


def attribute_definitions(model):
    columns = model.__meta__["dynamo.columns"]
    return [attribute_definition(column) for column in columns]


def attribute_definition(column):
    attr_name = column.dynamo_name
    attr_type = column.typedef.backing_type
    return {
        'AttributeType': attr_type,
        'AttributeName': attr_name
    }


def key_schema(model):
    columns = model.__meta__["dynamo.columns"]
    schema = []
    for column in columns:
        if column.is_hash:
            schema.append({
                'AttributeName': column.dynamo_name,
                'KeyType': 'HASH'
            })
        elif column.is_range:
            schema.append({
                'AttributeName': column.dynamo_name,
                'KeyType': 'RANGE'
            })
    if len(schema) > 2:
        raise AttributeError("Overdefined schema: {}".format(schema))
    elif len(schema) == 0:
        msg = "Underdefined schema, must provide at least 1 hash key"
        raise AttributeError(msg)
    return schema


def provisioned_throughput(model):
    return {
        'WriteCapacityUnits': model.__meta__["write_units"],
        'ReadCapacityUnits': model.__meta__["read_units"]
    }


def table_name(model):
    return model.__meta__["table_name"]


def global_secondary_indexes(model):
    is_gsi = lambda index: isinstance(index, bloop.index.GlobalSecondaryIndex)
    gsis = filter(is_gsi, model.__meta__["dynamo.indexes"])
    return {
        'GlobalSecondaryIndexes': [
            secondary_index(model, gsi) for gsi in gsis
        ]
    }


def local_secondary_indexes(model):
    is_lsi = lambda index: isinstance(index, bloop.index.LocalSecondaryIndex)
    lsis = filter(is_lsi, model.__meta__["dynamo.indexes"])
    return {
        'LocalSecondaryIndexes': [
            secondary_index(model, lsi) for lsi in lsis
        ]
    }


def secondary_index(model, index):
    '''
    model is required since gsi hash/range/keys will use model column names,
    and need to be translated to the appropriate dynamo_name for aliases.
    '''
    columns = model.__meta__["dynamo.columns.by.model_name"]
    provisioned_throughput = {
        'WriteCapacityUnits': index.write_units,
        'ReadCapacityUnits': index.read_units
    }

    key_schema = [
        {
            'AttributeName': columns[index.hash_key].dynamo_name,
            'KeyType': 'HASH'
        }
    ]
    if index.range_key:
        key_schema.append({
            'AttributeName': columns[index.range_key].dynamo_name,
            'KeyType': 'RANGE'
        })
    # TODO
    projection = {}

    return {
        'Projection': projection,
        'ProvisionedThroughput': provisioned_throughput,
        'IndexName': index.dynamo_name,
        'KeySchema': key_schema
    }
