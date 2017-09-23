import itertools


def get_tables(dynamodb):
    it = dynamodb.get_paginator("list_tables").paginate()
    tables = [response["TableNames"] for response in it]
    tables = itertools.chain(*tables)
    return tables
