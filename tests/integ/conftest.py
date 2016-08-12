import boto3
import itertools
import pytest
import random
import string

from bloop import Client, Engine, before_create_table
boto_client = boto3.client("dynamodb", region_name="us-west-2")


def pytest_addoption(parser):
    default_nonce = "-local-" + "".join(random.choice(string.ascii_letters) for _ in range(16))
    parser.addoption(
        "--nonce", action="store", default=default_nonce,
        help="make table names unique for parallel runs")


def pytest_unconfigure(config):
    it = boto_client.get_paginator("list_tables").paginate()
    tables = [response["TableNames"] for response in it]
    tables = itertools.chain(*tables)
    nonce = config.getoption("--nonce")
    print("Cleaning up tables with nonce '{}'".format(nonce))
    for table in tables:
        if nonce not in table:
            continue
        try:
            boto_client.delete_table(TableName=table)
        except Exception:
            print("Failed to clean up table '{}'".format(table))


@pytest.fixture
def nonce(request):
    return request.config.getoption("--nonce")


@pytest.fixture
def engine(nonce):
    bloop_client = Client(boto_client=boto_client)
    engine = Engine(client=bloop_client)

    def nonce_table_name(sender, model, **kwargs):
        table_name = model.Meta.table_name
        if not table_name.endswith(nonce):
            model.Meta.table_name += nonce

    before_create_table.connect(nonce_table_name, weak=False)
    return engine
