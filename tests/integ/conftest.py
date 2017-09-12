import itertools
import random
import string

import blinker
import boto3
import pytest

from bloop import Engine
from bloop.signals import model_created

from .models import User


fixed_session = boto3.Session(region_name="us-west-2")
fixed_ddb_client = fixed_session.client("dynamodb")
fixed_streams_client = fixed_session.client("dynamodbstreams")


def pytest_addoption(parser):
    default_nonce = "-local-" + "".join(random.choice(string.ascii_letters) for _ in range(16))
    parser.addoption(
        "--nonce", action="store", default=default_nonce,
        help="make table names unique for parallel runs")
    parser.addoption(
        "--skip-cleanup", action="store_true", default=False,
        help="don't clean up tables after tests run")


def pytest_configure(config):
    nonce = config.getoption("--nonce")

    @model_created.connect_via(sender=blinker.ANY, weak=False)
    def nonce_table_name(_, *, model, **kwargs):
        table_name = model.Meta.table_name
        if nonce not in table_name:
            model.Meta.table_name += nonce


def pytest_unconfigure(config):
    skip_cleanup = config.getoption("--skip-cleanup")
    if skip_cleanup:
        print("Skipping cleanup")
        return
    dynamodb_client = fixed_session.client("dynamodb")
    it = dynamodb_client.get_paginator("list_tables").paginate()
    tables = [response["TableNames"] for response in it]
    tables = itertools.chain(*tables)
    nonce = config.getoption("--nonce")
    print("Cleaning up tables with nonce '{}'".format(nonce))
    for table in tables:
        if nonce not in table:
            continue
        # noinspection PyBroadException
        try:
            dynamodb_client.delete_table(TableName=table)
        except Exception:
            print("Failed to clean up table '{}'".format(table))


@pytest.fixture(scope="session")
def nonce(request):
    return request.config.getoption("--nonce")


@pytest.yield_fixture(autouse=True)
def cleanup_objects(engine):
    yield

    # TODO track bound models w/model_bound signal (TODO), then use boto3 to scan/delete by Meta.table_name
    # Running tests individually may break if the User table isn't bound as part of that test
    users = list(engine.scan(User))
    engine.delete(*users)


@pytest.fixture(scope="session")
def dynamodb():
    return fixed_ddb_client


@pytest.fixture(scope="session")
def dynamodbstreams():
    return fixed_streams_client


@pytest.fixture
def engine(dynamodb, dynamodbstreams):
    return Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)
