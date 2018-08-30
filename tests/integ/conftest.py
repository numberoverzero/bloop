import random
import string
import subprocess

import boto3
import pytest
from tests.helpers.utils import get_tables

from bloop import BaseModel, BloopException, Engine
from bloop.session import SessionWrapper
from bloop.util import walk_subclasses


DOCKER_START_COMMAND = ["docker", "run", "-d", "-p", "8000:8000", "--name", "ddb-local", "amazon/dynamodb-local"]
DOCKER_STOP_COMMAND = ["docker", "stop", "ddb-local"]
DOCKER_RM_COMMAND = ["docker", "rm", "ddb-local"]


class PatchedDynamoDBClient:
    def __init__(self, real_client):
        self.__client = real_client

    def describe_time_to_live(self, **_):
        return {"TimeToLiveDescription": {"TimeToLiveStatus": "DISABLED"}}

    def describe_continuous_backups(self, **_):
        return {"ContinuousBackupsDescription": {"ContinuousBackupsStatus": "DISABLED"}}

    def __getattr__(self, name):
        return getattr(self.__client, name)


class DynamoDBLocal:
    def __init__(self) -> None:
        self.running = False

    @property
    def session(self) -> boto3.Session:
        assert self.running
        return boto3.Session(
            region_name="us-west-2",
            aws_access_key_id="NO_ACCESS_KEY",
            aws_secret_access_key="NO_SECRET_KEY",
        )

    @property
    def endpoint(self) -> str:
        assert self.running
        return "http://localhost:8000"

    @property
    def clients(self) -> tuple:
        session = self.session
        endpoint = self.endpoint
        return (
            # TODO | have to patch dynamodb until DynamoDBLocal supports DescribeTimeToLive
            # TODO | otherwise, SessionWrapper.describe_table throws UnknownOperationException
            PatchedDynamoDBClient(session.client("dynamodb", endpoint_url=endpoint)),

            session.client("dynamodbstreams", endpoint_url=endpoint)
        )

    def start(self) -> None:
        assert not self.running
        self.running = True
        subprocess.run(DOCKER_START_COMMAND, stdout=subprocess.PIPE, check=True)

    def stop(self) -> None:
        assert self.running
        subprocess.run(DOCKER_STOP_COMMAND, stdout=subprocess.PIPE, check=True)
        subprocess.run(DOCKER_RM_COMMAND, stdout=subprocess.PIPE, check=True)
        self.running = False


def pytest_addoption(parser):
    default_nonce = "-local-" + "".join(random.choice(string.ascii_letters) for _ in range(16))
    parser.addoption(
        "--nonce", action="store", default=default_nonce,
        help="make table names unique for parallel runs")
    parser.addoption(
        "--skip-cleanup", action="store_true", default=False,
        help="don't clean up the docker instance after tests run")

    default_localdir = ".dynamodb-local"
    parser.addoption(
        "--dynamodb-local-dir", action="store", default=default_localdir,
        help="directory that contains DynamoDBLocal jar"
    )


@pytest.fixture(scope="session")
def dynamodb_local(request):
    nonce = request.config.getoption("--nonce")
    skip_cleanup = request.config.getoption("--skip-cleanup")

    dynamodb_local = DynamoDBLocal()
    dynamodb_local.start()

    yield dynamodb_local

    if skip_cleanup:
        print("Skipping cleanup, leaving docker image intact")
        return
    try:
        print("Cleaning up tables with nonce '{}'".format(nonce))
        dynamodb, _ = dynamodb_local.clients
        tables = get_tables(dynamodb)
        for table in tables:
            if nonce not in table:
                continue
            # noinspection PyBroadException
            try:
                print("Removing table: {}".format(table))
                dynamodb.delete_table(TableName=table)
            except Exception:
                print("Failed to clean up table '{}'".format(table))
    finally:
        print("Shutting down ddb-local")
        dynamodb_local.stop()


@pytest.fixture
def dynamodb(dynamodb_local):
    dynamodb, _ = dynamodb_local.clients
    return dynamodb


@pytest.fixture
def dynamodbstreams(dynamodb_local):
    _, dynamodbstreams = dynamodb_local.clients
    return dynamodbstreams


@pytest.fixture
def engine(dynamodb, dynamodbstreams, request):
    engine = Engine(
        dynamodb=dynamodb, dynamodbstreams=dynamodbstreams,
        table_name_template="{table_name}" + request.config.getoption("--nonce")
    )
    yield engine

    # This collects all subclasses of BaseModel and are not abstract.  We are trying to delete any data in
    # dynamodb-local between unit tests so we don't step on each other's toes.
    concrete = set(filter(lambda m: not m.Meta.abstract, walk_subclasses(BaseModel)))
    for model in concrete:
        # we can run into a situation where the class was created, but not bound in the engine (or table created), so
        # we only try.  As the dynamodb-local process is only running in memory this isn't too much of a problem.
        try:
            objs = list(engine.scan(model))
            if objs:
                engine.delete(*objs)
        except BloopException:
            pass


@pytest.fixture
def session(dynamodb, dynamodbstreams):
    return SessionWrapper(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)
