import contextlib
import os
import random
import shutil
import socket
import string
import subprocess
import zipfile

import blinker
import boto3
import pytest
import requests

from bloop import Engine
from bloop.session import SessionWrapper
from bloop.signals import model_created
from tests.helpers.utils import get_tables

resource_options = {
    'region_name': 'us-east-1',
    'port': 8000,
    'aws_access_key_id': 'access',
    'aws_secret_access_key': 'secret',
}

fixed_session = boto3.Session(region_name="us-west-2")


@pytest.fixture(scope='session')
def do_provide_aws():
    proc = start_dynamodb_local()
    yield
    proc.terminate()


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
    tables = get_tables(dynamodb_client)
    nonce = config.getoption("--nonce")
    print("Cleaning up tables with nonce '{}'".format(nonce))
    for table in tables:
        if nonce not in table:
            continue
        # noinspection PyBroadException
        try:
            print("Removing table: {}".format(table))
            dynamodb_client.delete_table(TableName=table)
        except Exception:
            print("Failed to clean up table '{}'".format(table))


@pytest.fixture(scope="session")
def nonce(request):
    return request.config.getoption("--nonce")


@pytest.fixture(scope="session")
def dynamodb(do_provide_aws):
    return fixed_session.client("dynamodb", endpoint_url=resource_options['endpoint_url'])


@pytest.fixture(scope="session")
def dynamodbstreams(do_provide_aws):
    return fixed_session.client("dynamodbstreams", endpoint_url=resource_options['endpoint_url'])


@pytest.fixture
def engine(dynamodb, dynamodbstreams):
    yield Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)


def get_open_port():
    with contextlib.closing(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.listen(1)
        port = s.getsockname()[1]
        return port


def get_ddb_local():
    localdir = pytest.config.rootdir.join('.dynamodb-local').strpath
    if not os.path.exists(localdir):
        tempdir = localdir + '.tmp'
        if os.path.exists(tempdir):
            shutil.rmtree(tempdir)
        os.mkdir(tempdir)
        r = requests.get(
            'https://s3-us-west-2.amazonaws.com/' +
            'dynamodb-local/dynamodb_local_latest.zip',
            stream=True)
        dist = os.path.join(tempdir, 'dist.zip')
        with open(dist, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        zip_ref = zipfile.ZipFile(dist, 'r')
        zip_ref.extractall(tempdir)
        zip_ref.close()
        os.rename(tempdir, localdir)
    return localdir


def start_dynamodb_local():
    cwd = get_ddb_local()
    port = get_open_port()
    proc = subprocess.Popen(['java', '-Djava.library.path=./DynamoDBLocal_lib',
                             '-jar', 'DynamoDBLocal.jar', '-inMemory',
                             '-port', str(port)], cwd=cwd)
    resource_options['endpoint_url'] = 'http://localhost:{}'.format(port)
    return proc


# Use class-scoped fixtures for dynamodb access Use a
# separate test class for each group of tests that cumulatively update
# dynamodb.
@pytest.fixture(scope='class')
def dynamodb_resource_options():
    return resource_options


@pytest.fixture
def session(dynamodb, dynamodbstreams):
    return SessionWrapper(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)
