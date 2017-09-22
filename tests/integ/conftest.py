import contextlib

import hashlib
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
from tests.integ.models import User

resource_options = {
    'endpoint_url': None
}

LATEST_DYNAMODB_LOCAL_SHA = '70d9a92529782ac93713258fe69feb4ff6e007ae2c3319c7ffae7da38b698a61'
fixed_session = boto3.Session(region_name="us-west-2")


@pytest.fixture(scope='session')
def do_provide_aws():
    """
    Ensures that the dynamodb-local service is running, and shuts it down when all tests are done.
    :return:
    """
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
    """
    By using a socket and binding to "" (localhost), with a port of 0 (socket picks first non-privileged port
    that is not in use, we can ensure that the port is available.
    See:  https://stackoverflow.com/questions/2838244/get-open-tcp-port-in-python/2838309#2838309
    and   https://stackoverflow.com/a/45690594
    :return: The port to use
    """
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.listen(1)
        port = s.getsockname()[1]
        return port


def get_ddb_local():
    """
    Ensure that dynamodb-local is downloaded and available.  It must match the sha256 in this file.
    :return: The directory where it's installed.
    """
    localdir = pytest.config.rootdir.join('.dynamodb-local').strpath
    if not os.path.exists(localdir):

        # need a temp directory to download it in...
        tempdir = localdir + '.tmp'
        if os.path.exists(tempdir):
            shutil.rmtree(tempdir)
        os.mkdir(tempdir)

        r = requests.get('https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.zip', stream=True)
        dist = os.path.join(tempdir, 'dist.zip')

        # download in chucks, checking it's sha256 hash along the way
        sha = hashlib.sha256()
        with open(dist, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    sha.update(chunk)

        # Is this the file we're looking for?
        if sha.hexdigest() != LATEST_DYNAMODB_LOCAL_SHA:
            raise RuntimeError("Invalid hash of dynamodb_local_latest.zip")

        zip_ref = zipfile.ZipFile(dist, 'r')
        zip_ref.extractall(tempdir)
        zip_ref.close()

        # clean up
        os.rename(tempdir, localdir)
    return localdir


def start_dynamodb_local():
    """
    Starts dynamodb-local.
    :return: The dynamodb-local process
    """
    cwd = get_ddb_local()
    port = get_open_port()
    proc = subprocess.Popen(['java', '-Djava.library.path=./DynamoDBLocal_lib',
                             '-jar', 'DynamoDBLocal.jar', '-inMemory',
                             '-port', str(port)], cwd=cwd)
    resource_options['endpoint_url'] = 'http://localhost:{}'.format(port)
    return proc


@pytest.yield_fixture(autouse=True)
def cleanup_objects(engine):
    yield
    # TODO track bound models w/model_bound signal (TODO), then use boto3 to scan/delete by Meta.table_name
    # Running tests individually may break if the User table isn't bound as part of that test
    users = list(engine.scan(User))
    engine.delete(*users)


@pytest.fixture
def session(dynamodb, dynamodbstreams):
    return SessionWrapper(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)
