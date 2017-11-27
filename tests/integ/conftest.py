import contextlib
import hashlib
import os
import random
import shutil
import socket
import string
import subprocess
import zipfile

import boto3
import pytest
import requests
from tests.helpers.utils import get_tables

from bloop import BaseModel, BloopException, Engine
from bloop.session import SessionWrapper
from bloop.util import walk_subclasses


LATEST_DYNAMODB_LOCAL_SHA = "70d9a92529782ac93713258fe69feb4ff6e007ae2c3319c7ffae7da38b698a61"
DYNAMODB_LOCAL_SINGLETON = None


class DynamoDBLocal:
    def __init__(self, localdir: str) -> None:
        self.localdir = localdir
        self.process = None  # type: subprocess.Popen
        self.port = None

    @property
    def running(self) -> bool:
        return self.process is not None

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
        return "http://localhost:" + str(self.port)

    @property
    def clients(self) -> tuple:
        session = self.session
        endpoint = self.endpoint
        return (
            session.client("dynamodb", endpoint_url=endpoint),
            session.client("dynamodbstreams", endpoint_url=endpoint)
        )

    def start(self) -> None:
        assert not self.running
        self._download()
        self._reserve_port()
        self.process = self._run()

    def stop(self) -> None:
        assert self.running
        self.process.terminate()

    def _download(self) -> None:
        if os.path.exists(self.localdir):
            return
        print("\n".join((
            "*" * 79,
            "DynamoDBLocal doesn't exist, installing at {}".format(self.localdir),
            "*" * 79
        )))

        # need a temp directory to download it in...
        tempdir = self.localdir + ".tmp"
        if os.path.exists(tempdir):
            shutil.rmtree(tempdir)
        os.mkdir(tempdir)

        r = requests.get("https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.zip",
                         stream=True)
        dist = os.path.join(tempdir, "dynamodb_local_latest.zip")

        # download in chucks, checking its sha256 hash along the way
        sha = hashlib.sha256()
        with open(dist, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    sha.update(chunk)

        # Is this the file we're looking for?
        if sha.hexdigest() != LATEST_DYNAMODB_LOCAL_SHA:
            msg = "Invalid hash of {}/dynamodb_local_latest.zip -- expected {} but was {}"
            raise RuntimeError(msg.format(tempdir, LATEST_DYNAMODB_LOCAL_SHA, sha.hexdigest()))

        zip_ref = zipfile.ZipFile(dist, "r")
        zip_ref.extractall(tempdir)
        zip_ref.close()

        # clean up
        os.rename(tempdir, self.localdir)

    def _reserve_port(self) -> None:
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
            self.port = s.getsockname()[1]

    def _run(self) -> subprocess.Popen:
        return subprocess.Popen([
            "java", "-Djava.library.path=./DynamoDBLocal_lib",
            "-jar", "DynamoDBLocal.jar", "-inMemory",
            "-port", str(self.port)],
            cwd=self.localdir
        )


def pytest_addoption(parser):
    default_nonce = "-local-" + "".join(random.choice(string.ascii_letters) for _ in range(16))
    parser.addoption(
        "--nonce", action="store", default=default_nonce,
        help="make table names unique for parallel runs")
    parser.addoption(
        "--skip-cleanup", action="store_true", default=False,
        help="don't clean up tables after tests run")

    default_localdir = ".dynamodb-local"
    parser.addoption(
        "--dynamodb-local-dir", action="store", default=default_localdir,
        help="directory that contains DynamoDBLocal jar"
    )


@pytest.fixture(scope="session")
def dynamodb_local(request):
    nonce = request.config.getoption("--nonce")
    localdir = request.config.getoption("--dynamodb-local-dir")
    skip_cleanup = request.config.getoption("--skip-cleanup")

    dynamodb_local = DynamoDBLocal(localdir)
    dynamodb_local.start()

    yield dynamodb_local

    try:
        if skip_cleanup:
            print("Skipping cleanup")
        else:
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
