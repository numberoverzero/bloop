import logging
from unittest.mock import Mock

import pytest

from bloop import Engine
from bloop.session import SessionWrapper
from bloop.signals import (
    object_deleted,
    object_loaded,
    object_modified,
    object_saved,
)


@pytest.fixture(autouse=True)
def __set_log_level(caplog):
    """Always set caplog to capture DEBUG"""
    caplog.set_level(logging.DEBUG)


@pytest.fixture
def dynamodb():
    return Mock()


@pytest.fixture
def dynamodbstreams():
    return Mock()


@pytest.fixture
def session():
    s = Mock(spec=SessionWrapper)

    # Most tests won't send a "ReturnValues" so simplify mock setup by
    # assuming we don't inspect the response.
    s.save_item.return_value = None
    s.delete_item.return_value = None

    return s


@pytest.fixture
def engine(session, dynamodb, dynamodbstreams):
    # HACK: These clients won't be used.  We're going to replace the session immediately.
    engine = Engine(dynamodb=dynamodb, dynamodbstreams=dynamodbstreams)
    # Toss the clients above and hook up the mock session
    engine.session = session
    return engine


@pytest.fixture
def signals():
    calls = {
        "object_deleted": [],
        "object_loaded": [],
        "object_modified": [],
        "object_saved": []
    }

    @object_deleted.connect
    def on_deleted(**kwargs):
        calls["deleted"].append(kwargs)

    @object_loaded.connect
    def on_loaded(**kwargs):
        calls["loaded"].append(kwargs)

    @object_modified.connect
    def on_modified(**kwargs):
        calls["modified"].append(kwargs)

    @object_saved.connect
    def on_saved(**kwargs):
        calls["saved"].append(kwargs)

    return calls
