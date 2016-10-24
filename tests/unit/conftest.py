from unittest.mock import Mock

import boto3
import pytest
from bloop import BaseModel, Engine
from bloop.session import SessionWrapper
from bloop.signals import (
    object_deleted,
    object_loaded,
    object_modified,
    object_saved,
)


@pytest.fixture
def session():
    return Mock(spec=SessionWrapper)


@pytest.fixture
def engine(session):
    _engine = Engine(session=Mock(spec=boto3.Session))
    # Immediately replace that session wrapper
    _engine.session = session
    _engine.bind(base=BaseModel)
    return _engine


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
