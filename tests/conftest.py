import bloop
import bloop.client
import os
import pytest
import sys
from unittest.mock import Mock
sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))
from test_models import BaseModel  # noqa


@pytest.fixture
def engine():
    engine = bloop.Engine()
    engine.client = Mock(spec=bloop.client.Client)
    engine.bind(base=BaseModel)
    return engine


@pytest.fixture
def atomic(engine):
    with engine.context(atomic=True) as atomic:
        return atomic
