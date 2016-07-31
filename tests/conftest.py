from bloop import Client, Engine

import os
import pytest
import sys

from unittest.mock import Mock
sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))
from test_models import BaseModel  # noqa


@pytest.fixture
def engine():
    engine = Engine()
    engine.client = Mock(spec=Client)
    engine.bind(base=BaseModel)
    return engine


@pytest.fixture
def atomic(engine):
    atomic = Engine(client=engine.client, type_engine=engine.type_engine, **engine.config)
    atomic.config["atomic"] = True
    return atomic
