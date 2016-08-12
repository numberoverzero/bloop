from unittest.mock import Mock

import pytest
from bloop import Client, Engine

from ..helpers.models import BaseModel


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
