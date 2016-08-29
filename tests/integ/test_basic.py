"""Basic scenarios, symmetric tests"""
import pytest
from bloop import (
    BaseModel,
    Column,
    GlobalSecondaryIndex,
    Integer,
    MissingObjects,
)

from .models import User


def test_crud(engine):
    engine.bind(User)

    user = User(email="user@domain.com", username="user", profile="first")
    engine.save(user)

    same_user = User(email=user.email, username=user.username)
    engine.load(same_user)
    assert user.profile == same_user.profile

    same_user.profile = "second"
    engine.save(same_user)

    engine.load(user, consistent=True)
    assert user.profile == same_user.profile

    engine.delete(user)

    with pytest.raises(MissingObjects) as excinfo:
        engine.load(same_user, consistent=True)
    assert [same_user] == excinfo.value.objects


def test_projection_overlap(engine):
    class ProjectionOverlap(BaseModel):
        hash = Column(Integer, hash_key=True)
        range = Column(Integer, range_key=True)
        other = Column(Integer)

        by_other = GlobalSecondaryIndex(projection=["other", "range"], hash_key="other")
    # by_other's projected attributes overlap with the model and its own keys
    engine.bind(ProjectionOverlap)


def test_stream_creation(engine):
    class StreamCreation(BaseModel):
        class Meta:
            stream = {
                "include": ["keys"]
            }
        hash = Column(Integer, hash_key=True)
    engine.bind(StreamCreation)
