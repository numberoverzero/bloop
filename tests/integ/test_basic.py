"""Basic scenarios, symmetric tests"""
import pytest
from bloop import MissingObjects

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

    engine.load(user)
    assert user.profile == same_user.profile

    engine.delete(user)

    with pytest.raises(MissingObjects) as excinfo:
        engine.load(same_user)
    assert [same_user] == excinfo.value.objects
