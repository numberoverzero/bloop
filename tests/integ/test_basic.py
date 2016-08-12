"""Basic scenarios, symmetric tests"""
import pytest
from bloop import String
from bloop import new_base, Column, Integer, NotModified


class User(new_base()):
    id = Column(Integer, hash_key=True)
    name = Column(String)


def test_crud(engine):
    engine.bind(User)

    user = User(id=0, name="first")
    engine.save(user)

    same_user = User(id=user.id)
    engine.load(same_user)
    assert user.name == same_user.name

    same_user.name = "second"
    engine.save(same_user)

    engine.load(user)
    assert user.name == same_user.name

    engine.delete(user)

    with pytest.raises(NotModified) as excinfo:
        engine.load(same_user)
    assert same_user in excinfo.value.objects
