"""Basic scenarios, symmetric tests"""
from bloop import new_base, Column, Integer


def test_crud(engine):
    class User(new_base()):
        id = Column(Integer, hash_key=True)

    engine.bind(User)
