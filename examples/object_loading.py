"""
# Combined source from the README's "Custom Object Loading" section.
# To play around:

from object_loading import *

# Make a user to find
user = CustomUser()
user.id = uid = uuid.uuid4()
engine.save(user)

# This will find the result above, and load the result through `load_user`
print(engine.query(CustomUser).key(CustomUser.id == uid).first())
"""
from bloop import (Engine, Column, DateTime, Boolean, String, UUID)
import uuid  # flake8: noqa
engine = Engine()


class CustomUser(engine.model):

    def __init__(self):
        # Disallows **kwarg loading
        pass

    id = Column(UUID, hash_key=True)
    admin = Column(Boolean)
    joined = Column(DateTime)
    email = Column(String)
engine.bind()


def load_user(**kwargs):
    print("Using custom loader")
    user = CustomUser()
    for key, value in kwargs.items():
        setattr(user, key, value)
    return user
CustomUser.Meta.bloop_init = load_user
