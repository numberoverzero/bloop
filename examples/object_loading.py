"""
Combined source from the README "Custom Object Loading" section.
"""
from bloop import (Engine, Column, DateTime, Boolean, String, UUID)
import arrow
import uuid
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


def main():
    # Make a user to find
    user = CustomUser()
    user.id = uuid.uuid4()
    user.admin = False
    user.joined = arrow.now()
    user.email = "admin@support.com"
    engine.save(user)

    # This will find the result above, and load the result through `load_user`
    print(engine.query(CustomUser).key(CustomUser.id == user.id).first())

if __name__ == "__main__":
    main()
