import random
import string

from bloop import Column, String, new_base
BaseModel = new_base()


class User(BaseModel):
    email = Column(String, hash_key=True)
    username = Column(String, range_key=True)

    profile = Column(String)


def _letters(n):
    return "".join(random.choice(string.ascii_letters) for _ in range(n))


def valid_user():
    email = "e-{}@{}".format(_letters(3), _letters(4))
    username = "u-{}".format(_letters(7))
    return User(
        email=email,
        username=username
    )
