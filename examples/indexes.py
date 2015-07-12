"""
# Combined source from the README's "Local and Global Secondary Indexes" section.
# To play around:

from indexes import *

uid = uuid.uuid4

posts = 4
user1 = uid()
user2 = uid()
users = [user1, user1, user2, user2]

dates = [
    arrow.now(),
    arrow.now.replace(days=-1),
    arrow.now.replace(days=-2),
    arrow.now.replace(days=-3)]

posts = [Post(id=uid(), user=user, date=date, views=0) for
         (user, date) in zip(users, dates)]
engine.save(posts)
"""
from bloop import (Engine, Column, DateTime, GlobalSecondaryIndex,
                   LocalSecondaryIndex, Integer, String, UUID)
import arrow  # flake8: noqa
import uuid  # flake8: noqa
engine = Engine()


class IndexPost(engine.model):
    id = Column(UUID, hash_key=True)
    user = Column(UUID, range_key=True)
    date = Column(DateTime)
    views = Column(Integer)

    by_user = GlobalSecondaryIndex(hash_key='user',
                                   projection='keys_only',
                                   write_units=1, read_units=10)

    by_date = LocalSecondaryIndex(range_key='date',
                                  projection=['views'])
