import uuid
from datetime import datetime, timezone

from bloop import (
    UUID,
    BaseModel,
    Column,
    DateTime,
    Engine,
    GlobalSecondaryIndex,
    Integer,
    String,
)


# ================================================
# Model setup
# ================================================

class Account(BaseModel):
    class Meta:
        read_units = 5
        write_units = 2

    id = Column(UUID, hash_key=True)
    name = Column(String)
    email = Column(String)
    by_email = GlobalSecondaryIndex(
        hash_key='email', projection='keys',
        write_units=1, read_units=5)


class Tweet(BaseModel):
    class Meta:
        write_units = 10
    account = Column(UUID, hash_key=True)
    id = Column(String, range_key=True)
    content = Column(String)
    date = Column(DateTime)
    favorites = Column(Integer)

    by_date = GlobalSecondaryIndex(
        hash_key='date', projection='keys')


engine = Engine()
engine.bind(BaseModel)


# ================================================
# Usage
# ================================================

account = Account(
    id=uuid.uuid4(), name='@garybernhardt',
    email='REDACTED')
tweet = Tweet(
    account=account.id, id='616102582239399936',
    content='today, I wrote a type validator in Python, as you do',
    favorites=9,
    date=datetime.now(timezone.utc))

engine.save(account, tweet)
