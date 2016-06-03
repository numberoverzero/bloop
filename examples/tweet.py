import arrow
import boto3
import uuid
from bloop import (Engine, Column, Integer, DateTime, UUID,
                   GlobalSecondaryIndex, String, new_base)

# ================================================
# Model setup
# ================================================

session = boto3.session.Session(profile_name="test-user-bloop")
engine = Engine(session=session)
Base = new_base()


class Account(Base):
    class Meta:
        read_units = 5
        write_units = 2

    id = Column(UUID, hash_key=True)
    name = Column(String)
    email = Column(String)
    by_email = GlobalSecondaryIndex(
        hash_key='email', projection='keys_only',
        write_units=1, read_units=5)


class Tweet(Base):
    class Meta:
        write_units = 10
    account = Column(UUID, hash_key=True)
    id = Column(String, range_key=True)
    content = Column(String)
    date = Column(DateTime(timezone='EU/Paris'))
    favorites = Column(Integer)

    by_date = GlobalSecondaryIndex(
        hash_key='date', projection='keys_only')

engine.bind(base=Base)


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
    date=arrow.now())

engine.save([account, tweet])
