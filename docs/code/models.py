import arrow
import uuid
from bloop import (Engine, Column, Integer, DateTime, UUID,
                   GlobalSecondaryIndex, String, new_base)
engine = Engine()
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
    date = Column(DateTime)
    favorites = Column(Integer)

    by_date = GlobalSecondaryIndex(
        hash_key='date', projection='keys_only')

engine.bind(base=Base)
