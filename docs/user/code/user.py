import arrow
import uuid
from decimal import Decimal
from bloop import (
    Engine, Column, GlobalSecondaryIndex, LocalSecondaryIndex,
    DateTime, UUID, Integer, Float, String, Boolean)
engine = Engine()


class User(engine.model):
    class Meta:
        table_name = 'UserTableName'
        read_units = 2
        write_units = 1

    first = Column(String, hash_key=True)
    last = Column(String, range_key=True)

    admin = Column(Boolean)
    balance = Column(Float)
    credits = Column(Integer)
    email = Column(String)
    join = Column(DateTime(timezone='US/Pacific'))
    uid = Column(UUID)

    by_join = GlobalSecondaryIndex(hash_key='join', projection='keys_only')
    by_account = GlobalSecondaryIndex(hash_key='balance', range_key='credits',
                                      projection=['admin', 'uid'],
                                      read_units=3, write_units=1)
    by_uid = LocalSecondaryIndex(range_key='uid', projection='all')

engine.bind()


def create_users():
    richard = User(first="Richard", last="Pryor", admin=False,
                   balance=Decimal(15)/Decimal(100), credits=-1000,
                   email='richard@gmail.com',
                   join=arrow.now().replace(days=-10), uid=uuid.uuid4())
    george = User(first="George", last="Carlin", admin=False,
                  balance=Decimal(30.50), credits=-2, email='george@gmail.com',
                  join=arrow.now().replace(days=-9), uid=uuid.uuid4())
    louis = User(first="Louis", last="C.K.", admin=True,
                 balance=Decimal(2**15), credits=3,
                 email='louis@hotmail.com',
                 join=arrow.now().replace(days=-1), uid=uuid.uuid4())
    robin = User(first="Robin", last="Williams", admin=False,
                 balance=Decimal(2**25), credits=0,
                 email='robin@gmail.com',
                 join=arrow.now().replace(hours=-15), uid=uuid.uuid4())
    bill = User(first="Bill", last="Hicks", admin=True,
                balance=Decimal(3.50), credits=100, email='bill@hotmail.com',
                join=arrow.now().replace(hours=-9), uid=uuid.uuid4())
    dave = User(first="Dave", last="Chappelle", admin=False,
                balance=Decimal(2**19), credits=300, email='dave@hotmail.com',
                join=arrow.now().replace(minutes=-25), uid=uuid.uuid4())
    users = [richard, george, louis, robin, bill, dave]
    engine.save(users)
    return users
