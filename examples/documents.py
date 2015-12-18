import arrow
import boto3
import decimal
import random
import uuid
from bloop import (Engine, Column, DateTime, Integer,
                   UUID, String, Map, TypedMap, Float)


# ================================================
# Model setup
# ================================================

session = boto3.session.Session(profile_name="test-user-bloop")
engine = Engine(session=session)


Product = Map(**{
    'Name': String,
    'Rating': Float,
    'Updated': DateTime('US/Pacific'),
    'Description': Map(**{
        'Title': String,
        'Body': String
    }),
    'Sellers': TypedMap(Integer)
})


class Item(engine.model):
    id = Column(UUID, hash_key=True)
    data = Column(Product)
engine.bind()


# ================================================
# Usage
# ================================================

item = Item(id=uuid.uuid4())
item.data = {
    'Name': 'item-name',
    'Rating': decimal.Decimal(str(random.random())),
    'Updated': arrow.now(),
    'Description': {
        'Title': 'item-title',
        'Body': 'item-body',
    },
    'Sellers': {}
}

for i in range(4):
    seller_name = 'seller-{}'.format(i)
    item.data['Sellers'][seller_name] = random.randint(0, 100)

engine.save(item)
