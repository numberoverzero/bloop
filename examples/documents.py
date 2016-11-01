import decimal
import random
import uuid

import arrow
from bloop import (
    UUID,
    BaseModel,
    Column,
    DateTime,
    Engine,
    Integer,
    Map,
    Number,
    Set,
    String,
)


# ================================================
# Model setup
# ================================================

Product = Map(**{
    'Name': String,
    'Rating': Number,
    'Updated': DateTime('US/Pacific'),
    'Description': Map(**{
        'Title': String,
        'Body': String
    }),
    'Sellers': Set(Integer)
})


class Item(BaseModel):
    id = Column(UUID, hash_key=True)
    data = Column(Product)

engine = Engine()
engine.bind(BaseModel)


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
    'Sellers': set()
}

for i in range(4):
    seller_id = 'seller-{}'.format(i)
    item.data['Sellers'].add(seller_id)

engine.save(item)
