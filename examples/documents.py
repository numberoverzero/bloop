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
    Float,
    Integer,
    Map,
    String,
    TypedMap,
)


# ================================================
# Model setup
# ================================================

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
    'Sellers': {}
}

for i in range(4):
    seller_name = 'seller-{}'.format(i)
    item.data['Sellers'][seller_name] = random.randint(0, 100)

engine.save(item)
