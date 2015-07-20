"""
Combined source from the README's "Getting Started" section.
"""
from bloop import (Column, DateTime, Engine, Integer, String, UUID,
                   GlobalSecondaryIndex,
                   LocalSecondaryIndex)
engine = Engine()


class Model(engine.model):
    class Meta:
        write_units = 2
        read_units = 3
        table_name = 'CustomTableName'
    name = Column(UUID, hash_key=True)
    date = Column(DateTime, range_key=True)
    email = Column(String)
    joined = Column(String)
    not_projected = Column(Integer)

    by_email = GlobalSecondaryIndex(hash_key='email', read_units=4,
                                    projection='all', write_units=5)
    by_joined = LocalSecondaryIndex(range_key='joined',
                                    projection=['email'])
engine.bind()
