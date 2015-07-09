import arrow
import boto3
from bloop import Engine, Column, Integer, DateTime


boto3.setup_default_session(profile_name='test-user-bloop')
engine = Engine()


class Model(engine.model):
    id = Column(Integer, hash_key=True)
    date = Column(DateTime(timezone='US/Pacific'))
engine.bind()

obj = Model(id=1, date=arrow.now())
engine.save(obj)

paris_one_day_ago = arrow.now().to('Europe/Paris').replace(days=-1)

query = (engine.query(Model)
               .key(Model.id == 1)
               .filter(Model.date >= paris_one_day_ago))

# Equivalent for prefetch >= 0
print(query.first().date)
print(query.all().first.date)
