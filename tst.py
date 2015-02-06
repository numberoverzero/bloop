from bloop.engine import Engine
from bloop.column import Column
from bloop.types import StringType, NumberType, BooleanType
import botocore
import boto3

# patch session to use a specific profile
session = botocore.session.get_session()
session.profile = 'test-user-bloop'
boto3.setup_default_session(botocore_session=session)

engine = Engine()

# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
class GameScores(engine.model):
    user_id = Column(NumberType, hash_key=True)
    game_title = Column(StringType, range_key=True)
    top_score = Column(NumberType)
    top_score_date = Column(StringType)
    wins = Column(NumberType)
    losses = Column(NumberType)

engine.bind()

gs = engine.get(GameScores, user_id=101, game_title="Space Sim")
print(gs)
gs.losses += 1
engine.save(gs, overwrite=True)
print(gs)
