from bloop.engine import Engine
from bloop.column import Column
from bloop.expression import ConditionRenderer
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


renderer = ConditionRenderer(engine, GameScores)

condition = (
    ((GameScores.user_id > 102) & (GameScores.top_score.is_not(None)))
  | (GameScores.game_title == "Space Sim")
  | ~(GameScores.wins == 300)
)
renderer.render(condition)

print("{}\n{}\n{}".format(
    renderer.condition_expression,
    renderer.expression_attribute_names
    ,renderer.expression_attribute_values
))
