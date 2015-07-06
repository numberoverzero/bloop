from bloop import (
    Engine, Column, String, Integer,
    GlobalSecondaryIndex
)
import boto3
import logging
logging.basicConfig(level=logging.DEBUG)

# patch session to use a specific profile
boto3.setup_default_session(profile_name='test-user-bloop')

# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
engine = Engine()


class GameScores(engine.model):
    user_id = Column(Integer, hash_key=True)
    game_title = Column(String, range_key=True)
    top_score = Column(Integer)
    top_score_date = Column(String)
    wins = Column(Integer)
    losses = Column(Integer)
    game_title_index = GlobalSecondaryIndex(hash_key='game_title',
                                            range_key='top_score')
engine.bind()

query = engine.query(GameScores)\
              .key((GameScores.user_id == 101) &
                   GameScores.game_title.between("Random", "Treasure"))\
              .filter((GameScores.wins > 100) | (GameScores.wins < 50))\
              .select([GameScores.wins, GameScores.losses,
                      GameScores.game_title, GameScores.user_id])

for result in query:
    previous_losses = GameScores.losses == result.losses
    # Full load since we selected a few columns above
    engine.load(result)
    result.losses += 1
    engine.save(result, condition=previous_losses)
    print(result)
