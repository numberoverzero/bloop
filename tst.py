from bloop import (
    Engine, Column, String, Integer,
    GlobalSecondaryIndex, ObjectsNotFound
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
                                            range_key='top_score',
                                            projection=['wins', 'losses'])
engine.bind()

wow_score = GameScores(user_id=102, game_title="WoW", losses=4, top_score=9001)
engine.save(wow_score)

space_sim = GameScores(user_id=101, game_title="Space Sim")

try:
    engine.load(space_sim)
except ObjectsNotFound:
    space_sim.losses = 0
    space_sim.top_score_date = "Today"
    space_sim.wins = 42
    engine.save(space_sim)

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
