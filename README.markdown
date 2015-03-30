# bloop 0.2.2

[![Build Status]
(https://travis-ci.org/numberoverzero/bloop.svg?branch=master)]
(https://travis-ci.org/numberoverzero/bloop)[![Coverage Status]
(https://coveralls.io/repos/numberoverzero/bloop/badge.png?branch=master)]
(https://coveralls.io/r/numberoverzero/bloop?branch=master)

Downloads https://pypi.python.org/pypi/bloop

Source https://github.com/numberoverzero/bloop

ORM for DynamoDB

# Installation

`pip install bloop`

# Getting Started

```python
from bloop import (
    Engine, Column,
    NumberType, StringType,
    ObjectsNotFound, ConstraintViolation
)

engine = Engine()

class GameScores(engine.model):
    user_id = Column(NumberType, hash_key=True)
    game_title = Column(StringType, range_key=True)
    top_score = Column(NumberType)
    top_score_date = Column(StringType)
    wins = Column(NumberType)
    losses = Column(NumberType)


engine.bind()

pong_score = GameScores(user_id=101, game_title="Pong")
doom_score = GameScores(user_id=102, game_title="Doom")
scores = [pong_score, doom_score]

try:
    engine.load(scores, consistent_read=True)
except ObjectsNotFound as e:
    print("Failed to load")
    for obj in e.missing:
        print(obj)
    pong_score.wins = 0
    doom_score.losses = 0
else:
    print("Loaded")

pong_score.wins += 1
doom_score.losses += 1

engine.save(scores)
print("Saved")

try:
    engine.delete(doom_score, condition=GameScores.losses > 3)
except ConstraintViolation as e:
    print("Failed to delete")
else:
    print("Deleted")

query = (engine.query(GameScores)
               .key(GameScores.user_id == 101)
               .filter(GameScores.losses < 10))

print("Query not executed until iteration")
for result in query:
    print(result)

```

# Versioning

* bloop follows semver for its **public** API.

  * You should not rely on the internal api staying the same between minor versions.
  * Over time, private apis may be raised to become public.  The reverse will never occur.

# Contributing
Contributions welcome!  Please make sure `tox` passes (including flake8) before submitting a PR.

### Development
bloop uses `tox`, `pytest` and `flake8`.  To get everything set up:

```
# RECOMMENDED: create a virtualenv with:
#     mkvirtualenv bloop
git clone https://github.com/numberoverzero/bloop.git
pip install tox
tox
```

### TODO

* Tests?!?
* Query
* Scan
* Docs
