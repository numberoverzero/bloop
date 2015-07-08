# bloop 0.3.0

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
    Engine, Column, Integer, Float, String,
    ObjectsNotFound, ConstraintViolation
)

engine = Engine()

class GameScores(engine.model):
    user_id = Column(Integer, hash_key=True)
    game_title = Column(String, range_key=True)
    top_score = Column(Float)
    top_score_date = Column(String)
    wins = Column(Integer)
    losses = Column(Integer)


engine.bind()

pong_score = GameScores(user_id=101, game_title="Pong")
doom_score = GameScores(user_id=102, game_title="Doom")
scores = [pong_score, doom_score]

try:
    engine.load(scores, consistent=True)
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

# API

### [Amazon DynamoDB Actions](actions)

Let's work backwards - where is each Dynamo call used?

* [BatchGetItem](batch-get) - `engine.load(items)`
* [BatchWriteItem](batch-write) - `engine.save(items)` and `engine.delete(items)`
* [CreateTable](create-table) - Internal during `engine.bind()`
* [DeleteItem](delete-item) - `engine.delete(item)` (Singular delete with optional conditions)
* [DeleteTable](delete-table) - Unused
* [DescribeTable](describe-table) - Internal during `engine.bind()` (if table mutations are enabled)
* [GetItem](get-item) - Unused
* [ListTables](list-tables) - Unused
* [PutItem](put-item) - `engine.save(item)` (Singular save with optional conditions)
* [Query](query) - `engine.query(Model)`
* [Scan](scan) - `engine.scan(Model)`
* [UpdateItem](update-item) - Unused (no partial writes)
* [UpdateTable](update-table) - Internal during `engine.bind()` (if table mutations are enabled)

## Common Actions

### `engine.load`

Pass `consistent=True` for [Consistent Reads](consistent-reads).

```python
now = time.time  # TODO: UTC!
item = Visitor(email='joe.mcross@gmail.com', visit_date=now)
engine.load(item, consistent=True)

another_item = User(email='joe.mcross@gmail.com')
items = [item, another_item]
engine.load(items)
```

### `engine.save` & `engine.delete`

Both save and delete expose the same interface (these both map to [PutItem](put-item)).

```python
item = Visitor(email='joe.mcross@gmail.com', visit_date=now)
engine.load(item)
item.visits += 1

# Save with condition - bail if the visit count isn't what we saw during load
engine.save(item, Visitor.visits == item.visits-1)

another_item = User(email='joe.mcross@gmail.com')
items = [item, another_item]
engine.save(items)
```

### `engine.query` & `engine.scan`

Both query and scan expose the same interface.  Constraints that are semantically useless for the operation (such as KeyConditions for a scan) will be ignored when constructing the request.  If minimum constraints have not been met to form a valid request (such as omission of KeyConditions for a query) an exception will be raised.

Constraints can be added and modified through chaining.  Both queries and scans are immutable,
and each constraint specification will return a new query/scan object.  This allows the creation of base queries that can be re-used.

```python
base_query = engine.query(Model).filter(Model.visits > 50)

# All visits for joe.mcross@gmail.com with over 50 visits
visits = base_query.key(Model.email=="joe.mcross@gmail.com")

# Queries and scans are executed when iterated
for visit in visits:
    print(visit)

# Another visitor, again with over 50 visits
other_visits = base_query.key(Model.email=="foo@bar.com")
```

An index ([Local](lsi) or [Global](gsi)) can be specified only when constructing the base query, and cannot be changed through chaining.

```python
base_query = engine.query(Model, index=Model.visit_date)

# All the same options apply here
visits = base_query.key(...).consistent.filter(...)
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
* Docs
* All projection types
* CreateTable -> DescribeTable -> UpdateTable

[actions]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Operations.html
[consistent-reads]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/APISummary.html
[lsi]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html
[gsi]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html

[batch-get]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
[batch-write]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
[create-table]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html
[delete-item]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html
[delete-table]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html
[describe-table]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html
[get-item]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
[list-tables]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html
[put-item]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
[query]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
[scan]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
[update-item]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
[update-table]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html
