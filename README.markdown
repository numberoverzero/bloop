# bloop 0.7.3

[![Build Status]
(https://travis-ci.org/numberoverzero/bloop.svg?branch=master)]
(https://travis-ci.org/numberoverzero/bloop)[![Coverage Status]
(https://coveralls.io/repos/numberoverzero/bloop/badge.png?branch=master)]
(https://coveralls.io/r/numberoverzero/bloop?branch=master)

Downloads https://pypi.python.org/pypi/bloop

Source https://github.com/numberoverzero/bloop

DynamoDB object mapper for python 3.4+

# Installation

`pip install bloop`

# Getting Started

```python
from bloop import (Boolean, Engine, Column, DateTime,
                   GlobalSecondaryIndex, Integer, String, UUID)
import arrow
import uuid
engine = Engine()


class User(engine.model):
    id = Column(UUID, hash_key=True)
    admin = Column(Boolean)


class Post(engine.model):
    forum = Column(String, hash_key=True)
    id = Column(UUID, range_key=True)
    user = Column(UUID)
    date = Column(DateTime)
    views = Column(Integer)
    content = Column(String)

    by_user = GlobalSecondaryIndex(hash_key='user', projection='all')
engine.bind()


user = User(id=uuid.uuid4(), admin=False)
post = Post(forum='DynamoDB', id=uuid.uuid4(), user=user.id,
            date=arrow.now(), views=0, content='Hello, World!')
engine.save((user, post))


def user_posts(user_id):
    return engine.query(Post.by_user).key(Post.user == user_id).all()


def forum_posts(forum):
    return engine.query(Post).key(Post.forum == forum).all()

for post in user_posts(user.id):
    print(post)
for post in forum_posts('DynamoDB'):
    print(post)
```

# Complex Models

## Meta and Table Creation

Here we're going to use a custom table name, custom read/write units, as well
as specifying a custom projection for a GSI, and shortened attribute names.

```python
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
```

The equivalent table definition (that you'd pass to dynamodb.create_table):

```python
{
    'TableName': 'CustomTableName',
    'GlobalSecondaryIndexes': [{
        'KeySchema': [{'KeyType': 'HASH', 'AttributeName': 'email'}],
        'IndexName': 'by_email',
        'Projection': {'ProjectionType': 'ALL'},
        'ProvisionedThroughput': {
            'WriteCapacityUnits': 5,
            'ReadCapacityUnits': 4}}],
    'LocalSecondaryIndexes': [{
        'KeySchema': [
            {'KeyType': 'HASH', 'AttributeName': 'name'},
            {'KeyType': 'RANGE', 'AttributeName': 'joined'}],
        'IndexName': 'by_joined',
        'Projection': {
            'ProjectionType': 'INCLUDE',
            'NonKeyAttributes': ['joined', 'email', 'name', 'date']}}],
    'ProvisionedThroughput': {'WriteCapacityUnits': 2, 'ReadCapacityUnits': 3},
    'KeySchema': [
        {'KeyType': 'HASH', 'AttributeName': 'name'},
        {'KeyType': 'RANGE', 'AttributeName': 'date'}],
    'AttributeDefinitions': [
        {'AttributeType': 'S', 'AttributeName': 'name'},
        {'AttributeType': 'S', 'AttributeName': 'date'},
        {'AttributeType': 'S', 'AttributeName': 'email'},
        {'AttributeType': 'S', 'AttributeName': 'joined'}]
}
```

`engine.bind()` is doing more work than constructing the above - it also
handles retries with exponential backoff on create/describe calls, and polls
until the table reaches the expected state (ACTIVE).

Without using any addtional features, bloop is great for modeling your tables
and ensuring they are ready for use, simply by declaring classes and calling
engine.bind.


## Load, Save, Delete

```python
import arrow
obj = Model(name=uuid.uuid4(), date=arrow.now(), joined='today!')
another = Model(name=uuid.uuid4(), date=arrow.now().replace(days=-1),
                email='another@example.com')

engine.save([obj, another])

same_obj = Model(name=obj.name, date=obj.date)
engine.load(same_obj)
print(same_obj.joined)
engine.delete([obj, another])
```

`load`, `save`, and `delete` can take a single instance of a model, or an iterable
of model instances.  The instances do not need to be of the same model, and
any number can be loaded, saved, or deleted.  Loads will be optimally packed
into batches of 25 (the maximum number for BatchGetItem).

## Save & Delete: Conditions and Atomicity

DynamoDB offers powerful features to ease working with objects in a distributed
manner, and bloop works hard to expose those options simply and transparently.

Conditions offer the ability to only save or delete if the condition is met
**before** performing the operation.  This optimistic update can be used to
ensure no one has modified an item before this call.  For example, let's delete
a user profile, as long as it hasn't logged in in the last two years.

```python
def delete_old_profile(profile_id):
    two_years_ago = arrow.now().replace(years=-2)

    profile = UserProfile(id=profile_id)
    engine.load(profile)
    if profile.last_login <= two_years_ago:
        # WARNING: without a condition, someone could log in after we enter
        # this block and we'd delete their account immediately after they
        # logged in.
        condition = UserProfile.last_login <= two_years_ago
        try:
            engine.delete(profile, condition=condition)
        except bloop.ConstraintViolation:
            # We caught a race condition!  The profile's last_login no longer
            # meets the criteria we expected
            pass
```

### atomic

By constructing a set of conditions for all attributes of an object using the
last values loaded from DynamoDB, we can ensure the row hasn't been modified
since we last loaded it.  Instead of doing this by hand every time, bloop
exposes a config value `engine.config['atomic']` that will automatically
attach a condition based on the last loaded values.  Now, we can simplify the
`delete_old_profile` function above:

```python
engine.config['atomic'] = True
def delete_old_profile(profile_id):
    two_years_ago = arrow.now().replace(years=-2)

    profile = UserProfile(id=profile_id)
    engine.load(profile)
    if profile.last_login <= two_years_ago:
        # WARNING: without a condition, someone could log in after we enter
        # this block and we'd delete their account immediately after they
        # logged in.
        try:
            engine.delete(profile)
        except bloop.ConstraintViolation:
            # We caught a race condition!  The profile changed since we last
            # loaded it.
            pass
```

However this sets the engine to be atomic for all operations - to temporarily
set the engine to atomic we'd have to store its last value, set it atomic, and
revert it after the function.  There's a simpler option:

### context

Finally, engine also offers a `context` helper that can be used to temporarily
adjust config without modifying the underlying engine.  Within that context,
the engine will behave according to its original config, except those
explicitly modified.

```python
def delete_old_profile(profile_id):
    two_years_ago = arrow.now().replace(years=-2)

    profile = UserProfile(id=profile_id)
    engine.load(profile)
    if profile.last_login <= two_years_ago:
        # WARNING: without a condition, someone could log in after we enter
        # this block and we'd delete their account immediately after they
        # logged in.
        try:
            with engine.context(atomic=True) as atomic:
                atomic.delete(profile)
        except bloop.ConstraintViolation:
            # We caught a race condition!  The profile changed since we last
            # loaded it.
            pass
```

## Query & Scan

Taking after sqlalchemy where possible, we can query on models or their
indexes:

```
def explore_query(q):
    for result in q:
        print(result.name)

# By the 'by_email' index
q = engine.query(Model.by_email).key(Model.email == 'foo@domain.com')
explore_query(q)

# We can iteratively build a query's parameters
q = q.consistent.descending
q = q.filter(Model.email.contains('@domain.com'))
explore_query(q)

# By the model hash and range keys
name_condition = Model.name == uuid.uuid4()
date_condition = Model.date >= arrow.now().replace(years=-1)
q = engine.query(Model).key(name_condition & date_condition)
explore_query(q)
```
