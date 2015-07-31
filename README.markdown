# bloop 0.7.5

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

same_post = Post(forum=post.forum, id=post.id)
engine.load(same_post)
assert post.date == same_post.date
```

# Query, Scan

```
def explore_query(q):
    for result in q:
        print(result.name)

# Query the 'by_email' index
q = engine.query(Post.by_user).key(Post.user == some_uuid)
explore_query(q)

# We can iteratively build a query's parameters
q = q.consistent.descending
q = q.filter(Post.content.contains('#yolo'))
explore_query(q)

# Scan the model hash and range keys
date_condition = Post.date >= arrow.now().replace(years=-1)
q = engine.scan(Post).select(date_condition)
explore_query(q)
```

# Load, Save, Delete

```python
obj = Model(name=uuid.uuid4(), date=arrow.now(), joined='today!')
another = Model(name=uuid.uuid4(), date=arrow.now().replace(days=-1),
                email='another@example.com')
engine.save([obj, another])

same_obj = Model(name=obj.name, date=obj.date)
engine.load(same_obj)
print(same_obj.joined)

engine.delete([obj, another])
```

`load`, `save`, and `delete` can take a single instance of a model, or an
iterable of model instances.

# Meta and Table Creation

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

`engine.bind()` is also verifying existing tables against the expected table
for a model, and throwing on a mismatch.  It then busy polls until the table
(and any GSIs) are in an active state.

## Additional features

### conditions

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

If a condition is passed with multiple objects, it will be applied to each
object individually (there are no batch operations that support conditions).

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
        # Because we're using an atomic delete, this will ensure the object
        # is exactly as we read it on the third line of the function
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
        # We're only using an atomic load within the try/except,
        # and the engine 'atomic' setting is unchanged for other callers.
        try:
            with engine.context(atomic=True) as atomic:
                atomic.delete(profile)
        except bloop.ConstraintViolation:
            # We caught a race condition!  The profile changed since we last
            # loaded it.
            pass
```

### update vs overwrite

By default, bloop uses `UpdateItem` when you call `engine.save`.  This can be
changed through `engine.config['save']` - either `update` or
`overwrite`.

`overwrite` uses `PutItem`, which will overwrite **all** attributes of the
object - if no value is provided, the existing value will simply be deleted.

`update` uses `UpdateItem` and will only overwrite attributes that have changed
since they were last loaded from a `load`, `query`, or `scan`.  Setting a value
to `None` will not delete it, as None may have its own meaning for the type.
Instead, you should explicitly `del obj.attribute` to remove it on next update.

Remember that queries on an index may not return all attributes, depending on
the index projection.  For example, loading from a GlobalSecondaryIndex with
projection 'keys_only' and immediately saving it with overwrite will
immediately blank out all non-key attributes, as they were not loaded into the
object from the query.

# Versioning

* bloop follows semver for its **public** API.

  * You should not rely on the internal api staying the same between minor
    versions.
  * Over time, private apis may be raised to become public.  The reverse
    will never occur.

# Contributing
Contributions welcome!  Please make sure `tox` passes (including flake8)
before submitting a PR.

### Development
bloop uses `tox`, `pytest` and `flake8`.  To get everything set up:

```
# RECOMMENDED: create a virtualenv with:
#     pyenv virtualenv 3.4.3 bloop
git clone https://github.com/numberoverzero/bloop.git
pip install tox
tox
```

# Appendix

## update overhead

Creating the diff for an object when saving with "update" requires tracking the
values last loaded against the current values.  There are many ways to
accomplish this - commonly, the `Column` equivalent in an ORM will set a flag
when the column value is `set` or `del'd` to indicate its mutation.  Tracking
nested changes (eg. dicts and custom classes) require even more work.

Instead, bloop tracks the values loaded from a call, along with the expected
columns that such a load **should** have seen.  If a column was expected but
not loaded, its value is empty in DynamoDB.  If it was expected and seen,
the value is tracked as having been seen.  When creating a diff, any value that
was seen but is no longer in the object (`del obj.attr`) is added to the set
of DELETE signals for update.  Any value that was seen and is not equal to the
current, or that was not seen and now has a value, is added to the set of SET
signals for update.

In this way, a small amount of overhead is added to load an object from a
DynamoDB response; otherwise, there is no performance impact.  Space is
minimally impacted, as the dumped (serialized-ish) representation of values is
copied when tracking, which is usually very small (even for arbitrary types).
