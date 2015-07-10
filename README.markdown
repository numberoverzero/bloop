# bloop 0.5.0

[![Build Status]
(https://travis-ci.org/numberoverzero/bloop.svg?branch=master)]
(https://travis-ci.org/numberoverzero/bloop)[![Coverage Status]
(https://coveralls.io/repos/numberoverzero/bloop/badge.png?branch=master)]
(https://coveralls.io/r/numberoverzero/bloop?branch=master)

Downloads https://pypi.python.org/pypi/bloop

Source https://github.com/numberoverzero/bloop

DynamoDB object mapper for python 3.3+

# Installation

`pip install bloop`

# Getting Started

We'll be using some simplistic user and post models for a hypothetical forum.  First, we'll set up our models and bind the engine:

```python
from bloop import (Boolean, Engine, Column, DateTime,
                   GlobalSecondaryIndex, Integer, String, UUID)
import arrow
import uuid

engine = Engine()


class User(engine.model):
    id = Column(Integer, hash_key=True)
    admin = Column(Boolean, name='a')


class Post(engine.model):
    id = Column(UUID, hash_key=True)
    user = Column(UUID, name='u')

    date = Column(DateTime(timezone='US/Pacific'), name='d')
    views = Column(Integer, name='v')
    content = Column(String, name='c')

    by_user = GlobalSecondaryIndex(hash_key='user', projection='keys_only',
                                   write_units=1, read_units=10)

engine.bind()
```

Most of our columns pass a `name` parameter - this is because [attribute names count towards item size][dynamo-limits].  By default the model name is used (we'll still use `id`, for instance) but specifying the backing name allows us to use convenient names (`user`, `date`) while saving space for requests (`u`, `d`).

Let's create a new user - and make sure we don't overwrite an existing key!

```python
def create_user(admin=False):
    ''' Create a new user, throwing if the randomly generated id is in use '''
    user = User(id=uuid.uuid4(), admin=admin)
    does_not_exist = User.id.is_(None)
    engine.save(user, condition=does_not_exist)
```

bloop works hard to expose DynamoDB's [Conditional Expression][conditional-writes] system in a clean, intuitive interface.  To that end, we can construct conditions that must be met before performing an operation (`save`, `delete`) using the Model's columns and standard comparison operators.  In this case, `User.id.is_(None)` could also be written `User.id == None` which ensures there's no row where the id we want to save exists.

Next, let's take advantage of our GlobalSecondaryIndex `by_user` to find all posts by a user:

```python
def posts_by_user(user_id):
    ''' Returns an iterable of posts by the user '''
    return engine.query(Post, index=Post.by_user).key(Post.id == user_id)
```

Again we leverage standard comparison operators to define the key condition with `Post.id == user_id`.  There are a number of moving pieces that allow this function to stay so simple:

* query parameters can be chained together to create new queries.
* queries are executed when iterated;
* results are lazily loaded according to a configurable prefetch setting;
* continuation tokens are followed using appropriate retries with a configurable exponential backoff;

Let's write a few more helpers for common operations when rendering and updating pages:

```python
def increment_views(post_id):
    '''
    Load post, increment views, save with the condition that the view count
    still has its old value
    '''
    post = Post(id=post_id)
    engine.load(post)
    post.views += 1
    old_views = Post.views == (post.views - 1)
    engine.save(post, condition=old_views)


def edit(user_id, post_id, new_content):
    ''' Verify user can edit post, then change content and update date '''
    user = User(id=user_id)
    post = Post(id=post_id)
    engine.load([user, post])

    if (not user.admin) and (post.user != user.id):
        raise ValueError("User not authorized to edit post.")

    post.content = new_content
    post.date = arrow.now()  # timezone doesn't matter, bloop stores in UTC
    engine.save(post)
```

Here we see `engine.load` can load a single object or multiple - batching is automatically taken care of, even grouping models together to minimize request/response size.

bloop leverages the outstanding [`arrow`][arrow-docs] for `DateTime` objects, with values persisted as UTC [ISO 8601][iso-8601] strings.  In addition, comparisons can be made against any timezone, since all values are converted to UTC before they reach DynamoDB.  This makes locale-aware queries trivial to write:

```python
local_timezone = 'Europe/Paris'
now_local = arrow.now().to(local_timezone)
yesterday_local = now_local.replace(days=-1)

since_yesterday = Post.date.between(yesterday_local, now_local)
recent_posts = engine.scan(Post).filter(since_yesterday)
```

# Defining Models

There are 3 key components when building models:

1. A `bloop.Engine`'s base model
2. `bloop.Column` to define columns, hash and range keys, and indexes
3. The various types - `Integer, Float, String, UUID, DateTime, Boolean`

## Engine.model and sessions

A model can only be rendered by the engine it's base class is built on.  This allows multiple engines to render similar models differently; against differnt endpoints; across regions.  This can be particularly useful when performing migrations, with one engine reading the old format, while another writes data in the new format.

```python
import bloop
import boto3.session
regional_engines = {}


def engine(region):
    '''
    Ensure a single engine per region.

    Multiple calls with the same region return the same engine.
    '''
    engine = regional_engines.get(region)
    if not engine:
        session = boto3.session.Session(region_name=region)
        regional_engines[region] = engine = bloop.Engine(session=session)
    return engine


class EastModel(engine('us-east-1')):
    id = bloop.Column(bloop.UUID, hash_key=True)


class WestModel(engine('us-west-2')):
    id = bloop.Column(bloop.UUID, hash_key=True)
```

Here, we provided a custom [`boto3.session.Session`][boto3-session] so that we could use custom connection parameters instead of the global default profile.  We then constructed a minimum valid Model, with at least a hash_key.

We aren't quite ready to use the model yet.  The model <--> DynamoDB table binding step is independent from model definition.  This makes it easier to handle any errors that arise during table creation/validation, instead of requring class definitions to be inside try/catch blocks.  Binding the models is similarly straightforward:

```python
engine('us-east-1').bind()
engine('us-west-2').bind()
```

Remember that we can bind at any time, and the function is re-entrant.  Only models created since the last `bind` call, or those that previously failed to properly bind, will be bound.

## \_\_init\_\_ and model loading

By default, models provide **kwarg `__init__` methods, similar to `namedtuple` but without allowing positional arguments.  Let's use the `Post` model from above:

```python
class Post(engine.model):
    id = Column(UUID, hash_key=True)
    user = Column(UUID, name='u')

    date = Column(DateTime(timezone='US/Pacific'), name='d')
    views = Column(Integer, name='v')
    content = Column(String, name='c')

    by_user = GlobalSecondaryIndex(hash_key='user', projection='keys_only',
                                   write_units=1, read_units=10)
```

We can construct some instances using keyword args:

```python
uid = uuid.uuid4

troll_user = uid()
admin = uid()

troll_post = Post(id=uid(), user=troll_user, date=arrow.now())
announcement = Post(id=uid(), user=admin, date=arrow.now())
```

There is no default value for columns not specified - `announcement.views` will not return 0 or None, but instead throw a NameError.

When loading models from DynamoDB during a query or scan, models are loaded using the method specified in `__meta__["bloop.init"]`.  By default, this is the constructor for the model.  In other words, `Post.__meta__["bloop.init"] is Post`.  Any model can override this setting with another function that takes `**kwargs` and returns a model instance.

## Local and Global Secondary Indexes

## Model Inheritance

## Custom Types

## Custom Object Loading

## Custom Columns

## What's NOT Included

# Operations

## Load

## Save

## Delete

## Query and Scan

### Prefetch

### Chaining


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

* Tests
* Docs
* `__meta__` -> `class Meta` migration in declare
* Fix model inheritance

[dynamo-limits]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
[conditional-writes]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html
[arrow-docs]: http://crsmithdev.com/arrow/
[iso-8601]: https://tools.ietf.org/html/rfc3339
[boto3-session]: http://boto3.readthedocs.org/en/latest/reference/core/session.html
