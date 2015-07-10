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

We'll be using some simplistic user and post models for a hypothetical forum.

First, we'll set up our models and bind the engine:

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

Most of our columns pass a `name` parameter - this is because [attribute names count towards item size](dynamo-limits).  By default the model name is used (we'll still use `id`, for instance) but specifying the backing name allows us to use convenient names (`user`, `date`) while saving space for requests (`u`, `d`).

Let's create a new user - and make sure we don't overwrite an existing key!

```python
def create_user(admin=False):
    ''' Create a new user, throwing if the randomly generated id is in use '''
    user = User(id=uuid.uuid4(), admin=admin)
    does_not_exist = User.id.is_(None)
    engine.save(user, condition=does_not_exist)
```

bloop works hard to expose DynamoDB's [Conditional Expression](conditional-writes) system in a clean, intuitive interface.  To that end, we can construct conditions that must be met before performing an operation (`save`, `delete`) using the Model's columns and standard comparison operators.  In this case, `User.id.is_(None)` could also be written `User.id == None` which ensures there's no row where the id we want to save exists.

Next, let's take advantage of our GlobalSecondaryIndex `by_user` to find all posts by a user:

```python
def posts_by_user(user_id):
    ''' Returns an iterable of posts by the user '''
    return engine.query(Post, index=Post.by_user).key(Post.id == user_id)
```

Again we leverage standard comparison operators to define the key condition with `Post.id == user_id`.  There are a number of moving pieces that allow this function to stay so simple; queries are iterable, lazily loading results and following continuation tokens using appropriate retries with an exponential backoff (configurable).

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

bloop leverages the outstanding [`arrow`](arrow-docs) for `DateTime` objects, with values persisted as UTC [ISO 8601](iso-8601) strings.  In addition, comparisons can be made against any timezone, since all values are converted to UTC before they reach DynamoDB.  This makes locale-aware queries trivial to write:

```python
local_timezone = 'Europe/Paris'
now_local = arrow.now().to(local_timezone)
yesterday_local = now_local.replace(days=-1)

since_yesterday = Post.date.between(yesterday_local, now_local)
recent_posts = engine.scan(Post).filter(since_yesterday)
```

[dynamo-limits]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
[conditional-writes]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html
[arrow-docs]: http://crsmithdev.com/arrow/
[iso-8601]: https://tools.ietf.org/html/rfc3339


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
