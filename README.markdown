# bloop 0.5.2

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
    id = Column(UUID, hash_key=True)
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
    return user
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
def recent_posts_local_time(timezone, days_old):
    ''' ex: timezone='Europe/Paris', days_old=1 '''
    now_local = arrow.now().to(timezone)
    yesterday_local = now_local.replace(days=-days_old)

    since_yesterday = Post.date.between(yesterday_local, now_local)
    return engine.scan(Post).filter(since_yesterday)
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

When loading models from DynamoDB during a query or scan, models are loaded using the method specified in `Meta.bloop_init`.  By default, this is the constructor for the model.  In other words, `Post.Meta.bloop_init is Post`.  Any model can override this setting with another function that takes `**kwargs` and returns a model instance.  For more on defining a custom `Meta.bloop_init`, see Custom Object Loading.

## Local and Global Secondary Indexes

Global and local secondary indexes are defined similarly to Columns.  It's a good idea to read the documentation for both [GlobalSecondaryIndexes][docs-gsi] and [LocalSecondaryIndexes][docs-lsi] before using either.  A quick summary of how to construct them, and the constraints each has:

```python
class IndexPost(engine.model):
    id = Column(UUID, hash_key=True)
    user = Column(UUID, range_key=True)
    date = Column(DateTime)
    views = Column(Integer)

    by_user = GlobalSecondaryIndex(hash_key='user',
                                   projection='keys_only',
                                   write_units=1, read_units=10)

    by_date = LocalSecondaryIndex(range_key='date',
                                  projection=['views'])
```

Global Secondary Indexes provide additional primary keys for querying against, and can be added to any table.  They have their own throughput, indepedent from the table.  These are specified with `write_units` and `read_units`.  A GSI can have a range_key, but it is not required.

Local Secondary Indexes provide additional range keys for the same primary key - in this example, we can query against the hash/range pair (id, user) or with the index `by_date` we can query against the hash/range pair (id, date).  LSIs consume the table's read/write units, and do not have their own settings.

To use a Local Secondary Index:

* The table MUST have a range key (in our example, `user`)
* The LSI hash key will always be the table's hash key (`id`) and cannot be set.

In both cases, the `projection` option allows specifying which attributes of the table are available for retrieval from the given index.  The valid options are `'keys_only'`, `'all'`, or a list of model column names.  If model names are provided, they (along with any hash/range keys of the index/table) will be available when querying/scanning against the index.  Attributes not projected into the index will be retrieved for a LSI, but not a GSI.  I highly recommend reading the [LSI Throughput Considerations][lsi-throughput], which explains what happens when a query includes attributes not projected into the index.

## Model Inheritance

Inheritance is not supported for models.

## Custom Types

Building your own custom types should be easy.  In fact, most of the built-in types all branch off of either `Float` or `String`.  Here's all the code to implement DateTime:

```python
class DateTime(String):
    python_type = arrow.Arrow
    default_timezone = 'UTC'

    def __init__(self, timezone=None):
        self.timezone = timezone or DateTime.default_timezone

    def dynamo_load(self, value):
        if value is None:
            return None
        iso8601_string = super().dynamo_load(value)
        return arrow.get(iso8601_string).to(self.timezone)

    def dynamo_dump(self, value):
        iso8601_string = value.to('utc').isoformat()
        return super().dynamo_dump(iso8601_string)
```

And the Binary type:

```python
class Binary(Type):
    python_type = bytes
    backing_type = BINARY

    def dynamo_load(self, value):
        return base64.b64decode(value)

    def dynamo_dump(self, value):
        return base64.b64encode(value).decode('utf-8')
```

* `python_type` is mostly informational; it is used by the default `Type.can_dump` function, which is used for dumping values in `Map` and `List` columns.
* `backing_type` should be one of the valid DynamoDB types.  You don't need to worry about this if you subclass a concrete type, like `Float` or `Binary`.
* Types can be used directly when defining Columns, or instances of Types may be passed.  So far we've mostly been using raw Types, such as `Integer` and `String`.  Above however, you'll notice that `DateTime` can take a `timezone` on init.  The type still functions without initialization.

A good way to implement a custom type is to first find the closest existing type to the storage format you'd like.  Then, use `super()` to let the existing Type machinery do the heavy lifting:

```python
class MyCustomType(SomeExistingBloopType):
    def dynamo_load(self, value):
        existing_typed_value = super().load(value)
        # Manipulate the existing typed value here
        return existing_typed_value

    def dynamo_dump(self, value):
        # Manipulate the value into the type that already exists
        existing_typed_value = some_manipulation_here()
        return super().dynamo_dump(existing_typed_value)
```

When writing an (optional) constructor, keep in mind how you'll define columns.  If a constructor is required, your definitions will be:

```python

class Model(engine.model):
    foo = Column(MyType(foo='yet', bar='another'))
    bar = Column(MyType(foo='type', bar='setting'))
```

You may also define `can_dump(self, value)` and `can_load(self, value)` functions for your types, which will be called when trying to dump and load values in Maps or Lists.  Here are the default implementations:

```python
class Type(declare.TypeDefinition):
    # ...

    def can_load(self, value):
        '''
        whether this type can load the given
        {type: value} dictionary from dynamo
        '''
        backing_type = next(iter(value.keys()))
        return backing_type == self.backing_type

    def can_dump(self, value):
        ''' whether this type can dump the given value to dynamo '''
        return isinstance(value, self.python_type)

    # ...
```

Note that the argument to can_load will be in the form `{'S': 'value'}` which includes both the dynamo type (as the dict key) and the raw value (as the dict value).  This is required since some types will care about either/both the dynamo type and the actual value stored.

This contrasts with `can_dump` which will only receive the raw value - the dynamo type will be pulled from the type that dumps the value.

## Custom Object Loading

bloop will usually use a model's `__init__` method when instantiating new model objects from a query or scan result.  There are times when it's preferable to use a different initialization method; a base model may override the default `__init__` behavior and make it impossible for bloop to use, or a custom caching mechanism may want to intercept object creation.  In either case, the model's `Meta` class provides access to internal configuration.

Note that the method specified in meta is *not* used during load operations, as the objects already exist and are simply updated using `setattr`.

```python

class CustomUser(engine.model):

    def __init__(self):
        # Disallows **kwarg loading
        pass

    id = Column(UUID, hash_key=True)
    admin = Column(Boolean)
    joined = Column(DateTime)
    email = Column(String)
engine.bind()


def load_user(**kwargs):
    print("Using custom loader")
    user = CustomUser()
    for key, value in kwargs.items():
        setattr(user, key, value)
    return user
CustomUser.Meta.bloop_init = load_user
```

And to try things out:

```python
# Make a user to find
user = CustomUser()
user.id = uid = uuid.uuid4()
engine.save(user)

# This will find the result above, and load the result through `load_user`
print(engine.query(CustomUser).key(CustomUser.id == uid).first())
```

## Custom Columns

Columns have three important properties:

1. Column subclasses a `ComparisonMixin` which enables the use of rich comparators to generate ConditionExpressions.
2. Column also subclasses `declare.Field`to implement the [descriptor protocol][descriptors].
3. Column exposes a pair of functions for storing per-instance metadata without (probabilistically) colliding with an existing attribute for that object.

Global and Local Secondary Indexes are subclasses of Column, which take advantage of almost none of the above properties.  Before creating your own Column subclass, it is **highly** recommended that you fully review the descriptor protocol, as well as the following notes on the limitations imposed by the above subclassing.  Since bloop's type system is primarily responsible for loading/packing values from/to DynamoDB, it's usually correct to implement custom logic in your own Type.  A custom Column class is most appropriate for behavior that cuts across types.  In other words, `MyColumn(Integer)` and `MyColumn(String)` should probably both be fine.

### Rich comparisons

The Column class implements the various [rich comparison][python-datamodel] methods, making ConditionExpression construction natural: `Model.column <= 5` .  The methods are:

```python
ComparisonMixin.__lt__(self, other)  # <
ComparisonMixin.__le__(self, other)  # <=
ComparisonMixin.__eq__(self, other)  # ==
ComparisonMixin.__ne__(self, other)  # !=
ComparisonMixin.__gt__(self, other)  # >
ComparisonMixin.__ge__(self, other)  # >=
```

In addition, the following are defined to match the remaining DynamoDB expression operators:

```python
ComparisonMixin.is_(self, value)             # Alias for ==
ComparisonMixin.is_not(self, value)          # Alias for !=
ComparisonMixin.between(self, lower, upper)  # lower <= self <= upper
ComparisonMixin.in_(self, *values)           # self in values
ComparisonMixin.begins_with(self, value)     #
ComparisonMixin.contains(self, value)        # value in self
```

Unfortunately the `in` keyword in python uses `__contains__` and then `bool()`s the result, making it impossible to return anything other than True or False.  Therefore, we must use `in_()` instead.

Overloading `__eq__` requires explicitly stating that `__hash__` use the default hash method (`object.__hash__`) or it would try to use our custom `__eq__` which will do very bad things.

### Descriptor protocol

Values are stored/loaded from the object's dictionary, according to the name of the column.  The column name is available as `model_name`, and is set when the class is defined.

The descriptor methods require handling some special casing, such as the object being None (reference to the class, not an instance).  Instead of requiring every overriding implementation to handle these cases correctly, Column exposes the following interface that maps to the traditional descriptor methods:

```python
class Column(...):
    ...

    def set(self, obj, value):
        ...

    def get(self, obj):
        ...

    def delete(self, obj):
        ...
```

This lets us focus only on cases where the object reference is an instance of the class.

Let's create a contrived NegativeColumn that negates values on set, and negates them again on get:

```python
class NegativeColumn(bloop.Column):
    def set(self, obj, value):
        super().set(obj, -value)

    def get(self, obj):
        return -(super().get(obj))
```

Now when we set a value and inspect the dict, we see it's stored as its negative, but returned as its original value:

```python
class Model(engine.model):
    id = NegativeColumn(bloop.Integer, hash_key=True)

instance = Model(id=5)
print(instance.__dict__)  # {'id': -5}
print(instance.id)        # 5
```

### Per-instance metadata

Meta values are also stored in the object's dictionary, under a randomly generated key.  Let's inspect an instance to see:

```python
class Model(engine.model):
    id = Column(String, hash_key=True)
    other = Column(Integer)

instance = Model(id="foo")
print(instance.__dict__)  # {'id': 'foo'}

# Set a meta value for the `id` column of our instance
Model.id.meta_set(instance, "meta_id", "meta_id_value")
print(instance.__dict__)  # {'id': 'foo',
                          #  '__column_meta_2e1bf988a7124784be30652d61726612': {
                          #    '__Column_ea53efb34e7b431083328d4773ca2c37': {
                          #        'meta_id': 'meta_id_value'
                          #    }
                          #  }
                          # }
```

Meta values are stored first under a dict key that is global for all column metadata - if we add a meta value for a different column there will still be one top-level entry in the object's dictionary, but a second column key (unique to the column setting the metadata) will be created.

```python
Model.other.meta_set(instance, "meta_other", "meta_other_value")
print(instance.__dict__)  # {'id': 'foo',
                          #  '__column_meta_2e1bf988a7124784be30652d61726612': {
                          #    '__Column_ea53efb34e7b431083328d4773ca2c37': {
                          #        'meta_id': 'meta_id_value'
                          #    },
                          #    '__Column_883c5980eeaf4f77b87abb3d600a7b44': {
                          #        'meta_other': 'meta_other_value'
                          #    },
                          #  }
                          # }
```

This allows us to keep the overhead for objects low - each object holds any associated metadata, and is released when the object cleans up.  Otherwise, the Column would have to track when instances were no longer needed and clean up their metadata.

# Operations

The main interface to models is through an `bloop.Engine`.  After binding an engine (`engine.bind()`) the following can be used to manipulate your objects.

## Load

```python
Engine.load(self, objs, *, consistent=False)
```

* `objs` may be a single object, or an array of objects.
* If `consistent` is true, consistent reads will be used when loading objects.

## Save

```python
Engine.save(self, objs, *, condition=None)
```

* `objs` may be a single object, or an array of objects.
* `condition` may be specified only when saving a single object.  This should be a ConditionExpression, most easily constructed using rich comparators with model columns: `no_such_id = Post.id == None` or `new_posts = Post.date >= arrow.now().replace(days=-1)`

## Delete

```python
Engine.delete(self, objs, *, condition=None)
```

* `objs` may be a single object, or an array of objects.
* `condition` may be specified only when deleting a single object.  This should be a ConditionExpression, most easily constructed using rich comparators with model columns: `no_such_id = Post.id == None` or `new_posts = Post.date >= arrow.now().replace(days=-1)`

## Query and Scan

```python
Engine.query(self, model, index=None)
Engine.scan(self, model, index=None)
```

These methods return a Query or Scan object that can be refined by chaining method calls together.  The available methods are identical, although some will have no effect during a Scan (such as `.key`).

### Chaining

Each method call will return a new Query or Scan, so that queries can be built up from a base of default options.  Queries and Scans are also iterable, returnig the results of the query or scan.  `for result in engine.scan(Post).filter(Post.views > 100):`

Available methods:

* `key(condition)` - A ConditionExpression that includes a hash key (and optionally a range key).  This is **required** for a Query, and ignored for a Scan.
* `filter(condition)` - Any filters applied to results after the key condition, but before returning from the server.  These may cover any attributes including non-keys.  Subsequent calls to `filter` will AND the conditions together.
* `select(columns|'all'|'projected')` - Specify which attributes to load.  Useful when interested in sparse data in large tables.  Not all indexes work with all select options - this depends on the index type and index projection.  To specify columns, pass the column objects as a list: `select([Post.views, Post.date])`.  The key attributes for the index (or table if no index was given) are automatically included in results.
* `count()` - Immediately executes a COUNT query or scan, returning a dictionary with keys `count, scanned_count`.
* `all(prefetch=None)` - Compiles the query or scan into a request object and returns a Result object that can be iterated to yield results.  `prefetch` is discussed below, and controls the speed at which results are loaded from the server.
* `first()` - Compile the query or scan, load the first result using the fastest prefetch option, and return it.
* `ascending` and `descending` - Sort results according to the [DynamoDB ScanIndexForward setting][scan-index-forward].  `engine.query(Post).key(Post.id=='foo').ascending`
* `consistent` - Use [strongly consistent reads][consistent-read] when querying.  Invalid when querying a GSI, and ignored when scanning.

Every time a Query or Scan is iterated, a new set of calls is issued to DynamoDB.  To iterate over results from the same batch of calls, use `all()`.

```python
base_query = engine.query(Post, index=Post.by_user).consistent.ascending

def new_posts(user_id):
    yesterday = arrow.now.replace(days=-1)

    query = base_query.key(Post.id==user_id).filter(Post.date >= yesterday)
    query_result = query.all()
    posts = list(query_result)

    new = results.count
    scanned = results.scanned_count
    print("From {} posts, {} were less than a day old".format(posts, scanned))

    return posts
```

### Prefetch

prefetch controls how many pages are loaded at once when the existing page of results has been exhausted.  Prefetch must be an integer - less than 0 loads all pages immediately, 0 loads results as needed, and positive values load that many pages ahead.

For a prefetch of 1, and a page size of 25 results, let's imagine that the number of results for the first 3 pages are [0, 2, 16, 0].

1. There will be two calls to DynamoDB before the first result is yielded (load page 1, prefetch page 2).
2. There will be no calls before the second result is yielded.
3. There will be two calls to DynamoDB before the third result is yielded (load page 3, prefetch page 4).
4. There will be no calls until the 18th item has been yielded; at that point, page 5 will be loaded, and with no continuation token available to continue loading, StopIteration will be raised.

prefetch can be specified when calling `all()` or on the engine at any point `all` is called.  Engine.prefetch is a dictionary, with keys `scan` and `query`.

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

* Bug fixes:
  * Better handling for `engine.save` and `engine.load` on models that aren't bound
* Tests
* Documentation:
  * Refine README
  * Overhaul Docstrings
  * Set up Sphinx, ReadTheDocs
* Enhancements:
  * Allow specifying PutItem or UpdateItem for saves (engine config and per-call)
  * Wait on table during `engine.bind` if any part of the table status is Creating
  * Allow `strict` mode for Query/Scan where a LSI's projected_attributes are not
    a superset of the requested attributes

[dynamo-limits]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
[conditional-writes]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html
[arrow-docs]: http://crsmithdev.com/arrow/
[iso-8601]: https://tools.ietf.org/html/rfc3339
[boto3-session]: http://boto3.readthedocs.org/en/latest/reference/core/session.html
[docs-gsi]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
[docs-lsi]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html
[lsi-throughput]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html#LSI.ThroughputConsiderations
[descriptors]: https://docs.python.org/2/howto/descriptor.html
[python-datamodel]: https://docs.python.org/3.5/reference/datamodel.html#object.__lt__
[batch-write]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
[dynamodb-transactions]: https://github.com/awslabs/dynamodb-transactions
[ddb-trans-conditions]: https://github.com/awslabs/dynamodb-transactions/issues/10
[ddb-trans-bug]: https://github.com/awslabs/dynamodb-transactions/commit/c3470df17469517432133b1f33534795a4657366
[update-item-expression]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#DDB-UpdateItem-request-UpdateExpression
[scan-index-forward]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ScanIndexForward
[consistent-read]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ConsistentRead
