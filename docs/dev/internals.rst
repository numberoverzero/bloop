Internals
^^^^^^^^^

Loading
=======

Most python object mappers for DynamoDB limit you to loading batches of objects from a single model at a time.  bloop
avoids this by indexing the objects to load data into by table, and then by key values.

A naive algorithm will load ``n`` objects with ``m`` unique keys in ``O(n*m)``, or ``O(n^2)`` if all keys are unique.

bloops loads ``n`` objects with ``m`` distinct keys in ``O(m)`` by indexing the key shape for each table, and then
indexing objects by their key values for each table.  This requires flattening the key, which is kind of a pain, given
DynamoDB's wire format.


Wire Format
-----------

Objects are sent in a dict that looks like:

.. code-block:: python

    {
        "<T>": {
            "Keys": [
                {
                    "hash": {"type": "value"},
                    # range, if it has one
                },
                # more objects
                ...
            ]
        },
        # more tables
        ...
    }

And returned in a similar dict:

.. code-block:: python

    {
        "table_name": {
            "Items": [
                {
                    "attr": {"type": "value"},
                    # more attributes
                    ...
                },
                # more objects
                ...
            ]
        },
        # more tables
        ...
    }

Necessary Helpers
-----------------

Before we get started, there are a few helper functions that will keep the code cleaner; these mostly shuffle dicts
into different dicts, pull out values, or drop the dynamo types from a dict to create lists or tuples.

First, we'll use this in every algorithm, to pull specific fields out of the attribute blob to compare keys with:

.. code-block:: python

    def extract_key(key_shape, item):
        return {field: item[field] for field in key_shape}

Next, there's a simple method to drop the dynamo_type from a dict and get the single value:

.. code-block:: python

    def value_of(some_dict):
        """{'S': 'Space Invaders'}) -> 'Space Invaders'"""
        return next(iter(some_dict.values()))

Without this, the ``.values()`` of ``key`` is still an iterable of dicts, and un-hashable as a key.  This is the
easiest way to get a value out of a dict without creating an intermediate list or other structure.

Finally, to easily construct a hashable index from a key:

.. code-block:: python

    def index_for(key):
        return tuple(sorted(value_of(k) for k in key.values()))

For example, it will turn ``{"id": {"S": "foo"}, "range": {"S": "bar"}}`` into ``("bar", "foo")``.  Note that
``sorted`` is needed for stability - there is no guarantee that ``dict.values()`` will return the same order when the
index is constructed for the request, and when it is reconstructed to unpack.

Naive Single Table
------------------

To get to the current implementation, let's walk through the progression of algorithms.  First, the naive O(N^2)
method for 1:1 modeling and single-model batch loading.  We can make a convenient assumption since there's only one
table name; the 1:1 modeling doesn't really help us, since bloop loads into existing instances of objects instead
of loading blobs through the model class.

.. code-block:: python

    # build request
    table_name = objects[0].Meta.table_name
    request = {table_name: {"Keys": []}}
    keys = request[table_name]["Keys"]
    for obj in objects:
        key = dump_key(obj)
        # O(N) unless we flatten key
        if key not in keys:
            keys.append(key)

    # paginate, retries, UnprocessedKeys
    response = send(request)

    # unpack response
    items = response["Responses"][table_name]

    # single table makes it easy to build a list of
    # attributes that are part of the key
    key_shape = list(dump_key(objects[0].keys()))

    # O(N^2)
    for item in items:
        item_key = extract_key(key_shape, item)
        for obj in objects:
            obj_key = dump_key(obj)
            if obj_key == item_key:
                unpack_into(obj, item)

Note that this doesn't track which objects fail to load; we'll need to move to an indexed solution for that.

Indexed Single Table
--------------------

Notice that in the Naive Single Table above, we dump the key N+1 times - once for the request, and once for each
item in the response.  If we store the flattened key as an index, we can drop the inner loop , convert ``item_key``
into an index, and jump right to the object in ``object_index``.

.. code-block:: python

    # build request
    table_name = objects[0].Meta.table_name
    object_index = {}
    request = {table_name: {"Keys": []}}
    keys = request[table_name]["Keys"]
    for obj in objects:
        key = dump_key(obj)
        index = index_for(key)
        # O(1) because we have a flattened key!  Woo!
        if index not in object_index:
            keys.append(key)
            object_index[index] = obj

    # paginate, retries, UnprocessedKeys
    response = send(request)

    # unpack response
    items = response["Responses"][table_name]

    # single table makes it easy to build a list of
    # attributes that are part of the key
    key_shape = list(dump_key(objects[0].keys()))

    # O(N)
    for item in items:
        item_key = extract_key(key_shape, item)
        index = index_for(item_key)
        obj = object_index.pop(index)
        unpack_into(obj, item)

    # Because we pop from the index, any values left weren't found
    if object_index:
        raise NotFound(list(object_index.values()))

Still pretty good, but this will silently break on two objects that have the same key:

.. code-block:: python

    first = Model(id="foo")
    second = Model(id="foo")
    engine.load([first, second])

The load will build an index for ``second`` of ``("foo",)`` but that already exists, so it's not added to the request,
and not inserted into the index.  The call will succeed, because the index only has one item to load, and there's only
one item in the response.

M:N Single Table
----------------

This is only a minor jump; instead of ``{index: obj}`` we'll track ``{index: [obj]``.
Our missing objects logic is still the same - we're just unpacking each blob into one or more objects, and can still
pop the index when we're done with it.

.. code-block:: python

    # build request
    table_name = objects[0].Meta.table_name
    object_index = {}
    request = {table_name: {"Keys": []}}
    keys = request[table_name]["Keys"]
    for obj in objects:
        key = dump_key(obj)
        index = index_for(key)
        # O(1) because we have a flattened key!  Woo!
        if index not in object_index:
            object_index[index] = set()
            # Only append the key the
            # first time we see it
            keys.append(key)
        object_index[index].add(obj)

    # paginate, retries, UnprocessedKeys
    response = send(request)

    # unpack response
    items = response["Responses"][table_name]

    # single table makes it easy to build a list of
    # attributes that are part of the key
    key_shape = list(dump_key(objects[0].keys()))

    # O(N)
    for item in items:
        item_key = extract_key(key_shape, item)
        index = index_for(item_key)
        # No new logic here, just iterating a list
        # of objects instead of unpacking one
        for obj in object_index.pop(index):
            unpack_into(obj, item)

    # Because we pop from the index, any values left weren't found
    if object_index:
        raise NotFound(list(object_index.values()))

M:N Multiple Tables
-------------------

Pretty good, but we're still relying on having one table name.  Generalizing this one will require removing the single
table assumption, which is currently part of:

* How we build the request - This isn't too bad, just insert a new dict to hold ``{"Keys": []}`` when the table name
  isn't in the request.
* How we build the index - This is complex, since associating table -> index -> object will require us to keep the key
  shape for each table, and then the index for each set of objects for each table.
* How we find the key shape from a blob - This is easy once we have the index building above.

========
Notation
========

You should be familiar with the meaning of key shape, key values, index from above.  To describe the new indexed,
I'm going to introduce a few symbols::

    <T>         table name
    <O>         single object to load
    <Item>      attribute blob from DynamoDB
    <K>         key value in the wire format
    <KS>        flattened key shape for table <T>
    <I>         tuple of values to map <O> <==> <T,KS,KV> <==> <Item>

With those, our indexes are::

    TableIndex  {<T>: <KS>}
    ObjectIndex {<T>: {<I>: [<O>]}}
    Request     {<T>: {"Keys": [<K>]}}

================
Applied Notation
================

Let's go through what the values above will be during a load, for the following objects:

.. code-block:: python

    # Don't do this
    from bloop import *


    class Model(new_base()):
        class Meta:
            table_name = "My-Table-Name"
        id = Column(String, hash_key=True)
        sort = Column(String, range_key=True)
        data = Column(Integer)
    engine = Engine()
    engine.bind(base=Model)

    obj = Model(id="foo", sort="bar")
    engine.load([obj])

While we build the request::

    <T>         "My-Table-Name"
    <O>         obj
    <K>         {"id": {"S": "foo"}, "sort": {"S": "bar"}}
    <KS>        ["id", "sort"]
    <I>         ("foo", "bar")
    TableIndex  {"My-Table-Name": ["id", "sort"]}
    ObjectIndex {"My-Table-Name": {("foo", "bar"): set(obj)}}
    Request     {"My-Table-Name": {"Keys": set(<K>)}}

While we unpack the response::

    <T>         "My-Table-Name"
    <O>         obj
    <Item>      {"data": ..., "sort": {"S": "bar"}, "id": {"S": "foo"}}
    <K>         {"id": {"S": "foo"}, "sort": {"S": "bar"}}
    <KS>        ["id", "sort"]
    <I>         ("foo", "bar")
    TableIndex  {"My-Table-Name": ["id", "sort"]}
    ObjectIndex {"My-Table-Name": {("foo", "bar"): set(obj)}}

=======================
Creating the TableIndex
=======================

Up until now, we've just been using a simplified ``ObjectIndex`` that doesn't have the outer dict of table names.
This let us construct the key shape once before we parsed the results, like this:

.. code-block:: python

    key_shape = list(dump_key(objects[0].keys()))

Now, there are many key shapes.  We can get the table name from the response, which we use to look up the key shape:

.. code-block:: python

    response = call(request)

    for table_name, item in response["Responses"].items():
        # get key_shape from table_name

To build the TableIndex, we'll insert the key shape if the table hasn't been seen yet.  At the same time, we'll need a
new dict in the wire request, since there's no longer a single ``{"Keys": []}`` dict in the request:

.. code-block:: python

    object_index = {}
    table_index = {}

    # build request
    request = {}
    for obj in objects:
        table_name = obj.Meta.table_name
        key = dump_key(obj)
        index = index_for(key)

        # new table!  save the key_shape, and create
        # a new Keys dict in the request
        if table_name not in table_index:
            key_shape = list(sorted(key.keys()))
            table_index[table_name] = key_shape
            request[table_name] = {"Keys": []}

Next, we can't check ``if index not in object_index`` because we have multiple tables; object_index gained a level,
so we'll add this line to the check above to prime the object_index when we see a new table:

.. code-block:: python

    for obj in objects:
        ...

        if table_name not in table_index:
            ...
            # New line - prep an empty dict for <I> -> set(<O>)
            object_index[table_name] = {}

Now we can do nearly the same index check - create a set of indexed objects if this is the first one, and then always
add to the existing set.  Like above, we'll only append the key to the request one time:

.. code-block:: python

    for obj in objects:
        ...

        if table_name not in table_index:
            ...

        if index not in object_index[table_name]:
            # Insert key once
            request[table_name]["Keys"].append(key)
            # New list of indexed objects
            object_index[table_name][index] = set()
        object_index[table_name][index].add(obj)

===================
Tracking not loaded
===================

Before, we popped keys from object_index and checked if it was empty at the end, but we can't do that anymore.  We're
only popping the inner keys, and the object_index won't be empty unless there are no table keys.  That is, the
following is not empty, even though we loaded all the objects:

.. code-block:: python

    # bool(object_index) is True, no longer equivalent to empty
    object_index = {
        "SomeTable": {},
        "AnotherTable": {},
        "AlsoEmpty": {}
    }

There are a few ways to solve this, such as keeping a set of all objects, and removing each when its index is loaded.
Another way would be to pop the table dict from the object index when it's empty, and then flatten any remaining sets.
Bloop uses the latter, to save on space:

.. code-block:: python

    not_loaded = set()
    for index in object_index.values():
        for index_set in index.values():
            not_loaded.update(index_set)
    if not_loaded:
        raise bloop.exceptions.NotModified("load", not_loaded)

======================
Unpacking the response
======================

Now that we have the TableIndex, we just need to change our iterator to grab the key as well, then look up the
key_shape in the TableIndex instead of pre-computing it outside the loop.

To start, we'll drop the hardcoded table_name.  Our unpacking is now:

.. code-block:: python

    # unpack response
    tables = response["Responses"]

    for table_name, items in tables.items():
        key_shape = table_index[table_name]

The rest of the code is about the same, with an extra level in the object_index for table_name:

.. code-block:: python

    for table_name, items in tables.items():
        key_shape = table_index[table_name]

        # Still building the index the same way
        item_key = extract_key(key_shape, item)
        index = index_for(item_key)

        # look up the index on the table's object index
        for obj in object_index[table_name].pop(index):
            unpack_into(obj, item)

For the simple cleanup logic, we'd have an ``objects.remove(obj)`` after the unpack, so the object is considered
loaded.  For the more complex cleanup, we'd try to clean up the table from the object_index if it's empty:

.. code-block:: python

    for ...:

        for obj in object_index[table_name].pop(index):
            ...
        # If this pops all the tables, object_index will be empty
        if not object_index[table_name]:
            object_index.pop(table_name)

    # If any table indexes are left in the object_index,
    # then we failed to load the objects under that index
    if object_index:
        # Flatten the object_index into a single set.
        ...

============
All together
============

Here's the final M:N multi-table loader, with a space-efficient ``not_loaded`` check:

.. code-block:: python

    object_index = {}
    table_index = {}

    # build request
    request = {}
    for obj in set(objects):
        table_name = obj.Meta.table_name
        key = dump_key(obj)
        index = index_for(key)

        # new table
        if table_name not in table_index:
            key_shape = list(key.keys())
            table_index[table_name] = key_shape
            request[table_name] = {"Keys": []}

        if index not in object_index[table_name]:
            # insert key once
            request[table_name]["Keys"].append(key)
            # new list of indexed objects
            object_index[table_name][index] = set()
        object_index[table_name][index].add(obj)

    # unpack response
    tables = response["Responses"]

    for table_name, items in tables.items():
        key_shape = table_index[table_name]

        # Still building the index the same way
        item_key = extract_key(key_shape, item)
        index = index_for(item_key)

        # look up the index on the table's object index
        for obj in object_index[table_name].pop(index):
            unpack_into(obj, item)
        # If this pops all the tables, object_index will be empty
        if not object_index[table_name]:
            object_index.pop(table_name)

    if object_index:
        # Flatten the object_index into a single set and raise
        not_loaded = set()
        for index in object_index.values():
            for index_set in index.values():
                not_loaded.update(index_set)
        raise bloop.exceptions.NotModified("load", not_loaded)

Tracking
========

Synchronized
------------

TODO

Snapshots
---------

TODO

Marking
-------

TODO

Binding
=======

Model Declaration
-----------------

TODO

Engine Binding
--------------

TODO

