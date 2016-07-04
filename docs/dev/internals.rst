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

Utility Functions
-----------------

Before we get started, there are a few helper functions that will keep the code cleaner; these mostly shuffle dicts
into different dicts, pull out values, or drop the dynamo types from a dict to create lists or tuples.

First, we'll use this to pull specific fields out of the attribute blob to compare keys with:

.. code-block:: python

    def extract_key(key_shape, item):
        """('a', 'b'), {'a': 'f', 'b': 'g', 'c': 'h'} ->
               {'a': 'f', 'b': 'g'}
           (Doesn't pull out the ``c`` field)
        """
        return {field: item[field] for field in key_shape}

Next, there's a simple method to drop the dynamo_type from a dict and get the single value.  We need this because
``.values()`` of a key is still an iterable of dicts, and un-hashable:

.. code-block:: python

    def value_of(some_dict):
        """{'S': 'Space Invaders'}) -> 'Space Invaders'"""
        return next(iter(some_dict.values()))


Finally, this will create a tuple of the inner values of a key, so that we can use it as the key in our dicts.  We
use ``sorted`` because ``dict.values()`` doesn't guarantee stability:

.. code-block:: python

    def index_for(key):
        """{'id': {'S': 'foo'}, 'range': {'S': 'bar'}} ->
               ('foo', 'bar')
        """
        return tuple(sorted(value_of(k) for k in key.values()))

We'll also need a function that gets the key in dynamo's format from an object.  It needs to check that the object has
all the key values; that is, if the object's model has a range key but it's missing on the object, we can't build a
valid key for the object.  It also needs to handle models with and without range keys.  Because this one is mostly
error checking and conditionals, it's just pseudocode.  Check out ``dump_key`` in engine.py for the full code.

::

    def dump_key(engine, obj):
        meta = obj.Meta
        hash_key, range_key = meta.hash_key, meta.range_key
        get hash_value from obj @ hash_key
        if hash_value is missing:
            raise

        key = {hash_key.dynamo_name: dump(hash_value)}

        if range_key:
            range_value = getattr(obj, range_key.model_name, MISSING)
            get range_value from obj @ range_key
            if range_value is missing:
                raise
            key[range_key.dynamo_name] = dump(range_value))
        return key

Pseudocode
----------

The algorithm is pretty straightforward::

    ObjectIndex = {TableName: {Index: set(Object)}}
    TableIndex = {TableName: KeyShape}
    Request = {TableName: {"Keys": [Key]}}

    NameFrom(Object => TableName
    DumpKey(Object) => Key
    ShapeOf(Key) => KeyShape
    IndexFrom(Key) => Index
    IndexFor(KeyShape, Blob) => Index
    UnpackBlob(Blob, Object) => None


    for Object in Input
        TableName = NameFrom(Object)
        Key = DumpKey(Object)
        KeyShape = ShapeOf(Key)
        Index = IndexFrom(Key)

        ObjectIndex[TableName][Index].add(Object)
        TableIndex[TableName] = KeyShape
        Request[TableName]["Keys"].append(Key)

    Response = Call(Request)

    for TableName, Blobs in Response.items()
        for Blob in Blobs
            KeyShape = TableIndex[TableName]
            Index = IndexFor(KeyShape, Blob)

            Objects = ObjectIndex[TableName][Index]
            for Object in Objects
                UnpackBlob(Blob, Object)

The bookkeeping details are omitted:

* Create new dicts in TableIndex
* Create new dicts in ObjectIndex[TableName]
* Create new sets in ObjectIndex[TableName][Index]
* Pop empty ObjectIndex[TableName] to track missing objects
* Flatten remaining objects in ObjectIndex[\*][\*][\*] to raise NotModified

Layout
------

First, the function signature is:

.. code-block:: python

    def load(self, objs, consistent=None):

``objs`` can either be a single object, or an iterable of objects.  We'll use the consistent value unless it's
``None``, in which case we'll fall back to the engine's config value.

This is the shell we'll inject the algorithm above into; it sets up our indexes and request, guards against trying to
load abstract models, and flattening the ObjectIndex if any objects aren't loaded.  The ``TODO``\s mark where we'll
build the request and unpack the response.

.. code-block:: python

    consistent = config(self, "consistent", consistent)
    objs = set_of(objs)
    for obj in objs:
        if obj.Meta.abstract:
            raise bloop.exceptions.AbstractModelException(obj)

    table_index, object_index, request = {}, {}, {}

    for obj in objs:
        # TODO
        ...

    response = self.client.batch_get_items(request)

    for table_name, blobs in response.items():
        for blob in blobs:
            # TODO
            ...

    # Flatten objects that weren't loaded and raise
    if object_index:
        not_loaded = set()
        for index in object_index.values():
            for index_set in index.values():
                not_loaded.update(index_set)
        raise bloop.exceptions.NotModified("load", not_loaded)

Building the Request
--------------------

The first portion of the pseudocode above translates very closely to the actual implementation, although we need to
take care of missing keys the first time we see a new table or key within a table.  For now, let's just translate the
pseudocode.  Here's the section we care about::

    for Object in Input
        TableName = NameFrom(Object)
        Key = DumpKey(Object)
        KeyShape = ShapeOf(Key)
        Index = IndexFrom(Key)

        ObjectIndex[TableName][Index].add(Object)
        TableIndex[TableName] = KeyShape
        Request[TableName]["Keys"].append(Key)

This becomes:

.. code-block:: python

    for obj in objs:
        table_name = obj.Meta.table_name
        key = dump_key(self, obj)
        key_shape = list(sorted(key.keys()))
        index = index_for(key)

        table_index[table_name] = key_shape
        object_index[table_name][index].add(obj)
        request[table_name]["Keys"].append(key)

Aside from creating the nested dicts where necessary, there are two things we need to fix.  First, there will only
ever be one ``key_shape`` for a given table; we don't want to recompute this for every object, especially since loading
multiple objects from the same table is a common pattern.  We'll move that into wherever we check for new tables.
Second, and more pressing, is that we unconditionally append to the request's keys.  For most cases this will be fine,
but consider the following two objects:

.. code-block:: python

    some_user = User(id=from_database)
    another_user = User(id=from_input)
    engine.load([some_user, another_user])

If the ids are the same, we'll insert the same key into the request table twice!  If this doesn't fail with DynamoDB
(which can occur if the objects get split into two different batches) it will cause us to double load the values.  For
the built-in types this is fine, but any custom type may not expect ``Type.load`` to be idempotent.  We must put the
append in the same check we use for new indexes within a table.

We don't have to worry about the same object appearing in the source list, twice, because we converted the input to
a set when it came in:

.. code-block:: python

    def load(self, objs, ...):
        objs = set_of(objs)
        ...

    # This is safe
    engine.load([some_obj, some_obj])

First, we'll handle new table names.  We can move the key shape in here as well, so that we don't do it per object, but
per unique model.

.. code-block:: python

    if table_name not in object_index:
        # Inlined key_shape
        table_index[table_name] = list(sorted(key.keys()))
        # We'll handle the inner {index: set} in
        # the new index block below
        object_index[table_name] = {}
        # Don't put the key in the new list yet;
        # Take care of it on new index below
        request[table_name] = {
            "Keys": [], "ConsistentRead": consistent}

While we could have set ``"Keys"`` to ``[key]`` it would prevent us from doing an append in the next check we'll add:

.. code-block:: python

    if index not in object_index[table_name]:
        request[table_name]["Keys"].append(key)
        object_index[table_name][index] = set()

If we haven't pushed this key into the request yet (and we'll come in here if it's a new table) then we set add it to
the request once, and create a new set for objects that have the same (table_name, index).

Because we use a set for the object_index's inner dicts, we can still do an unconditional add:

.. code-block:: python

    object_index[table_name][index].add(obj)

Putting it all together in the shell above, we now have:

.. code-block:: python

    consistent = config(self, "consistent", consistent)
    objs = set_of(objs)
    for obj in objs:
        if obj.Meta.abstract:
            raise bloop.exceptions.AbstractModelException(obj)

    table_index, object_index, request = {}, {}, {}

    for obj in objs:
        table_name = obj.Meta.table_name
        key = dump_key(self, obj)
        index = index_for(key)

        if table_name not in object_index:
            table_index[table_name] = list(sorted(key.keys()))
            object_index[table_name] = {}
            request[table_name] = {
                "Keys": [], "ConsistentRead": consistent}

        if index not in object_index[table_name]:
            request[table_name]["Keys"].append(key)
            object_index[table_name][index] = set()
        object_index[table_name][index].add(obj)

    response = self.client.batch_get_items(request)

    for table_name, blobs in response.items():
        for blob in blobs:
            # TODO
            ...

    # Flatten objects that weren't loaded and raise
    if object_index:
        not_loaded = set()
        for index in object_index.values():
            for index_set in index.values():
                not_loaded.update(index_set)
        raise bloop.exceptions.NotModified("load", not_loaded)

Unpacking the Response
----------------------

This translates more easily from our pseudocode, since we won't have to create any new nested structures, and can
simply iterate and fetch from the indexes.  Here's that section of pseudocode again::

    for TableName, Blobs in Response.items()
        for Blob in Blobs
            KeyShape = TableIndex[TableName]
            Index = IndexFor(KeyShape, Blob)

            Objects = ObjectIndex[TableName][Index]
            for Object in Objects
                UnpackBlob(Blob, Object)

This becomes:

.. code-block:: python

    for table_name, blobs in response.items():
        for blob in blobs:
            key_shape = table_index[table_name]
            key = extract_key(key_shape, blob)
            index = index_for(key)

            for obj in object_index[table_name].pop(index):
                self._update(obj, blob, obj.Meta.columns)
                bloop.tracking.sync(obj, self)
            # See note below
            if not object_index[table_name]:
                    object_index.pop(table_name)

The only thing we added was popping the table dict from the object index if it's empty, so that we can quickly tell
if there are missing objects.  With that, we have the full load function:

.. code-block:: python

    def load(self, objs, consistent=None):
        consistent = config(self, "consistent", consistent)
        objs = set_of(objs)
        for obj in objs:
            if obj.Meta.abstract:
                raise bloop.exceptions.AbstractModelException(obj)

        table_index, object_index, request = {}, {}, {}

        for obj in objs:
            table_name = obj.Meta.table_name
            key = dump_key(self, obj)
            index = index_for(key)

            if table_name not in object_index:
                table_index[table_name] = list(sorted(key.keys()))
                object_index[table_name] = {}
                request[table_name] = {
                    "Keys": [], "ConsistentRead": consistent}

            if index not in object_index[table_name]:
                request[table_name]["Keys"].append(key)
                object_index[table_name][index] = set()
            object_index[table_name][index].add(obj)

        response = self.client.batch_get_items(request)

        for table_name, blobs in response.items():
            for blob in blobs:
                key_shape = table_index[table_name]
                key = extract_key(key_shape, blob)
                index = index_for(key)

                for obj in object_index[table_name].pop(index):
                    self._update(obj, blob, obj.Meta.columns)
                    bloop.tracking.sync(obj, self)
                if not object_index[table_name]:
                    object_index.pop(table_name)

        if object_index:
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

