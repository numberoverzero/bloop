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
    ExtractKey(KeyShape, Blob) => Key
    IndexFor(Key, Blob) => Index
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
            Key = ExtractKey(KeyShape, Blob)
            Index = IndexFor(Key, Blob)

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

        TableIndex[TableName] = KeyShape
        Request[TableName]["Keys"].append(Key)
        ObjectIndex[TableName][Index].add(Object)

This becomes:

.. code-block:: python

    for obj in objs:
        table_name = obj.Meta.table_name
        key = dump_key(self, obj)
        key_shape = list(sorted(key.keys()))
        index = index_for(key)

        table_index[table_name] = key_shape
        request[table_name]["Keys"].append(key)
        object_index[table_name][index].add(obj)

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
        # In-lined key_shape
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

Because we use a set for the object_index's inner dicts, we can still do an unconditional add for the object index.

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
            Key = ExtractKey(KeyShape, Blob)
            Index = IndexFor(Key, Blob)

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

The tracking system is used for any metadata on models and their instances that shouldn't be exposed to users, and
so can't be stored on the instance/class/class.Meta.  It uses :py:class:`weakref.WeakKeyDictionary`\s so
that any tracking is cleaned up when the objects are.  A normal dict would hold a reference to the object forever,
leaking memory for every instance of a model.

Models
------

Right now, the only tracking on models is whether the class has been verified against its backing table.

.. code-block:: python

    tracking.is_model_verified(model) -> bool
    tracking.verify_model(model) -> None

Usage is as you'd expect.  You should only call ``verify_model`` on a model that has been verified.  Currently, there
is no way to mark a model as unverified.

Here's a sample that shortcuts the create/describe if the model's verified, and verifies the model on success:

.. code-block:: python

    def prepare(model):
        # No work to do, model already verified
        if tracking.is_model_verified(model):
            return

        # Assume either call will raise on failure
        create_table(model)
        validate_table(model)

        tracking.verify_model(model)

This is the same pattern that ``Engine.bind`` uses when iterating the base's subclasses, so that
CreateTable/DescribeTable aren't called more than once for each model.

Note that the verified flag is tied to the class, not the backing table.  If two models are backed by the same table,
both will have to verify that the table matches their expectation.

Object Marking
--------------

To build the set of columns to update during a save, bloop records when the ``__set__`` or ``_del__`` descriptors are
called on a specific model.  Like model verification, marking is strictly additive; once a column is set or deleted
on an instance, that column will be included in the UpdateItem call (possible as a DEL instead of SET).

.. code-block:: python

    tracking.mark(obj, column) -> None

All of bloop's column tracking is done by two calls to ``tracking.mark``:

#. Once during ``Column.set(self, obj, value)``
#. Once during ``Column.del(self, obj)``

These are syntactic sugar over the descriptor protocol, provided by ``declare.Field``, and map to ``__set__`` and
``__del__`` respectively.

To load an attribute dict in dynamo's wire format into an instance of a class:

.. code-block:: python

    ``Engine._update(obj: Any, attrs: Dict, expected: Iterable)``

Where ``expected`` is an iterable of ``Column`` instances, usually some subset of ``obj.Meta.columns``.  This iterable
acts as a whitelist for keys to extract from ``attrs``, and indicates which columns *should* have been present, so
that you don't set None on a column that wasn't loaded (for instance, a non-projected column on a GSI query).

=======================
Intention and Ambiguity
=======================

There are a few ways to handle tracking columns; most users will come in with a slightly different expectation of how
changes are preserved and communicated.  Instead of trying to accommodate all expectations through config options,
bloop tries to optimize for two things:

#. Minimize damage when the system doesn't match the user's expectation
#. Minimize deviation from user's expectations by trying to conform to the maximum overlap
   between different expectations

Without going into the particular behaviors that different systems encourage, and how bloop tries to conform to them,
here are bloop's intentions for how changes should be relayed to DynamoDB.

`In the following, "local object" refers to an object that didn't come from a query or scan and has never been loaded
from or saved to DynamoDB.  Local objects may not even be valid (for instance, the hash key isn't set).  The opposite,
"remote object" is any object that came from a query or scan, or has been loaded from or saved to DynamoDB.  The local
state can still be invalid, say by deleting the hash key after loading it.  Regardless, some portion of its data may
have come from DynamoDB, or been saved to DynamoDB.`

If the user never sets or deletes a column on a local object, then that column is **not** included in the UpdateItem
during a save.  This is one of the blurriest cases, since we can't tell `"I don't care what that column is"` from `"I
didn't explicitly delete this since it isn't set, and I want it that way"`. The first rule
basically exists because of this case.  It's much worse to delete the column in DynamoDB when the user expects the
column to have its previous value in DynamoDB, than to find the column still exists when the user expects the column
to be deleted.

If the user sets or deletes a column on a local object at least once, it **will be** included in the UpdateItem during
a save.  This is true even if the column is set, and then deleted.  This tracks the user intent "I want this attribute
to be deleted when I save it" and not the delta between creation and save (none in this example).

If a remote object is loaded at least once through ``engine.load`` then all of its columns will be marked.  It's
again safer to expect that when a user saves an object back without changing a column, they intend for its state
in DynamoDB to reflect their local copy after a save.  Not pushing unchanged columns in the UpdateItem could result
in a mismatch, since another caller modified them since the load.

If a remote object comes from a query or scan, **only the projected attributes are marked**.  If an object is loaded
from a GSI that only projects keys, a value won't be loaded for a column that's not part of the projection.  If the
user were to immediately save the object back, it would be surprising if those columns were deleted, since there was
no user intent (through ``__del__`` or setting to ``None``) to clear the column.

Object Snapshots
----------------

Snapshots are atomic conditions on an object, and should be updated whenever an operation modifies the object in
DynamoDB (save or delete), or updates are made to the local object with data from DynamoDB (query, scan, load).  This
way, the atomic condition applied is against the last state that was loaded from DynamoDB (for new objects, this
condition is computed to expect every column to be empty).

.. code-block:: python

    tracking.sync(obj, engine) -> None
    tracking.clear(obj) -> None
    tracking.get_snapshot(obj) -> bloop.Condition

``tracking.sync`` works with ``tracking.mark`` so that the condition only builds the expectation on columns that have
been marked.  This means that, for a query that doesn't load all columns, the atomic condition will only include
conditions on the columns that should have been loaded by the query.

Because sync builds the condition on marked columns, and every column is marked on ``Column.set``, if you call sync
after the user modifies the object then those modifications will become part of the expected atomic condition.  For
this reason, sync should **only** be called immediately after a dynamo operation, and should not be called on an object
that may have been modified since the dynamo operation.

In ``engine.load``, it's safe to call ``tracking.sync`` on the object right after it's loaded, because load will
overwrite any user changes to columns with the last value in DynamoDB.  However, it would be incorrect to sync an
object that **wasn't** loaded, since it will rebuild the snapshot, and expect any changes the user has made since
the last call.

In ``engine.save``, we can call ``tracking.sync`` right after the update call completes, because the last values stored
are the atomic state the user will expect when making subsequent changes.  It would be incorrect to call sync just
before the save; if the call was made with ``atomic=True``, we would end up telling DynamoDB to only change the state
of the object if it currently matches the state that we want to save.

In ``FilterIterator.__iter__``, each attribute blob is unpacked into an instance with ``Engine._update``, and then
the entire object is synced.  This won't grab any columns not loaded, because the update call only marked columns that
the query expected to find.

Binding
=======

There are two stages where the modeling pieces are bound together: when a subclass of an instance of
``bloop.new_base()`` is created, and when an engine binds a model (and all its subclasses).

The two stages are not related, except that they share the name, and are a process of associating information between
components: models and columns in the first, and models and tables in the second.

Model Declaration
-----------------

Columns and Indexes are bound to a class at declaration; that is, when a subclass of some ``bloop.new_base()`` instance
is defined:

.. code-block:: python

    MyBase = bloop.new_base()


    class MyModel(MyBase):
        # After this line executes there will be an entry
        # "id": Column(Integer, hash_key=True) in the
        # attrs dict that is used to construct the
        # MyModel class.
        id = Column(Integer, hash_key=True)

        # Column isn't bound yet, so it doesn't have a model_name
        # or dynamo_name at this point
        pass
    # The binding happens here, when the class declaration
    # finishes, and the metaclass is called to create a new
    # subclass of (in this case) MyBase.

The class that ``new_base`` creates is a mix of ``model.BaseClass`` and the metaclass ``ModelMetaclass``.
``BaseClass`` provides the default model scaffolding: ``_load``, ``_dump``, ``__init__``, and ``__str__``,
``__repr__``, and ``__eq__`` all use ``Meta.columns`` to render and compare using the modeled columns of the class.

During class creation in ``ModelMetaclass``, columns are associated to the model by setting the column's model_name.
This is also where checks are performed to ensure there's exactly one column with ``hash_key=True``, and at most one
column with ``range_key=True``.  Next, any indexes are associated with the model through the index's ``_bind`` method:

.. code-block:: python

    _Index._bind(self, model) -> None

Until now, the index's ``hash_key`` and ``range_key`` attributes have been strings (or ``None``, depending on type).

The ``_bind`` call will replace these with the appropriate Column instances from the model's ``Meta.columns``,
searching by the ``model_name`` attribute.  This makes it possible to pass ``_Index`` or ``model.Meta`` to a method
that will access the hash and range key attributes without special-casing the type it gets.

Next the indexes ``projection_attributes`` are computed, based on the kwarg ``projection`` provided when the index was
created.  For ``projection="all"`` this will simply be ``Meta.columns``.  For ``projection="keys"`` this will be
the table hash and range keys, and the indexes hash and range keys (filtering out empty keys).

When a list of strings is provided, they indicate the columns (by model name) to include in ``projection_attributes``,
and the projection is set to ``"include"``.  In this case, the projection attributes is the set of columns by model
name, and the keys of the table and the index (these are always projected into the index).

Engine Binding
--------------

The second binding happens when ``Engine.bind(base=SomeBase)`` is called with a base class.  This walks the subclasses
of the provided class to discover all models deriving from it (see ``util.walk_subclasses``), and then create and
validate the tables in DynamoDB against the expected tables for the models.

Two subsets are calculated from the set of subclasses: ``concrete`` and ``unverified``.  Concrete is any model where
``model.Meta.abstract == False``, while unverified is any concrete model where ``not tracking.is_verified(model)``.

First, a CreateTable is issued for each unverified model.  These calls don't wait for the table to be created, so that
multiple tables can be created at the same time.

For each unverified model, a busy poll against DescribeTable will wait for the model's table to be in a ready state
before comparing the returned description against the expected description for the model (see bloop/tables.py).

If the descriptions match, the model is marked as verified so the model's table doesn't need to be checked again.
Each concrete model and the Type of each column is then registered in the Engine's backing declare.TypeEngine.

Finally the ``type_engine`` is bound, with the engine available in the ``context`` parameter for any types that want
to create ``_load``, ``_dump`` functions based on the engine that is using them.
