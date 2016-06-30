Internals
=========

Loading
-------

A bit of cleverness is required to load instances of different models, since
the blobs that are returned have no way to be associated with the objects whose
keys were used to fetch them.  A double index allows O(1) lookups. The second
hash key takes O(K) to build, where K is the number of keys for the object
(either 1 for a hash-only table, or 2 for a hash + range key).

Objects are sent in a dict that looks like:

.. code-block:: python

    {
        "table_name": {
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

To associate returned values with input objects without an O(N) scan per
object, we create indexes for each object (table name -> key values -> obj)
and for each table (table name -> key names).  Then the lookup is as follows:

1. Find key names for the table in table_keys by table name
2. Create object index by combining table name and returned value for each
   of the key names from #1


A table "objects" with a hash_key named "first" and range key named "last"
(assume that the dynamo and model names are the same):

.. code-block:: python

    instance = Foo(first="foo", last="bar")

This will be loaded as:

.. code-block:: python

    table_keys = {"objects": ("first", "last")}
    indexed_objects = {"objects": {("foo", "bar"): instance}}
    request = {
        "objects": {
            "Keys": [
                {"first": {"S": "foo"}, "last": {"S": "bar}},
            ]
        }
    }

And the response will contain:

.. code-block:: python

    response = {
        "objects": {
            "Items": [
                {
                    "some_attr": {"S": "data"},
                    "first": {"S": "foo"},
                    "last": {"S": "bar"}
                }
            ]
        }
    }

Processing this object will first find the table_key for "objects":

.. code-block:: python

    ("first", "last")

And then pull the corresponding values from the item in that order, to
construct the object index:

.. code-block:: python

    indexed_objects["objects"][("foo", "bar")]

Which finally, can be used to look up the object in indexed_objects.

Tracking
--------

TODO

Binding
-------

TODO
