Working with Models
===================

Define Models
-------------

Every model must subclass a single engine's base model.  It isn't required to
call ``engine.bind`` immediately after the class is constructed - instances
can be created and modified locally without binding the model to DynamoDB.
This allows you to define models and then handle the binding in a try/except
to handle any failure modes (network failure, model mismatches).

When defining a model, we can specify an optional ``Meta`` instance within the
class, which lets us customize properties of the table, as well as holding
most of the cached data used internally::

    class MyModel(engine.model):
        class Meta:
            # Defaults to class name
            table_name = 'MyCustomTableName'
            write_units = 10  # Defaults to 1
            read_units = 5    # Defaults to 1
        id = Column(Integer, hash_key=True)
        content = Column(Binary)

    engine.bind()

The ``engine.model`` base class provides an \_\_init\_\_ method that takes
\*\*kwargs and sets those values on the object (if they match a column).  For
the model above, we could do the following::

    instance = MyModel(content=b'hello, world', id=0, unused='not set')
    assert instance.content == b'hello, world'
    assert instance.id == 0
    # AttributeError, not set by __init__
    print(instance.unused)

.. seealso::
    * :ref:`bind` for a detailed look at what happens when models are bound.
    * :ref:`loading` to customize the entry point for model creation.

Create Instances
----------------

Load
----

see also: Engine-> config-> consistent

Save
----

see also: Engine-> config-> save, atomic

Delete
------

see also: Engine-> config-> atomic

.. _conditions:

Conditions
----------

Query
-----

see also: Engine->config-> strict, prefetch

Scan
----

see also: Engine->config-> strict, prefetch
