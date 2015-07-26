Working with Models
===================

.. _define:

Define Models
-------------

Every model must subclass a single engine's base model.  It isn't required to
call ``engine.bind`` immediately after the class is constructed - instances
can be created and modified locally without binding the model to DynamoDB.
This allows you to define models and then handle the binding in a try/except
to handle any failure modes (network failure, model mismatches).

When defining a model, we can specify an optional ``Meta`` attribute within the
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

.. seealso::
    * :ref:`meta` for a full list of Meta's attributes.
    * :ref:`bind` for a detailed look at what happens when models are bound.

.. _create:

Create Instances
----------------

The ``engine.model`` base class provides an \_\_init\_\_ method that takes
\*\*kwargs and sets those values on the object (if they match a column).  For
the model above, we could do the following::

    instance = MyModel(content=b'hello, world', id=0, unused='not set')

    # AttributeError, not set by __init__
    print(instance.unused)

In this case ``unused`` is not set, since it is not a defined column for the
model.

It's not necessary to keep the default instantiation signature - for example,
you may want to only allow setting non-key attributes and let your init method
take care of generating a unique key for the instance.  If you change the init
signature, or want bloop to take a different path when instantiating instances
for any reason (to differentiate user creation from engine loading, for
example) you must set the model's ``Meta.bloop_init`` to a function that takes
``**kwargs`` and returns an instance of the model. You can find more details on
custom loading in the advanced section.

.. seealso::
    * :ref:`loading` to customize the entry point for model creation.

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

.. _meta:

Meta
----

.. warning::
    Modifying the generated values in a model's ``Meta`` will result in
    **bad things**, including things like not saving attributes, loading values
    incorrectly, and kicking your dog.

Discussed above, the ``Meta`` attribute of a model class stores info about the
table (read and write units, the table name) as well as metadata used by bloop
internally (like ``bloop_init``).

Meta exposes the following attributes:

* ``read_units`` and ``write_units`` - mentioned above, the table read/write
  units.  Both default to 1.
* ``table_name`` - mentioned above, the name of the table.  Defaults to the
  class name.
* ``bloop_init`` - covered in detail in :ref:`loading`, this is the entry point
  bloop uses when creating new instances of a model.  It is NOT used during
  ``bloop.load`` which updates attributes on existing instances.
* ``colums`` - a ``set`` of ``Column`` objects that are part of the model.
* ``indexes`` - a ``set`` of ``Index`` objects that are part of the model.
* ``hash_key`` - the ``Column`` that is the model's hash key.
* ``range_key`` - the ``Column`` that is the model's range key.  Is ``None`` if
  there is no range key for the table.
* ``bloop_engine`` - the engine that the model is associated with.  It may not
  be bound yet.
