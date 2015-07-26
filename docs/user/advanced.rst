Advanced Features
=================

.. _loading:

Custom Loading
--------------

The ``bloop_init`` attribute of a model's ``Meta`` specifies a function that
can be passed \*\*kwargs and returns instances of the model::

    def default_init(**kwargs):
        missing = object()
        instance = MyModel()
        for column in instance.Meta.columns:
            value = kwargs.get(column.model_name, _missing)
            if value is not missing:
                setattr(instance, column.model_name, value)
        return instance


    class MyModel(engine.model):
        id = Column(Integer, hash_key=True)
        content = Column(Binary)

        def __init__(self):
            print("Init does no work")
    engine.bind()

    # bloop will enter through this function
    MyModel.Meta.bloop_init = default_init

This example just re-implements the default init function in the meta
bloop_init.  The bloop_init function can even be swapped out for a particular
set of calls::

    import contextlib

    @contextlib.contextmanager
    def custom_init(model, init):
        old_init = model.Meta.bloop_init
        model.Meta.bloop_init = init
        yield
        model.Meta.bloop_init = old_init


    def load_nothing(**kwargs):
        pass

    with custom_init(MyModel, load_nothing):
        instance = engine.query(MyModel).key(MyModel.id == 0).first()

    # Nothing is loaded
    print(instance)

TODO: Talk about flexibility to make modeling a mixin since \_\_init\_\_ isn't
in the critical call path for any functionality.

.. seealso::
    * :ref:`model` for more details info on the base model class.
    * :ref:`define` for more info on defining models.

Custom Types
------------

subclass bloop.types.Type
warning with List/Map

Custom Columns
--------------

subclass bloop.column.Column

validation

Manual tracking
---------------

see also: Engine-> Config-> save
