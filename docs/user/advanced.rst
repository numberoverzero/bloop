Advanced Features
=================

.. _loading:

Custom Loading
--------------

The ``bloop_init`` attribute of a model's ``Meta`` specifies a function that
can be passed \*\*kwargs and returns instances of the model::

    def default_init(**kwargs):
        instance = MyModel()
        for column in instance.Meta.columns:
            value = kwargs.get(column.model_name, None)
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

The example above simply re-implements the default init function in the meta
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

Because bloop doesn't require the ``__init__`` function to load models, it's
possible to add the modeling functionality entirely as a mixin to an existing
class.  Assuming the following classes exist::

    import uuid

    class Vector3:
        def __init__(self, x=0, y=0, z=0):
            self.x = x
            self.y = y
            self.z = z
        def __iter__(self):
            return iter([self.x, self.y, self.z])


    class Shape:
        def __init__(self, id=None, center=None):
            if id is None:
                id = uuid.uuid4()
            if center is None:
                center = Vector3()
            self.center = center


    class Sphere(Shape):
        def __init__(self, id=None, center=None, radius=0):
            super().__init__(id=id, center=center)
            self.radius = radius


    class Rectangle(Shape):
        def __init__(self, id=None, center=None, dimensions=None):
            super().__init__(id=id, center=center)
            if dimensions is None:
                dimensions = Vector3()
            self.dimensions = dimensions

You can add DynamoDB serialization with the following::

    import bloop
    import uuid
    from bloop import Engine, Column, Float, UUID
    engine = bloop.Engine()


    class Vector3Type(bloop.List):
        def dynamo_load(self, value):
            value = super().dynamo_load(value)
            if len(value) != 3:
                raise ValueError("Invalid data stored in DynamoDB!")
            return value
        def dynamo_dump(self, value):
            if len(value) != 3:
                raise ValueError("Invalid data stored in DynamoDB!")
            return super().dynamo_dump(value)


    class Sphere(Shape, engine.model):
        def __init__(self, id=None, center=None, radius=None):
            super().__init__(id=id, center=center)
            if radius is None:
                radius = 0
            self.radius = radius

        id = Column(UUID, hash_key=True)
        center = Column(Vector3Type)
        radius = Column(Float)


    class Rectangle(Shape, engine.model):
        def __init__(self, id=None, center=None, dimensions=None):
            super().__init__(id=id, center=center)
            if dimensions is None:
                dimensions = Vector3(0, 0, 0)
            self.dimensions = dimensions

        id = Column(UUID, hash_key=True)
        center = Column(Vector3Type)
        dimensions = Column(Vector3Type)


    def setup_init(model):
        def init(**kwargs):
            instance = model()
            for column in model.Meta.columns:
                key = column.model_name
                value = kwargs.get(key, None)
                setattr(instance, key, value)
            return instance
        model.Meta.bloop_init = init

    setup_init(Sphere)
    setup_init(Rectangle)

    engine.bind()

It's important that the ``engine.model`` base is last in this list, so that the
``super()`` still points to the original parent class.

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

.. _tracking:

Manual tracking
---------------

see also: Engine-> Config-> save
