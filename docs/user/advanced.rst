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

.. _advanced-types:

Custom Types
------------

A common task involves one of a finite set of options.  However, it's tedious
and error-prone to try to remember whether the color red is 0, 1, or 2.  This
can be simplified with a custom type that maps to a Color `enum`_.  First
you'll use a backing integer, and then a string::

    from enum import Enum
    class Color(Enum):
        red = 1
        green = 2
        blue = 3


    from bloop import Integer

    class ColorType(Integer):
        python_type = Color

        def dynamo_load(self, value):
            # Load the value through the Integer type first
            value = super().dynamo_load(value)
            return Color(value)

        def dynamo_dump(self, value):
            # Dump the value through the Integer type first
            return super().dynamo_dump(value.value)

    from bloop import Engine, Column
    engine = Engine()


    class Cube(engine.model):
        id = Column(Integer, hash_key=True)
        size = Column(Integer)
        color = Column(ColorType)
    engine.bind()

    cube = Cube(id=0, size=1, color=Color.green)
    engine.save(cube)

If you load up the console after this, you'll see a table ``Cube`` with the
row::

    | id | size | color |
    | 0  | 1    | 2     |

Here's the same mapping, but backed by the enum name instead of the integer::

    from bloop import String

    class ColorType(String):
        python_type = Color

        def dynamo_load(self, value):
            # Load the value through the String type first
            value = super().dynamo_load(value)
            return Color[value]

        def dynamo_dump(self, value):
            value = value.name
            # Dump the resulting value through the Integer type
            return super().dynamo_dump(value)

Now you'd see::

    | id | size | color |
    | 0  | 2    | green |

The ``python_type`` attribute is largely informational - the default serializer
will sometimes use it to try and determine which type can load a given value,
but the default serializer is broken for custom types as noted below.

It is most often valuable when debugging behavior, as a loggable property::

    some_column = Model.column
    print(some_column.typedef.python_type)
    # Although the repr of a column already includes this
    print(some_column.typedef)

Finally, note that there's nothing specific the the ``Color`` enum in the new
type's load or dump functions.  One could in fact create a general Enum by
passing the enum class in the \_\_init\_\_ method::

    class Enum(bloop.String):
        def __init__(self, enum):
            super().__init__()
            self.python_type = enum
        def dynamo_load(self, value):
            return Color[super().dynamo_load(value)]
        def dynamo_dump(self, value):
            return super.dynamo_dump(value.name)

And its use::

    class Cube(engine.model):
        id = Column(Integer, hash_key=True)
        size = Column(Integer)
        color = Column(Enum(Color))
    engine.bind()

What about a custom document type?  This example will create a Type that can
store arbitrary types, instead of the single-typed list that already exists::

    class MultiList(Type):
        def __init__(self, *types):
            self.types = types
            super().__init__()

        def dynamo_load(self, values):
            # Possible to load a list with less
            # values than defined slots
            length = min(len(self.types), len(values))

            loaded_values = [None] * len(self.types)
            for i in range(length):
                loaded_values.append(self.types[i]._load(values[i]))
            return loaded_values

        def dynamo_dump(self, values):
            # Possible to dump a list with less
            # values than defined slots
            length = min(len(self.types), len(values))

            dumped_values = []
            for i in range(length):
                value = values[i]
                # This double check is because None values
                # MUST NOT be sent to DynamoDB.  They represent
                # a lack of value, and MUST be omitted.
                if value is not None:
                    value = self.types[i]._dump(value)
                if value is not None:
                    dumped_values.append(value)
            return dumped_values

        def _register(self, engine):
            """Register all types contained in the list"""
            for typedef in self.types:
                engine.register(typedef)

        def __getitem__(self, index):
            """
            Required to correctly dump values
            when constructing conditions against
            specific indexes of the list
            """
            return self.types[index]

And it can be used as such::

    class Model(engine.model):
        id = Column(Integer, hash_key=True)
        objects = Column(MultiList(String, Integer(), Float, UUID()))


Unlike the provided ``List`` class which can take an arbitrary number of
objects of the *same* type, this class can take a fixed number of arbitary
objects.  If more values are provided that the number of types specified, the
MultiList type won't serialize them (this is the ``min`` in the code above).

.. _enum: https://docs.python.org/3/library/enum.html

.. note::
    bloop provides all of the current DynamoDB types, with the exception
    of ``NULL``.  This is because the null type can have only one value,
    ``True``. While it is useful with untyped values, it has no place in an
    object mapper that enforces typed data.  Consider a column of Null::

        class MyModel(engine.model):
            id = Column(Integer, hash_key=True)
            is_null = Column(Null)

    Because Null stores only one value, every model would have the same value
    for the attribute.  If a column could store multiple values, then an
    explicit sentinel ``NULL`` would be useful.  However, this is already
    represented by python's ``None`` and in DynamoDB by a lack of value.

Custom Columns
--------------

Sometimes there are customizations you'd like to make across different types,
such as attaching a validation function.  These should be handled by the
Column, not the type::

    from bloop import Column


    class ValidatingColumn(Column):
        def __init__(self, *args, validate=None, **kwargs):
            super().__init__(*args, **kwargs)
            if validate is None:
                validate = lambda obj, value: True
            self.validate = validate

        def set(self, obj, value):
            if not self.validate(obj, value):
                raise ValueError("Cannot set {} on {} to {}".format(
                    self.model_name, obj, value))
            super().set(obj, value)

And using that column::

    from bloop import Engine, Integer
    engine = Engine()

    def positive(obj, value):
        return value > 0


    class Model(engine.model):
        id = Column(Integer, hash_key=True)
        content = ValidatingColumn(Integer, validate=positive)
    engine.bind()

Remember, this will be run every time the value is set, **even when the object
is loaded from DynamoDB**.  This means that a ValueError will be raised if the
content was ever negative before this validation was added.

What about aliasing a persisted value without changing its stored value?  The
following renders ``green`` as ``blue`` without changing what's persisted in
DynamoDB::

    class SneakyColumn(Column):
        def get(self, obj):
            value = super().get(obj)
            if value == "green":
                value = "blue"
            return value

You'll note that these are not the regular descriptor functions ``__get__``,
``__set__``, and ``__del__``.  These are simplified functions that the
Column class delegates to when common conditions are met - for instance, when
obj is not None (class access).  Additionally, the base Column class handles
storing or retrieving the value from the object's \_\_dict\_\_ by the model
name (set during class creation) and raising if there is no model name.  This
allows your set/get/del methods to focus on manipulating data, instead of
handling the various edge-cases of incorrect initialization.  Here's the full
signature for overriding the descriptor protocol as used by Column::

    class CustomColumn(Column):
        def get(self, obj):
            return super().get(obj)

        def set(self, obj, value):
            super().set(obj, value)

        def delete(self, obj):
            super().delete(obj)

To add a ``nullable`` flag to the Column constructor::

    class Column(bloop.Column):
        def __init__(self, *args, nullable=True, **kwargs):
            super().__init__(*args, **kwargs)
            self.nullable = nullable

        def set(self, obj, value):
            if (value is None) and (not self.nullable):
                raise ValueError(
                    "{} is not nullable".format(self.model_name))
            super().set(obj, value)

        def delete(self, obj):
            if not self.nullable:
                raise ValueError(
                    "{} is not nullable".format(self.model_name))
            super().delete(obj)

Usage::

    from customization import Column
    from bloop import Engine, Integer, Boolean
    engine = Engine()


    class Model(engine.model):
        id = Column(Integer, nullable=False, hash_key=True)
        content = Column(Integer, nullable=True)
        flag = Column(Boolean)
    engine.bind()

    # Raises
    instance = Model(content=4, flag=True)
