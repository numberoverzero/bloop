Advanced Features
=================

.. _loading:

Custom Loading
--------------

The ``init`` attribute of a model's ``Meta`` specifies a function that
takes no arguments and returns instances of the model.  By default this is
simply the model's ``__init__`` method, but any object-returning function
can be used::

    Base = new_base()


    def default_model():
        # Ensure content is not None
        instance = MyModel()
        instance.content = ""
        return instance


    class MyModel(Base):
        class Meta:
            init = default_model

        id = Column(Integer, hash_key=True)
        content = Column(Binary)

    engine.bind(base=Base)

The non-default init method can be useful for data migrations, such as loading
from an old model, and initializing into instances of a new model.

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

        def dynamo_load(self, value, *, context=None, **kwargs):
            # Load the value through the Integer type first
            value = super().dynamo_load(value, context=context, **kwargs)
            return Color(value)

        def dynamo_dump(self, value, *, context=None, **kwargs):
            # Dump the value through the Integer type first
            return super().dynamo_dump(value.value, context=context, **kwargs)

    from bloop import Engine, Column, new_base
    Base = new_base()
    engine = Engine()


    class Cube(Base):
        id = Column(Integer, hash_key=True)
        size = Column(Integer)
        color = Column(ColorType)
    engine.bind(base=Base)

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

        def dynamo_load(self, value, *, context=None, **kwargs):
            # Load the value through the String type first
            value = super().dynamo_load(value, context=context, **kwargs)
            return Color[value]

        def dynamo_dump(self, value, *, context=None, **kwargs):
            value = value.name
            # Dump the resulting value through the Integer type
            return super().dynamo_dump(value, context=context, **kwargs)

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

Finally, note that there's nothing specific to the ``Color`` enum in the new
type's load or dump functions.  One could in fact create a general Enum by
passing the enum class in the \_\_init\_\_ method::

    class Enum(bloop.String):
        def __init__(self, enum):
            super().__init__()
            self.python_type = enum

        def dynamo_load(self, value, *, context=None, **kwargs):
            value = super().dynamo_load(value, context=context, **kwargs)
            return Color[value]

        def dynamo_dump(self, value, *, context=None, **kwargs):
            return super.dynamo_dump(value.name, context=context, **kwargs)

And its use::

    class Cube(Base):
        id = Column(Integer, hash_key=True)
        size = Column(Integer)
        color = Column(Enum(Color))
    engine.bind(base=Base)

What about a custom document type?  This example will create a Type that can
store arbitrary types, instead of the single-typed list that already exists::

    class MultiList(Type):
        def __init__(self, *types):
            self.types = types
            super().__init__()

        def dynamo_load(self, values, *, context=None, **kwargs):
            # Possible to load a list with less
            # values than defined slots
            length = min(len(self.types), len(values))

            loaded_values = [None] * len(self.types)
            for i in range(length):
                loaded_values.append(
                    self.types[i]._load(values[i], context=context, **kwargs))
            return loaded_values

        def dynamo_dump(self, values, *, context=None, **kwargs):
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
                    value = self.types[i]._dump(
                        value, context=context, **kwargs)
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

    class Model(Base):
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

        class MyModel(Base):
            id = Column(Integer, hash_key=True)
            is_null = Column(Null)

    Because Null stores only one value, every model would have the same value
    for the attribute.  If a column could store multiple values, then an
    explicit sentinel ``NULL`` would be useful.  However, this is already
    represented by python's ``None`` and in DynamoDB by a lack of value.

.. _custom-columns:

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

    from bloop import Engine, Integer, new_base
    Base = new_base()
    engine = Engine()

    def positive(obj, value):
        return value > 0


    class Model(Base):
        id = Column(Integer, hash_key=True)
        content = ValidatingColumn(Integer, validate=positive)
    engine.bind(base=Base)

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
    from bloop import Engine, Integer, Boolean, new_base
    Base = new_base()
    engine = Engine()
    missing = object()


    class Model(Base):
        id = Column(Integer, nullable=False, hash_key=True)
        content = Column(Integer, nullable=True)
        flag = Column(Boolean)

        def __init__(self, **attrs):
            for column in self.Meta.columns:
                value = attrs.get(column.model_name, missing)
                if value is missing and not column.nullable:
                    raise ValueError(
                        "{} is not nullable".format(column.model_name))
                elif value is not missing:
                    setattr(self, column.model_name, value)
    engine.bind(base=Base)

    # Each of these raises
    instance = Model(content=4, flag=True)
    instance.id = None
    del instance.id
