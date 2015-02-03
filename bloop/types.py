import base64
import collections.abc
import decimal
import declare
import numbers

TYPES = []
ENCODING = 'utf-8'
STRING = 'S'
NUMBER = 'N'
BINARY = 'B'
STRING_SET = 'SS'
NUMBER_SET = 'NS'
BINARY_SET = 'BS'
NULL = 'NULL'
BOOLEAN = 'BOOL'
MAP = 'M'
LIST = 'L'

# Dynamo takes numbers as strings to reduce inter-language problems
DYNAMODB_CONTEXT = decimal.Context(
    Emin=-128, Emax=126, rounding=None, prec=38,
    traps=[
        decimal.Clamped, decimal.Overflow, decimal.Inexact,
        decimal.Rounded, decimal.Underflow
    ]
)


class ColumnType(declare.TypeDefinition):
    def __load__(self, value):
        '''
        take a {type: value} dictionary from dynamo and return a python value
        '''
        value = next(iter(value.values()))
        return self.dynamo_load(value)

    def can_load(self, value):
        '''
        whether this type can load the given
        {type: value} dictionary from dynamo
        '''
        backing_type, _ = next(iter(value.keys()))
        return backing_type == self.backing_type

    def __dump__(self, value):
        '''
        dump a python value to a {type: value} dictionary for dynamo storage
        '''
        return {self.backing_type: self.dynamo_dump(value)}

    def can_dump(self, value):
        ''' whether this type can dump the given value to dynamo '''
        return isinstance(value, self.python_type)

    def __repr__(self, *a, **kw):
        return "ColumnType({}, {})".format(self.python_type, self.backing_type)
    __str__ = __repr__


class StringType(ColumnType):
    python_type = str
    backing_type = STRING

    def dynamo_load(self, value):
        return value

    def dynamo_dump(self, value):
        return value


class NumberType(ColumnType):
    python_type = numbers.Number
    backing_type = NUMBER

    def dynamo_load(self, value):
        return DYNAMODB_CONTEXT.create_decimal(value)

    def dynamo_dump(self, value):
        n = str(DYNAMODB_CONTEXT.create_decimal(value))
        if any(filter(lambda x: x in n, ('Infinity', 'NaN'))):
            raise TypeError('Infinity and NaN not supported')
        return n

    def can_dump(self, value):
        ''' explicitly disallow bool and subclasses '''
        return (isinstance(value, self.backing_type)
                and not isinstance(value, bool))


class BinaryType(ColumnType):
    python_type = bytes
    backing_type = BINARY

    def dynamo_load(self, value):
        return base64.b64decode(value)

    def dynamo_dump(self, value):
        return base64.b64encode(value).decode('utf-8')


class SetType(ColumnType):
    ''' Adapter for sets of objects '''
    python_type = collections.abc.Set
    backing_type = None

    def __init__(self, typedef, dynamo_type):
        self.typedef = typedef
        self.backing_type = dynamo_type
        super().__init__()

    def dynamo_load(self, value):
        return set(self.typedef.load(v) for v in value)

    def dynamo_dump(self, value):
        return [self.typedef.dump(v) for v in value]

    def can_dump(self, value):
        return (super().can_dump(value)
                and all(map(self.typedef.can_dump, value)))


StringSetType = SetType(StringType, STRING_SET)
NumberSetType = SetType(NumberType, NUMBER_SET)
BinarySetType = SetType(BinaryType, BINARY_SET)


class NullType(ColumnType):
    python_type = type(None)
    backing_type = NULL

    def dynamo_load(self, value):
        return None

    def dynamo_dump(self, value):
        return True


class BooleanType(ColumnType):
    python_type = bool
    backing_type = BOOLEAN

    def dynamo_load(self, value):
        return value

    def dynamo_dump(self, value):
        return value


class MapType(ColumnType):
    python_type = collections.abc.Mapping
    backing_type = MAP

    def dynamo_load(self, value):
        return {k: load(v) for (k, v) in value.items()}

    def dynamo_dump(self, value):
        return {k: dump(v) for (k, v) in value.items()}


class ListType(ColumnType):
    python_type = collections.abc.Iterable
    backing_type = LIST

    def dynamo_load(self, value):
        return [load(v) for v in value]

    def dynamo_dump(self, value):
        return [dump(v) for v in value]


TYPES.extend([
    StringType,
    NumberType,
    BinaryType,
    StringSetType,
    NumberSetType,
    BinarySetType,
    NullType,
    BooleanType,
    MapType,
    ListType
])


def load(value):
    ''' value is a dictionary {dynamo_type: value} '''
    # TODO: Improve on O(n) search
    for type_class in TYPES:
        if type_class.can_load(value):
            return type_class.load(value)
    raise TypeError("Don't know how to load " + str(value))


def dump(value):
    # TODO: Improve on O(n) search
    for type_class in TYPES:
        if type_class.can_dump(value):
            return type_class.dump(value)
    raise TypeError("Don't know how to dump " + str(value))
