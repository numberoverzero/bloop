import bloop.condition
import operator
import declare
import uuid
import collections.abc
missing = object()
_meta_key = "__column_meta_{}".format(uuid.uuid4().hex)


class ComparisonMixin(object):
    def __hash__(self):
        # With single inheritance this looks stupid, but as a Mixin this
        # ensures we kick hashing back to the other base class so things
        # don't get fucked up, like `set()`.
        return super().__hash__()

    def __eq__(self, value):
        # Special case - None should use function attribute_not_exists
        if value is None:
            return bloop.condition.AttributeExists(self, negate=True)
        comparator = operator.eq
        return bloop.condition.Comparison(self, comparator, value)

    def __ne__(self, value):
        # Special case - None should use function attribute_exists
        if value is None:
            return bloop.condition.AttributeExists(self, negate=False)
        comparator = operator.ne
        return bloop.condition.Comparison(self, comparator, value)

    def __lt__(self, value):
        comparator = operator.lt
        return bloop.condition.Comparison(self, comparator, value)

    def __gt__(self, value):
        comparator = operator.gt
        return bloop.condition.Comparison(self, comparator, value)

    def __le__(self, value):
        comparator = operator.le
        return bloop.condition.Comparison(self, comparator, value)

    def __ge__(self, value):
        comparator = operator.ge
        return bloop.condition.Comparison(self, comparator, value)

    def is_(self, value):
        ''' alias for == '''
        return self == value

    def is_not(self, value):
        ''' alias for != '''
        return self != value

    def between(self, lower, upper):
        ''' lower <= column.value <= upper '''
        return bloop.condition.Between(self, lower, upper)

    def in_(self, *values):
        ''' column.value in [3, 4, 5] '''
        return bloop.condition.In(self, values)

    def begins_with(self, value):
        return bloop.condition.BeginsWith(self, value)

    def contains(self, value):
        return bloop.condition.Contains(self, value)


class Column(declare.Field, ComparisonMixin):
    def __init__(self, *args, hash_key=None, range_key=None,
                 name=missing, **kwargs):
        self._hash_key = hash_key
        self._range_key = range_key
        self._dynamo_name = name

        self.column_key = "__{}_{}".format(
            self.__class__.__name__, uuid.uuid4().hex)
        super().__init__(*args, **kwargs)

    def __str__(self):
        attrs = ["model_name", "dynamo_name", "hash_key", "range_key"]

        def _attr(attr):
            return "{}={}".format(attr, getattr(self, attr))
        attrs = ", ".join(_attr(attr) for attr in attrs)
        return "Column({})".format(attrs)

    @property
    def hash_key(self):
        '''
        - Non-index columns return True/False.
        - Indexes return the `bloop.Column` that is their hash_key.
        '''
        return self._hash_key

    @property
    def range_key(self):
        '''
        - Non-index columns return True/False.
        - Indexes return the `bloop.Column` that is their range_key (or None).
        '''
        return self._range_key

    @property
    def dynamo_name(self):
        if self._dynamo_name is missing:
            return self.model_name
        return self._dynamo_name

    def __meta__(self, obj):
        ''' Return the column-specific metadata dict for a given object '''
        meta = obj.__dict__.get(_meta_key, None)
        if meta is None:
            meta = obj.__dict__[_meta_key] = {}
        column_meta = meta.get(self.column_key, None)
        if column_meta is None:
            column_meta = meta[self.column_key] = {}
        return column_meta

    def meta_get(self, obj, name, default=missing):
        '''
        look up and return the value of a property in the column metadata,
        setting and return the default value if specified.

        if `default` is not set, KeyError is raised and the metadata dict is
        not mutated.
        '''
        obj_meta = self.__meta__(obj)
        value = obj_meta.get(name, missing)
        # Failed to find - either set and return default, or raise
        if value is missing:
            # Don't mutate on fail to find
            if default is missing:
                raise KeyError("Unknown column meta property {}".format(name))
            else:
                value = obj_meta[name] = default
        return value

    def meta_set(self, obj, name, value):
        obj_meta = self.__meta__(obj)
        obj_meta[name] = value
        return value


def validate_projection(projection):
    invalid = ValueError("Index projections must be either 'KEYS_ONLY', 'ALL',"
                         " or an iterable of model attributes to include.")
    # String check first since it is also an Iterable
    if isinstance(projection, str):
        projection = projection.upper()
        if projection not in ["KEYS_ONLY", "ALL"]:
            raise invalid
    elif isinstance(projection, collections.abc.Iterable):
        projection = list(projection)
        for attribute in projection:
            if not isinstance(attribute, str):
                raise invalid
    else:
        raise invalid
    return projection


class Index(Column):
    def __init__(self, *args, projection='KEYS_ONLY', **kwargs):
        super().__init__(*args, **kwargs)

        # projection_attributes will be set up by `bloop.model.ModelMetaclass`
        self.projection = validate_projection(projection)


class GlobalSecondaryIndex(Index):
    def __init__(self, *args, write_units=1, read_units=1, **kwargs):
        if 'hash_key' not in kwargs:
            raise ValueError(
                "Must specify a hash_key for a GlobalSecondaryIndex")
        super().__init__(*args, **kwargs)
        self.write_units = write_units
        self.read_units = read_units


class LocalSecondaryIndex(Index):
    ''' LSIs don't have individual read/write units '''
    def __init__(self, *args, **kwargs):
        # Hash key MUST be the table hash, pop any other values
        if 'hash_key' in kwargs:
            raise ValueError(
                "Can't specify the hash_key of a LocalSecondaryIndex")
        if 'range_key' not in kwargs:
            raise ValueError(
                "Must specify a range_key for a LocalSecondaryIndex")
        if ('write_units' in kwargs) or ('read_units' in kwargs):
            raise ValueError(
                "A LocalSecondaryIndex does not have its own read/write units")
        super().__init__(*args, **kwargs)


def is_column(field):
    return isinstance(field, Column)


def is_index(field):
    return isinstance(field, Index)


def is_local_index(index):
    return isinstance(index, LocalSecondaryIndex)


def is_global_index(index):
    return isinstance(index, GlobalSecondaryIndex)
