import collections.abc
import declare


def validate_projection(projection):
    invalid = ValueError("Index projections must be either 'keys_only', 'all',"
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


class Index(declare.Field):
    def __init__(self, *args, projection='KEYS_ONLY', **kwargs):
        super().__init__(*args, **kwargs)

        # projection_attributes will be set up by `bloop.model.ModelMetaclass`
        self.projection = validate_projection(projection)

    @property
    def projection_attributes(self):
        '''
        All attributes available through this index.

        Includes table hash/range, index hash/range, and any projected attrs
        '''
        return self._projection_attributes


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


def is_index(field):
    return isinstance(field, Index)


def is_local_index(index):
    return isinstance(index, LocalSecondaryIndex)


def is_global_index(index):
    return isinstance(index, GlobalSecondaryIndex)
